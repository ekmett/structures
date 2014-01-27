{-# LANGUAGE CPP #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE PatternGuards #-}
-----------------------------------------------------------------------------
-- |
-- Copyright   :  (C) 2013 Edward Kmett
-- License     :  BSD-style (see the file LICENSE)
-- Maintainer  :  Edward Kmett <ekmett@gmail.com>
-- Stability   :  experimental
-- Portability :  non-portable
--
-- This module provides a 'Vector'-based 'Map' that is loosely based on the
-- Cache Oblivious Lookahead Array (COLA) by Bender et al. from
-- <http://supertech.csail.mit.edu/papers/sbtree.pdf "Cache-Oblivious Streaming B-Trees">,
-- but with inserts deamortized using a technique from Overmars and van Leeuwen.
--
-- Currently this 'Map' is implemented in an insert-only fashion. Deletions are left to future work
-- or to another derived structure in case they prove expensive.
--
-- Currently, we also do not use fractional cascading, as it affects the constant factors badly enough
-- to not pay for itself at the scales we are interested in. The naive /O(log^2 n)/ lookup
-- consistently outperforms the alternative.
--
-- Compared to the venerable @Data.Map@, this data structure currently consumes more memory, but it
-- provides a more limited palette of operations with different asymptotics (~10x faster inserts at a million entries)
-- and enables us to utilize contiguous storage.
--
-- /NB:/ when used with boxed data this structure may hold onto references to old versions
-- of things for many updates to come until sufficient operations have happened to merge them out
-- of the COLA.
-----------------------------------------------------------------------------
module Data.Vector.Map
  ( Map(..)
  , empty
  , null
  , singleton
  , lookup
  , insert
  , fromList
  ) where

import Control.Monad.ST.Unsafe as Unsafe
import Control.Monad.ST.Class
import Control.Monad.Trans.Iter
import Data.Bits
import qualified Data.List as List
import Data.Vector.Array
import qualified Data.Vector.Generic as G
import qualified Data.Vector.Generic.Mutable as GM
-- import qualified Data.Vector.Map.Fusion as Fusion
import Data.Vector.Slow as Slow
import Prelude hiding (null, lookup)
import System.IO.Unsafe as Unsafe

#define BOUNDS_CHECK(f) (Ck.f __FILE__ __LINE__ Ck.Bounds)

-- | This Map is implemented as an insert-only Cache Oblivious Lookahead Array (COLA) with amortized complexity bounds
-- that are equal to those of a B-Tree, except for an extra log factor slowdown on lookups due to the lack of fractional
-- cascading.

data Chunk k a = Chunk !(Array k) !(Array a)

data Map k a
  = M0
  | M1 !(Array k) !(Array a)
  | M2 !(Array k) !(Array a)
       !(Array k) !(Array a) !(Partial (Chunk k a)) !(Map k a)
  | M3 !(Array k) !(Array a)
       !(Array k) !(Array a)
       !(Array k) !(Array a) !(Partial (Chunk k a)) !(Map k a)

deriving instance (Show (Arr v v), Show (Arr k k)) => Show (Map k v)
deriving instance (Show (Arr v v), Show (Arr k k)) => Show (Chunk k v)

-- | /O(1)/. Identify if a 'Map' is the 'empty' 'Map'.
null :: Map k v -> Bool
null M0 = True
null _  = False
{-# INLINE null #-}

-- | /O(1)/ The 'empty' 'Map'.
empty :: Map k v
empty = M0
{-# INLINE empty #-}

-- | /O(1)/ Construct a 'Map' from a single key/value pair.
singleton :: (Arrayed k, Arrayed v) => k -> v -> Map k v
singleton k v = M1 (G.singleton k) (G.singleton v)
{-# INLINE singleton #-}

-- | /O(log^2 N)/ worst-case. Lookup an element.
lookup :: (Ord k, Arrayed k, Arrayed v) => k -> Map k v -> Maybe v
lookup !k m0 = go m0 where
  {-# INLINE go #-}
  go M0 = Nothing
  go (M1 ka va)                 = lookup1 k ka va Nothing
  go (M2 ka va kb vb _ m)       = lookup1 k ka va $ lookup1 k kb vb $ go m
  go (M3 ka va kb vb kc vc _ m) = lookup1 k ka va $ lookup1 k kb vb $ lookup1 k kc vc $ go m
{-# INLINE lookup #-}

lookup1 :: (Ord k, Arrayed k, Arrayed v) => k -> Array k -> Array v -> Maybe v -> Maybe v
lookup1 k ks vs r
  | j <- search (\i -> ks G.! i >= k) 0 (G.length ks - 1)
  , ks G.! j == k = Just $ vs G.! j
  | otherwise = r
{-# INLINE lookup1 #-}

-- | O((log N)\/B) worst-case loads for each cache. Insert an element.
insert :: (Ord k, Arrayed k, Arrayed v) => k -> v -> Map k v -> Map k v
insert k0 v0 s0 = go (G.singleton k0) (G.singleton v0) s0 where
  go ka a M0                    = M1 ka a
  go ka a (M1 kb b)             = M2 ka a kb b (merge ka a kb b) M0
  go ka a (M2 kb b kc c mbc xs) = M3 ka a kb b kc c (step mbc) (steps xs)
  go ka a (M3 kb b _ _ _ _ mcd xs) = case mcd of
    Stop (Chunk kcd cd) -> M2 ka a kb b (merge ka a kb b) (go kcd cd xs)
    _       -> error "insert: stop Step"

  steps (M2 kx x ky y mxy xs)   = M2 kx x ky y (step mxy) (steps xs)
  steps (M3 kx x ky y kz z myz xs) = M3 kx x ky y kz z (step myz) (steps xs)
  steps m = m
{-# INLINE insert #-}

step :: Partial a -> Partial a
step (Stop _)  = error "insert: step Stop"
step (Step m) = m
{-# INLINE step #-}

merge :: forall k a. (Ord k, Arrayed k, Arrayed a) => Array k -> Array a -> Array k -> Array a -> Partial (Chunk k a)
merge ka va kb vb = step (walkDupableST mergeST) where
 mergeST :: forall s. IterST s (Chunk k a)
 mergeST = do
  let
    !la = G.length ka
    !lb = G.length kb
  kc <- liftST $ GM.unsafeNew (la + lb)
  vc <- liftST $ GM.unsafeNew (la + lb)
  let
    goL :: Int -> Int -> IterST s ()
    goL !i !j -- left exhausted
      | j >= lb = return ()
      | !k <- i + j = do
        liftST $ do
          kj <- G.unsafeIndexM kb j
          GM.unsafeWrite kc k kj
          vj <- G.unsafeIndexM vb j
          GM.unsafeWrite vc k vj
        delay $ goL i (j+1)

    goR :: Int -> Int -> IterST s ()
    goR !i !j -- right exhausted
      | i >= la = return ()
      | !k <- i + j = do
        liftST $ do
          ki <- G.unsafeIndexM ka i
          GM.unsafeWrite kc k ki
          vi <- G.unsafeIndexM va i
          GM.unsafeWrite vc k vi
        delay $ goR (i+1) j

    go :: Int -> Int -> IterST s ()
    go !i !j
      | i >= la = goL i j
      | j >= lb = goR i j
      | !k <- i + j = do
        ki <- liftST $ G.unsafeIndexM ka i
        kj <- liftST $ G.unsafeIndexM kb j
        case compare ki kj of
          LT -> do
            liftST $ do
              vi <- G.unsafeIndexM va i
              GM.unsafeWrite kc k ki
              GM.unsafeWrite vc k vi
            delay $ go (i+1) j
          EQ -> do
            liftST $ do
              vi <- G.unsafeIndexM va i
              GM.unsafeWrite kc k ki
              GM.unsafeWrite vc k vi
            delay $ go (i+1) (i+j)
          GT -> do
            liftST $ do
              vj <- G.unsafeIndexM vb j
              GM.unsafeWrite kc k kj
              GM.unsafeWrite vc k vj
            delay $ go i (j+1)
  go 0 0
  liftST $ do
    fkc <- G.unsafeFreeze kc
    fvc <- G.unsafeFreeze vc
    return $ Chunk fkc fvc
{-# INLINE merge #-}

walkDupableST :: (forall s. IterST s a) -> Partial a
walkDupableST m0 = go m0 where
  go (IterT m) =
    case Unsafe.unsafeDupablePerformIO $
         Unsafe.unsafeSTToIO m of
      Left  a -> Stop a
      Right n -> Step (go n)

{-
merge :: (Ord k, Arrayed k, Arrayed a) => Array k -> Array a -> Array k -> Array a -> Partial (Chunk k a)
merge km m kn n = step $ walkDupableST $ do
  V_Pair _ ks vs <- Slow.unstreamM $ Slow.streamST $ Fusion.merge (zips km m) (zips kn n)
  return $ Chunk ks vs
{-# INLINE merge #-}
-}

fromList :: (Ord k, Arrayed k, Arrayed v) => [(k,v)] -> Map k v
fromList xs = List.foldl' (\m (k,v) -> insert k v m) empty xs
{-# INLINE fromList #-}

-- | Offset binary search
--
-- Assuming @l <= h@. Returns @h@ if the predicate is never @True@ over @[l..h)@
search :: (Int -> Bool) -> Int -> Int -> Int
search p = go where
  go l h
    | l == h    = l
    | p m       = go l m
    | otherwise = go (m+1) h
    where hml = h - l
          m = l + unsafeShiftR hml 1 + unsafeShiftR hml 6
{-# INLINE search #-}

{-
zips :: (G.Vector v a, G.Vector u b) => v a -> u b -> Stream Id (a, b)
zips va ub = Stream.zip (G.stream va) (G.stream ub)
{-# INLINE zips #-}
-}
