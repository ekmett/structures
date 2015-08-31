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
#if __GLASGOW_HASKELL__ >= 708
{-# LANGUAGE RoleAnnotations #-}
#endif
-----------------------------------------------------------------------------
-- |
-- Copyright   :  (C) 2013-2014 Edward Kmett
-- License     :  BSD-style (see the file LICENSE)
-- Maintainer  :  Edward Kmett <ekmett@gmail.com>
-- Stability   :  experimental
-- Portability :  non-portable
--
-- This module provides a 'Vector'-based 'Map' that is loosely based on the
-- Cache Oblivious Lookahead Array (COLA) by Bender et al. from
-- <http://supertech.csail.mit.edu/papers/sbtree.pdf "Cache-Oblivious Streaming B-Trees">,
-- but with inserts deamortized by using a varant of a technique from Overmars and van Leeuwen.
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
module Data.Vector.Map.Deamortized
  ( Map
  , empty
  , null
  , singleton
  , lookup
  , insert
  , fromList
  ) where

import Control.Applicative hiding (empty)
import Data.Bits
import Data.Foldable as Foldable hiding (null) 
import qualified Data.List as List
import Data.Vector.Array
import qualified Data.Map.Strict as Map
import qualified Data.Vector.Generic as G
import qualified Data.Vector.Generic.Mutable as GM
import GHC.Prim (RealWorld)
import Prelude hiding (null, lookup)
import System.IO.Unsafe as Unsafe

-- | How many items is it worth batching up in the Nursery?
_THRESHOLD :: Int
_THRESHOLD = 1000

-- | This Map is implemented as an insert-only Cache Oblivious Lookahead Array (COLA) with amortized complexity bounds
-- that are equal to those of a B-Tree, except for an extra log factor slowdown on lookups due to the lack of fractional
-- cascading. It uses a traditional Data.Map as a nursery.

data Map k a = Map !(Map.Map k a) !(LA k a)

-- | Cache-Oblivious Lookahead Array internals
data LA k a
  = M0
  | M1 !(Array k) !(Array a)
  | M2 !(Array k) !(Array a)
       !(Array k) !(Array a)
       {-# UNPACK #-} !Int {-# UNPACK #-} !Int
       !(MArray RealWorld k) !(MArray RealWorld a)
       !(LA k a)
  | M3 !(Array k) !(Array a)
       !(Array k) !(Array a)
       !(Array k) !(Array a)
       {-# UNPACK #-} !Int {-# UNPACK #-} !Int
       !(MArray RealWorld k) !(MArray RealWorld a)
       !(LA k a)

#if __GLASGOW_HASKELL__ >= 708
type role Map nominal nominal
#endif

instance (Show (Arr v v), Show (Arr k k)) => Show (LA k v) where
  showsPrec _ M0 = showString "M0"
  showsPrec d (M1 ka a) = showParen (d > 10) $
    showString "M1 " . showsPrec 11 ka . showChar ' ' . showsPrec 11 a
  showsPrec d (M2 ka a kb b ra rb _ _ xs) = showParen (d > 10) $
    showString "M2 " .
    showsPrec 11 ka . showChar ' ' . showsPrec 11 a . showChar ' ' .
    showsPrec 11 kb . showChar ' ' . showsPrec 11 b . showChar ' ' .
    showsPrec 11 ra . showChar ' ' . showsPrec 11 rb . showString " _ _ " .
    showsPrec 11 xs
  showsPrec d (M3 ka a kb b kc c rb rc _ _ xs) = showParen (d > 10) $
    showString "M3 " .
    showsPrec 11 ka . showChar ' ' . showsPrec 11 a . showChar ' ' .
    showsPrec 11 kb . showChar ' ' . showsPrec 11 b . showChar ' ' .
    showsPrec 11 kc . showChar ' ' . showsPrec 11 c . showChar ' ' .
    showsPrec 11 rb . showChar ' ' . showsPrec 11 rc . showString " _ _ " .
    showsPrec 11 xs

instance (Show (Arr v v), Show (Arr k k), Show k, Show v) => Show (Map k v) where
  showsPrec d (Map n l) = showParen (d > 10) $
    showString "Map " . showsPrec 11 n . showChar ' ' . showsPrec 11 l

-- | /O(1)/. Identify if a 'Map' is the 'empty' 'Map'.
null :: Map k v -> Bool
null (Map n M0) = Map.null n
null _          = False
{-# INLINE null #-}

-- | /O(1)/ The 'empty' 'Map'.
empty :: Map k v
empty = Map Map.empty M0
{-# INLINE empty #-}

-- | /O(1)/ Construct a 'Map' from a single key/value pair.
singleton :: (Arrayed k, Arrayed v) => k -> v -> Map k v
singleton k v = Map (Map.singleton k v) M0
{-# INLINE singleton #-}

-- | /O(log^2 N)/ worst-case. Lookup an element.
lookup :: (Ord k, Arrayed k, Arrayed v) => k -> Map k v -> Maybe v
lookup !k (Map m0 la) = case Map.lookup k m0 of
  Nothing -> go la
  mv      -> mv
 where
  {-# INLINE go #-}
  go M0 = Nothing
  go (M1 ka va)                       = lookup1 k ka va Nothing
  go (M2 ka va kb vb _ _ _ _ m)       = lookup1 k ka va $ lookup1 k kb vb $ go m
  go (M3 ka va kb vb kc vc _ _ _ _ m) = lookup1 k ka va $ lookup1 k kb vb $ lookup1 k kc vc $ go m
{-# INLINE lookup #-}

lookup1 :: (Ord k, Arrayed k, Arrayed v) => k -> Array k -> Array v -> Maybe v -> Maybe v
lookup1 k ks vs r
  | j <- search (\i -> ks G.! i >= k) 0 (G.length ks - 1)
  , ks G.! j == k = Just $ vs G.! j
  | otherwise = r
{-# INLINE lookup1 #-}

-- | O((log N)\/B) worst-case loads for each cache. Insert an element.
insert :: (Ord k, Arrayed k, Arrayed v) => k -> v -> Map k v -> Map k v
insert k0 v0 (Map m0 xs0)
  | n0 <= _THRESHOLD = Map (Map.insert k0 v0 m0) xs0
  | otherwise = Map Map.empty $ unsafeDupablePerformIO $ inserts (G.fromListN n0 (Map.keys m0)) (G.fromListN n0 (Foldable.toList m0)) xs0
 where
  n0 = Map.size m0
  inserts ka a M0 = return $ M1 ka a
  inserts ka a (M1 kb b) = do
    let n = G.length ka + G.length kb
    kab <- GM.basicUnsafeNew n
    ab  <- GM.basicUnsafeNew n
    (ra,rb) <- steps ka a kb b 0 0 kab ab
    return $ M2 ka a kb b ra rb kab ab M0
  inserts ka a (M2 kb b kc c rb rc kbc bc xs) = do
    (rb',rc') <- steps kb b kc c rb rc kbc bc
    M3 ka a kb b kc c rb' rc' kbc bc <$> stepTail xs
  inserts ka a (M3 kb b _ _ _ _ _ _ kcd cd xs) = do
    let n = G.length ka + G.length kb
    kab <- GM.basicUnsafeNew n
    ab  <- GM.basicUnsafeNew n
    (ra,rb) <- steps ka a kb b 0 0 kab ab
    kcd' <- G.unsafeFreeze kcd
    cd' <- G.unsafeFreeze cd
    M2 ka a kb b ra rb kab ab <$> inserts kcd' cd' xs

  stepTail (M2 kx x ky y rx ry kxy xy xs) = do
    (rx',ry') <- steps kx x ky y rx ry kxy xy
    M2 kx x ky y rx' ry' kxy xy <$> stepTail xs
  stepTail (M3 kx x ky y kz z ry rz kyz yz xs) = do
    (ry',rz') <- steps ky y kz z ry rz kyz yz
    M3 kx x ky y kz z ry' rz' kyz yz <$> stepTail xs
  stepTail m = return m
{-# INLINE insert #-}

steps :: (Ord k, Arrayed k, Arrayed v) => Array k -> Array v -> Array k -> Array v -> Int -> Int -> MArray RealWorld k -> MArray RealWorld v -> IO (Int, Int)
steps ka a kb b ra0 rb0 kab ab = go ra0 rb0 where
  n = min (ra0 + rb0 + _THRESHOLD) (GM.length kab)
  na = G.length ka
  nb = G.length kb
  go !ra !rb
    | r >= n = return (ra, rb)
    | ra == na = do
      k <- G.basicUnsafeIndexM kb rb
      v <- G.basicUnsafeIndexM b rb
      GM.basicUnsafeWrite kab r k
      GM.basicUnsafeWrite ab r v
      go ra (rb + 1)
    | rb == nb = do
      k <- G.basicUnsafeIndexM ka ra
      v <- G.basicUnsafeIndexM a ra
      GM.basicUnsafeWrite kab r k
      GM.basicUnsafeWrite ab r v
      go (ra + 1) rb
    | otherwise = do
      k1 <- G.basicUnsafeIndexM ka ra
      k2 <- G.basicUnsafeIndexM kb rb
      case compare k1 k2 of
        LT -> do
          v <- G.basicUnsafeIndexM a ra
          GM.basicUnsafeWrite kab r k1
          GM.basicUnsafeWrite ab r v
          go (ra + 1) rb
        EQ -> do -- collision, overwrite with newer value
          v <- G.basicUnsafeIndexM a ra
          GM.basicUnsafeWrite kab r k1
          GM.basicUnsafeWrite ab r v
          go (ra + 1) (rb + 1)
        GT -> do
          v <- G.basicUnsafeIndexM b rb
          GM.basicUnsafeWrite kab r k2
          GM.basicUnsafeWrite ab r v
          go ra (rb + 1)
    where r = ra + rb

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
