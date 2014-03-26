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
module Data.Vector.Map.Overmars
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
import qualified Data.List as List
import Data.Vector.Array
import qualified Data.Vector.Generic as G
import qualified Data.Vector.Generic.Mutable as GM
import GHC.Prim (RealWorld)
import Prelude hiding (null, lookup)
import System.IO.Unsafe as Unsafe

-- | This Map is implemented as an insert-only Cache Oblivious Lookahead Array (COLA) with amortized complexity bounds
-- that are equal to those of a B-Tree, except for an extra log factor slowdown on lookups due to the lack of fractional
-- cascading.

data Map k a
  = M0
  | M1 !(Array k) !(Array a)
  | M2 !(Array k) !(Array a)
       !(Array k) !(Array a)
       {-# UNPACK #-} !Int {-# UNPACK #-} !Int
       !(MArray RealWorld k) !(MArray RealWorld a)
       !(Map k a)
  | M3 !(Array k) !(Array a)
       !(Array k) !(Array a)
       !(Array k) !(Array a)
       {-# UNPACK #-} !Int {-# UNPACK #-} !Int
       !(MArray RealWorld k) !(MArray RealWorld a)
       !(Map k a)
  | E1 !k !a
  | E2 !k !a !k !a !(Map k a)
  | E3 !k !a !k !a !k !a !(Map k a)

#if __GLASGOW_HASKELL__ >= 708
type role Map nominal nominal
#endif

instance (Show k, Show v, Show (Arr v v), Show (Arr k k)) => Show (Map k v) where
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
  showsPrec d (E1 ka a) = showParen (d > 10) $
    showString "E1 " . showsPrec 11 ka . showChar ' ' . showsPrec 11 a
  showsPrec d (E2 ka a kb b xs) = showParen (d > 10) $
    showString "E2 " .
    showsPrec 11 ka . showChar ' ' . showsPrec 11 a . showChar ' ' .
    showsPrec 11 kb . showChar ' ' . showsPrec 11 b . showChar ' ' .
    showsPrec 11 xs
  showsPrec d (E3 ka a kb b kc c xs) = showParen (d > 10) $
    showString "E3 " .
    showsPrec 11 ka . showChar ' ' . showsPrec 11 a . showChar ' ' .
    showsPrec 11 kb . showChar ' ' . showsPrec 11 b . showChar ' ' .
    showsPrec 11 kc . showChar ' ' . showsPrec 11 c . showChar ' ' .
    showsPrec 11 xs

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
singleton = E1
{-# INLINE singleton #-}

-- | /O(log^2 N)/ worst-case. Lookup an element.
lookup :: (Ord k, Arrayed k, Arrayed v) => k -> Map k v -> Maybe v
lookup !k m0 = go m0 where
  {-# INLINE go #-}
  go M0 = Nothing
  go (M1 ka va)                       = lookup1 k ka va Nothing
  go (M2 ka va kb vb _ _ _ _ m)       = lookup1 k ka va $ lookup1 k kb vb $ go m
  go (M3 ka va kb vb kc vc _ _ _ _ m) = lookup1 k ka va $ lookup1 k kb vb $ lookup1 k kc vc $ go m
  go (E1 ka va)
     | k == ka   = Just va
     | otherwise = Nothing
  go (E2 ka va kb vb xs)
     | k == ka   = Just va
     | k == kb   = Just vb
     | otherwise = go xs
  go (E3 ka va kb vb kc vc xs)
     | k == ka   = Just va
     | k == kb   = Just vb
     | k == kc   = Just vc
     | otherwise = go xs
{-# INLINE lookup #-}

lookup1 :: (Ord k, Arrayed k, Arrayed v) => k -> Array k -> Array v -> Maybe v -> Maybe v
lookup1 k ks vs r
  | j <- search (\i -> ks G.! i >= k) 0 (G.length ks - 1)
  , ks G.! j == k = Just $ vs G.! j
  | otherwise = r
{-# INLINE lookup1 #-}

-- | O((log N)\/B) worst-case loads for each cache. Insert an element.
insert :: (Ord k, Arrayed k, Arrayed v) => k -> v -> Map k v -> Map k v
insert k0 v0 M0 = E1 k0 v0
insert k0 v0 (E1 k1 v1) = case compare k0 k1 of
  LT -> E2 k0 v0 k1 v1 M0
  EQ -> E1 k0 v0
  GT -> E2 k1 v1 k0 v0 M0
insert k0 v0 (E2 k1 v1 k2 v2 xs) = case compare k0 k1 of
  LT -> E3 k0 v0 k1 v1 k2 v2 xs
  EQ -> E2 k0 v0 k2 v2 xs
  GT -> case compare k0 k2 of
    LT -> E3 k1 v1 k0 v0 k2 v2 xs
    EQ -> E2 k1 v1 k0 v0 xs
    GT -> E3 k1 v1 k2 v2 k0 v0 xs
insert k0 v0 (E3 k1 v1 k2 v2 k3 v3 xs0) = case compare k0 k2 of
  LT -> case compare k0 k1 of
    LT -> E2 k0 v0 k1 v1 $ go (G.fromList [k2,k3]) (G.fromList [v2,v3]) xs0
    EQ -> E3 k0 v0 k2 v2 k3 v3 xs0
    GT -> E2 k1 v1 k0 v0 $ go (G.fromList [k2,k3]) (G.fromList [v2,v3]) xs0
  EQ -> E3 k1 v1 k0 v0 k3 v3 xs0
  GT -> case compare k0 k3 of
    LT -> E2 k0 v0 k3 v3 $ go (G.fromList [k1,k2]) (G.fromList [v1,v2]) xs0
    EQ -> E3 k1 v1 k2 v2 k0 v0 xs0
    GT -> E2 k3 v3 k0 v0 $ go (G.fromList [k1,k2]) (G.fromList [v1,v2]) xs0
-- insert k0 v0 s0 = unsafeDupablePerformIO $ inserts (G.singleton k0) (G.singleton v0) s0 where
 where
  go ks vs xs = unsafeDupablePerformIO $ inserts ks vs xs
  inserts ka a M0 = return $ M1 ka a
  inserts ka a (M1 kb b) = do
    let n = G.length ka + G.length kb
    kab <- GM.basicUnsafeNew n
    ab  <- GM.basicUnsafeNew n
    (ra,rb) <- step2 ka a kb b 0 0 kab ab
    return $ M2 ka a kb b ra rb kab ab M0
  inserts ka a (M2 kb b kc c rb rc kbc bc xs) = do
    (rb',rc') <- step2 kb b kc c rb rc kbc bc
    M3 ka a kb b kc c rb' rc' kbc bc <$> steps xs
  inserts ka a (M3 kb b _ _ _ _ _ _ kcd cd xs) = do
    let n = G.length ka + G.length kb
    kab <- GM.basicUnsafeNew n
    ab  <- GM.basicUnsafeNew n
    (ra,rb) <- step2 ka a kb b 0 0 kab ab
    kcd' <- G.unsafeFreeze kcd
    cd' <- G.unsafeFreeze cd
    M2 ka a kb b ra rb kab ab <$> inserts kcd' cd' xs
  inserts _ _ _ = error "structural invariants violated: E inside"

  steps (M2 kx x ky y rx ry kxy xy xs) = do
    (rx',ry') <- step2 kx x ky y rx ry kxy xy
    M2 kx x ky y rx' ry' kxy xy <$> steps xs
  steps (M3 kx x ky y kz z ry rz kyz yz xs) = do
    (ry',rz') <- step2 ky y kz z ry rz kyz yz
    M3 kx x ky y kz z ry' rz' kyz yz <$> steps xs
  steps m = return m
insert _ _ _ = error "structural invariants violated: M outside"
{-# INLINE insert #-}

step2 :: (Ord k, Arrayed k, Arrayed v) => Array k -> Array v -> Array k -> Array v -> Int -> Int -> MArray RealWorld k -> MArray RealWorld v -> IO (Int, Int)
step2 ka a kb b ra rb kab ab = do
  (ra',rb') <- step ka a kb b ra rb kab ab
  step ka a kb b ra' rb' kab ab
{-# INLINE step2 #-}

step :: (Ord k, Arrayed k, Arrayed v) => Array k -> Array v -> Array k -> Array v -> Int -> Int -> MArray RealWorld k -> MArray RealWorld v -> IO (Int, Int)
step ka a kb b ra rb kab ab
  | ra == na = do
    k <- G.basicUnsafeIndexM kb rb
    v <- G.basicUnsafeIndexM b rb
    ret k v ra (rb + 1)
  | rb == nb = do
    k <- G.basicUnsafeIndexM ka ra
    v <- G.basicUnsafeIndexM a ra
    ret k v (ra + 1) rb
  | otherwise = do
    k1 <- G.basicUnsafeIndexM ka ra
    k2 <- G.basicUnsafeIndexM kb rb
    case compare k1 k2 of
      LT -> do
        v <- G.basicUnsafeIndexM a ra
        ret k1 v (ra + 1) rb
      EQ -> do -- collision, overwrite with newer value
        v <- G.basicUnsafeIndexM a ra
        ret k1 v (ra + 1) (rb + 1)
      GT -> do
        v <- G.basicUnsafeIndexM b rb
        ret k2 v ra (rb + 1)
  where
    na = G.length ka
    nb = G.length kb
    r = ra + rb
    ret k v !ra' !rb' = do
      GM.basicUnsafeWrite kab r k
      GM.basicUnsafeWrite ab r v
      return (ra',rb')

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
