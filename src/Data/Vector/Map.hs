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
-- <http://supertech.csail.mit.edu/papers/sbtree.pdf "Cache-Oblivious Streaming B-Trees">.
--
-- Currently this 'Map' is implemented in an insert-only fashion. Deletions are left to future work
-- or to another derived structure in case they prove expensive.
--
-- Unlike the COLA, this version merely provides amortized complexity bounds as this permits us to
-- provide a fully functional API. However, even those asymptotics are only guaranteed if you do not
-- modify the \"old\" versions of the 'Map'. If you do, then while correctness is preserved, the
-- asymptotic analysis becomes inaccurate.
--
-- Reading from \"old\" versions of the 'Map' does not affect the asymptotic analysis and is fine.
--
-- Fractional cascading was originally replaced with the use of a hierarchical bloom filter per level containing
-- the elements for that level, with the false positive rate tuned to balance the lookup cost against
-- the costs of the cache misses for a false positive at that depth. This avoids the need to collect
-- forwarding pointers from the next level, reducing pressure on the cache dramatically, while providing
-- the same asymptotic complexity.
--
-- With either of these two techniques when used ephemerally, this 'Map' had asymptotic performance equal to that
-- of a B-Tree tuned to the parameters of your caches with requiring such parameter tuning.
--
-- However, the constants were still bad enough that the naive /O(log^2 n)/ version of the COLA actually wins
-- at lookups in benchmarks at the scale this data structure is interesting, say around a few million entries,
-- by a factor of 10x! Consequently, we're currently not even Bloom filtering.
--
-- Compared to the venerable @Data.Map@, this data structure currently consumes more memory, but it
-- provides a more limited palette of operations with different asymptotics (~10x faster inserts at a million entries)
-- and enables us to utilize contiguous storage.
--
-- /NB:/ when used with boxed data this structure may hold onto references to old versions
-- of things for many updates to come until sufficient operations have happened to merge them out
-- of the COLA.
--
-- TODO: track actual percentage of occupancy for each vector compared to the source vector it was based on.
-- This would permit 'split' and other operations that trim a 'Map' to properly reason about space usage by
-- borrowing the 1/3rd occupancy rule from a Stratified Doubling Array.
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

import Data.Bits
import qualified Data.List as List
import Data.Vector.Array
import Data.Vector.Fusion.Stream.Monadic (Stream(..))
import qualified Data.Vector.Fusion.Stream.Monadic as Stream
import Data.Vector.Fusion.Util
import qualified Data.Vector.Generic as G
import qualified Data.Vector.Map.Fusion as Fusion
import Prelude hiding (null, lookup)

#define BOUNDS_CHECK(f) (Ck.f __FILE__ __LINE__ Ck.Bounds)

data Map k v
  = Nil
  | Map !Int !(Array k) !(Array v) !(Map k v)

deriving instance (Show (Arr v v), Show (Arr k k), Show k, Show v) => Show (Map k v)
deriving instance (Read (Arr v v), Read (Arr k k), Read k, Read v) => Read (Map k v)

-- | /O(1)/. Identify if a 'Map' is the 'empty' 'Map'.
null :: Map k v -> Bool
null Nil = True
null _   = False
{-# INLINE null #-}

-- | /O(1)/ The 'empty' 'Map'.
empty :: Map k v
empty = Nil
{-# INLINE empty #-}

-- | /O(1)/ Construct a 'Map' from a single key/value pair.
singleton :: (Arrayed k, Arrayed v) => k -> v -> Map k v
singleton k v = Map 1 (G.singleton k) (G.singleton v) Nil
{-# INLINE singleton #-}

-- | /O(log^2 N)/ persistently amortized, /O(N)/ worst case. Lookup an element.
lookup :: (Ord k, Arrayed k, Arrayed v) => k -> Map k v -> Maybe v
lookup !k m0 = go m0 where
  {-# INLINE go #-}
  go Nil = Nothing
  go (Map n ks vs m)
    | j <- search (\i -> ks G.! i >= k) 0 (n-1)
    , ks G.! j == k = Just $ vs G.! j
    | otherwise = go m
{-# INLINE lookup #-}

threshold :: Int -> Int -> Bool
threshold n1 n2 = n1 > unsafeShiftR n2 1
{-# INLINE threshold #-}

-- | O((log N)\/B) ephemerally amortized loads for each cache, O(N\/B) worst case. Insert an element.
insert :: (Ord k, Arrayed k, Arrayed v) => k -> v -> Map k v -> Map k v
insert !k v (Map n1 ks1 vs1 (Map n2 ks2 vs2 m))
  | threshold n1 n2 = case G.unstream $ Fusion.insert k v (zips ks1 vs1) `Fusion.merge` zips ks2 vs2 of
  V_Pair n ks3 vs3 -> Map n ks3 vs3 m
insert k v m = Map 1 (G.singleton k) (G.singleton v) m
{-# INLINABLE insert #-}

fromList :: (Ord k, Arrayed k, Arrayed v) => [(k,v)] -> Map k v
fromList = List.foldl' (\m (k,v) -> insert k v m) empty
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

zips :: (G.Vector v a, G.Vector u b) => v a -> u b -> Stream Id (a, b)
zips va ub = Stream.zip (G.stream va) (G.stream ub)
{-# INLINE zips #-}
