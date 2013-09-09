{-# LANGUAGE CPP #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE StandaloneDeriving #-}
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
-----------------------------------------------------------------------------
module Data.Vector.Map
  ( Map(..)
  , empty
  , null
  , singleton
  , lookup
  , insert
  , fromDistinctAscList
  , fromList
  , shape
  , split
  , union
  -- * Non-normalized operations
  , split'
  -- * Rebuild
  , rebuild
  ) where

import Data.Bits
import Data.Hashable
import qualified Data.List as List
import Data.Vector.Array
import Data.Vector.Fusion.Stream.Monadic (Stream(..))
import qualified Data.Vector.Fusion.Stream.Monadic as Stream
import Data.Vector.Fusion.Util
import qualified Data.Vector.Generic as G
import qualified Data.Vector.Map.Fusion as Fusion
import Prelude hiding (null, lookup)

#define BOUNDS_CHECK(f) (Ck.f __FILE__ __LINE__ Ck.Bounds)

-- | This Map is implemented as an insert-only Cache Oblivious Lookahead Array (COLA) with amortized complexity bounds
-- that are equal to those of a B-Tree when it is used ephemerally, using Bloom filters to replace the fractional
-- cascade.
data Map k v
  = Nil
  | One !k v !(Map k v)
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
singleton :: Arrayed v => k -> v -> Map k v
singleton k v = v `vseq` One k v Nil
{-# INLINE singleton #-}

-- | /O(log n)/ persistently amortized, /O(n)/ worst case. Lookup an element.
lookup :: (Hashable k, Ord k, Arrayed k, Arrayed v) => k -> Map k v -> Maybe v
lookup !k m0 = go m0 where
  {-# INLINE go #-}
  go Nil = Nothing
  go (One i a m)
    | k == i    = Just a
    | otherwise = go m
  go (Map n ks vs m)
    | j <- search (\i -> ks G.! i >= k) 0 (n-1)
    , ks G.! j == k = Just $ vs G.! j
    | otherwise = go m
{-# INLINE lookup #-}

threshold :: Int -> Int -> Bool
threshold n1 n2 = n1 > unsafeShiftR n2 1
{-# INLINE threshold #-}

-- two :: (Arrayed k, Arrayed v) => k -> v -> k -> v -> Map k v -> Map k v
-- three :: (Arrayed k, Arrayed v) => k -> v -> k -> v -> k -> v -> Map k v -> Map k v

-- force a value as much as it would be forced by inserting it into an Array
vseq :: forall a b. Arrayed a => a -> b -> b
vseq a b = G.elemseq (undefined :: Array a) a b

-- | /O(log n)/ ephemerally amortized, /O(n)/ worst case. Insert an element.
insert :: (Hashable k, Ord k, Arrayed k, Arrayed v) => k -> v -> Map k v -> Map k v
insert !k v (Map n1 ks1 vs1 (Map n2 ks2 vs2 m))
  | threshold n1 n2 = insert2 k v ks1 vs1 ks2 vs2 m
insert !ka va (One kb vb (One kc vc m)) = case G.unstream $ Fusion.insert ka va rest of
    V_Pair n ks vs -> Map n ks vs m
  where
    rest = case compare kb kc of
      LT -> Stream.fromListN 2 [(kb,vb),(kc,vc)]
      EQ -> Stream.fromListN 1 [(kb,vb)]
      GT -> Stream.fromListN 2 [(kc,vc),(kb,vb)]
insert k v m = v `vseq` One k v m
{-# INLINABLE insert #-}

insert2 :: (Hashable k, Ord k, Arrayed k, Arrayed v) => k -> v -> Array k -> Array v -> Array k -> Array v -> Map k v -> Map k v
insert2 k v ks1 vs1 ks2 vs2 m = case G.unstream $ Fusion.insert k v (zips ks1 vs1) `Fusion.merge` zips ks2 vs2 of
  V_Pair n ks3 vs3 -> Map n ks3 vs3 m
{-# INLINE insert2 #-}

fromDistinctAscList :: (Hashable k, Ord k, Arrayed k, Arrayed v) => [(k,v)] -> Map k v
fromDistinctAscList kvs = fromList kvs
{-# INLINE fromDistinctAscList #-}

fromList :: (Hashable k, Ord k, Arrayed k, Arrayed v) => [(k,v)] -> Map k v
fromList xs = List.foldl' (\m (k,v) -> insert k v m) empty xs
{-# INLINE fromList #-}

split :: (Hashable k, Ord k, Arrayed k, Arrayed v) => k -> Map k v -> (Map k v, Map k v)
split k m0 = go m0 where
  go Nil = (Nil, Nil)
  go (One j a m) = case go m of
    (xs,ys)
       | j < k     -> (insert j a xs, ys)
       | otherwise -> (xs, insert j a ys)
  go (Map n ks vs m) = case go m of
    (xs,ys) -> case G.splitAt j ks of
      (kxs,kys) -> case G.splitAt j vs of
        (vxs,vys) -> ( cons j     kxs vxs xs
                     , cons (n-j) kys vys ys
                     )
      where j = search (\i -> ks G.! i >= k) 0 n
{-# INLINE split #-}

-- | This trashes size invariants and inherently uses the structure twice, so asymptotic analysis if you
-- continue to edit this structure will be hard, but it can still be used for reads efficiently.
split' :: (Hashable k, Ord k, Arrayed k, Arrayed v) => k -> Map k v -> (Map k v, Map k v)
split' k m0 = go m0 where
  go Nil = (Nil, Nil)
  go (One j a m) = case go m of
    (xs,ys)
       | j < k     -> (One j a xs, ys)
       | otherwise -> (xs, One j a ys)
  go (Map n ks vs m) = case go m of
    (xs,ys) -> case G.splitAt j ks of
      (kxs,kys) -> case G.splitAt j vs of
        (vxs,vys) -> ( Map j     kxs vxs xs
                     , Map (n-j) kys vys ys
                     )
      where j = search (\i -> ks G.! i >= k) 0 n
{-# INLINE split' #-}

union :: (Hashable k, Ord k, Arrayed k, Arrayed v) => Map k v -> Map k v -> Map k v
union ys0 xs = go ys0 where
  go Nil = xs
  go (One k v m) = insert k v (go m)
  go (Map n ks vs m) = cons n ks vs (go m)
{-# INLINE union #-}

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

cons :: (Hashable k, Ord k, Arrayed k, Arrayed v) => Int -> Array k -> Array v -> Map k v -> Map k v
cons 0 _  _  m = m
cons 1 ks vs m = insert (G.unsafeHead ks) (G.unsafeHead vs) m
cons n ks vs (Map n2 ks' vs' m)
  | threshold n n2
  , nc <- min (n-1) n2
  , (ks1, ks2) <- G.splitAt nc ks
  , (vs1, vs2) <- G.splitAt nc vs
  , k <- G.unsafeHead ks2, ks3 <- G.unsafeTail ks2
  , v <- G.unsafeHead vs2, vs3 <- G.unsafeTail vs2
  = cons (n-nc-1) ks3 vs3 $ insert2 k v ks1 vs1 ks' vs' m
cons n ks vs m = Map n ks vs m
{-# INLINABLE cons #-}

-- | If using have trashed our size invariants we can use this to restore them
rebuild :: (Hashable k, Ord k, Arrayed k, Arrayed v) => Map k v -> Map k v
rebuild Nil = Nil
rebuild (One k v m) = insert k v m
rebuild (Map n ks vs m) = cons n ks vs (rebuild m)
{-# INLINABLE rebuild #-}

zips :: (G.Vector v a, G.Vector u b) => v a -> u b -> Stream Id (a, b)
zips va ub = Stream.zip (G.stream va) (G.stream ub)
{-# INLINE zips #-}

-- * Debugging

shape :: Map k v -> [Int]
shape Nil           = []
shape (One _ _ m)   = 1 : shape m
shape (Map n _ _ m) = n : shape m
