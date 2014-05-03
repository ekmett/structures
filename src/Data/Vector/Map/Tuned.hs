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
-- but with inserts converted from ephemerally amortized to persisently amortized using a technique from Overmars and van Leeuwen.
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
module Data.Vector.Map.Tuned
  ( Map
  , empty
  , null
  , singleton
  , lookup
  , insert
  , fromList
  ) where

import Data.Bits
import qualified Data.Foldable as Foldable
import qualified Data.List as List
import Data.Vector.Array
import Data.Vector.Fusion.Stream.Monadic (Stream(..))
import qualified Data.Vector.Fusion.Stream.Monadic as Stream
import Data.Vector.Fusion.Util
import qualified Data.Map as Map
import qualified Data.Vector.Map.Fusion as Fusion
import qualified Data.Vector.Generic as G
import Prelude hiding (null, lookup)

-- | This Map is implemented as an insert-only Cache Oblivious Lookahead Array (COLA) with amortized complexity bounds
-- that are equal to those of a B-Tree, except for an extra log factor slowdown on lookups due to the lack of fractional
-- cascading. It uses a traditional Data.Map as a nursery.

data Map k a = Map !(Map.Map k a) !(LA k a)

_THRESHOLD :: Int
_THRESHOLD = 10

data LA k a
  = M0
  | M1 !(Chunk k a)
  | M2 !(Chunk k a) !(Chunk k a) (Chunk k a) !(LA k a) -- merged chunk is deliberately lazy
  | M3 !(Chunk k a) !(Chunk k a) !(Chunk k a) (Chunk k a) !(LA k a)

data Chunk k a = Chunk !(Array k) !(Array a)

deriving instance (Show (Arr k k), Show (Arr a a)) => Show (Chunk k a)
deriving instance (Show (Arr k k), Show (Arr a a)) => Show (LA k a)

#if __GLASGOW_HASKELL__ >= 708
type role LA nominal nominal
#endif

-- | /O(1)/. Identify if a 'LA' is the 'empty' 'LA'.
null :: Map k v -> Bool
null (Map m M0) = Map.null m
null _          = False
{-# INLINE null #-}

-- | /O(1)/ The 'empty' 'LA'.
empty :: Map k v
empty = Map Map.empty M0
{-# INLINE empty #-}

-- | /O(1)/ Construct a 'LA' from a single key/value pair.
singleton :: (Arrayed k, Arrayed v) => k -> v -> Map k v
singleton k v = Map (Map.singleton k v) M0
{-# INLINE singleton #-}

-- | /O(log^2 N)/ worst-case. Lookup an element.
lookup :: (Ord k, Arrayed k, Arrayed v) => k -> Map k v -> Maybe v
lookup !k (Map m0 la) = case Map.lookup k m0 of
  Nothing -> go la
  mv      -> mv
 where
  go M0                = Nothing
  go (M1 as)           = lookup1 k as Nothing
  go (M2 as bs _ m)    = lookup1 k as $ lookup1 k bs $ go m
  go (M3 as bs cs _ m) = lookup1 k as $ lookup1 k bs $ lookup1 k cs $ go m
{-# INLINE lookup #-}

lookup1 :: (Ord k, Arrayed k, Arrayed v) => k -> Chunk k v -> Maybe v -> Maybe v
lookup1 k (Chunk ks vs) r
  | j <- search (\i -> ks G.! i >= k) 0 (G.length ks - 1)
  , ks G.! j == k = Just $ vs G.! j
  | otherwise = r
{-# INLINE lookup1 #-}

zips :: (Arrayed k, Arrayed v) => Chunk k v -> Stream Id (k, v)
zips (Chunk ks vs) = Stream.zip (G.stream ks) (G.stream vs)
{-# INLINE zips #-}

merge :: (Ord k, Arrayed k, Arrayed v) => Chunk k v -> Chunk k v -> Chunk k v
merge as bs = case G.unstream $ zips as `Fusion.merge` zips bs of
  V_Pair _ ks vs -> Chunk ks vs
{-# INLINE merge #-}

-- | O((log N)\/B) worst-case loads for each cache. Insert an element.
insert :: (Ord k, Arrayed k, Arrayed v) => k -> v -> Map k v -> Map k v
insert k0 v0 (Map m0 xs0)
  | n0 <= _THRESHOLD = Map (Map.insert k0 v0 m0) xs0
  | otherwise = Map Map.empty $ inserts (Chunk (G.fromListN n0 (Map.keys m0)) (G.fromListN n0 (Foldable.toList m0))) xs0
 where
  n0 = Map.size m0
  inserts as M0                 = M1 as
  inserts as (M1 bs)            = M2 as bs (merge as bs) M0
  inserts as (M2 bs cs bcs xs)  = M3 as bs cs bcs xs
  inserts as (M3 bs _ _ cds xs) = cds `seq` M2 as bs (merge as bs) (inserts cds xs)
{-# INLINE insert #-}

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
