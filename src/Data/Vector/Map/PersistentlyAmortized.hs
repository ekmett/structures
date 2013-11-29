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
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE NamedFieldPuns #-}
-----------------------------------------------------------------------------
-- |
-- Copyright   :  (C) 2013 Edward Kmett, Mike Zuser
-- License     :  BSD-style (see the file LICENSE)
-- Maintainer  :  Edward Kmett <ekmett@gmail.com>
-- Stability   :  experimental
-- Portability :  non-portable
--
-- This module provides a version on Data.Vector.'Data.Vector.Map' that supports efficiently inserting
-- into \"old\" versions of the 'Map'. It is somewhat slower for the ephemeral use case, especially for
-- lookups.
-----------------------------------------------------------------------------
module Data.Vector.Map.PersistentlyAmortized
  ( Map(..)
  , empty
  , null
  , singleton
  , lookup
  , insert
  , fromList
--  , shape
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
  | One !k v !(Map k v)
  | Map !(Merge k v) !(Map k v)

deriving instance (Show (Arr v v), Show (Arr k k), Show k, Show v) => Show (Map k v)
deriving instance (Read (Arr v v), Read (Arr k k), Read k, Read v) => Read (Map k v)

-- To achieve persistently amortized bounds, we create a suspension for merge
-- but continue to perform queries against the unmerged components until
-- the merge has been paid for.
-- We will maintain the invariant that there can only be one
-- Unmerged constructor of each size in the Map.
data Merge k v
  = Unmerged
    -- The size is an upper bound, since we can't know the true size
    -- until after we run the merge.
    { size       :: !Int
    , key        :: !k
    , val        :: !v
    , leftMerge  :: !(Merge k v)
    , rightKeys  :: !(Array k)
    , rightVals  :: !(Array v)
    , mergedKeys :: (Array k)
    , mergedVals :: (Array v)
    }
  | Merged
    { mergedKeys :: !(Array k)
    , mergedVals :: !(Array v)
    }

deriving instance (Show (Arr v v), Show (Arr k k), Show k, Show v) => Show (Merge k v)
deriving instance (Read (Arr v v), Read (Arr k k), Read k, Read v) => Read (Merge k v)

mergeSize :: Arrayed k => Merge k v -> Int
mergeSize Unmerged{size}     = size
mergeSize Merged{mergedKeys} = G.length mergedKeys
{-# INLINE mergeSize #-}

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

-- | /O(log^2 N)/ persistently amortized, /O(N)/ worst case. Lookup an element.
lookup :: (Ord k, Arrayed k, Arrayed v) => k -> Map k v -> Maybe v
lookup !k m0 = go m0 where
  {-# INLINE go #-}
  go Nil = Nothing
  go (One i a m)
    | k == i    = Just a
    | otherwise = go m
  go (Map (Merged ks vs) m)
    | j <- search (\i -> ks G.! i >= k) 0 (G.length ks - 1)
    , ks G.! j == k = Just $ vs G.! j
    | otherwise = go m
  -- By the invariant, there may only be one Unmerged node of each
  -- logarithmic size. If they are all nested in a single node,
  -- lookup on that node can degrade from O(log N) to O(log^2 N),
  -- but since this can only happen on one of the possible O(log N)
  -- nodes, the overall time is still O(log^2 N)
  go (Map (Unmerged _ k' v mg ks vs _ _) m)
    | k == k'   = Just v
    | Just v <- go (Map mg Nil) = Just v
    | j <- search (\i -> ks G.! i >= k) 0 (G.length ks - 1)
    , ks G.! j == k = Just $ vs G.! j
    | otherwise = go m
{-# INLINE lookup #-}

threshold :: Int -> Int -> Bool
threshold n1 n2 = n1 > unsafeShiftR n2 1
{-# INLINE threshold #-}

-- force a value as much as it would be forced by inserting it into an Array
vseq :: forall a b. Arrayed a => a -> b -> b
vseq a b = G.elemseq (undefined :: Array a) a b
{-# INLINE vseq #-}

insert :: (Ord k, Arrayed k, Arrayed v) => k -> v -> Map k v -> Map k v
insert !k v (Map ms1 (Map ms2 m))
  | threshold (mergeSize ms1) (mergeSize ms2) = insertUnmerged k v ms1 ms2 m
insert !ka va (One kb vb (One kc vc m)) = case G.unstream $ Fusion.insert ka va rest of
    V_Pair _ ks vs -> Map (Merged ks vs) m
  where
    rest = case compare kb kc of
      LT -> Stream.fromListN 2 [(kb,vb),(kc,vc)]
      EQ -> Stream.fromListN 1 [(kb,vb)]
      GT -> Stream.fromListN 2 [(kc,vc),(kb,vb)]
insert k v m = v `vseq` One k v m
{-# INLINABLE insert #-}

-- Save the inputs, set up the merge, and restore the invariant.
insertUnmerged :: (Ord k, Arrayed k, Arrayed v) => k -> v -> Merge k v -> Merge k v -> Map k v -> Map k v
insertUnmerged k v lms (Merged rks rvs) m
  = Map (Unmerged n k v lms rks rvs nks nvs) (mergeOne m)
  where
    n = mergeSize lms + G.length rks + 1
    merge = G.unstream $ Fusion.insert k v (zips (mergedKeys lms) (mergedVals lms)) `Fusion.merge` zips rks rvs
    (nks, nvs) = case merge of V_Pair _ ks' vs' -> (ks', vs')
insertUnmerged _ _ _ _ _ = error "insert: invariant violation, only the right half of a merge should be suspended"
{-# INLINABLE insertUnmerged #-}

-- The merge discharged by mergeOne will always be the same size as the one created
-- by insertUnmerged because of the structure of skew binary arithmetic.
-- Since the new merge that was just created is the same size as the old one,
-- and all the steps to create the new one took place after the old one was
-- created, enough time must have past to amortize the cost of discharging the merge.
mergeOne :: Map k v -> Map k v
mergeOne Nil          = Nil
mergeOne (One _ _ _)  = error "insert: invariant violation, One after a merge"
mergeOne (Map ms m)   = Map (go ms) m
  where
    go Merged{..} = error "insert: invariant violation, expecting"
    go Unmerged{leftMerge = Merged{}, mergedKeys, mergedVals} = Merged mergedKeys mergedVals
    go mg@Unmerged{leftMerge} = mg{leftMerge = go leftMerge}
{-# INLINABLE mergeOne #-}

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

zips :: (G.Vector v a, G.Vector u b) => v a -> u b -> Stream Id (a, b)
zips va ub = Stream.zip (G.stream va) (G.stream ub)
{-# INLINE zips #-}

-- * Debugging
{-
shape :: Map k v -> [Int]
shape Nil           = []
shape (One _ _ m)   = 1 : shape m
shape (Map n _ _ m) = n : shape m
-}
