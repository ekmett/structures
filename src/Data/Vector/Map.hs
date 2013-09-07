{-# LANGUAGE CPP #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
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
-- When used ephemerally, this 'Map' has asymptotic performance equal to that
-- of a B-Tree tuned to the parameters of your caches. However, no such
-- parameter tuning is required.
--
-- Currently this 'Map' is implemented in an insert-only fashion. Deletions are left to future work
-- or to another derived structure in case they prove expensive.
--
-- Fractional cascading has been replaced with the use of a hierarchical bloom filter per level containing
-- the elements for that level, with the false positive rate tuned to balance the lookup cost against
-- the costs of the cache misses for a false positive at that depth. This avoids the need to collect
-- forwarding pointers from the next level, reducing pressure on the cache dramatically, while providing
-- the same asymptotic complexity.
--
-- Unlike the COLA, this version merely provides amortized complexity bounds as this permits us to
-- provide a fully functional API. However, even those asymptotics are only guaranteed if you do not
-- modify the \"old\" versions of the 'Map'. If you do, then while correctness is preserved, the
-- asymptotic analysis becomes inaccurate.
--
-- Reading from \"old\" versions of the 'Map' will not affect the asymptotic analysis, however.
--
-- Compared to the venerable @Data.Map@, this data structure currently consumes more memory, but it
-- provides a more limited palette of operations in less time and enables us to utilize contiguous storage.
-----------------------------------------------------------------------------
module Data.Vector.Map
  ( Map(..)
  , empty
  , null
  , singleton
  , lookup
  , insert
  , insert4
  , insert6
  , fromList
  , shape
  ) where

import Control.Monad.ST
import Control.Parallel
import Data.Bits
import Data.Hashable
import Data.Vector.Array
import qualified Data.Vector.Bloom as B
import qualified Data.Vector.Bloom.Mutable as MB
import Data.Vector.Bloom.Util
import Data.Vector.Fusion.Stream.Monadic (Stream(..))
import qualified Data.Vector.Fusion.Stream.Monadic as Stream
import Data.Vector.Fusion.Util
import qualified Data.Vector.Generic as G
import qualified Data.Vector.Map.Fusion as Fusion
import Prelude hiding (null, lookup)

#define BOUNDS_CHECK(f) (Ck.f __FILE__ __LINE__ Ck.Bounds)

baseRate :: Double
baseRate = 0.01

blooming :: (Hashable k, Arrayed k) => Array k -> Maybe B.Bloom
blooming ks
  | n < 100 = Nothing
  | m <- optimalWidth n $ baseRate / log (fromIntegral n)
  , k <- optimalHashes n m = Just $ runST $ do
    mb <- MB.mbloom k m
    G.forM_ ks $ \a -> MB.insert a mb
    B.freeze mb
  where n = G.length ks

-- | This Map is implemented as an insert-only Cache Oblivious Lookahead Array (COLA) with amortized complexity bounds
-- that are equal to those of a B-Tree when it is used ephemerally, using Bloom filters to replace the fractional
-- cascade.
data Map k v = Map !Int (Maybe B.Bloom) !(Array k) !(Array v) !(Map k v) | Nil

deriving instance (Show (Arr v v), Show (Arr k k)) => Show (Map k v)
deriving instance (Read (Arr v v), Read (Arr k k)) => Read (Map k v)

null :: Map k v -> Bool
null Nil = True
null _   = False
{-# INLINE null #-}

empty :: Map k v
empty = Nil
{-# INLINE empty #-}

singleton :: (Arrayed k, Arrayed v) => k -> v -> Map k v
singleton k v = cons1 k v Nil
{-# INLINE singleton #-}

cons1 :: (Arrayed k, Arrayed v) => k -> v -> Map k v -> Map k v
cons1 k v = Map 1 Nothing (G.singleton k) (G.singleton v)
{-# INLINE cons1 #-}

lookup :: (Hashable k, Ord k, Arrayed k, Arrayed v) => k -> Map k v -> Maybe v
lookup !k m0 = go m0 where
  {-# INLINE go #-}
  go Nil = Nothing
  go (Map n mbf ks vs m)
    | maybe True (B.elem k) mbf
    , j <- search (\i -> ks G.! i >= k) 0 (n-1)
    , ks G.! j == k = Just $ vs G.! j
    | otherwise = go m
{-# INLINE lookup #-}

zips :: (G.Vector v a, G.Vector u b) => v a -> u b -> Stream Id (a, b)
zips va ub = Stream.zip (G.stream va) (G.stream ub)
{-# INLINE zips #-}

insert :: (Hashable k, Ord k, Arrayed k, Arrayed v) => k -> v -> Map k v -> Map k v
insert !k v (Map n1 _ ks1 vs1 (Map n2 _ ks2 vs2 m2))
  | n1 > unsafeShiftR n2 1 = case G.unstream $ Fusion.insert k v (zips ks1 vs1) `Fusion.merge` zips ks2 vs2 of
    V_Pair n ks3 vs3 -> Map n (blooming ks3) ks3 vs3 m2
insert k v m = cons1 k v m
{-# INLINE insert #-}

insert4 :: forall k v. (Hashable k, Ord k, Arrayed k, Arrayed v) => k -> v -> Map k v -> Map k v
insert4 !k v (Map n1 _ ks1 vs1 (Map _  _ ks2 vs2
             (Map _  _ ks3 vs3 (Map n4  _ ks4 vs4 m))))
  | n1 > unsafeShiftR n4 2 = spawn $ case va of
    V_Pair na ksa vsa -> Map na (blooming ksa) ksa vsa $ case vb of
      V_Pair nb ksb vsb -> Map nb (blooming ksb) ksb vsb m
  where va :: V_Pair (k,v)
        va = G.unstream $ Fusion.insert k v (zips ks1 vs1) `Fusion.merge` zips ks2 vs2
        vb :: V_Pair (k,v)
        vb = G.unstream $ zips ks3 vs3 `Fusion.merge` zips ks4 vs4
        spawn xs
          | n1 <= 10000 = xs
          | otherwise   = vb `par` va `pseq` xs
insert4 k v m = cons1 k v m
{-# INLINE insert4 #-}


insert6 :: forall k v. (Hashable k, Ord k, Arrayed k, Arrayed v) => k -> v -> Map k v -> Map k v
insert6 !k v (Map n1 _ ks1 vs1 (Map _  _ ks2 vs2
             (Map _  _ ks3 vs3 (Map _  _ ks4 vs4
             (Map _  _ ks5 vs5 (Map n6 _ ks6 vs6 m))))))
  | n1 > unsafeShiftR n6 2 = vc `par` vb `par` va `pseq` case va of
    V_Pair na ksa vsa -> Map na (blooming ksa) ksa vsa $ case vb of
      V_Pair nb ksb vsb -> Map nb (blooming ksb) ksb vsb $ case vc of
        V_Pair nc ksc vsc -> Map nc (blooming ksc) ksc vsc m
  where va :: V_Pair (k,v)
        va = G.unstream $ Fusion.insert k v (zips ks1 vs1) `Fusion.merge` zips ks2 vs2
        vb :: V_Pair (k,v)
        vb = G.unstream $ zips ks3 vs3 `Fusion.merge` zips ks4 vs4
        vc :: V_Pair (k,v)
        vc = G.unstream $ zips ks5 vs5 `Fusion.merge` zips ks6 vs6
insert6 k v m = cons1 k v m
{-# INLINE insert6 #-}


fromList :: (Hashable k, Ord k, Arrayed k, Arrayed v) => [(k,v)] -> Map k v
fromList = foldr (\(k,v) m -> insert k v m) empty
{-# INLINE fromList #-}

-- | assuming @l <= h@. Returns @h@ if the predicate is never @True@ over @[l..h)@
search :: (Int -> Bool) -> Int -> Int -> Int
search p = go where
  go l h
    | l == h    = l
    | p m       = go l m
    | otherwise = go (m+1) h
    where m = l + div (h-l) 2
{-# INLINE search #-}

-- * Debugging

shape :: Map k v -> [Int]
shape Nil = []
shape (Map n _ _ _ m) = n : shape m
