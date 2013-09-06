{-# LANGUAGE CPP #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
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
-- This module provides a functional variant on the Cache Oblivious Lookahead Array (COLA)
-- by Bender et al. in <http://supertech.csail.mit.edu/papers/sbtree.pdf "Cache-Oblivious Streaming B-Trees">. 
--
-- When used ephemerally, this 'Map' has asymptotic performance equal to that of a B-Tree
-- tuned to the parameters of your caches. However, no such parameter tuning is required.
--
-- Currently this 'Map' is implemented in an insert-only fashion. Deletions are left to future work
-- or to another derived structure in case they prove expensive.
--
-- Moreover, unlike the COLA from the paper, this version merely provides amortized complexity bounds
-- as this permits us to provide a fully functional API. However, even those asymptotics are
-- only guaranteed if you do not modify the \"old\" versions of the 'Map'. If you do, then while correctness
-- is preserved, the asymptotic analysis is inaccurate.
--
-- However, reading from \"old\" versions of the 'Map' will not affect the asymptotic analysis.
--
-- Compared to @Data.Map@, this data structure currently consumes both more memory and more time,
-- but it enables us to utilize contiguous storage.
-----------------------------------------------------------------------------
module Data.Vector.Map
  ( Map(..)
  , empty
  , null
  , singleton
  , lookup
  , insert
  , fromList
  , shape
  ) where

import Control.Lens as L
import Control.Monad.ST
import Data.Bits
import Data.Hashable
import Data.Vector.Array
import qualified Data.Vector.Bloom as B
import qualified Data.Vector.Bloom.Mutable as MB
import Data.Vector.Bloom.Util
import Data.Vector.Bit (BitVector, _BitVector)
import qualified Data.Vector.Bit as BV
import Data.Vector.Fusion.Stream.Monadic (Stream(..))
import qualified Data.Vector.Fusion.Stream.Monadic as Stream
import Data.Vector.Fusion.Util
import qualified Data.Vector.Generic as G
import Data.Vector.Map.Fusion
import Data.Vector.Map.Tuning
import GHC.Magic
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
singleton k v = Map 1 Nothing (G.singleton k) (G.singleton v) Nil
{-# INLINE singleton #-}

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

insert :: (Hashable k, Ord k, Arrayed k, Arrayed v) => k -> v -> Map k v -> Map k v
insert !k v Nil = singleton k v
insert !k v m   = inserts (Stream.singleton (k, v)) 1 m
{-# INLINE insert #-}

inserts :: (Hashable k, Ord k, Arrayed k, Arrayed v) => Stream Id (k, v) -> Int -> Map k v -> Map k v
inserts xs n Nil = unstreams xs Nil
inserts xs n om@(Map m _ ks vs nm)
  | mergeThreshold n m = inserts (mergeStreams xs $ G.stream $ V_Pair m ks vs) (n + m) nm
  | otherwise          = unstreams xs om
{-# INLINABLE inserts #-}

unstreams :: (Hashable k, Arrayed k, Arrayed v) => Stream Id (k, v) -> Map k v -> Map k v
unstreams s m = case G.unstream s of
  V_Pair n ks vs -> Map n (blooming ks) ks vs m
{-# INLINE unstreams #-}

fromList :: (Hashable k, Ord k, Arrayed k, Arrayed v) => [(k,v)] -> Map k v
fromList xs = foldr (\(k,v) m -> insert k v m) empty xs
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
