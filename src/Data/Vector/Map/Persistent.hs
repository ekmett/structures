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
module Data.Vector.Map.Persistent
  ( Map
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
import qualified Data.Vector.Map.Fusion as Fusion
import qualified Data.Vector.Generic as G
import Prelude hiding (null, lookup)

-- | This Map is implemented as an insert-only Cache Oblivious Lookahead Array (COLA) with amortized complexity bounds
-- that are equal to those of a B-Tree, except for an extra log factor slowdown on lookups due to the lack of fractional
-- cascading. It uses a traditional Data.Map as a nursery.

data Map k a
  = M0
  | M1 !(Array k) !(Array a)
  | M2 !(Array k) !(Array a) !(Array k) !(Array a) (Chunk k a) !(Map k a)
  | M3 !(Array k) !(Array a) !(Array k) !(Array a) !(Array k) !(Array a) (Chunk k a) !(Map k a)

data Chunk k a = Chunk !(Array k) !(Array a)

#if __GLASGOW_HASKELL__ >= 708
type role Map nominal nominal
#endif

instance (Show (Arr v v), Show (Arr k k)) => Show (Map k v) where
  showsPrec _ M0 = showString "M0"
  showsPrec d (M1 ka a) = showParen (d > 10) $
    showString "M1 " . showsPrec 11 ka . showChar ' ' . showsPrec 11 a
  showsPrec d (M2 ka a kb b _ xs) = showParen (d > 10) $
    showString "M2 " .
    showsPrec 11 ka . showChar ' ' . showsPrec 11 a . showChar ' ' .
    showsPrec 11 kb . showChar ' ' . showsPrec 11 b . showChar ' ' .
    showsPrec 11 xs
  showsPrec d (M3 ka a kb b kc c _ xs) = showParen (d > 10) $
    showString "M3 " .
    showsPrec 11 ka . showChar ' ' . showsPrec 11 a . showChar ' ' .
    showsPrec 11 kb . showChar ' ' . showsPrec 11 b . showChar ' ' .
    showsPrec 11 kc . showChar ' ' . showsPrec 11 c . showString " _ " .
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
singleton k v = M1 (G.singleton k) (G.singleton v)
{-# INLINE singleton #-}

-- | /O(log^2 N)/ worst-case. Lookup an element.
lookup :: (Ord k, Arrayed k, Arrayed v) => k -> Map k v -> Maybe v
lookup !k m0 = go m0 where
  {-# INLINE go #-}
  go M0                         = Nothing
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

zips :: (G.Vector v a, G.Vector u b) => v a -> u b -> Stream Id (a, b)
zips va ub = Stream.zip (G.stream va) (G.stream ub)
{-# INLINE zips #-}

merge :: (Ord k, Arrayed k, Arrayed v) => Array k -> Array v -> Array k -> Array v -> Chunk k v
merge ka va kb vb = case G.unstream $ zips ka va `Fusion.merge` zips kb vb of
  V_Pair _ kc vc -> Chunk kc vc
{-# INLINE merge #-}

-- | O((log N)\/B) worst-case loads for each cache. Insert an element.
insert :: (Ord k, Arrayed k, Arrayed v) => k -> v -> Map k v -> Map k v
insert k0 v0 xs0 = inserts (G.singleton k0) (G.singleton v0) xs0
 where
  inserts ka a M0                                  = M1 ka a
  inserts ka a (M1 kb b)                           = M2 ka a kb b (merge ka a kb b) M0
  inserts ka a (M2 kb b kc c bc xs)                = M3 ka a kb b kc c bc xs
  inserts ka a (M3 kb b _ _ _ _ (Chunk kcd cd) xs) = M2 ka a kb b (merge ka a kb b) (inserts kcd cd xs)
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
