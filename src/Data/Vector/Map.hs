{-# LANGUAGE CPP #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE PatternGuards #-}
module Data.Vector.Map
  ( Map(..)
  , empty
  , null
  , singleton
  , lookup
  , insert
  ) where

import Control.Lens as L
import Data.Bits
import qualified Data.Vector.Generic as G
import Data.Vector.Array
import qualified Data.Vector.Bit as BV
import Data.Vector.Bit (BitVector)
import Prelude hiding (null, lookup)

#define BOUNDS_CHECK(f) (Ck.f __FILE__ __LINE__ Ck.Bounds)

-- | This Map is implemented as an insert-only Cache Oblivious Lookahead Array (COLA) with amortized complexity bounds
-- that are equal to those of a B-Tree when it is used ephemerally.
data Map k v = Map !(Array k) {-# UNPACK #-} !BitVector !(Array v) !(Map k v) | Nil

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
singleton k v = Map (G.singleton k) (BV.singleton False) (G.singleton v) Nil
{-# INLINE singleton #-}

lookup :: (Ord k, Arrayed k, Arrayed v) => k -> Map k v -> Maybe v
lookup k m0 = start m0 where
  {-# INLINE start #-}
  start Nil = Nothing
  start (Map ks fwd vs m)
    | ks G.! j == k, not (fwd^.contains j) = Just (vs G.! l)
    | otherwise = continue (dilate l)  m
    where j = search (\i -> ks G.! i >= k) 0 (BV.size fwd - 1)
          l = BV.rank fwd j

  continue _ Nil = Nothing
  continue lo (Map ks fwd vs m)
    | ks G.! j == k, not (fwd^.contains j) = Just (vs G.! l)
    | otherwise = continue (dilate l) m
    where j = search (\i -> ks G.! i >= k) lo (min (lo+7) (BV.size fwd - 1))
          l = BV.rank fwd j
{-# INLINE lookup #-}

insert :: (Ord k, Arrayed k, Arrayed v) => k -> v -> Map k v -> Map k v
insert k v Nil = singleton k v
insert _ _ _ = error "TODO" -- (Map n ks bv vs m) = undefined

-- * Utilities

dilate :: Int -> Int
dilate x = unsafeShiftL x 3
{-# INLINE dilate #-}

-- | assuming @l <= h@. Returns @h@ if the predicate is never @True@ over @[l..h)@
search :: (Int -> Bool) -> Int -> Int -> Int
search p = go where
  go l h
    | l == h    = l
    | p m       = go l m
    | otherwise = go (m+1) h
    where m = l + div (h-l) 2
{-# INLINE search #-}
