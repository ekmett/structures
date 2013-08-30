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
module Data.Vector.Map
  ( Map(..)
  , empty
  , null
  , singleton
  , lookup
  , insert
  , shape
  ) where

import Control.Lens as L
import Control.Monad.ST
import Data.Bits
import Data.Vector.Array
import Data.Vector.Bit (BitVector, _BitVector)
import qualified Data.Vector.Bit as BV
import Data.Vector.Fusion.Stream.Monadic (Stream(..))
import qualified Data.Vector.Fusion.Stream.Monadic as Stream
import Data.Vector.Fusion.Util
import qualified Data.Vector.Generic as G
import Data.Vector.Map.Fusion
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
lookup !k m0 = start m0 where
  {-# INLINE start #-}
  start Nil = Nothing
  start (Map ks fwd vs m)
    | ks G.! j == k, not (fwd^.contains j) = Just $ vs G.! (j-l)
    | otherwise = continue (dilate l)  m
    where j = search (\i -> ks G.! i >= k) 0 (BV.size fwd - 1)
          l = BV.rank fwd j

  continue _ Nil = Nothing
  continue lo (Map ks fwd vs m)
    | ks G.! j == k, not (fwd^.contains j) = Just $ vs G.! (j-l)
    | otherwise = continue (dilate l) m
    where j = search (\i -> ks G.! i >= k) (max 0 (lo-7)) (min (lo+7) (BV.size fwd - 1)) -- TODO tighten!
          l = BV.rank fwd j
{-# INLINE lookup #-}

insert :: (Ord k, Arrayed k, Arrayed v) => k -> v -> Map k v -> Map k v
insert !k v Nil = singleton k v
insert !k v m   = inserts (Stream.singleton (k, v)) 1 m
{-# INLINE insert #-}

-- TODO: make this manually unroll a few times so we can get fusion at common shapes?
-- TODO: if we don't know n, carve up the stream into size @log n@ (?) chunks online using effectful ST
-- actions to capture the tail, then just recursively merge them in.

inserts :: (Ord k, Arrayed k, Arrayed v) => Stream Id (k, v) -> Int -> Map k v -> Map k v
inserts xs n Nil = unstreams (unforwarded xs) n Nil
inserts xs n om@(Map ks fwds vs nm)
  | mergeThreshold n m = inserts (mergeStreams xs (actual ks fwds vs)) (n + m) nm
  | otherwise          = unstreams (mergeForwards xs ks) (n + unsafeShiftR (BV.size fwds + 7) 3) om
  where m = BV.size fwds
{-# INLINE inserts #-}

mergeThreshold :: Int -> Int -> Bool
mergeThreshold n m = n >= unsafeShiftR m 1

unstreams :: (Arrayed k, Arrayed v) => Stream Id (k, Maybe v) -> Int -> Map k v -> Map k v
unstreams (Stream stepa sa sz) n m = runST $ do
  (mks, mfs, mvs) <- munstreamsMax (Stream (return . unId . stepa) sa sz) n
  ks <- G.unsafeFreeze mks
  fs <- G.unsafeFreeze mfs
  vs <- G.unsafeFreeze mvs
  return (Map ks (_BitVector # fs) vs m)
{-# INLINE unstreams #-}

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

-- * Debugging

shape :: Map k v -> [Int]
shape Nil = []
shape (Map _ fwds _ m) = BV.size fwds : shape m
