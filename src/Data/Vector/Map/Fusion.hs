{-# LANGUAGE CPP #-}
{-# LANGUAGE BangPatterns #-}
-----------------------------------------------------------------------------
-- |
-- Copyright   :  (C) 2013 Edward Kmett
-- License     :  BSD-style (see the file LICENSE)
-- Maintainer  :  Edward Kmett <ekmett@gmail.com>
-- Stability   :  experimental
-- Portability :  non-portable
--
-- COLA fusion internals
--
-----------------------------------------------------------------------------
module Data.Vector.Map.Fusion
  ( mergeStreamsWith, mergeStreamsWith0
  , actual, forwards
  , munstreams
  ) where

import Control.Lens
import Control.Monad.Primitive
import Data.Bits
import Data.Vector.Bit as Bit
import Data.Vector.Fusion.Stream.Monadic as Stream
import Data.Vector.Fusion.Stream.Size as Stream
import Data.Vector.Fusion.Util
import Data.Vector.Internal.Check as Ck
import qualified Data.Vector.Generic as G
import qualified Data.Vector.Generic.Mutable as GM
import qualified Data.Vector.Unboxed as U

#define BOUNDS_CHECK(f) (Ck.f __FILE__ __LINE__ Ck.Bounds)
#define INTERNAL_CHECK(f) (Ck.f __FILE__ __LINE__ Ck.Internal)

-- | The state for 'Stream' fusion that is used by 'mergeStreamsWith'.
--
-- This form permits cancellative addition.
data MergeState sa sb i a
  = MergeL sa sb i a
  | MergeR sa sb i a
  | MergeLeftEnded sb
  | MergeRightEnded sa
  | MergeStart sa sb

-- | This is the internal stream fusion combinator used to merge streams for addition.
--
-- This form permits cancellative addition.
mergeStreamsWith0 :: (Monad m, Ord k) => (a -> a -> Maybe a) -> Stream m (k, a) -> Stream m (k, a) -> Stream m (k, a)
mergeStreamsWith0 f (Stream stepa sa0 na) (Stream stepb sb0 nb)
  = Stream step (MergeStart sa0 sb0) (toMax na + toMax nb) where
  step (MergeStart sa sb) = do
    r <- stepa sa
    return $ case r of
      Yield (i, a) sa' -> Skip (MergeL sa' sb i a)
      Skip sa'         -> Skip (MergeStart sa' sb)
      Done             -> Skip (MergeLeftEnded sb)
  step (MergeL sa sb i a) = do
    r <- stepb sb
    return $ case r of
      Yield (j, b) sb' -> case compare i j of
        LT -> Yield (i, a)     (MergeR sa sb' j b)
        EQ -> case f a b of
           Just c  -> Yield (i, c) (MergeStart sa sb')
           Nothing -> Skip (MergeStart sa sb')
        GT -> Yield (j, b)     (MergeL sa sb' i a)
      Skip sb' -> Skip (MergeL sa sb' i a)
      Done     -> Yield (i, a) (MergeRightEnded sa)
  step (MergeR sa sb j b) = do
    r <- stepa sa
    return $ case r of
      Yield (i, a) sa' -> case compare i j of
        LT -> Yield (i, a)     (MergeR sa' sb j b)
        EQ -> case f a b of
          Just c  -> Yield (i, c) (MergeStart sa' sb)
          Nothing -> Skip (MergeStart sa' sb)
        GT -> Yield (j, b)     (MergeL sa' sb i a)
      Skip sa' -> Skip (MergeR sa' sb j b)
      Done     -> Yield (j, b) (MergeLeftEnded sb)
  step (MergeLeftEnded sb) = do
    r <- stepb sb
    return $ case r of
      Yield (j, b) sb' -> Yield (j, b) (MergeLeftEnded sb')
      Skip sb'         -> Skip (MergeLeftEnded sb')
      Done             -> Done
  step (MergeRightEnded sa) = do
    r <- stepa sa
    return $ case r of
      Yield (i, a) sa' -> Yield (i, a) (MergeRightEnded sa')
      Skip sa'         -> Skip (MergeRightEnded sa')
      Done             -> Done
  {-# INLINE [0] step #-}
{-# INLINE [1] mergeStreamsWith0 #-}


-- | This is the internal stream fusion combinator used to merge streams for addition.
mergeStreamsWith :: (Monad m, Ord k) => (a -> a -> a) -> Stream m (k, a) -> Stream m (k, a) -> Stream m (k, a)
mergeStreamsWith f (Stream stepa sa0 na) (Stream stepb sb0 nb)
  = Stream step (MergeStart sa0 sb0) (toMax na + toMax nb) where
  step (MergeStart sa sb) = do
    r <- stepa sa
    return $ case r of
      Yield (i, a) sa' -> Skip (MergeL sa' sb i a)
      Skip sa'         -> Skip (MergeStart sa' sb)
      Done             -> Skip (MergeLeftEnded sb)
  step (MergeL sa sb i a) = do
    r <- stepb sb
    return $ case r of
      Yield (j, b) sb' -> case compare i j of
        LT -> Yield (i, a)     (MergeR sa sb' j b)
        EQ -> Yield (i, f a b) (MergeStart sa sb')
        GT -> Yield (j, b)     (MergeL sa sb' i a)
      Skip sb' -> Skip (MergeL sa sb' i a)
      Done     -> Yield (i, a) (MergeRightEnded sa)
  step (MergeR sa sb j b) = do
    r <- stepa sa
    return $ case r of
      Yield (i, a) sa' -> case compare i j of
        LT -> Yield (i, a)     (MergeR sa' sb j b)
        EQ -> Yield (i, f a b) (MergeStart sa' sb)
        GT -> Yield (j, b)     (MergeL sa' sb i a)
      Skip sa' -> Skip (MergeR sa' sb j b)
      Done     -> Yield (j, b) (MergeLeftEnded sb)
  step (MergeLeftEnded sb) = do
    r <- stepb sb
    return $ case r of
      Yield (j, b) sb' -> Yield (j, b) (MergeLeftEnded sb')
      Skip sb'         -> Skip (MergeLeftEnded sb')
      Done             -> Done
  step (MergeRightEnded sa) = do
    r <- stepa sa
    return $ case r of
      Yield (i, a) sa' -> Yield (i, a) (MergeRightEnded sa')
      Skip sa'         -> Skip (MergeRightEnded sa')
      Done             -> Done
  {-# INLINE [0] step #-}
{-# INLINE [1] mergeStreamsWith #-}

data Actual sk sb sv k
  = GetK sk sb sv
  | GetB k sk sb sv
  | GetV k sk sb sv

actual :: (G.Vector u k, G.Vector v a) => u k -> BitVector -> v a -> Stream Id (k, Maybe a)
actual uk bv va = case G.stream uk of
  Stream stepk sk0 sz -> case G.stream (bv^._BitVector) of
    Stream stepb sb0 _ -> case G.stream va of
      Stream stepv sv0 _ -> Stream step (GetK sk0 sb0 sv0) sz
        where
          step (GetK sk sb sv) = stepk sk >>= \ s -> return $ case s of
            Yield k sk' -> Skip $ GetB k sk' sb sv
            Skip sk'    -> Skip $ GetK sk' sb sv
            Done        -> Done
          step (GetB k sk sb sv) = stepb sb >>= \ s -> return $ case s of
            Yield (Bit False) sb' -> Skip $ GetV k sk sb' sv
            Yield (Bit True) sb'  -> Skip $ GetK sk sb' sv -- no value, forwarding pointer
            Skip sb'        -> Skip $ GetB k sk sb' sv
            Done            -> Prelude.error "actual: BitVector stopped short"
          step (GetV k sk sb sv) = stepv sv >>= \ s -> return $ case s of
            Yield v sv' -> Yield (k, Just v) $ GetK sk sb sv'
            Skip sv'    -> Skip $ GetV k sk sb sv'
            Done        -> Prelude.error "actual: values stopped short"
          {-# INLINE step #-}
{-# INLINE [0] actual #-}

-- * Utilities

-- forwarding pointers
forwards :: (Monad m, G.Vector v k) => v k -> Stream m (k, Maybe a)
forwards v = Stream.generateM (unsafeShiftR (G.length v) 3) $ \i -> do
  k <- G.basicUnsafeIndexM v (unsafeShiftL i 3)
  return (k, Nothing)
{-# INLINE forwards #-}

munstreamsMax :: (PrimMonad m, GM.MVector u k, GM.MVector v a) => Stream m (k, Maybe a) -> Int -> m (u (PrimState m) k, U.MVector (PrimState m) Bit, v (PrimState m) a)
{-# INLINE munstreamsMax #-}
munstreamsMax s n
  = do
      ks <- INTERNAL_CHECK(checkLength) "munstreamMax: keys" n
           $ GM.unsafeNew n
      fwds <- INTERNAL_CHECK(checkLength) "munstreamMax: bits" n
           $ GM.unsafeNew n
      vs <- INTERNAL_CHECK(checkLength) "munstreamMax: values" n
           $ GM.unsafeNew n
      let put (i,j) (k, Nothing) = do
                       INTERNAL_CHECK(checkIndex) "munstreamMax" i n
                         $ GM.unsafeWrite ks i k
                       INTERNAL_CHECK(checkIndex) "munstreamMax" i n
                         $ GM.unsafeWrite fwds i (Bit True)
                       return (i+1,j+1)
          put (i,j) (k, Just a) = do
                       INTERNAL_CHECK(checkIndex) "munstreamMax" i n
                         $ GM.unsafeWrite ks i k
                       INTERNAL_CHECK(checkIndex) "munstreamMax" i n
                         $ GM.unsafeWrite fwds i (Bit False)
                       INTERNAL_CHECK(checkIndex) "munstreamMax" j n
                         $ GM.unsafeWrite vs j a
                       return (i+1,j+1)
      (m',n') <- Stream.foldM' put (0,0) s
      return ( INTERNAL_CHECK(checkSlice) "munstreamMax" 0 m' n
               $ GM.unsafeSlice 0 m' ks
             , INTERNAL_CHECK(checkSlice) "munstreamMax" 0 m' n
               $ GM.unsafeSlice 0 m' fwds
             , INTERNAL_CHECK(checkSlice) "munstreamMax" 0 n' n
               $ GM.unsafeSlice 0 n' vs
             )

munstreamsUnknown :: (PrimMonad m, GM.MVector u k, GM.MVector v a) => Stream m (k, Maybe a) -> m (u (PrimState m) k, U.MVector (PrimState m) Bit, v (PrimState m) a)
munstreamsUnknown = Prelude.error "TODO"

munstreams :: (PrimMonad m, GM.MVector u k, GM.MVector v a) => Stream m (k, Maybe a) -> m (u (PrimState m) k, U.MVector (PrimState m) Bit, v (PrimState m) a)
munstreams s = case upperBound (Stream.size s) of
               Just n  -> munstreamsMax     s n
               Nothing -> munstreamsUnknown s
