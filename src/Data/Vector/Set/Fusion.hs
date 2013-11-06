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
-----------------------------------------------------------------------------
module Data.Vector.Set.Fusion
  ( merge
  ) where

import Data.Vector.Fusion.Stream.Monadic as Stream
import Data.Vector.Fusion.Stream.Size as Stream

-- |
-- This form permits cancellative addition.
data MergeState sa sb a
  = MergeL sa sb a
  | MergeR sa sb a
  | MergeLeftEnded sb
  | MergeRightEnded sa
  | MergeStart sa sb

-- | This is the internal stream fusion combinator used to merge streams for addition.
merge :: (Monad m, Ord k) => Stream m k -> Stream m k -> Stream m k
merge (Stream stepa sa0 na) (Stream stepb sb0 nb) = Stream step (MergeStart sa0 sb0) (toMax na + toMax nb) where
  step (MergeStart sa sb) = do
    r <- stepa sa
    return $ case r of
      Yield i sa' -> Skip (MergeL sa' sb i)
      Skip sa'         -> Skip (MergeStart sa' sb)
      Done             -> Skip (MergeLeftEnded sb)
  step (MergeL sa sb i) = do
    r <- stepb sb
    return $ case r of
      Yield j sb' -> case compare i j of
        LT -> Yield i (MergeR sa sb' j)
        EQ -> Yield i (MergeStart sa sb')
        GT -> Yield j (MergeL sa sb' i)
      Skip sb' -> Skip (MergeL sa sb' i)
      Done     -> Yield i (MergeRightEnded sa)
  step (MergeR sa sb j) = do
    r <- stepa sa
    return $ case r of
      Yield i sa' -> case compare i j of
        LT -> Yield i (MergeR sa' sb j)
        EQ -> Yield i (MergeStart sa' sb)
        GT -> Yield j (MergeL sa' sb i)
      Skip sa' -> Skip (MergeR sa' sb j)
      Done     -> Yield j (MergeLeftEnded sb)
  step (MergeLeftEnded sb) = do
    r <- stepb sb
    return $ case r of
      Yield j sb' -> Yield j (MergeLeftEnded sb')
      Skip sb'    -> Skip (MergeLeftEnded sb')
      Done        -> Done
  step (MergeRightEnded sa) = do
    r <- stepa sa
    return $ case r of
      Yield i sa' -> Yield i (MergeRightEnded sa')
      Skip sa'    -> Skip (MergeRightEnded sa')
      Done        -> Done
  {-# INLINE [0] step #-}
{-# INLINE [1] merge #-}
