{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE PatternGuards #-}
module Data.Vector.Heap
  ( Heap
  , heapify
  , sift
  , findMin
  , deleteMin
  , updateMin
  , null
  , length
  ) where

-- TODO: switch to vector-algorithm's quaternary heaps?

import Control.Monad (when)
import Control.Monad.ST
import Data.Bits
import Data.Vector.Array
import Data.Vector.Generic.Mutable as G
import Prelude hiding (length, null)

type Heap s a = MArray s a

-- /O(n)/ min heapify for an implicit binary heap
heapify :: (G.MVector v a, Ord a) => v s a -> ST s ()
heapify v = go (unsafeShiftR (n-2) 1) where
  !n = G.length v
  go k = when (k >= 0) $ do
    sift v k
    go (k-1)

-- /O(log n)/ push own a given element
sift :: (G.MVector v a, Ord a) => v s a -> Int -> ST s ()
sift v root0 = go root0 where
  !n = G.length v
  go root | child1 <- unsafeShiftL root 1 + 1 = do
    when (child1 < n) $ do -- in bounds
      r  <- G.unsafeRead v root
      c1 <- G.unsafeRead v child1
      let swap0 = if r > c1 then child1 else root
      let child2 = child1 + 1
      swap1 <- if child2 < n -- in bounds
        then do
          s0 <- G.unsafeRead v swap0
          c2 <- G.unsafeRead v child2
          return $ if s0 > c2 then child2 else swap0
        else return swap0
      when (swap1 /= root) $ do
        s1 <- G.unsafeRead v swap1
        G.unsafeWrite v swap1 r
        G.unsafeWrite v root s1
        go swap1

-- /O(1)/
findMin :: G.MVector v a => v s a -> ST s a
findMin v = G.unsafeRead v 0

-- /O(log n)/
deleteMin :: (G.MVector v a, Ord a) => v s a -> ST s (v s a)
deleteMin v = do
  let !n = G.length v
  a <- G.unsafeRead v (n-1)
  let !v' = G.unsafeSlice 0 (n-1) v
  updateMin a v'
  return v'

-- /O(log n)/
updateMin :: (G.MVector v a, Ord a) => a -> v s a -> ST s ()
updateMin a v = do
  G.unsafeWrite v 0 a
  sift v 0
