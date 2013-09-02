module Data.Vector.Map.Tuning
  ( window, logWindow, mergeThreshold
  ) where

import Data.Bits

-- take every 'window'th entry as a forwarding pointer
window :: Int
window = 16
{-# INLINE window #-}

-- take every '2^logWindow'th entry as a forwarding pointer
logWindow :: Int
logWindow = 4
{-# INLINE logWindow #-}

-- Use approximately a 4-COLA (slightly less due to muddlying counts with forwarding pointers)
mergeThreshold :: Int -> Int -> Bool
mergeThreshold n m = n >= unsafeShiftR m 2
{-# INLINE mergeThreshold #-}

