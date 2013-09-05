{-# LANGUAGE BangPatterns #-}
module Data.Vector.Bloom.Util
  ( rehash
  , optimalHashes
  , optimalWidth
  ) where

import Data.Bits
import Data.Hashable

pepper :: Int
pepper = 0x53dffa872f4d7341

-- | Compute several hashes using a variant of
-- <http://www.eecs.harvard.edu/~kirsch/pubs/bbbf/esa06.pdf Kirsch and Mitzenmacher>'s
-- double hashing.
rehash :: Hashable a => Int -> a -> [Int]
rehash k a = go k where
  go 0 = []
  go i = h : go (i - 1) where !h = h1 + shiftR h2 i
  !h1 = hash a
  !h2 = hashWithSalt pepper a

-- * Utility Functions

-- |
-- @optimalHashes n m@ calculates the optimal number of hash functions for a given number of entries @n@ in a
-- Bloom filter that is @m@ bits wide.
--
-- @k = m/n log 2@
optimalHashes :: Int -> Int -> Int
optimalHashes n m = ceiling (fromIntegral m / fromIntegral n * log 2 :: Double)

-- |
-- @optimalWidth n p@ calculate the optimal width @m@ of a bloom filter given the expected number of entries @n@
-- and target failure rate @p@ at capacity.
--
-- @m = -n log p / (log 2)^2@
optimalWidth :: Int -> Double -> Int
optimalWidth n p = ceiling (-1 * fromIntegral n * log p / log 2 / log 2 :: Double)
