{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE FlexibleContexts #-}

-- | A non-standard zeroless-binary deamortized cache-oblivious Set
module Data.Vector.Set where

import Data.Bits
import qualified Data.Vector.Set.Fusion as Fusion
import Data.Vector.Array
import Data.Vector.Slow as Slow
import qualified Data.Vector.Generic as G

data Set a
  = S0
  | S1                       !(Array a)
  | S2            !(Array a) !(Array a) !(Partial (Array a)) !(Set a)
  | S3 !(Array a) !(Array a) !(Array a) !(Partial (Array a)) !(Set a)

deriving instance Show (Array a) => Show (Set a)

empty :: Set a
empty = S0
{-# INLINE empty #-}

null :: Set a -> Bool
null S0 = True
null _ = False
{-# INLINE null #-}

-- | /O(log n)/ gives a conservative upper bound on size, assuming no collisions
size :: Set a -> Int
size S0                             = 0
size (S1 _)                         = 1
size (S2 _ _ _ xs)   | n <- size xs = n + n + 2
size (S3 _ _ _ _ xs) | n <- size xs = n + n + 3

-- | /O(log n)/ worst case
insert :: (Arrayed a, Ord a) => a -> Set a -> Set a
insert z0 s0 = go (G.singleton z0) s0 where
  go a S0     = S1 a
  go a (S1 b) = S2 a b (merge a b) S0
  go a (S2 b c   mbc xs) = S3 a b c (step mbc) (steps xs)
  go a (S3 b _ _ mcd xs) = case mcd of
    Stop cd -> S2 a b (merge a b) (go cd xs)
    _       -> error "insert: stop Step"

  steps (S2 x y mxy xs)   = S2 x y (step mxy) (steps xs)
  steps (S3 x y z myz xs) = S3 x y z (step myz) (steps xs)
  steps m = m
{-# INLINE insert #-}

-- | /O(log^n)/ worst and amortized
member :: (Arrayed a, Ord a) => a -> Set a -> Bool
member _ S0 = False
member x (S1 a) = member1 x a
member x (S2 a b _ xs) = member1 x a || member1 x b || member x xs
member x (S3 a b c _ xs) = member1 x a || member1 x b || member1 x c || member x xs
{-# INLINE member #-}

member1 :: (Arrayed a, Ord a) => a -> Array a -> Bool
member1 x xs = xs G.! search (\i -> xs G.! i >= x) 0 (G.length xs - 1) == x
{-# INLINE member1 #-}

merge :: (Arrayed a, Ord a) => Array a -> Array a -> Partial (Array a)
merge m n = step $ walkST $ Slow.unstreamM $ Slow.streamST $ Fusion.merge (G.stream m) (G.stream n)
{-# INLINE merge #-}

step :: Partial a -> Partial a
step (Stop _)  = error "insert: step Stop"
step (Step m) = m
{-# INLINE step #-}

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
