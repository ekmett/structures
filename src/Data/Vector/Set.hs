{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE FlexibleContexts #-}

module Data.Vector.Set where

import qualified Data.Vector.Set.Fusion as Fusion
import Data.Vector.Array
import Data.Vector.Slow as Slow
import qualified Data.Vector.Generic as G

data Set a
  = S0
  | S1 !(Array a)
  | S2 !(Partial (Array a))            !(Array a) !(Array a) !(Set a)
  | S3 !(Partial (Array a)) !(Array a) !(Array a) !(Array a) !(Set a)

deriving instance Show (Array a) => Show (Set a)

empty :: Set a
empty = S0
{-# INLINE empty #-}

null :: Set a -> Bool
null S0 = True
null _ = False
{-# INLINE null #-}

-- | /O(log n)/
size :: Set a -> Int
size S0                             = 0
size (S1 _)                         = 1
size (S2 _ _ _ xs)   | n <- size xs = n + n + 2
size (S3 _ _ _ _ xs) | n <- size xs = n + n + 3

-- | /O(log n)/ worst case
insert :: (Arrayed a, Ord a) => a -> Set a -> Set a
insert z0 s0 = go (G.singleton z0) s0 where
  go a S0     = S1 a
  go a (S1 b) = S2 (merge a b) a b S0
  go a (S2 mbc b c xs) = S3 (step mbc) a b c (steps xs)
  go a (S3 mcd b _ _ xs) = case mcd of
    Stop cd -> S2 (merge a b) a b (go cd xs)
    _       -> error "insert: stop Step"

  steps (S2 mxy x y xs)   = S2 (step mxy) x y (steps xs)
  steps (S3 myz x y z xs) = S3 (step myz) x y z (steps xs)
  steps m = m
{-# INLINE insert #-}

merge :: (Arrayed a, Ord a) => Array a -> Array a -> Partial (Array a)
merge m n = step $ walkST $ Slow.unstreamM $ Slow.streamST $ Fusion.merge (G.stream m) (G.stream n)
{-# INLINE merge #-}

step :: Partial a -> Partial a
step (Stop _)  = error "insert: step Stop"
step (Step m) = m
{-# INLINE step #-}
