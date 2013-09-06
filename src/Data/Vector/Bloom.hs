{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE RankNTypes #-}
-- | Hierarchical Bloom filters
module Data.Vector.Bloom
  ( Bloom(Bloom)
  -- * Information
  , entries
  , hashes
  , width
  -- * Construction
  , bloom
  -- * Modification
  , modify
  , insert
  -- * Testing
  , elem
  -- * Combining Blooms
  , union
  , intersection
  -- * Freezing/Thawing
  , freeze, thaw
  , unsafeFreeze, unsafeThaw
  ) where

import Control.Monad hiding (forM_)
import Control.Monad.Primitive
import Control.Monad.ST
import Data.Bits
import Data.Data
import qualified Data.Foldable as F
import Data.Hashable
import Data.Semigroup
import qualified Data.Vector.Unboxed as U
import qualified Data.Vector.Unboxed.Mutable as UM
import qualified Data.Vector.Bloom.Mutable as MB
import Data.Vector.Bloom.Mutable (MBloom(MBloom))
import Data.Vector.Bloom.Util
import Data.Word
import Prelude hiding (elem)

-- TODO: switch to a hash that we can persist to disk cross-platform!

data Bloom
  = Bloom
    { hashes :: {-# UNPACK #-} !Int -- number of hash functions to use
    , _mask  :: {-# UNPACK #-} !Int -- 2^p-1
    , _bits  :: !(U.Vector Word64)  -- when length > 512, then it is an integral multiple of 512, and data is binned into pages
    }
  deriving (Eq,Ord,Show,Read,Typeable,Data)

-- | @'bloom' k m@ builds an @m@-bit wide 'Bloom' filter that uses @k@ hashes.
bloom :: (F.Foldable f, Hashable a) => Int -> Int -> f a -> Bloom
bloom k m0 fa = runST $ do
  let m1 = m0 .|. unsafeShiftR m0 1
  let m2 = m1 .|. unsafeShiftR m1 2
  let m3 = m2 .|. unsafeShiftR m2 4
  let m4 = m3 .|. unsafeShiftR m3 8
  let m5 = m4 .|. unsafeShiftR m4 16
  let m6 = m5 .|. shiftR m5 32
  v <- UM.replicate (unsafeShiftR m6 6 + 1) 0
  let mb = MB.MBloom k m6 v
  F.forM_ fa $ \a -> MB.insert a mb
  freeze mb
{-# INLINE bloom #-}

-- | Number of bits set
entries :: Bloom -> Int
entries (Bloom _ _ v) = U.foldl' (\r a -> r + popCount a) 0 v
{-# INLINE entries #-}

-- | Compute the union of two 'Bloom' filters.
union :: Bloom -> Bloom -> Bloom
union (Bloom k1 m v1) (Bloom k2 n v2) = Bloom (min k1 k2) (max m n) v3 where
  v3 = U.generate (U.length v1 `max` U.length v2) $ \i -> U.unsafeIndex v1 (i .&. m) .|. U.unsafeIndex v2 (i .&. n)
{-# INLINE union #-}

-- | Compute the intersection of two 'Bloom' filters.
intersection :: Bloom -> Bloom -> Bloom
intersection (Bloom k1 m v1) (Bloom k2 n v2) = Bloom (min k1 k2) (max m n) v3 where
  v3 = U.generate (U.length v1 `max` U.length v2) $ \i -> U.unsafeIndex v1 (i .&. m) .&. U.unsafeIndex v2 (i .&. n)
{-# INLINE intersection #-}

-- | Check if an element is a member of a 'Bloom' filter.
--
-- This may return false positives, but never a false negative.
elem :: Hashable a => a -> Bloom -> Bool
elem a (Bloom k m v)
  | m > 32767, h:hs <- rehash k a, p <- unsafeShiftL h 15 =
    all (\i -> let im = (p+(i.&.32767)).&.m in testBit (U.unsafeIndex v (unsafeShiftR im 6)) (i .&. 63)) hs
  | otherwise =
    all (\i -> let im = i.&.m in testBit (U.unsafeIndex v (unsafeShiftR im 6)) (im .&. 63)) (rehash k a)
{-# INLINE elem #-}

-- | Insert an element into a 'Bloom' filter.
insert :: Hashable a => a -> Bloom -> Bloom
insert a b = modify (MB.insert a) b
{-# INLINE insert #-}

-- | Given an action on a mutable 'Bloom' filter, modify this one.
modify :: (forall s. MBloom s -> ST s ()) -> Bloom -> Bloom
modify f (Bloom a m v) = Bloom a m (U.modify (f . MBloom a m) v)
{-# INLINE modify #-}

-- | The number of bits in our 'Bloom' filter. Always an integral multiple of 64.
width :: Bloom -> Int
width (Bloom _ m _) = m + 1
{-# INLINE width #-}

instance Semigroup Bloom where
  (<>) = union
  {-# INLINE (<>) #-}

-- | /O(m)/
freeze :: PrimMonad m => MBloom (PrimState m) -> m Bloom
freeze (MBloom k m bs) = Bloom k m `liftM` U.freeze bs
{-# INLINE freeze #-}

-- | /O(m)/
thaw :: PrimMonad m => Bloom -> m (MBloom (PrimState m))
thaw (Bloom k m bs) = MBloom k m `liftM` U.thaw bs
{-# INLINE thaw #-}

-- | /O(1)/
unsafeFreeze :: PrimMonad m => MBloom (PrimState m) -> m Bloom
unsafeFreeze (MBloom k m bs) = Bloom k m `liftM` U.unsafeFreeze bs
{-# INLINE unsafeFreeze #-}

-- | /O(1)/
unsafeThaw :: PrimMonad m => Bloom -> m (MBloom (PrimState m))
unsafeThaw (Bloom k m bs) = MBloom k m `liftM` U.unsafeThaw bs
{-# INLINE unsafeThaw #-}
