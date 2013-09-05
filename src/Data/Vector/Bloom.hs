{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE RankNTypes #-}
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

data Bloom = Bloom
  { hashes  :: {-# UNPACK #-} !Int -- number of hash functions to use
  , _bits    :: !(U.Vector Word64)  -- data
  } deriving (Eq,Ord,Show,Read,Typeable,Data)

-- | @'bloom' k m@ builds an @m@-bit wide 'Bloom' filter that uses @k@ hashes.
bloom :: (F.Foldable f, Hashable a) => Int -> Int -> f a -> Bloom
bloom k m fa = runST $ do
  v <- UM.replicate (unsafeShiftR (m + 63) 6) 0
  let mb = MB.MBloom k v
  F.forM_ fa $ \a -> MB.insert a mb
  freeze mb
{-# INLINE bloom #-}

-- | Number of bits set
entries :: Bloom -> Int
entries (Bloom _ v) = U.foldl' (\r a -> r + popCount a) 0 v
{-# INLINE entries #-}

-- | Compute the union of two 'Bloom' filters.
union :: Bloom -> Bloom -> Bloom
union (Bloom k1 v1) (Bloom k2 v2) = Bloom (min k1 k2) v3 where
  m1 = U.length v1
  m2 = U.length v2
  v3 = U.generate (lcm m1 m2) $ \i -> U.unsafeIndex v1 (mod i m1) .|. U.unsafeIndex v2 (mod i m2)
{-# INLINE union #-}

-- | Compute the intersection of two 'Bloom' filters.
intersection :: Bloom -> Bloom -> Bloom
intersection (Bloom k1 v1) (Bloom k2 v2) = Bloom (min k1 k2) v3 where
  m1 = U.length v1
  m2 = U.length v2
  v3 = U.generate (lcm m1 m2) $ \i -> U.unsafeIndex v1 (mod i m1) .&. U.unsafeIndex v2 (mod i m2)
{-# INLINE intersection #-}

-- | Check if an element is a member of a 'Bloom' filter.
--
-- This may return false positives, but never a false negative.
elem :: Hashable a => a -> Bloom -> Bool
elem a (Bloom h v) = all hit (rehash h a) where
  !m = U.length v
  hit i = testBit (U.unsafeIndex v (mod (unsafeShiftR i 6) m)) (i .&. 63)
{-# INLINE elem #-}

-- | Insert an element into a 'Bloom' filter.
insert :: Hashable a => a -> Bloom -> Bloom
insert a b = modify (MB.insert a) b
{-# INLINE insert #-}

-- | Given an action on a mutable 'Bloom' filter, modify this one.
modify :: (forall s. MBloom s -> ST s ()) -> Bloom -> Bloom
modify f (Bloom a v) = Bloom a (U.modify (f . MBloom a) v)
{-# INLINE modify #-}

-- | The number of bits in our 'Bloom' filter. Always an integral multiple of 64.
width :: Bloom -> Int
width (Bloom _ w) = unsafeShiftL (U.length w) 6
{-# INLINE width #-}

instance Semigroup Bloom where
  (<>) = union
  {-# INLINE (<>) #-}

-- | /O(m)/
freeze :: PrimMonad m => MBloom (PrimState m) -> m Bloom
freeze (MBloom k bs) = Bloom k `liftM` U.freeze bs
{-# INLINE freeze #-}

-- | /O(m)/
thaw :: PrimMonad m => Bloom -> m (MBloom (PrimState m))
thaw (Bloom k bs) = MBloom k `liftM` U.thaw bs
{-# INLINE thaw #-}

-- | /O(1)/
unsafeFreeze :: PrimMonad m => MBloom (PrimState m) -> m Bloom
unsafeFreeze (MBloom k bs) = Bloom k `liftM` U.unsafeFreeze bs
{-# INLINE unsafeFreeze #-}

-- | /O(1)/
unsafeThaw :: PrimMonad m => Bloom -> m (MBloom (PrimState m))
unsafeThaw (Bloom k bs) = MBloom k `liftM` U.unsafeThaw bs
{-# INLINE unsafeThaw #-}
