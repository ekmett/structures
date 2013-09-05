{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE RankNTypes #-}
module Data.Vector.Bloom
  ( Bloom(Bloom)
  , width
  , bloom
  , elem
  , modify
  , insert
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
  { _hashes  :: {-# UNPACK #-} !Int -- number of hash functions to use
  , _bits    :: !(U.Vector Word64)  -- data
  } deriving (Eq,Ord,Show,Read,Typeable,Data)

bloom :: (F.Foldable f, Hashable a) => Int -> Int -> f a -> Bloom
bloom k m fa = runST $ do
  v <- UM.replicate (unsafeShiftR (m + 63) 6) 0
  let mb = MB.MBloom k v
  F.forM_ fa $ \a -> MB.insert a mb
  freeze mb
{-# INLINE bloom #-}

elem :: Hashable a => a -> Bloom -> Bool
elem a (Bloom h v) = all hit (hashes h a) where
  !m = U.length v
  hit i = testBit (U.unsafeIndex v (mod (unsafeShiftR i 6) m)) (i .&. 63)
{-# INLINE elem #-}

insert :: Hashable a => a -> Bloom -> Bloom
insert a b = modify (MB.insert a) b
{-# INLINE insert #-}

modify :: (forall s. MBloom s -> ST s ()) -> Bloom -> Bloom
modify f (Bloom a v) = Bloom a (U.modify (f . MBloom a) v)
{-# INLINE modify #-}

width :: Bloom -> Int
width (Bloom _ w) = unsafeShiftL (U.length w) 6
{-# INLINE width #-}

instance Semigroup Bloom where
  Bloom k1 v1 <> Bloom k2 v2 =  Bloom (min k1 k2) v3
    where m1 = U.length v1
          m2 = U.length v2
          v3 = U.generate (lcm m1 m2) $ \i -> U.unsafeIndex v1 (mod i m1) .|. U.unsafeIndex v2 (mod i m2)
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
