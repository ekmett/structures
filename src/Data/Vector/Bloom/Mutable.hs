{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE BangPatterns #-}
module Data.Vector.Bloom.Mutable
  (
  -- * Mutable Bloom filters
    MBloom(MBloom)
  , hashes
  , width
  , insert
  ) where

import Control.Monad.Primitive
import Data.Bits
import Data.Typeable
import Data.Foldable (forM_)
import Data.Hashable
import Data.Vector.Bloom.Util
import qualified Data.Vector.Unboxed.Mutable as UM
import Data.Word

data MBloom s = MBloom
  { hashes :: {-# UNPACK #-} !Int
  , _mask  :: {-# UNPACK #-} !Int
  , _bits  :: !(UM.MVector s Word64)
  } deriving Typeable

width :: MBloom s -> Int
width (MBloom _ m _) = m + 1
{-# INLINE width #-}

insert :: (PrimMonad m, Hashable a) => a -> MBloom (PrimState m) -> m ()
insert a (MBloom k m bs)
  | m > 511, h:hs <- rehash k a, p <- unsafeShiftL h 16 .&. unsafeShiftR m 6 = forM_ hs $ \i -> do
    let !iw = p + (unsafeShiftR i 6 .&. 511)
    w <- UM.unsafeRead bs iw
    UM.unsafeWrite bs iw $ setBit w (i .&. 63)
  | otherwise = forM_ (rehash k a) $ \ i -> do
    let !iw = mod (unsafeShiftR i 6) m
    w <- UM.unsafeRead bs iw
    UM.unsafeWrite bs iw $ setBit w (i .&. 63)
{-# INLINE insert #-}
