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
  | m > 32767, h:hs <- rehash k a, p <- unsafeShiftL h 15 = forM_ hs $ \i -> do
    let !im = (p + (i.&.32767)) .&. m
    let !iw = unsafeShiftR im 6
    w <- UM.unsafeRead bs iw
    UM.unsafeWrite bs iw $ setBit w (im .&. 63)
  | otherwise = forM_ (rehash k a) $ \ i -> do
    let !im = i .&. m
    let !iw = unsafeShiftR im 6
    w <- UM.unsafeRead bs iw
    UM.unsafeWrite bs iw $ setBit w (im .&. 63)
{-# INLINE insert #-}
