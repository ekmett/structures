{-# LANGUAGE DeriveDataTypeable #-}
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
  , _bits  :: !(UM.MVector s Word64)
  } deriving Typeable

width :: MBloom s -> Int
width (MBloom _ v) = unsafeShiftL (UM.length v) 6 where
{-# INLINE width #-}

insert :: (PrimMonad m, Hashable a) => a -> MBloom (PrimState m) -> m ()
insert a (MBloom h bs) = do
  let !m = UM.length bs
  forM_ (rehash h a) $ \ i -> do
    let !iw = mod (unsafeShiftR i 6) m
    w <- UM.unsafeRead bs iw
    UM.unsafeWrite bs iw $ setBit w (i .&. 64)
{-# INLINE insert #-}
