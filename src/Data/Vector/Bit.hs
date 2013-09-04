{-# LANGUAGE CPP #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE DeriveDataTypeable #-}
module Data.Vector.Bit
  (
  -- * Bits
    Bit(..)
  , _Bit
  -- * Bit Vectors with rank
  , BitVector(..)
  , _BitVector
  , rank
  , null
  , size
  , singleton
  , empty
  -- * Vectors of Bits
  , UM.MVector(MV_Bit)
  , U.Vector(V_Bit)
  ) where

import Control.Lens as L
import Control.Monad
import Data.Bits
import Data.Data
import Data.Vector.Array
import Data.Vector.Internal.Check as Ck
import Data.Word
import qualified Data.Vector.Generic as G
import qualified Data.Vector.Generic.Mutable as GM
import qualified Data.Vector.Unboxed as U
import qualified Data.Vector.Unboxed.Mutable as UM
import Prelude hiding (null)

#define BOUNDS_CHECK(f) (Ck.f __FILE__ __LINE__ Ck.Bounds)

-- | A simple newtype around a 'Bool'
--
-- The principal use of this is that a 'U.Vector' 'Bit' is densely
-- packed into individual bits rather than stored as one entry per 'Word8'.
newtype Bit = Bit { getBit :: Bool }
  deriving (Show,Read,Eq,Ord,Enum,Bounded,Data,Typeable)

-- | 'Bit' and 'Bool' are isomorphic.
_Bit :: Iso' Bit Bool
_Bit = iso getBit Bit
{-# INLINE _Bit #-}

instance Arrayed Bit
instance UM.Unbox Bit

data instance UM.MVector s Bit = MV_Bit {-# UNPACK #-} !Int !(UM.MVector s Word64)

instance GM.MVector U.MVector Bit where
  {-# INLINE basicLength #-}
  {-# INLINE basicUnsafeSlice #-}
  {-# INLINE basicOverlaps #-}
  {-# INLINE basicUnsafeNew #-}
  {-# INLINE basicUnsafeReplicate #-}
  {-# INLINE basicUnsafeRead #-}
  {-# INLINE basicUnsafeWrite #-}
  {-# INLINE basicClear #-}
  {-# INLINE basicSet #-}
  {-# INLINE basicUnsafeCopy #-}
  {-# INLINE basicUnsafeGrow #-}
  basicLength (MV_Bit n _) = n
  basicUnsafeSlice i n (MV_Bit _ u) = MV_Bit n $ GM.basicUnsafeSlice i (wds n) u
  basicOverlaps (MV_Bit _ v1) (MV_Bit _ v2) = GM.basicOverlaps v1 v2
  basicUnsafeNew n = do
    v <- GM.basicUnsafeNew (wds n)
    return $ MV_Bit n v
  basicUnsafeReplicate n (Bit b) = do
    v <- GM.basicUnsafeReplicate (wds n) (if b then -1 else 0)
    return $ MV_Bit n v
  basicUnsafeRead (MV_Bit _ u) i = do
    w <- GM.basicUnsafeRead u (wd i)
    return $ Bit $ testBit w (bt i)
  basicUnsafeWrite (MV_Bit _ u) i (Bit b) = do
    let wn = wd i
    w <- GM.basicUnsafeRead u wn
    GM.basicUnsafeWrite u wn $ if b then setBit w (bt i) else clearBit w (bt i)
  basicClear (MV_Bit _ u) = GM.basicClear u
  basicSet (MV_Bit _ u) (Bit b) = GM.basicSet u $ if b then -1 else 0
  basicUnsafeCopy (MV_Bit _ u1) (MV_Bit _ u2) = GM.basicUnsafeCopy u1 u2
  basicUnsafeMove (MV_Bit _ u1) (MV_Bit _ u2) = GM.basicUnsafeMove u1 u2
  basicUnsafeGrow (MV_Bit _ u) n = liftM (MV_Bit n) (GM.basicUnsafeGrow u (wds n))

data instance U.Vector Bit = V_Bit {-# UNPACK #-} !Int !(U.Vector Word64)
instance G.Vector U.Vector Bit where
  {-# INLINE basicLength #-}
  {-# INLINE basicUnsafeFreeze #-}
  {-# INLINE basicUnsafeThaw #-}
  {-# INLINE basicUnsafeSlice #-}
  {-# INLINE basicUnsafeIndexM #-}
  {-# INLINE elemseq #-}
  basicLength (V_Bit n _)          = n
  basicUnsafeFreeze (MV_Bit n u)   = liftM (V_Bit n) (G.basicUnsafeFreeze u)
  basicUnsafeThaw (V_Bit n u)      = liftM (MV_Bit n) (G.basicUnsafeThaw u)
  basicUnsafeSlice i n (V_Bit _ u) = V_Bit n (G.basicUnsafeSlice i (wds n) u)
  basicUnsafeIndexM (V_Bit _ u) i  = do
    w <- G.basicUnsafeIndexM u (wd i)
    return $ Bit $ testBit w (bt i)
  basicUnsafeCopy (MV_Bit _ mu) (V_Bit _ u) = G.basicUnsafeCopy mu u
  elemseq _ b z = b `seq` z

#define BOUNDS_CHECK(f) (Ck.f __FILE__ __LINE__ Ck.Bounds)

-- | A BitVector support for naïve /O(1)/ 'rank'.
data BitVector = BitVector {-# UNPACK #-} !Int !(Array Bit) !(U.Vector Int)
  deriving (Eq,Ord,Show,Read)

-- | /O(n) embedding/ A 'BitVector' is isomorphic to a vector of bits. It just carries extra information.
_BitVector :: Iso' BitVector (Array Bit)
_BitVector = iso (\(BitVector _ v _) -> v) $ \v@(V_Bit n ws) -> BitVector n v $ G.scanl (\a b -> a + popCount b) 0 ws
{-# INLINE _BitVector #-}

-- | /O(1)/. @'rank' i v@ counts the number of 'True' bits up through and including the position @i@
rank :: BitVector -> Int -> Int
rank (BitVector n (V_Bit _ ws) ps) i
  = BOUNDS_CHECK(checkIndex) "rank" i n
  $ (ps U.! w) + popCount ((ws U.! w) .&. (bit (bt i + 1) - 1))
  where w = wd i
{-# INLINE rank #-}

-- | The 'empty' 'BitVector'
empty :: BitVector
empty = _BitVector # G.empty
{-# INLINE empty #-}

-- | /O(1)/. Is the 'BitVector' 'empty'?
null :: BitVector -> Bool
null (BitVector n _ _) = n == 0
{-# INLINE null #-}

-- | /O(1)/. Return the size of the 'BitVector'.
size :: BitVector -> Int
size (BitVector n _ _) = n
{-# INLINE size #-}

type instance Index BitVector = Int

instance (Functor f, Contravariant f) => Contains f BitVector where
  contains i f (BitVector n as _) = coerce $ L.indexed f i (0 <= i && i < n && getBit (as U.! i))

-- | Construct a 'BitVector' with a single element.
singleton :: Bool -> BitVector
singleton True = true1
singleton False = false1
{-# INLINE singleton #-}

-- * Implementation Details

true1 :: BitVector
true1 = _BitVector # U.singleton (Bit True)

false1 :: BitVector
false1 = _BitVector # U.singleton (Bit False)

wds :: Int -> Int
wds x = unsafeShiftR (x + 63) 6
{-# INLINE wds #-}

wd :: Int -> Int
wd x = unsafeShiftR x 6
{-# INLINE wd #-}

bt :: Int -> Int
bt x = x .&. 63
{-# INLINE bt #-}

