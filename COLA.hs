{-# LANGUAGE CPP #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE GADTs #-}
module COLA
  ( Bit(..)
  -- * Bit vectors
  , UM.MVector(MV_Bit)
  , U.Vector(V_Bit)
  -- * compact indexed dictionaries
  , BitVector(..)
  , bitVector
  , rank
  ) where

import Control.Monad
import Data.Bits
import Data.Vector.Generic as G
import Data.Vector.Generic.Mutable as GM
import Data.Vector.Unboxed as U
import Data.Vector.Unboxed.Mutable as UM
import Data.Word
import Data.Vector.Internal.Check as Ck
import Sparse.Matrix.Internal.Array

#define BOUNDS_CHECK(f) (Ck.f __FILE__ __LINE__ Ck.Bounds)

newtype Bit = Bit { getBit :: Bool } deriving (Show,Read,Eq,Ord)
instance Arrayed Bit
instance Unbox Bit

wds :: Int -> Int
wds x = unsafeShiftR (x + 63) 6

wd :: Int -> Int
wd x = unsafeShiftR x 6

bt :: Int -> Int
bt x = x .&. 63

#ifndef HLINT
data instance UM.MVector s Bit = MV_Bit {-# UNPACK #-} !Int !(UM.MVector s Word64)
#endif

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

-- TODO: Eq, Ord, Show, Read, Monoid

-- a compact indexed dictionary representation with broadword rank in @2n@ bits space
data BitVector = BitVector {-# UNPACK #-} !Int !(Array Bit) !(U.Vector Int)
  deriving (Eq,Ord,Show,Read)

bitVector :: Array Bit -> BitVector
bitVector v@(V_Bit n ws) = BitVector n v $ G.scanl (\a b -> a + popCount b) 0 ws

-- | @'rank' i v@ counts the number of 'True' bits up through and including the position @i@
rank :: Int -> BitVector -> Int
rank i (BitVector n bv@(V_Bit _ ws) ps)
  = BOUNDS_CHECK(checkIndex) "rank" i n
  $ (ps U.! w) + popCount ((ws U.! w) .&. (bit (bt i + 1) - 1))
  where w = wd i

-- | This data structure is an insert-only Cache Oblivious Lookahead Array (COLA) with amortized complexity bounds
-- that are equal to those of a B-Tree when it is used ephemerally, but which are cache oblivious.
--
-- No redundant binary counter is used. This means it is insufficient to fully deamortize the complexity bounds,
-- but the deamortization approach isn't functional and requires mutation and a leads to a much clunkier API in
-- Haskell
data Map k v = Map !(Array k) {-# UNPACK #-} !BitVector !(Array v) !(Map k v) | Nil

{-
lookup :: (Arrayed k, Arrayed v) => k -> Map k v -> Maybe v
lookup m0@(Map ks0 _ _ _) = go 0 (G.length ks0) m0 where
  go lo hi (Map ks fwd vs m) =
    j = search (\i -> ks ! i >= k) lo hi
-}

{-
-- generate a thinned set of forwarding pointers from an array.
pointers :: Arrayed v => Array v -> Array v
pointers vs = G.backpermute vs (G.generate (unsafeShiftR (G.length vs) 3) (`unsafeShiftL` 3))

insert :: (Arrayed v, Arrayed a) => v -> a -> Map v a -> Map v a
insert 

cons :: (Arrayed v, Arrayed a) => Array v -> U.Vector Bit -> U.Vector Word -> Array a -> Map v a -> Map v a
-}
