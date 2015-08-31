{-# LANGUAGE CPP #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
-----------------------------------------------------------------------------
-- |
-- Copyright   :  (C) 2013-2015 Edward Kmett
-- License     :  BSD-style (see the file LICENSE)
-- Maintainer  :  Edward Kmett <ekmett@gmail.com>
-- Stability   :  experimental
-- Portability :  non-portable
--
-- This module provides a choice of a "best" vector type for a given type
-- that is as unboxed as possible.
-----------------------------------------------------------------------------
module Data.Vector.Array

  ( Arrayed(..)
  , Array
  , MArray
  -- * Internals
  , V_Complex(V_Complex)
  , MV_Complex(MV_Complex)
  , V_Pair(V_Pair)
  , MV_Pair(MV_Pair)
  , prefetchArray0#, prefetchArray1#, prefetchArray2#, prefetchArray3#
  , prefetchSmallArray0#, prefetchSmallArray1#, prefetchSmallArray2#, prefetchSmallArray3#
  , prefetchMutableArray0#, prefetchMutableArray1#, prefetchMutableArray2#, prefetchMutableArray3#
  , prefetchSmallMutableArray0#, prefetchSmallMutableArray1#, prefetchSmallMutableArray2#, prefetchSmallMutableArray3#
  , prefetchPrim0#, prefetchPrim1#, prefetchPrim2#, prefetchPrim3#
  , prefetchMutablePrim0#, prefetchMutablePrim1#, prefetchMutablePrim2#, prefetchMutablePrim3#
  ) where

import Control.Monad
import Data.Complex
import Data.Int
import Data.Primitive hiding (Array)
import qualified Data.Primitive as Primitive
import qualified Data.Vector.Generic as G
import qualified Data.Vector.Generic.Mutable as GM
import qualified Data.Vector.Primitive as P
import qualified Data.Vector.Primitive.Mutable as PM
import qualified Data.Vector.Unboxed as U
import qualified Data.Vector.Unboxed.Base as UB
import qualified Data.Vector.Fusion.Stream as Stream
import qualified Data.Vector as B
import qualified Data.Vector.Mutable as BM
import Data.Word
import GHC.Prim
import GHC.Types
import Text.Read

#include "ghcautoconf.h"
#include "DerivedConstants.h"

#if SIZEOF_VOID_P == 4
#define W 4#
#elif SIZEOF_VOID_P == 8
#define W 8#
#else
#error unknown pointer size
#endif

#if OFFSET_StgMutArrPtrs_size != SIZEOF_VOID_P
#error did the card marking machinery change?
#endif


-- | A vector of product-like data types that know how to store themselves in a Vector optimally,
-- maximizing the level of unboxing provided, but not guaranteeing to unbox it all.

type Array a = Arr a a
type MArray s a = G.Mutable (Arr a) s a

prefetchArray0#, prefetchArray1#, prefetchArray2#, prefetchArray3# :: Array# a -> Int# -> State# d -> State# d
prefetchArray0# a i s = unsafeCoerce# prefetchByteArray0# a ((i *# W) +# W) s
prefetchArray1# a i s = unsafeCoerce# prefetchByteArray1# a ((i *# W) +# W) s
prefetchArray2# a i s = unsafeCoerce# prefetchByteArray2# a ((i *# W) +# W) s
prefetchArray3# a i s = unsafeCoerce# prefetchByteArray3# a ((i *# W) +# W) s

prefetchMutableArray0#, prefetchMutableArray1#, prefetchMutableArray2#, prefetchMutableArray3# :: MutableArray# d a -> Int# -> State# d -> State# d
prefetchMutableArray0# a i s = unsafeCoerce# prefetchMutableByteArray0# a ((i *# W) +# W) s
prefetchMutableArray1# a i s = unsafeCoerce# prefetchMutableByteArray1# a ((i *# W) +# W) s
prefetchMutableArray2# a i s = unsafeCoerce# prefetchMutableByteArray2# a ((i *# W) +# W) s
prefetchMutableArray3# a i s = unsafeCoerce# prefetchMutableByteArray3# a ((i *# W) +# W) s

prefetchSmallArray0#, prefetchSmallArray1#, prefetchSmallArray2#, prefetchSmallArray3# :: SmallArray# a -> Int# -> State# d -> State# d
prefetchSmallArray0# a i s = unsafeCoerce# prefetchByteArray0# a (i *# W) s
prefetchSmallArray1# a i s = unsafeCoerce# prefetchByteArray1# a (i *# W) s
prefetchSmallArray2# a i s = unsafeCoerce# prefetchByteArray2# a (i *# W) s
prefetchSmallArray3# a i s = unsafeCoerce# prefetchByteArray3# a (i *# W) s

prefetchSmallMutableArray0#, prefetchSmallMutableArray1#, prefetchSmallMutableArray2#, prefetchSmallMutableArray3# :: SmallMutableArray# d a -> Int# -> State# d -> State# d
prefetchSmallMutableArray0# a i s = unsafeCoerce# prefetchMutableByteArray0# a (i *# W) s
prefetchSmallMutableArray1# a i s = unsafeCoerce# prefetchMutableByteArray1# a (i *# W) s
prefetchSmallMutableArray2# a i s = unsafeCoerce# prefetchMutableByteArray2# a (i *# W) s
prefetchSmallMutableArray3# a i s = unsafeCoerce# prefetchMutableByteArray3# a (i *# W) s

class (G.Vector (Arr a) a, Monoid (Arr a a)) => Arrayed a where
  type Arr a :: * -> *
  type Arr a = B.Vector

  prefetchArr0# :: Arr a a -> Int# -> State# s -> State# s
  default prefetchArr0# :: (Arr a ~ B.Vector) => Arr a a -> Int# -> State# s -> State# s
  prefetchArr0# (coerceVector -> BVector (I# o) _ (Primitive.Array arr)) i s = prefetchArray0# arr (o +# i) s

  prefetchArr1# :: Arr a a -> Int# -> State# s -> State# s
  default prefetchArr1# :: (Arr a ~ B.Vector) => Arr a a -> Int# -> State# s -> State# s
  prefetchArr1# (coerceVector -> BVector (I# o) _ (Primitive.Array arr)) i s = prefetchArray1# arr (o +# i) s

  prefetchArr2# :: Arr a a -> Int# -> State# s -> State# s
  default prefetchArr2# :: (Arr a ~ B.Vector) => Arr a a -> Int# -> State# s -> State# s
  prefetchArr2# (coerceVector -> BVector (I# o) _ (Primitive.Array arr)) i s = prefetchArray2# arr (o +# i) s

  prefetchArr3# :: Arr a a -> Int# -> State# s -> State# s
  default prefetchArr3# :: (Arr a ~ B.Vector) => Arr a a -> Int# -> State# s -> State# s
  prefetchArr3# (coerceVector -> BVector (I# o) _ (Primitive.Array arr)) i s = prefetchArray3# arr (o +# i) s

  prefetchMutableArr0# :: G.Mutable (Arr a) s a -> Int# -> State# s -> State# s
  default prefetchMutableArr0# :: (G.Mutable (Arr a) ~ BM.MVector) => G.Mutable (Arr a) s a -> Int# -> State# s -> State# s
  prefetchMutableArr0# (BM.MVector (I# o) _ (MutableArray arr)) i s = prefetchMutableArray0# arr (o +# i) s

  prefetchMutableArr1# :: G.Mutable (Arr a) s a -> Int# -> State# s -> State# s
  default prefetchMutableArr1# :: (G.Mutable (Arr a) ~ BM.MVector) => G.Mutable (Arr a) s a -> Int# -> State# s -> State# s
  prefetchMutableArr1# (BM.MVector (I# o) _ (MutableArray arr)) i s = prefetchMutableArray1# arr (o +# i) s

  prefetchMutableArr2# :: G.Mutable (Arr a) s a -> Int# -> State# s -> State# s
  default prefetchMutableArr2# :: (G.Mutable (Arr a) ~ BM.MVector) => G.Mutable (Arr a) s a -> Int# -> State# s -> State# s
  prefetchMutableArr2# (BM.MVector (I# o) _ (MutableArray arr)) i s = prefetchMutableArray2# arr (o +# i) s

  prefetchMutableArr3# :: G.Mutable (Arr a) s a -> Int# -> State# s -> State# s
  default prefetchMutableArr3# :: (G.Mutable (Arr a) ~ BM.MVector) => G.Mutable (Arr a) s a -> Int# -> State# s -> State# s
  prefetchMutableArr3# (BM.MVector (I# o) _ (MutableArray arr)) i s = prefetchMutableArray3# arr (o +# i) s

prefetchPrim0#, prefetchPrim1#, prefetchPrim2#, prefetchPrim3# :: forall s a. Prim a => P.Vector a -> Int# -> State# s -> State# s
prefetchPrim0# (P.Vector (I# o) _ (ByteArray arr)) i s = prefetchByteArray0# arr ((o +# i) *# sizeOf# (undefined :: a)) s
prefetchPrim1# (P.Vector (I# o) _ (ByteArray arr)) i s = prefetchByteArray1# arr ((o +# i) *# sizeOf# (undefined :: a)) s
prefetchPrim2# (P.Vector (I# o) _ (ByteArray arr)) i s = prefetchByteArray2# arr ((o +# i) *# sizeOf# (undefined :: a)) s
prefetchPrim3# (P.Vector (I# o) _ (ByteArray arr)) i s = prefetchByteArray3# arr ((o +# i) *# sizeOf# (undefined :: a)) s

prefetchMutablePrim0#, prefetchMutablePrim1#, prefetchMutablePrim2#, prefetchMutablePrim3# :: forall s a. Prim a => PM.MVector s a -> Int# -> State# s -> State# s
prefetchMutablePrim0# (PM.MVector (I# o) _ (MutableByteArray arr)) i s = prefetchMutableByteArray0# arr ((o +# i) *# sizeOf# (undefined :: a)) s
prefetchMutablePrim1# (PM.MVector (I# o) _ (MutableByteArray arr)) i s = prefetchMutableByteArray1# arr ((o +# i) *# sizeOf# (undefined :: a)) s
prefetchMutablePrim2# (PM.MVector (I# o) _ (MutableByteArray arr)) i s = prefetchMutableByteArray2# arr ((o +# i) *# sizeOf# (undefined :: a)) s
prefetchMutablePrim3# (PM.MVector (I# o) _ (MutableByteArray arr)) i s = prefetchMutableByteArray3# arr ((o +# i) *# sizeOf# (undefined :: a)) s

-- * Unboxed vectors

instance Arrayed () where
  type Arr () = U.Vector
  prefetchArr0# _ _ s = s
  prefetchArr1# _ _ s = s
  prefetchArr2# _ _ s = s
  prefetchArr3# _ _ s = s
  prefetchMutableArr0# _ _ s = s
  prefetchMutableArr1# _ _ s = s
  prefetchMutableArr2# _ _ s = s
  prefetchMutableArr3# _ _ s = s

instance Arrayed Double where
  type Arr Double = U.Vector
  prefetchArr0# (UB.V_Double v) i s = prefetchPrim0# v i s
  prefetchArr1# (UB.V_Double v) i s = prefetchPrim1# v i s
  prefetchArr2# (UB.V_Double v) i s = prefetchPrim2# v i s
  prefetchArr3# (UB.V_Double v) i s = prefetchPrim3# v i s
  prefetchMutableArr0# (UB.MV_Double v) i s = prefetchMutablePrim0# v i s
  prefetchMutableArr1# (UB.MV_Double v) i s = prefetchMutablePrim1# v i s
  prefetchMutableArr2# (UB.MV_Double v) i s = prefetchMutablePrim2# v i s
  prefetchMutableArr3# (UB.MV_Double v) i s = prefetchMutablePrim3# v i s

instance Arrayed Float where
  type Arr Float = U.Vector
  prefetchArr0# (UB.V_Float v) i s = prefetchPrim0# v i s
  prefetchArr1# (UB.V_Float v) i s = prefetchPrim1# v i s
  prefetchArr2# (UB.V_Float v) i s = prefetchPrim2# v i s
  prefetchArr3# (UB.V_Float v) i s = prefetchPrim3# v i s
  prefetchMutableArr0# (UB.MV_Float v) i s = prefetchMutablePrim0# v i s
  prefetchMutableArr1# (UB.MV_Float v) i s = prefetchMutablePrim1# v i s
  prefetchMutableArr2# (UB.MV_Float v) i s = prefetchMutablePrim2# v i s
  prefetchMutableArr3# (UB.MV_Float v) i s = prefetchMutablePrim3# v i s

instance Arrayed Int where
  type Arr Int = U.Vector
  prefetchArr0# (UB.V_Int v) i s = prefetchPrim0# v i s
  prefetchArr1# (UB.V_Int v) i s = prefetchPrim1# v i s
  prefetchArr2# (UB.V_Int v) i s = prefetchPrim2# v i s
  prefetchArr3# (UB.V_Int v) i s = prefetchPrim3# v i s
  prefetchMutableArr0# (UB.MV_Int v) i s = prefetchMutablePrim0# v i s
  prefetchMutableArr1# (UB.MV_Int v) i s = prefetchMutablePrim1# v i s
  prefetchMutableArr2# (UB.MV_Int v) i s = prefetchMutablePrim2# v i s
  prefetchMutableArr3# (UB.MV_Int v) i s = prefetchMutablePrim3# v i s

instance Arrayed Int8 where
  type Arr Int8 = U.Vector
  prefetchArr0# (UB.V_Int8 v) i s = prefetchPrim0# v i s
  prefetchArr1# (UB.V_Int8 v) i s = prefetchPrim1# v i s
  prefetchArr2# (UB.V_Int8 v) i s = prefetchPrim2# v i s
  prefetchArr3# (UB.V_Int8 v) i s = prefetchPrim3# v i s
  prefetchMutableArr0# (UB.MV_Int8 v) i s = prefetchMutablePrim0# v i s
  prefetchMutableArr1# (UB.MV_Int8 v) i s = prefetchMutablePrim1# v i s
  prefetchMutableArr2# (UB.MV_Int8 v) i s = prefetchMutablePrim2# v i s
  prefetchMutableArr3# (UB.MV_Int8 v) i s = prefetchMutablePrim3# v i s

instance Arrayed Int16 where
  type Arr Int16 = U.Vector
  prefetchArr0# (UB.V_Int16 v) i s = prefetchPrim0# v i s
  prefetchArr1# (UB.V_Int16 v) i s = prefetchPrim1# v i s
  prefetchArr2# (UB.V_Int16 v) i s = prefetchPrim2# v i s
  prefetchArr3# (UB.V_Int16 v) i s = prefetchPrim3# v i s
  prefetchMutableArr0# (UB.MV_Int16 v) i s = prefetchMutablePrim0# v i s
  prefetchMutableArr1# (UB.MV_Int16 v) i s = prefetchMutablePrim1# v i s
  prefetchMutableArr2# (UB.MV_Int16 v) i s = prefetchMutablePrim2# v i s
  prefetchMutableArr3# (UB.MV_Int16 v) i s = prefetchMutablePrim3# v i s

instance Arrayed Int32 where
  type Arr Int32 = U.Vector
  prefetchArr0# (UB.V_Int32 v) i s = prefetchPrim0# v i s
  prefetchArr1# (UB.V_Int32 v) i s = prefetchPrim1# v i s
  prefetchArr2# (UB.V_Int32 v) i s = prefetchPrim2# v i s
  prefetchArr3# (UB.V_Int32 v) i s = prefetchPrim3# v i s
  prefetchMutableArr0# (UB.MV_Int32 v) i s = prefetchMutablePrim0# v i s
  prefetchMutableArr1# (UB.MV_Int32 v) i s = prefetchMutablePrim1# v i s
  prefetchMutableArr2# (UB.MV_Int32 v) i s = prefetchMutablePrim2# v i s
  prefetchMutableArr3# (UB.MV_Int32 v) i s = prefetchMutablePrim3# v i s

instance Arrayed Int64 where
  type Arr Int64 = U.Vector
  prefetchArr0# (UB.V_Int64 v) i s = prefetchPrim0# v i s
  prefetchArr1# (UB.V_Int64 v) i s = prefetchPrim1# v i s
  prefetchArr2# (UB.V_Int64 v) i s = prefetchPrim2# v i s
  prefetchArr3# (UB.V_Int64 v) i s = prefetchPrim3# v i s
  prefetchMutableArr0# (UB.MV_Int64 v) i s = prefetchMutablePrim0# v i s
  prefetchMutableArr1# (UB.MV_Int64 v) i s = prefetchMutablePrim1# v i s
  prefetchMutableArr2# (UB.MV_Int64 v) i s = prefetchMutablePrim2# v i s
  prefetchMutableArr3# (UB.MV_Int64 v) i s = prefetchMutablePrim3# v i s

instance Arrayed Word where
  type Arr Word = U.Vector
  prefetchArr0# (UB.V_Word v) i s = prefetchPrim0# v i s
  prefetchArr1# (UB.V_Word v) i s = prefetchPrim1# v i s
  prefetchArr2# (UB.V_Word v) i s = prefetchPrim2# v i s
  prefetchArr3# (UB.V_Word v) i s = prefetchPrim3# v i s
  prefetchMutableArr0# (UB.MV_Word v) i s = prefetchMutablePrim0# v i s
  prefetchMutableArr1# (UB.MV_Word v) i s = prefetchMutablePrim1# v i s
  prefetchMutableArr2# (UB.MV_Word v) i s = prefetchMutablePrim2# v i s
  prefetchMutableArr3# (UB.MV_Word v) i s = prefetchMutablePrim3# v i s

instance Arrayed Word8 where
  type Arr Word8 = U.Vector
  prefetchArr0# (UB.V_Word8 v) i s = prefetchPrim0# v i s
  prefetchArr1# (UB.V_Word8 v) i s = prefetchPrim1# v i s
  prefetchArr2# (UB.V_Word8 v) i s = prefetchPrim2# v i s
  prefetchArr3# (UB.V_Word8 v) i s = prefetchPrim3# v i s
  prefetchMutableArr0# (UB.MV_Word8 v) i s = prefetchMutablePrim0# v i s
  prefetchMutableArr1# (UB.MV_Word8 v) i s = prefetchMutablePrim1# v i s
  prefetchMutableArr2# (UB.MV_Word8 v) i s = prefetchMutablePrim2# v i s
  prefetchMutableArr3# (UB.MV_Word8 v) i s = prefetchMutablePrim3# v i s

instance Arrayed Word16 where
  type Arr Word16 = U.Vector
  prefetchArr0# (UB.V_Word16 v) i s = prefetchPrim0# v i s
  prefetchArr1# (UB.V_Word16 v) i s = prefetchPrim1# v i s
  prefetchArr2# (UB.V_Word16 v) i s = prefetchPrim2# v i s
  prefetchArr3# (UB.V_Word16 v) i s = prefetchPrim3# v i s
  prefetchMutableArr0# (UB.MV_Word16 v) i s = prefetchMutablePrim0# v i s
  prefetchMutableArr1# (UB.MV_Word16 v) i s = prefetchMutablePrim1# v i s
  prefetchMutableArr2# (UB.MV_Word16 v) i s = prefetchMutablePrim2# v i s
  prefetchMutableArr3# (UB.MV_Word16 v) i s = prefetchMutablePrim3# v i s

instance Arrayed Word32 where
  type Arr Word32 = U.Vector
  prefetchArr0# (UB.V_Word32 v) i s = prefetchPrim0# v i s
  prefetchArr1# (UB.V_Word32 v) i s = prefetchPrim1# v i s
  prefetchArr2# (UB.V_Word32 v) i s = prefetchPrim2# v i s
  prefetchArr3# (UB.V_Word32 v) i s = prefetchPrim3# v i s
  prefetchMutableArr0# (UB.MV_Word32 v) i s = prefetchMutablePrim0# v i s
  prefetchMutableArr1# (UB.MV_Word32 v) i s = prefetchMutablePrim1# v i s
  prefetchMutableArr2# (UB.MV_Word32 v) i s = prefetchMutablePrim2# v i s
  prefetchMutableArr3# (UB.MV_Word32 v) i s = prefetchMutablePrim3# v i s

instance Arrayed Word64 where
  type Arr Word64 = U.Vector
  prefetchArr0# (UB.V_Word64 v) i s = prefetchPrim0# v i s
  prefetchArr1# (UB.V_Word64 v) i s = prefetchPrim1# v i s
  prefetchArr2# (UB.V_Word64 v) i s = prefetchPrim2# v i s
  prefetchArr3# (UB.V_Word64 v) i s = prefetchPrim3# v i s
  prefetchMutableArr0# (UB.MV_Word64 v) i s = prefetchMutablePrim0# v i s
  prefetchMutableArr1# (UB.MV_Word64 v) i s = prefetchMutablePrim1# v i s
  prefetchMutableArr2# (UB.MV_Word64 v) i s = prefetchMutablePrim2# v i s
  prefetchMutableArr3# (UB.MV_Word64 v) i s = prefetchMutablePrim3# v i s

-- * Boxed vectors

instance Arrayed Integer
instance Arrayed [a]
instance Arrayed (Maybe a)
instance Arrayed (Either a b)
instance Arrayed (IO a)

-- * Pairs are boxed or unboxed based on their components

#ifndef HLINT
data MV_Pair :: * -> * -> * where
  MV_Pair:: {-# UNPACK #-} !Int -> !(G.Mutable (Arr a) s a) -> !(G.Mutable (Arr b) s b) -> MV_Pair s (a, b)

data V_Pair :: * -> * where
  V_Pair :: {-# UNPACK #-} !Int -> !(Array a) -> !(Array b) -> V_Pair (a, b)
#endif

type instance G.Mutable V_Pair = MV_Pair

instance (Arrayed a, Arrayed b) => GM.MVector MV_Pair (a, b) where
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
  basicLength (MV_Pair l _ _) = l
  basicUnsafeSlice i n (MV_Pair _ u v)                   = MV_Pair n (GM.basicUnsafeSlice i n u) (GM.basicUnsafeSlice i n v)
  basicOverlaps (MV_Pair _ u1 v1) (MV_Pair _ u2 v2)      = GM.basicOverlaps u1 u2 || GM.basicOverlaps v1 v2
  basicUnsafeNew n                                       = liftM2 (MV_Pair n) (GM.basicUnsafeNew n) (GM.basicUnsafeNew n)
  basicUnsafeReplicate n (x, y)                          = liftM2 (MV_Pair n) (GM.basicUnsafeReplicate n x) (GM.basicUnsafeReplicate n y)
  basicUnsafeRead (MV_Pair _ u v) i                      = liftM2 (,) (GM.basicUnsafeRead u i) (GM.basicUnsafeRead v i)
  basicUnsafeWrite (MV_Pair _ u v) i (x, y)              = GM.basicUnsafeWrite u i x >> GM.basicUnsafeWrite v i y
  basicClear (MV_Pair _ u v)                             = GM.basicClear u >> GM.basicClear v
  basicSet (MV_Pair _ u v) (x, y)                        = GM.basicSet u x >> GM.basicSet v y
  basicUnsafeCopy (MV_Pair _ u1 v1) (MV_Pair _ u2 v2)    = GM.basicUnsafeCopy u1 u2 >> GM.basicUnsafeCopy v1 v2
  basicUnsafeMove (MV_Pair _ u1 v1) (MV_Pair _ u2 v2)    = GM.basicUnsafeMove u1 u2 >> GM.basicUnsafeMove v1 v2
  basicUnsafeGrow (MV_Pair _ u v) n                      = liftM2 (MV_Pair n) (GM.basicUnsafeGrow u n) (GM.basicUnsafeGrow v n)

instance (Arrayed a, Arrayed b) => G.Vector V_Pair (a, b) where
  {-# INLINE basicLength #-}
  {-# INLINE basicUnsafeFreeze #-}
  {-# INLINE basicUnsafeThaw #-}
  {-# INLINE basicUnsafeSlice #-}
  {-# INLINE basicUnsafeIndexM #-}
  {-# INLINE elemseq #-}
  basicLength (V_Pair v _ _) = v
  basicUnsafeFreeze (MV_Pair n u v)                = liftM2 (V_Pair n) (G.basicUnsafeFreeze u) (G.basicUnsafeFreeze v)
  basicUnsafeThaw (V_Pair n u v)                   = liftM2 (MV_Pair n) (G.basicUnsafeThaw u) (G.basicUnsafeThaw v)
  basicUnsafeSlice i n (V_Pair _ u v)              = V_Pair n (G.basicUnsafeSlice i n u) (G.basicUnsafeSlice i n v)
  basicUnsafeIndexM (V_Pair _ u v) i               = liftM2 (,) (G.basicUnsafeIndexM u i) (G.basicUnsafeIndexM v i)
  basicUnsafeCopy (MV_Pair _ mu mv) (V_Pair _ u v) = G.basicUnsafeCopy mu u >> G.basicUnsafeCopy mv v
  elemseq _ (x, y) z  = G.elemseq (undefined :: Array a) x
                       $ G.elemseq (undefined :: Array b) y z

instance (Arrayed a, Arrayed b, Show a, Show b, c ~ (a, b)) => Show (V_Pair c) where
  showsPrec = G.showsPrec

instance (Arrayed a, Arrayed b, Read a, Read b, c ~ (a, b)) => Read (V_Pair c) where
  readPrec = G.readPrec
  readListPrec = readListPrecDefault

instance (Arrayed a, Arrayed b, Eq a, Eq b, c ~ (a, b)) => Eq (V_Pair c) where
  xs == ys = Stream.eq (G.stream xs) (G.stream ys)
  {-# INLINE (==) #-}

instance (Arrayed a, Arrayed b, c ~ (a, b)) => Monoid (V_Pair c) where
  mappend = (G.++)
  {-# INLINE mappend #-}
  mempty = G.empty
  {-# INLINE mempty #-}
  mconcat = G.concat
  {-# INLINE mconcat #-}

instance (Arrayed a, Arrayed b) => Arrayed (a, b) where
  type Arr (a, b) = V_Pair
  prefetchArr0# (V_Pair _ u v) i s = prefetchArr0# v i (prefetchArr0# u i s)
  prefetchArr1# (V_Pair _ u v) i s = prefetchArr1# v i (prefetchArr1# u i s)
  prefetchArr2# (V_Pair _ u v) i s = prefetchArr2# v i (prefetchArr2# u i s)
  prefetchArr3# (V_Pair _ u v) i s = prefetchArr3# v i (prefetchArr3# u i s)
  prefetchMutableArr0# (MV_Pair _ u v) i s = prefetchMutableArr0# v i (prefetchMutableArr0# u i s)
  prefetchMutableArr1# (MV_Pair _ u v) i s = prefetchMutableArr1# v i (prefetchMutableArr1# u i s)
  prefetchMutableArr2# (MV_Pair _ u v) i s = prefetchMutableArr2# v i (prefetchMutableArr2# u i s)
  prefetchMutableArr3# (MV_Pair _ u v) i s = prefetchMutableArr3# v i (prefetchMutableArr3# u i s)

-- * Complex numbers are boxed or unboxed based on their components

#ifndef HLINT
data MV_Complex :: * -> * -> * where
  MV_Complex :: {-# UNPACK #-} !Int -> !(G.Mutable (Arr a) s a) -> !(G.Mutable (Arr a) s a) -> MV_Complex s (Complex a)

data V_Complex :: * -> * where
  V_Complex :: {-# UNPACK #-} !Int -> !(Array a) -> !(Array a) -> V_Complex (Complex a)
#endif

type instance G.Mutable V_Complex = MV_Complex

instance (Arrayed a, RealFloat a) => GM.MVector MV_Complex (Complex a) where
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
  basicLength (MV_Complex l _ _) = l
  basicUnsafeSlice i n (MV_Complex _ u v)                   = MV_Complex n (GM.basicUnsafeSlice i n u) (GM.basicUnsafeSlice i n v)
  basicOverlaps (MV_Complex _ u1 v1) (MV_Complex _ u2 v2)   = GM.basicOverlaps u1 u2 || GM.basicOverlaps v1 v2
  basicUnsafeNew n                                          = liftM2 (MV_Complex n) (GM.basicUnsafeNew n) (GM.basicUnsafeNew n)
  basicUnsafeReplicate n (x :+ y)                           = liftM2 (MV_Complex n) (GM.basicUnsafeReplicate n x) (GM.basicUnsafeReplicate n y)
  basicUnsafeRead (MV_Complex _ u v) i                      = liftM2 (:+) (GM.basicUnsafeRead u i) (GM.basicUnsafeRead v i)
  basicUnsafeWrite (MV_Complex _ u v) i (x :+ y)            = GM.basicUnsafeWrite u i x >> GM.basicUnsafeWrite v i y
  basicClear (MV_Complex _ u v)                             = GM.basicClear u >> GM.basicClear v
  basicSet (MV_Complex _ u v) (x :+ y)                      = GM.basicSet u x >> GM.basicSet v y
  basicUnsafeCopy (MV_Complex _ u1 v1) (MV_Complex _ u2 v2) = GM.basicUnsafeCopy u1 u2 >> GM.basicUnsafeCopy v1 v2
  basicUnsafeMove (MV_Complex _ u1 v1) (MV_Complex _ u2 v2) = GM.basicUnsafeMove u1 u2 >> GM.basicUnsafeMove v1 v2
  basicUnsafeGrow (MV_Complex _ u v) n                      = liftM2 (MV_Complex n) (GM.basicUnsafeGrow u n) (GM.basicUnsafeGrow v n)

instance (Arrayed a, RealFloat a) => G.Vector V_Complex (Complex a) where
  {-# INLINE basicLength #-}
  {-# INLINE basicUnsafeFreeze #-}
  {-# INLINE basicUnsafeThaw #-}
  {-# INLINE basicUnsafeSlice #-}
  {-# INLINE basicUnsafeIndexM #-}
  {-# INLINE elemseq #-}
  basicLength (V_Complex v _ _) = v
  basicUnsafeFreeze (MV_Complex n u v)                   = liftM2 (V_Complex n) (G.basicUnsafeFreeze u) (G.basicUnsafeFreeze v)
  basicUnsafeThaw (V_Complex n u v)                      = liftM2 (MV_Complex n) (G.basicUnsafeThaw u) (G.basicUnsafeThaw v)
  basicUnsafeSlice i n (V_Complex _ u v)                 = V_Complex n (G.basicUnsafeSlice i n u) (G.basicUnsafeSlice i n v)
  basicUnsafeIndexM (V_Complex _ u v) i                  = liftM2 (:+) (G.basicUnsafeIndexM u i) (G.basicUnsafeIndexM v i)
  basicUnsafeCopy (MV_Complex _ mu mv) (V_Complex _ u v) = G.basicUnsafeCopy mu u >> G.basicUnsafeCopy mv v
  elemseq _ (x :+ y) z = G.elemseq (undefined :: Arr a a) x
                       $ G.elemseq (undefined :: Arr a a) y z

instance (Arrayed a, RealFloat a, Show a, b ~ Complex a) => Show (V_Complex b) where
  showsPrec = G.showsPrec

instance (Arrayed a, RealFloat a, Read a, b ~ Complex a) => Read (V_Complex b) where
  readPrec = G.readPrec
  readListPrec = readListPrecDefault

instance (Arrayed a, RealFloat a, Eq a, b ~ Complex a) => Eq (V_Complex b) where
  xs == ys = Stream.eq (G.stream xs) (G.stream ys)
  {-# INLINE (==) #-}

instance (Arrayed a, RealFloat a, b ~ Complex a) => Monoid (V_Complex b) where
  mappend = (G.++)
  {-# INLINE mappend #-}
  mempty = G.empty
  {-# INLINE mempty #-}
  mconcat = G.concat
  {-# INLINE mconcat #-}

instance (Arrayed a, RealFloat a) => Arrayed (Complex a) where
  type Arr (Complex a) = V_Complex
  prefetchArr0# (V_Complex _ u v) i s = prefetchArr0# v i (prefetchArr0# u i s)
  prefetchArr1# (V_Complex _ u v) i s = prefetchArr1# v i (prefetchArr1# u i s)
  prefetchArr2# (V_Complex _ u v) i s = prefetchArr2# v i (prefetchArr2# u i s)
  prefetchArr3# (V_Complex _ u v) i s = prefetchArr3# v i (prefetchArr3# u i s)
  prefetchMutableArr0# (MV_Complex _ u v) i s = prefetchMutableArr0# v i (prefetchMutableArr0# u i s)
  prefetchMutableArr1# (MV_Complex _ u v) i s = prefetchMutableArr1# v i (prefetchMutableArr1# u i s)
  prefetchMutableArr2# (MV_Complex _ u v) i s = prefetchMutableArr2# v i (prefetchMutableArr2# u i s)
  prefetchMutableArr3# (MV_Complex _ u v) i s = prefetchMutableArr3# v i (prefetchMutableArr3# u i s)

-- LAME!
data BVector a
  = BVector {-# UNPACK #-}!Int
            {-# UNPACK #-}!Int
            {-# UNPACK #-}!(Primitive.Array a)

coerceVector :: B.Vector a -> BVector a
coerceVector = unsafeCoerce#
