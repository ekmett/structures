{-# LANGUAGE CPP #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
module Data.Vector.Array
  ( Arrayed(..)
  , Array
  , MArray
  -- * Internals
  , V_Complex(V_Complex)
  , MV_Complex(MV_Complex)
  , V_Pair(V_Pair)
  , MV_Pair(MV_Pair)
  ) where

import Control.Monad
import Data.Complex
import Data.Int
import Data.Monoid
import qualified Data.Vector.Generic as G
import qualified Data.Vector.Generic.Mutable as GM
import qualified Data.Vector.Unboxed as U
import qualified Data.Vector.Fusion.Stream as Stream
import qualified Data.Vector as B
import Data.Word
import Text.Read

-- | A vector of product-like data types that know how to store themselves in a Vector optimally,
-- maximizing the level of unboxing provided, but not guaranteeing to unbox it all.

type Array a = Arr a a
type MArray s a = G.Mutable (Arr a) s a

class (G.Vector (Arr a) a, Monoid (Arr a a)) => Arrayed a where
  type Arr a :: * -> *
  type Arr a = U.Vector

-- * Unboxed vectors

instance Arrayed ()
instance Arrayed Double
instance Arrayed Float
instance Arrayed Int
instance Arrayed Int8
instance Arrayed Int16
instance Arrayed Int32
instance Arrayed Int64
instance Arrayed Word
instance Arrayed Word8
instance Arrayed Word16
instance Arrayed Word32
instance Arrayed Word64

-- * Boxed vectors

instance Arrayed Integer where
  type Arr Integer = B.Vector

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
