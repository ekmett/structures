{-# LANGUAGE CPP #-}
{-# LANGUAGE RankNTypes  #-}
{-# LANGUAGE Trustworthy #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# OPTIONS_GHC -fno-warn-unused-matches -fno-warn-unused-binds #-}

module Data.Vector.Slow
  ( IterST
  , Iter
  , delay
  , walkST
  , munstream
  , unstreamM
  , foldM'
  , foldM
  ) where

import Control.Monad.ST
import Control.Monad.ST.Class
import Control.Monad.ST.Unsafe as Unsafe
import Control.Monad.Trans.Iter
import Data.Functor.Identity
import qualified Data.Vector.Fusion.Stream.Monadic as M
import qualified Data.Vector.Fusion.Stream.Size as SS
import Data.Vector.Internal.Check as Ck
import qualified Data.Vector.Generic as G
import qualified Data.Vector.Generic.Mutable as GM
import System.IO.Unsafe as Unsafe

import SpecConstr ( SpecConstrAnnotation(..) )
data SPEC = SPEC | SPEC2
{-# ANN type SPEC ForceSpecConstr #-}

#define BOUNDS_CHECK(f)   (Ck.f __FILE__ __LINE__ Ck.Bounds)
#define INTERNAL_CHECK(f) (Ck.f __FILE__ __LINE__ Ck.Internal)

type IterST s = IterT (ST s)
type Iter     = IterT Identity

walkST :: (forall s. IterST s a) -> Iter a
walkST m0 = go m0 where
  go (IterT m) =
    case Unsafe.unsafePerformIO $
         Unsafe.unsafeSTToIO m of
      Pure a -> return a
      Iter n -> delay (go n)

unstreamM :: G.Vector v a => M.Stream (ST s) a -> IterST s (v a)
unstreamM s = munstream s >>= liftST . G.unsafeFreeze

munstream :: GM.MVector v a => M.Stream (ST s) a -> IterST s (v s a)
munstream s = case SS.upperBound (M.size s) of
  Just n  -> munstreamMax     s n
  Nothing -> munstreamUnknown s
{-# INLINE [1] munstream #-}

-- pay once per entry
foldM' :: (a -> b -> ST s a) -> a -> M.Stream (ST s) b -> IterST s a
foldM' m z0 (M.Stream step s0 _) = foldM'_loop SPEC z0 s0
  where
    foldM'_loop !_SPEC z s
      = z `seq`
        do
          r <- liftST (step s)
          case r of
            M.Yield x s' -> do { z' <- liftST (m z x); delay $ foldM'_loop SPEC z' s' }
            M.Skip    s' -> foldM'_loop SPEC z s'
            M.Done       -> return z
{-# INLINE [1] foldM' #-}

-- | Left fold with a monadic operator
foldM :: (a -> b -> ST s a) -> a -> M.Stream (ST s) b -> IterST s a
foldM m z0 (M.Stream step s0 _) = foldM_loop SPEC z0 s0
  where
    foldM_loop !_SPEC z s
      = do
          r <- liftST (step s)
          case r of
            M.Yield x s' -> do { z' <- liftST (m z x); delay $ foldM_loop SPEC z' s' }
            M.Skip    s' -> foldM_loop SPEC z s'
            M.Done       -> return z
{-# INLINE [1] foldM #-}


munstreamMax :: GM.MVector v a => M.Stream (ST s) a -> Int -> IterST s (v s a)
munstreamMax s n = do
  v <- INTERNAL_CHECK(checkLength) "munstreamMax" n
       $ liftST (GM.unsafeNew n)
  let put i x = do
                   INTERNAL_CHECK(checkIndex) "munstreamMax" i n
                     $ GM.unsafeWrite v i x
                   return (i+1)
  n' <- foldM' put 0 s
  return $ INTERNAL_CHECK(checkSlice) "munstreamMax" 0 n' n
         $ GM.unsafeSlice 0 n' v
{-# INLINE munstreamMax #-}

munstreamUnknown :: GM.MVector v a => M.Stream (ST s) a -> IterST s (v s a)
munstreamUnknown s = do
  v <- liftST (GM.unsafeNew 0)
  (v', n) <- foldM put (v, 0) s
  return $ INTERNAL_CHECK(checkSlice) "munstreamUnknown" 0 n (GM.length v')
         $ GM.unsafeSlice 0 n v'
  where
    {-# INLINE [0] put #-}
    put (v,i) x = do
      v' <- unsafeAppend1 v i x
      return (v',i+1)
{-# INLINE munstreamUnknown #-}

unsafeAppend1 :: GM.MVector v a => v s a -> Int -> a -> ST s (v s a)
{-# INLINE [0] unsafeAppend1 #-}
unsafeAppend1 v i x
  | i < GM.length v = do
    GM.unsafeWrite v i x
    return v
  | otherwise    = do
    v' <- enlarge v
    INTERNAL_CHECK(checkIndex) "unsafeAppend1" i (GM.length v')
      $ GM.unsafeWrite v' i x
    return v'

enlarge_delta :: GM.MVector v a => v s a -> Int
enlarge_delta v = max (GM.length v) 1

-- | Grow a vector logarithmically
enlarge :: GM.MVector v a => v s a -> ST s (v s a)
enlarge v = GM.unsafeGrow v (enlarge_delta v)
{-# INLINE enlarge #-}
