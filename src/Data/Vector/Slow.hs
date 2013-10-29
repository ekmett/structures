{-# LANGUAGE CPP #-}
{-# LANGUAGE RankNTypes  #-}
{-# LANGUAGE Trustworthy #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# OPTIONS_GHC -fno-warn-unused-matches -fno-warn-unused-binds #-}

module Data.Vector.Slow
  ( Slow(..)
  , yield
  , Partial(..)
  , walkST
  , munstream
  , unstreamM
  , foldM'
  , foldM
  ) where

import Control.Applicative
import Control.Monad hiding (foldM)
import Control.Monad.Free
import Control.Monad.Primitive
import Control.Monad.ST
import Control.Monad.ST.Unsafe as Unsafe
import Control.Monad.Trans.Class
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

data Slow m a = Slow { runSlow :: forall r. (a -> r) -> (r -> r) -> (m r -> r) -> r }

yield :: Slow m ()
yield = Slow $ \kp kd _ -> kd $ kp ()

instance Functor (Slow m) where
  fmap f (Slow g) = Slow $ \kp -> g (kp . f)
  {-# INLINE fmap #-}

instance Applicative (Slow m) where
  pure a = Slow $ \kp _ _ -> kp a
  {-# INLINE pure #-}
  mf <*> ma = Slow $ \kp kd kf -> runSlow mf (\f -> runSlow ma (\a -> kp (f a)) kd kf) kd kf
  {-# INLINE (<*>) #-}

instance Monad (Slow m) where
  return a = Slow $ \kp _ _ -> kp a
  {-# INLINE return #-}
  m >>= f = Slow $ \kp kd kf -> runSlow m (\a -> runSlow (f a) kp kd kf) kd kf
  {-# INLINE (>>=) #-}

instance MonadTrans Slow where
  lift f = Slow $ \kp _ kf -> kf (liftM kp f)

instance Monad m => MonadFree m (Slow m) where
  wrap f = Slow $ \kp kd kf -> kf (liftM (\m -> runSlow m kp kd kf) f)
  {-# INLINE wrap #-}

data Partial a
  = Stop a
  | Step (Partial a)

instance Functor Partial where
  fmap f (Stop a)  = Stop (f a)
  fmap f (Step as) = Step (fmap f as)

instance Applicative Partial where
  pure = Stop
  {-# INLINE pure #-}
  (<*>) = ap
  {-# INLINE (<*>) #-}

instance Monad Partial where
  return = Stop
  {-# INLINE return #-}
  Stop a  >>= f = f a
  Step as >>= f = Step (as >>= f)

instance MonadFree Identity Partial where
  wrap = Step . runIdentity
  {-# INLINE wrap #-}

walkST :: (forall s. Slow (ST s) a) -> Partial a
walkST l = runSlow l Stop Step (Unsafe.unsafePerformIO . Unsafe.unsafeSTToIO)

-- While this definition on paper makes more sense than the version above, GHC doesn't currently
-- attempt to use 'noDuplicate' in 'Unsafe.unsafeInterleaveST', rendering this unduly dangerous
-- unless we make all effects we want to Slow idempotent!
--
-- > walkST l = runST $ runSlow l (return . Stop) (fmap Step) $ \m -> do
-- >  n <- Unsafe.unsafeInterleaveST m
-- >  o <- Unsafe.unsafeInterleaveST n
-- >  return o
{-# INLINE walkST #-}

unstreamM :: (G.Vector v a, PrimMonad m) => M.Stream m a -> Slow m (v a)
unstreamM s = munstream s >>= lift . G.unsafeFreeze

munstream :: (PrimMonad m, GM.MVector v a) => M.Stream m a -> Slow m (v (PrimState m) a)
munstream s = case SS.upperBound (M.size s) of
  Just n  -> munstreamMax     s n
  Nothing -> munstreamUnknown s
{-# INLINE [1] munstream #-}

-- pay once per entry
foldM' :: Monad m => (a -> b -> m a) -> a -> M.Stream m b -> Slow m a
foldM' m z0 (M.Stream step s0 _) = foldM'_loop SPEC z0 s0
  where
    foldM'_loop !_SPEC z s
      = z `seq`
        do
          r <- lift (step s)
          case r of
            M.Yield x s' -> do { z' <- lift (m z x); yield; foldM'_loop SPEC z' s' }
            M.Skip    s' -> foldM'_loop SPEC z s'
            M.Done       -> return z
{-# INLINE [1] foldM' #-}

-- | Left fold with a monadic operator
foldM :: Monad m => (a -> b -> m a) -> a -> M.Stream m b -> Slow m a
foldM m z0 (M.Stream step s0 _) = foldM_loop SPEC z0 s0
  where
    foldM_loop !_SPEC z s
      = do
          r <- lift (step s)
          case r of
            M.Yield x s' -> do { z' <- lift (m z x); yield; foldM_loop SPEC z' s' }
            M.Skip    s' -> foldM_loop SPEC z s'
            M.Done       -> return z
{-# INLINE [1] foldM #-}


munstreamMax :: (PrimMonad m, GM.MVector v a) => M.Stream m a -> Int -> Slow m (v (PrimState m) a)
munstreamMax s n = do
  v <- INTERNAL_CHECK(checkLength) "munstreamMax" n
       $ lift (GM.unsafeNew n)
  let put i x = do
                   INTERNAL_CHECK(checkIndex) "munstreamMax" i n
                     $ GM.unsafeWrite v i x
                   return (i+1)
  n' <- foldM' put 0 s
  return $ INTERNAL_CHECK(checkSlice) "munstreamMax" 0 n' n
         $ GM.unsafeSlice 0 n' v
{-# INLINE munstreamMax #-}

munstreamUnknown :: (PrimMonad m, GM.MVector v a) => M.Stream m a -> Slow m (v (PrimState m) a)
munstreamUnknown s = do
  v <- lift (GM.unsafeNew 0)
  (v', n) <- foldM put (v, 0) s
  return $ INTERNAL_CHECK(checkSlice) "munstreamUnknown" 0 n (GM.length v')
         $ GM.unsafeSlice 0 n v'
  where
    {-# INLINE [0] put #-}
    put (v,i) x = do
      v' <- unsafeAppend1 v i x
      return (v',i+1)
{-# INLINE munstreamUnknown #-}

unsafeAppend1 :: (PrimMonad m, GM.MVector v a) => v (PrimState m) a -> Int -> a -> m (v (PrimState m) a)
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
enlarge :: (PrimMonad m, GM.MVector v a) => v (PrimState m) a -> m (v (PrimState m) a)
enlarge v = GM.unsafeGrow v (enlarge_delta v)
{-# INLINE enlarge #-}
