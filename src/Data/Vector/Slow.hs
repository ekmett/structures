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
import Control.Monad.ST
import Control.Monad.ST.Class
import Control.Monad.ST.Unsafe as Unsafe
import Data.Data
import Data.Foldable
import Data.Functor.Identity
import GHC.Generics
import Data.Traversable
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

data Slow s a = Slow { runSlow :: forall r. (a -> r) -> (r -> r) -> (ST s r -> r) -> r }

yield :: Slow s ()
yield = Slow $ \kp kd _ -> kd $ kp ()

instance Functor (Slow s) where
  fmap f (Slow g) = Slow $ \kp -> g (kp . f)
  {-# INLINE fmap #-}

instance Applicative (Slow s) where
  pure a = Slow $ \kp _ _ -> kp a
  {-# INLINE pure #-}
  mf <*> ma = Slow $ \kp kd kf -> runSlow mf (\f -> runSlow ma (\a -> kp (f a)) kd kf) kd kf
  {-# INLINE (<*>) #-}

instance Monad (Slow s) where
  return a = Slow $ \kp _ _ -> kp a
  {-# INLINE return #-}
  m >>= f = Slow $ \kp kd kf -> runSlow m (\a -> runSlow (f a) kp kd kf) kd kf
  {-# INLINE (>>=) #-}

instance MonadST (Slow s) where
  type World (Slow s) = s
  liftST f = Slow $ \kp _ kf -> kf (liftM kp f)

instance MonadFree (ST s) (Slow s) where
  wrap f = Slow $ \kp kd kf -> kf (fmap (\m -> runSlow m kp kd kf) f)
  {-# INLINE wrap #-}

data Partial a
  = Stop a
  | Step (Partial a)
  deriving (Show, Read, Data, Typeable, Generic)

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

instance Foldable Partial where
  foldMap f = go where
    go (Stop a) = f a
    go (Step m) = go m
  {-# INLINE foldMap #-}

instance Traversable Partial where
  traverse f = go where
    go (Stop a) = Stop <$> f a
    go (Step m) = Step <$> go m
  {-# INLINE traverse #-}

walkST :: (forall s. Slow s a) -> Partial a
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

unstreamM :: G.Vector v a => M.Stream (ST s) a -> Slow s (v a)
unstreamM s = munstream s >>= liftST . G.unsafeFreeze

munstream :: GM.MVector v a => M.Stream (ST s) a -> Slow s (v s a)
munstream s = case SS.upperBound (M.size s) of
  Just n  -> munstreamMax     s n
  Nothing -> munstreamUnknown s
{-# INLINE [1] munstream #-}

-- pay once per entry
foldM' :: (a -> b -> ST s a) -> a -> M.Stream (ST s) b -> Slow s a
foldM' m z0 (M.Stream step s0 _) = foldM'_loop SPEC z0 s0
  where
    foldM'_loop !_SPEC z s
      = z `seq`
        do
          r <- liftST (step s)
          case r of
            M.Yield x s' -> do { z' <- liftST (m z x); yield; foldM'_loop SPEC z' s' }
            M.Skip    s' -> foldM'_loop SPEC z s'
            M.Done       -> return z
{-# INLINE [1] foldM' #-}

-- | Left fold with a monadic operator
foldM :: (a -> b -> ST s a) -> a -> M.Stream (ST s) b -> Slow s a
foldM m z0 (M.Stream step s0 _) = foldM_loop SPEC z0 s0
  where
    foldM_loop !_SPEC z s
      = do
          r <- liftST (step s)
          case r of
            M.Yield x s' -> do { z' <- liftST (m z x); yield; foldM_loop SPEC z' s' }
            M.Skip    s' -> foldM_loop SPEC z s'
            M.Done       -> return z
{-# INLINE [1] foldM #-}


munstreamMax :: GM.MVector v a => M.Stream (ST s) a -> Int -> Slow s (v s a)
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

munstreamUnknown :: GM.MVector v a => M.Stream (ST s) a -> Slow s (v s a)
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
