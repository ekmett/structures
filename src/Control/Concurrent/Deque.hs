{-# LANGUAGE CPP #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE MagicHash #-}
-- |
-- | Chase-Lev work-stealing Deques
--
-- This implementation derives directly from the pseudocode in the <http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.170.1097&rep=rep1&type=pdf 2005 SPAA paper>.
module Control.Concurrent.Deque
  ( Deque
  -- * Initialization
  , empty
  , fromList
  , fromListN
  -- * Size
  , null
  , size
  -- * Local Operations
  , push
  , pop
  -- * Work-Stealing
  , steal
  ) where

import Control.Exception (evaluate)
import Data.Atomics (storeLoadBarrier, writeBarrier, loadLoadBarrier)
import Data.Atomics.Counter.Reference
import Data.IORef
import Data.Vector.Array
import qualified Data.Vector.Generic as V
import qualified Data.Vector.Generic.Mutable as MV
import GHC.Prim (RealWorld)
import Prelude hiding (null)

-- | A Chase-Lev circular work-stealing deque
data Deque a = Deque
  { _bottom :: {-# UNPACK #-} !AtomicCounter
  , _top    :: {-# UNPACK #-} !AtomicCounter
  , _array  :: {-# UNPACK #-} !(IORef (MArray RealWorld a))
  }

-- | Create a new 'empty' 'Deque'.
empty :: Arrayed a => IO (Deque a)
empty = do
  v <- MV.new 32
  bot <- newCounter 0
  top <- newCounter 0
  ref <- newIORef v
  return (Deque bot top ref)

fromList :: forall a. Arrayed a => [a] -> IO (Deque a)
fromList as = do
  v <- V.unsafeThaw (V.fromList as :: Array a)
  bot <- newCounter (MV.length v - 1)
  top <- newCounter 0
  ref <- newIORef v
  return (Deque bot top ref)

fromListN :: forall a. Arrayed a => Int -> [a] -> IO (Deque a)
fromListN n as = do
  v <- V.unsafeThaw (V.fromListN n as :: Array a)
  bot <- newCounter (MV.length v - 1)
  top <- newCounter 0
  ref <- newIORef v
  return (Deque bot top ref)

-- | Return 'True' if the 'Deque' is definitely empty.
null :: Deque a -> IO Bool
null (Deque bot top _) = do
  b <- readCounter bot
  t <- readCounter top
  let sz = b - t
  return (sz <= 0)

-- | Compute a lower and upper bound on the number of elements left in the 'Deque'.
size :: Deque a -> IO (Int,Int)
size (Deque bot top _) = do
  b1 <- readCounter bot
  t  <- readCounter top
  b2 <- readCounter bot
  let size1 = b1 - t
      size2 = b2 - t
  return (min size1 size2, max size1 size2)

-- * Queue Operations

-- | For a work-stealing queue `push` is the ``local'' push.  Thus
--   only a single thread should perform this operation.
push :: Arrayed a => a -> Deque a -> IO ()
push obj (Deque bottom top array) = do
  b   <- readCounter bottom
  t   <- readCounter top
  arr <- readIORef array
  let len = MV.length arr
      sz = b - t

  arr' <- if sz < len - 1 then return arr else do
    arr' <- growCirc t b arr -- Double in size, don't change b/t.
    -- Only a single thread will do this!:
    writeIORef array arr'
    return arr'

  putCirc arr' b obj
  {-
     KG: we need to put write barrier here since otherwise we might
     end with elem not added to q->elements, but q->bottom already
     modified (write reordering) and with stealWSDeque_ failing
     later when invoked from another thread since it thinks elem is
     there (in case there is just added element in the queue). This
     issue concretely hit me on ARMv7 multi-core CPUs
   -}
  writeBarrier
  writeCounter bottom (b+1)
  return ()

-- | This is the steal operation.  Multiple threads may concurrently
-- attempt steals from the same thread.
steal :: Arrayed a => Deque a -> IO (Maybe a)
steal (Deque bottom top array) = do
  -- NB. these loads must be ordered, otherwise there is a race
  -- between steal and pop.
  tt  <- readCounterForCAS top
  loadLoadBarrier
  b   <- readCounter bottom
  arr <- readIORef array
  let t = peekCTicket tt
      sz = b - t
  if sz <= 0 then return Nothing else do
    a <- getCirc arr t
    (b',_) <- casCounter top tt (t+1)
    return $! if b' then Just a
                    else Nothing -- Someone beat us, abort

-- | Locally pop the deque.
pop :: Arrayed a => Deque a -> IO (Maybe a)
pop (Deque bottom top array) = do
  b0  <- readCounter bottom
  arr <- readIORef array
  b   <- evaluate (b0-1)
  writeCounter bottom b

  -- very important that the following read of q->top does not occur
  -- before the earlier write to q->bottom.
  storeLoadBarrier
  tt   <- readCounterForCAS top
  let t = peekCTicket tt
      sz = b - t
  if sz < 0 then do
    writeCounter bottom t
    return Nothing
   else do
    obj <- getCirc arr b
    if sz > 0 then do
      return (Just obj)
     else do
      (b',_) <- casCounter top tt (t+1)
      writeCounter bottom (t+1)
      return $! if b' then Just obj
                      else Nothing

-- * Utilities

-- My own forM for numeric ranges (not requiring deforestation optimizations).
-- Inclusive start, exclusive end.
for_ :: Monad m => Int -> Int -> (Int -> m ()) -> m ()
for_ start end _ | start > end = error "for_: start is greater than end"
for_ start end fn = loop start
  where
   loop !i | i == end  = return ()
           | otherwise = do fn i; loop (i+1)
{-# INLINE for_ #-}

-- * Circular array routines:

-- TODO: make a "grow" that uses memcpy.
growCirc :: Arrayed a => Int -> Int -> MArray RealWorld a -> IO (MArray RealWorld a)
growCirc s e old = do
  let len = MV.length old
  new <- MV.unsafeNew (len + len)
  for_ s e $ \i -> do
    x <- getCirc old i
    _ <- evaluate x
    putCirc new i x
  return new
{-# INLINE growCirc #-}

getCirc :: Arrayed a => MArray RealWorld a -> Int -> IO a
getCirc arr ind = MV.unsafeRead arr (ind `mod` MV.length arr)
{-# INLINE getCirc #-}

putCirc :: Arrayed a => MArray RealWorld a -> Int -> a -> IO ()
putCirc arr ind x = MV.unsafeWrite arr (ind `mod` MV.length arr) x
{-# INLINE putCirc #-}

