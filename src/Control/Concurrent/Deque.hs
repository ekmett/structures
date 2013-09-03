{-# LANGUAGE CPP #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE MagicHash #-}
-- | Chase-Lev work-stealing Deques
--
-- This implementation derives directly from the pseudocode in the <http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.170.1097&rep=rep1&type=pdf 2005 SPAA paper>.
--
-- TODO: local topBound optimization.
-- TODO: Do the more optimized version of growCirc
module Control.Concurrent.Deque
  ( Deque
  , empty
  , null
  , push
  , pop
  , steal
  ) where

import Control.Exception (evaluate)
import Data.Atomics (storeLoadBarrier, writeBarrier, loadLoadBarrier)
import Data.Atomics.Counter.Reference
       ( AtomicCounter, newCounter, readCounter, writeCounter
       , casCounter, readCounterForCAS, peekCTicket
       )
import Data.IORef
import Data.Vector.Array
import qualified Data.Vector.Generic.Mutable as MV
import GHC.Prim (RealWorld)
import Prelude hiding (null)

-- | A Chase-Lev circular work-stealing deque
data Deque a = Deque
  { _bottom :: {-# UNPACK #-} !AtomicCounter
  , _top    :: {-# UNPACK #-} !AtomicCounter
  , _array  :: {-# UNPACK #-} !(IORef (MArray RealWorld a))
  }

empty :: Arrayed a => IO (Deque a)
empty = do
  v <- MV.new 32
  bot <- newCounter 0
  top <- newCounter 0
  ref <- newIORef v
  return (Deque bot top ref)

null :: Deque a -> IO Bool
null (Deque bot top _) = do
  b <- readCounter bot
  t <- readCounter top
  let size = b - t
  return (size <= 0)

-- * Circular array routines:

-- TODO: make a "grow" that uses memcpy.
growCirc :: Arrayed a => Int -> Int -> MArray RealWorld a -> IO (MArray RealWorld a)
growCirc strt end oldarr = do
  let len   = MV.length oldarr
      -- elems = end - strt
  -- putStrLn$ "Grow to size "++show (len+len)++", copying over "++show elems
  newarr <- MV.unsafeNew (len + len)
  for_ strt end $ \ind -> do
    x <- getCirc oldarr ind
    _ <- evaluate x
    putCirc newarr ind x
  return newarr
{-# INLINE growCirc #-}

getCirc :: Arrayed a => MArray RealWorld a -> Int -> IO a
getCirc arr ind = MV.unsafeRead arr (ind `mod` MV.length arr)
{-# INLINE getCirc #-}

putCirc :: Arrayed a => MArray RealWorld a -> Int -> a -> IO ()
putCirc arr ind x = MV.unsafeWrite arr (ind `mod` MV.length arr) x
{-# INLINE putCirc #-}

-- * Queue Operations

-- | For a work-stealing queue `push` is the ``local'' push.  Thus
--   only a single thread should perform this operation.
push :: Arrayed a => a -> Deque a -> IO ()
push obj (Deque bottom top array) = do
  b   <- readCounter bottom
  t   <- readCounter top
  arr <- readIORef array
  let len = MV.length arr
      size = b - t

  arr' <- if size < len - 1 then return arr else do
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
      size = b - t
  if size <= 0 then return Nothing else do
    a <- getCirc arr t
    (b',_) <- casCounter top tt (t+1)
    return $! if b' then Just a
                    else Nothing -- Someone beat us, abort

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
      size = b - t
  if size < 0 then do
    writeCounter bottom t
    return Nothing
   else do
    obj <- getCirc arr b
    if size > 0 then do
      return (Just obj)
     else do
      (b',_) <- casCounter top tt (t+1)
      writeCounter bottom (t+1)
      return $! if b' then Just obj
                      else Nothing

------------------------------------------------------------

-- My own forM for numeric ranges (not requiring deforestation optimizations).
-- Inclusive start, exclusive end.
for_ :: Monad m => Int -> Int -> (Int -> m ()) -> m ()
for_ start end _ | start > end = error "for_: start is greater than end"
for_ start end fn = loop start
  where
   loop !i | i == end  = return ()
           | otherwise = do fn i; loop (i+1)
{-# INLINE for_ #-}
