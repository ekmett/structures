{-# LANGUAGE ExistentialQuantification #-}
module Data.Vector.Map.Heap
  ( Heap(..)
  , fby
  , fromStreams
  ) where

import Control.Lens
import Data.Semigroup
import Data.Vector.Fusion.Stream.Monadic
import Data.Vector.Fusion.Stream.Size
import Data.Vector.Fusion.Util

-- bootstrapped non-empty catenable pairing heaps of streams, used for unioning data sets
data Heap m k v = forall s. Heap !k {-# UNPACK #-} !Int v !s (s -> m (Step s (k, v))) [Heap m k v] [Heap m k v] [Heap m k v]

fby :: Heap m k v -> Heap m k v -> Heap m k v
fby (Heap i p a sa stepa as ls rs) r = Heap i p a sa stepa as ls (r:rs)
{-# INLINE fby #-}

instance Ord k => Semigroup (Heap m k v) where
  x@(Heap i p a sa stepa as al ar) <> y@(Heap j q b sb stepb bs bl br) = case compare i j of
    LT -> Heap i p a sa stepa (y:pops as al ar) [] []
    EQ | p <= q    -> Heap i p a sa stepa (y:pops as al ar) [] []
       | otherwise -> Heap j q b sb stepb (x:pops bs bl br) [] []
    GT -> Heap j q b sb stepb (x:pops bs bl br) [] []
  {-# INLINE (<>) #-}

pops :: Ord k => [Heap m k v] -> [Heap m k v] -> [Heap m k v] -> [Heap m k v]
pops xs     []     [] = xs
pops (x:xs) ls     rs = [fbys (Prelude.foldl (<>) x xs) ls rs]
pops []     (l:ls) rs = [fbys l ls rs]
pops []     []     rs = case reverse rs of
  f:fs -> [fbys f fs []]
  _    -> [] -- caught above by the 'go as [] []' case
{-# INLINE pops #-}

fbys :: Heap m k v -> [Heap m k v] -> [Heap m k v] -> Heap m k v
fbys (Heap i p a sa stepa as [] []) ls' rs' = Heap i p a sa stepa as ls' rs'
fbys (Heap i p a sa stepa as ls []) ls' rs' = Heap i p a sa stepa as ls $ rs' <> reverse ls'
fbys (Heap i p a sa stepa as ls rs) ls' rs' = Heap i p a sa stepa as ls $ rs' <> reverse ls' <> rs
{-# INLINE fbys #-}

fromStreams :: Ord k => [Stream Id (k, v)] -> Maybe (Heap Id k v)
fromStreams = getOption . ifoldMap prime where
  prime p (Stream stepa sa0 _) = go sa0 where
    go sa = case unId (stepa sa) of
      Done            -> Option Nothing
      Skip sa'        -> go sa'
      Yield (k,v) sa' -> Option $ Just $ Heap k p v sa' stepa [] [] []
{-# INLINE fromStreams #-}

{-
-- we've already primed the heaps
toStream :: Maybe (Heap m k v) -> Stream (k, v)
toStream f h0
-}

{-
data HeapState m k v
  = Start !(Heap k v)
  | forall s. PumpingTo s (s -> m (Step s (k, v)) k {-# UNPACK #-} !Int !(Heap a)
  | forall s. Finishing s (s -> m (Step s (k, v)))
  | Final {-# UNPACK #-} !Key a
  | Finished

-- | Convert a 'Heap' into a 'Stream' folding together values with identical keys using the supplied
-- addition operator.
streamHeapWith :: Monad m => (a -> a -> a) -> Maybe (Heap a) -> Stream m (Key, a)
streamHeapWith f h0 = Stream step (maybe Finished Start h0) Unknown where
  step (Start (Heap i p a sa stepa xs ls rs) = return $ case pop xs ls rs of
    Nothing -> Yield (i, a) (Finishing sa stepa)
    Just next@(Heap j q b sa stepb xs ls rs) -> Yield (i, a) $ PumpingTo sa stepa j q next
  step (PumpingTo sa stepa j q = case getId (stepa sa) of
    Yield (i, a) sa' 
      | i < j -> Yield (i, a) -> PumpingTo sa stepa j q next
      | i == j = if p < q then -> Yield (


  step (Start (Heap i a xs ls rs))     = return $ Yield i a $ maybe (Final i a) (Ready i a) $ pop xs ls rs
  step (Ready i a (Heap j b xs ls rs)) = return $ case compare i j of
    LT -> Yield (i, a)      $ maybe (Final j b) (Ready j b) $ pop xs ls rs
    EQ | c <- f a b -> Skip $ maybe (Final i c) (Ready i c) $ pop xs ls rs
    GT -> Yield (j, b)      $ maybe (Final i a) (Ready i a) $ pop xs ls rs
  step (Final i a) = return $ Yield (i,a) Finished
  step Finished    = return Done
  {-# INLINE [1] step #-}
{-# INLINE [0] streamHeapWith #-}
-}
