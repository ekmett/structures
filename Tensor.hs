{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}

module Tensor where

import Data.Word

-- * Heterogeneous Lists

data List :: [*] -> * where
  Cons :: i -> !(List is) -> List (i ':is)
  Nil  :: List '[]

type family (++) (as :: [*]) (bs :: [*]) :: [*]
type instance '[] ++ bs = bs
type instance (a ': as) ++ bs = a ': (as ++ bs)

singleton :: a -> List '[a]
singleton a = Cons a Nil

singular :: List '[a] -> a
singular (Cons a Nil) = a

-- * Steps

data Pair s = Pair !s !s

-- non-linear table "fusion"
data Step s js a
  = Single !(List js) a
  | Split !(List js) (Pair s) !(List js)
  | Done

-- simple tabls don't really need fusion
data Steps m js a = In { out :: m (Step (Steps m js a) js a) }

done :: Monad m => Steps m js a
done = In (return Done)

single :: Monad m => List js -> a -> Steps m js a
single js a = In $ return $ Single js a

split :: Monad m => List js -> Steps m js a -> Steps m js a -> List js -> Steps m js a
split js0 x y js1 = In $ return $ Split js0 x y js1

-- @'T' is js v m a@ represents a tensor/table with codata-like inputs is, data-like outputs js, variable bindings in v, with associated values of type a using effects in m
data T :: [*] -> [*] -> (* -> *) -> (* -> *) -> * -> * where
  Union     :: (a -> a -> a) -> T is js v m a -> T is js v m a -> T is js v m a -- merge
  UnionWith :: (a -> b -> c) -> (a -> c) -> (b -> c) -> T is js v a -> T is js v m b -> T is js v m c -- merge
  Weaken    :: Sel is is' -> T is' js v m a -> T is js v m a  -- observably forget input
  Up        :: T (i ': is) js v m a -> In v i -> T is js v m a -- attach a codata argument
  Down      :: T is js v m a -> Out v j -> T is (j ':js) v m a -- attach a data result
  Then      :: T is js v m (a -> b) -> T js ks v m a -> T is ks v m b -- chain together a pipeline, expensive
  Join      :: (a -> b -> c) -> On js js' ks -> T is js v m a -> T is js' v m b -> T is ks v m c
  T         :: (s -> m (Step s js a)) -> (List is -> s) -> T is js v m a -- From inputs @is@, produce a table with keys @js@ and values of type @a@ using effects in @m@.
                                                                         -- use two List is for branch and bound to allow this to work with partially realized inputs?
  Cup       :: Eq i => a -> T '[i,i] [] v m a
  Project   :: Eq j => (a -> a -> a) -> (Out v j -> T is js v m a) -> T is js v m a -- we can contract/weaken to get non-linear patterns
  Cross     :: (a -> b -> c) -> T is js v m a -> T is js' v m b -> T is (js ++ js') v m c -- (&&&)
  Output    :: Eq j => (Out v j -> T is js v m a) -> T is (j ': js) v m a
  Input     :: (In v i -> T is js v m a) -> T (i ': is) js v m a

class Weak 

instance Applicative (T is [] v m) where
  pure = T 

filter :: Monad m => (i -> Bool) -> T [i] [] v m ()
filter p = T out $ \is -> if p (singular i) then single Nil () else done

filterMaps :: Monad m => (List is -> Maybe (List js)) -> T is js v m ()
filterMaps p = T out $ \is -> p is <&> \mjs -> case mjs ofsingle js ()

filterMap :: Monad m => (i -> Maybe j) -> T [i] [j] v m ()
filterMap p = T out $ \is -> p (singular is) <&> \j -> single (singleton j) ()

map :: 

delta :: a -> T is is v a
delta a = Filter a Just


-- run the absolute value function backwards on an index
abs_oi :: T [Int] [Int] v ()
abs_oi = T out $ \(Cons r Nil) -> case compare r 0 of
  LT -> Nothing
  EQ -> Just $ single (singleton r) ()
  GT | nr <- negate r, snr = singleton nr, sr = singleton r -> Just $ split snr (single snr) (single sr) sr

-- lift a function into an UpDown tensor
fun :: (a -> b) -> T [a] [b] v ()
fun = Filter $ \(Cons a Nil) -> Just $ singleton (f a)

-- lift a function into an UpDown tensor
fun2 :: (a -> b -> c) -> T [a,b] [c] v ()
fun2 = Filter $ \(Cons a (Cons b Nil)) -> Just $ singleton (f a b)

filter :: (a -> Bool) -> T [a] [] v ()
filter p = Filter $ \(Cons a Nil) -> if p a then Just $ singleton () else Nothing

instance Num (T []) where
  (+) = Align' (+)
  (-) = Align (-) id negate
  negate = fmap negate
  (*) = liftA2 (*)
  fromIntegral = Scalar . fromIntegral
  abs = fmap abs
  signum = fmap signum

-- permit convenient multiplication, etc for square matrices.
instance (Eq i, is ~ [i]) => Num (T (i ': is)) where
  (+) = Align' (+)
  (-) = Align (-) id negate
  negate = fmap negate
  x * y = tensor $ \ i k -> sum $ \ j -> x!i!j * y!j!k
  fromIntegral = Delta . fromIntegral
  abs = fmap abs
  signum = fmap signum

instance Functor (T is) where
  fmap f (Scalar a) = Scalar (f a)
  fmap f (Delta a) = Delta (f a)
  fmap f (Ix t ix) = Ix (fmap f t) ix
  -- ... use free applicative stuff

instance Apply (T is) where
  (<.>) = Ap

instance (is ~ []) => Applicative (T is) where
  pure  = Scalar
  (<*>) = Ap

data Model a = Model a

newtype In (v :: Model) a = forall b. In (b -> a) (v b)

instance Functor (In v) where
  fmap f (In g vb) = In (f . g) vb

data family Out (v :: Model) :: * -> *

(?) :: T (i ': is) js v a -> v (In i) -> T is js v a
(?) = Up

(!) :: T is (j ': js) v a -> v (Out j) -> T is js v a
(!) = Down

sum :: (v i -> T is v a) -> T is v a
sum = Reduce (+)

class Eq i => Cup i
instance Cup Word

class Tensor x is v a | x -> is v a where
  tensor :: x -> T is v a

instance Tensor (T is v a) is v a where
  tensor = id

instance (Tensor x is v a, Cup i, vi ~ v i) => Tensor (vi -> x) (i ': is) v a where
  tensor f = Tensor $ \x -> tensor (f x)

delta :: (Eq i, Num a) => T [i,i] v a
delta = Delta 1

delta3 :: (Eq i, Num a) => T [i,i,i] a
delta3 = tensor $ \ i j k -> delta!i!k *> delta!j!k

instance Applicative (T [])


a :: T [Int,Int] Double
b :: T [Int,Int,Int] Double
d :: T [Int,Int,Int] Double
d = tensor $ \i l -> sum $ \j k -> a!i!j!k * b!j!k!l

data Sel : * -> [*] -> [*] -> * where
  Keep :: Sel is js -> Sel (i ': is) (i ': ks)
  Skip :: Sel is js -> Sel (i ': is) js
  Fini :: Sel is []

