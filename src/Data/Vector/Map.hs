{-# LANGUAGE CPP #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE PatternGuards #-}
-----------------------------------------------------------------------------
-- |
-- Copyright   :  (C) 2013 Edward Kmett
-- License     :  BSD-style (see the file LICENSE)
-- Maintainer  :  Edward Kmett <ekmett@gmail.com>
-- Stability   :  experimental
-- Portability :  non-portable
--
-- This module provides a 'Vector'-based 'Map' that is loosely based on the
-- Cache Oblivious Lookahead Array (COLA) by Bender et al. from
-- <http://supertech.csail.mit.edu/papers/sbtree.pdf "Cache-Oblivious Streaming B-Trees">.
--
-- Currently this 'Map' is implemented in an insert-only fashion. Deletions are left to future work
-- or to another derived structure in case they prove expensive.
--
-- Unlike the COLA, this version merely provides amortized complexity bounds as this permits us to
-- provide a fully functional API. However, even those asymptotics are only guaranteed if you do not
-- modify the \"old\" versions of the 'Map'. If you do, then while correctness is preserved, the
-- asymptotic analysis becomes inaccurate.
--
-- Reading from \"old\" versions of the 'Map' does not affect the asymptotic analysis and is fine.
--
-- Fractional cascading was originally replaced with the use of a hierarchical bloom filter per level containing
-- the elements for that level, with the false positive rate tuned to balance the lookup cost against
-- the costs of the cache misses for a false positive at that depth. This avoids the need to collect
-- forwarding pointers from the next level, reducing pressure on the cache dramatically, while providing
-- the same asymptotic complexity.
--
-- With either of these two techniques when used ephemerally, this 'Map' had asymptotic performance equal to that
-- of a B-Tree tuned to the parameters of your caches with requiring such parameter tuning.
--
-- However, the constants were still bad enough that the naive /O(log^2 n)/ version of the COLA actually wins
-- at lookups in benchmarks at the scale this data structure is interesting, say around a few million entries,
-- by a factor of 10x! Consequently, we're currently not even Bloom filtering.
--
-- Compared to the venerable @Data.Map@, this data structure currently consumes more memory, but it
-- provides a more limited palette of operations with different asymptotics (~10x faster inserts at a million entries)
-- and enables us to utilize contiguous storage.
--
-- /NB:/ when used with boxed data this structure may hold onto references to old versions
-- of things for many updates to come until sufficient operations have happened to merge them out
-- of the COLA.
--
-- TODO: track actual percentage of occupancy for each vector compared to the source vector it was based on.
-- This would permit 'split' and other operations that trim a 'Map' to properly reason about space usage by
-- borrowing the 1/3rd occupancy rule from a Stratified Doubling Array.
-----------------------------------------------------------------------------
module Data.Vector.Map
  ( Map(..)
  , empty
  , null
  , singleton
  , lookup
  , insert
  , insert4
  -- , insert6
  , fromDistinctAscList
  , fromList
  , fromList4
  , shape
  , split
  , union
  -- * Rebuild
  , rebuild
  ) where

import Control.Monad.ST
import Control.Monad.ST.Unsafe as Unsafe
import Data.Bits
import qualified Data.Foldable as F
import qualified Data.List as List
import Data.Monoid
import qualified Data.Vector.Heap as H
import qualified Data.Vector as B
import qualified Data.Vector.Mutable as BM
import Data.Vector.Array
import Data.Vector.Fusion.Stream.Monadic (Stream(..))
import qualified Data.Vector.Fusion.Stream.Monadic as Stream
import Data.Vector.Fusion.Util
import qualified Data.Vector.Generic as G
import qualified Data.Vector.Generic.Mutable as GM
import qualified Data.Vector.Map.Fusion as Fusion
import Prelude hiding (null, lookup)

#define BOUNDS_CHECK(f) (Ck.f __FILE__ __LINE__ Ck.Bounds)

-- | This Map is implemented as an insert-only Cache Oblivious Lookahead Array (COLA) with amortized complexity bounds
-- that are equal to those of a B-Tree when it is used ephemerally, using Bloom filters to replace the fractional
-- cascade.
data Map k v
  = Nil
  | One !k v !(Map k v)
  | Map !Int !(Array k) !(Array v) !(Map k v)

deriving instance (Show (Arr v v), Show (Arr k k), Show k, Show v) => Show (Map k v)
deriving instance (Read (Arr v v), Read (Arr k k), Read k, Read v) => Read (Map k v)

-- | /O(1)/. Identify if a 'Map' is the 'empty' 'Map'.
null :: Map k v -> Bool
null Nil = True
null _   = False
{-# INLINE null #-}

-- | /O(1)/ The 'empty' 'Map'.
empty :: Map k v
empty = Nil
{-# INLINE empty #-}

-- | /O(1)/ Construct a 'Map' from a single key/value pair.
singleton :: Arrayed v => k -> v -> Map k v
singleton k v = v `vseq` One k v Nil
{-# INLINE singleton #-}

-- | /O(log^2 N)/ persistently amortized, /O(N)/ worst case. Lookup an element.
lookup :: (Ord k, Arrayed k, Arrayed v) => k -> Map k v -> Maybe v
lookup !k m0 = go m0 where
  {-# INLINE go #-}
  go Nil = Nothing
  go (One i a m)
    | k == i    = Just a
    | otherwise = go m
  go (Map n ks vs m)
    | j <- search (\i -> ks G.! i >= k) 0 (n-1)
    , ks G.! j == k = Just $ vs G.! j
    | otherwise = go m
{-# INLINE lookup #-}

threshold :: Int -> Int -> Bool
threshold n1 n2 = n1 > unsafeShiftR n2 1
{-# INLINE threshold #-}

-- force a value as much as it would be forced by inserting it into an Array
vseq :: forall a b. Arrayed a => a -> b -> b
vseq a b = G.elemseq (undefined :: Array a) a b
{-# INLINE vseq #-}

-- | O((log N)\/B) ephemerally amortized loads for each cache, O(N\/B) worst case. Insert an element.
insert :: (Ord k, Arrayed k, Arrayed v) => k -> v -> Map k v -> Map k v
insert !k v (Map n1 ks1 vs1 (Map n2 ks2 vs2 m))
  | threshold n1 n2 = merges [element 0 k v, elements 1 n1 ks1 vs1, elements 2 n2 ks2 vs2] m
insert !ka va (One kb vb (One kc vc m)) = merges [element 0 ka va, element 1 kb vb, element 3 kc vc] m
insert k v m = v `vseq` One k v m
{-# INLINABLE insert #-}

insert2 :: (Ord k, Arrayed k, Arrayed v) => k -> v -> Array k -> Array v -> Array k -> Array v -> Map k v -> Map k v
insert2 k v ks1 vs1 ks2 vs2 m = case G.unstream $ Fusion.insert k v (zips ks1 vs1) `Fusion.merge` zips ks2 vs2 of
  V_Pair n ks3 vs3 -> Map n ks3 vs3 m
{-# INLINE insert2 #-}

fromDistinctAscList :: (Ord k, Arrayed k, Arrayed v) => [(k,v)] -> Map k v
fromDistinctAscList kvs = fromList kvs
{-# INLINE fromDistinctAscList #-}

insert4 :: forall k v. (Ord k, Arrayed k, Arrayed v) => k -> v -> Map k v -> Map k v
insert4 !k v (Map n1 ks1 vs1 (Map n2  ks2 vs2 (Map n3  ks3 vs3 (Map n4 ks4 vs4 m))))
  | n1 > unsafeShiftR n4 2 = merges [element 0 k v, elements 1 n1 ks1 vs1, elements 2 n2 ks2 vs2, elements 3 n3 ks3 vs3, elements 4 n4 ks4 vs4] m
insert4 !ka va (One kb vb (One kc vc (One kd vd (One ke ve m)))) = merges [element 0 ka va, element 1 kb vb, element 3 kc vc, element 4 kd vd, element 5 ke ve] m
insert4 k v m = v `vseq` One k v m
{-# INLINABLE insert4 #-}

fromList :: (Ord k, Arrayed k, Arrayed v) => [(k,v)] -> Map k v
fromList xs = List.foldl' (\m (k,v) -> insert k v m) empty xs
{-# INLINE fromList #-}

fromList4 :: (Ord k, Arrayed k, Arrayed v) => [(k,v)] -> Map k v
fromList4 xs = List.foldl' (\m (k,v) -> insert4 k v m) empty xs
{-# INLINE fromList4 #-}


-- | Generates two legal maps from a source 'Map'. The asymptotic analysis is preserved if only one of these is updated.
split :: (Ord k, Arrayed k, Arrayed v) => k -> Map k v -> (Map k v, Map k v)
split k m0 = go m0 where
  go Nil = (Nil, Nil)
  go (One j a m) = case go m of
    (xs,ys)
       | j < k     -> (insert j a xs, ys)
       | otherwise -> (xs, insert j a ys)
  go (Map n ks vs m) = case go m of
    (xs,ys) -> case G.splitAt j ks of
      (kxs,kys) -> case G.splitAt j vs of
        (vxs,vys) -> (cons j kxs vxs xs, cons (n-j) kys vys ys)
      where j = search (\i -> ks G.! i >= k) 0 n
{-# INLINE split #-}

union :: (Ord k, Arrayed k, Arrayed v) => Map k v -> Map k v -> Map k v
union ys0 xs = go ys0 where
  go Nil = xs
  go (One k v m) = insert k v (go m)
  go (Map n ks vs m) = cons n ks vs (go m)
{-# INLINE union #-}

-- | Offset binary search
--
-- Assuming @l <= h@. Returns @h@ if the predicate is never @True@ over @[l..h)@
search :: (Int -> Bool) -> Int -> Int -> Int
search p = go where
  go l h
    | l == h    = l
    | p m       = go l m
    | otherwise = go (m+1) h
    where hml = h - l
          m = l + unsafeShiftR hml 1 + unsafeShiftR hml 6
{-# INLINE search #-}

cons :: (Ord k, Arrayed k, Arrayed v) => Int -> Array k -> Array v -> Map k v -> Map k v
cons 0 _  _  m = m
cons 1 ks vs m = insert (G.unsafeHead ks) (G.unsafeHead vs) m
cons n ks vs (Map n2 ks' vs' m)
  | threshold n n2
  , nc <- min (n-1) n2
  , (ks1, ks2) <- G.splitAt nc ks
  , (vs1, vs2) <- G.splitAt nc vs
  , k <- G.unsafeHead ks2, ks3 <- G.unsafeTail ks2
  , v <- G.unsafeHead vs2, vs3 <- G.unsafeTail vs2
  = cons (n-nc-1) ks3 vs3 $ insert2 k v ks1 vs1 ks' vs' m
cons n ks vs m = Map n ks vs m
{-# INLINABLE cons #-}

-- | If using have trashed our size invariants we can use this to restore them
rebuild :: (Ord k, Arrayed k, Arrayed v) => Map k v -> Map k v
rebuild Nil = Nil
rebuild (One k v m) = insert k v m
rebuild (Map n ks vs m) = cons n ks vs (rebuild m)
{-# INLINABLE rebuild #-}

zips :: (G.Vector v a, G.Vector u b) => v a -> u b -> Stream Id (a, b)
zips va ub = Stream.zip (G.stream va) (G.stream ub)
{-# INLINE zips #-}

-- * Debugging

shape :: Map k v -> [Int]
shape Nil           = []
shape (One _ _ m)   = 1 : shape m
shape (Map n _ _ m) = n : shape m

-- * Merging

data Entry k v = Entry {-# UNPACK #-} !Int !k v {-# UNPACK #-} !Int {-# UNPACK #-} !Int (Array k) (Array v)

deriving instance (Show (Arr v v), Show (Arr k k), Show k, Show v) => Show (Entry k v)
deriving instance (Read (Arr v v), Read (Arr k k), Read k, Read v) => Read (Entry k v)

instance Eq k => Eq (Entry k v) where
  Entry i ki _ _ _ _ _ == Entry j kj _ _ _ _ _ = i == j && ki == kj
  {-# INLINE (==) #-}

instance Ord k => Ord (Entry k v) where
  compare (Entry i ki _ _ _ _ _) (Entry j kj _ _ _ _ _) = compare ki kj `mappend` compare i j
  {-# INLINE compare #-}

element :: (Arrayed k, Arrayed v) => Int -> k -> v -> Entry k v
element i k v = Entry i k v 0 0 (error "BAD") (error "VERY BAD")
{-# INLINE element #-}

elements :: (Arrayed k, Arrayed v) => Int -> Int -> Array k -> Array v -> Entry k v
elements i n ks vs = Entry i (G.unsafeHead ks) (G.unsafeHead vs) 1 n ks vs
{-# INLINE elements #-}

merges :: forall k v. (Ord k, Arrayed k, Arrayed v) => [Entry k v] -> Map k v -> Map k v
merges [] m = m
merges es m = runST $ do
  mv0   <- G.unsafeThaw (G.fromList es :: B.Vector (Entry k v))
  let nmv0 = BM.length mv0
  let tally !acc k
        | k == nmv0 = return acc
        | otherwise = do
        Entry _ _ _ x y _ _ <- BM.unsafeRead mv0 k
        tally (acc + 1 + y - x) (k+1)
  r_max <- tally 0 0
  mks   <- GM.new r_max
  mvs   <- GM.new r_max
  let go mv li lk lo ln lks lvs lr
        | GM.null mv = do
          F.forM_ [lo..ln-1] $ \ i -> do
            k <- G.unsafeIndexM lks i
            v <- G.unsafeIndexM lvs i
            let j = lr+i-lo
            GM.unsafeWrite mks j k
            GM.unsafeWrite mvs j v
          return (lr+ln-lo)
        | otherwise = do
        Entry ni nk nv no nn nks nvs <- H.findMin mv
        let put r i k v
              | k == lk && i > li = return r
              | otherwise = do
                GM.unsafeWrite mks r k
                GM.unsafeWrite mvs r v
                return (r + 1)
            run r o
              | o == ln = do
                mv' <- H.deleteMin mv
                nr  <- put r ni nk nv
                go mv' ni nk no nn nks nvs nr
              | otherwise = do
                k <- G.unsafeIndexM lks o
                v <- G.unsafeIndexM lvs o
                case compare k nk of
                  LT -> do
                    GM.unsafeWrite mks r k
                    GM.unsafeWrite mvs r v
                    run (r+1) (o+1)
                  EQ | li < ni -> do
                    GM.unsafeWrite mks r k
                    GM.unsafeWrite mvs r v
                    run (r+1) (o+1)
                  _ -> do
                    H.updateMin (Entry li k v (o+1) ln lks lvs) mv
                    nr <- put r ni nk nv
                    go mv ni nk no nn nks nvs nr
        run lr lo
  H.heapify mv0
  entry@(Entry li lk lv o n ks vs) <- H.findMin mv0
  mv1 <- H.deleteMin mv0
  GM.unsafeWrite mks 0 lk
  GM.unsafeWrite mvs 0 lv
  r  <- go mv1 li lk o n ks vs 1
  if r == 1
    then return $ One lk lv m
    else do
     zks <- G.unsafeFreeze (GM.unsafeSlice 0 r mks)
     zvs <- G.unsafeFreeze (GM.unsafeSlice 0 r mvs)
     return $ Map r zks zvs m
{-# INLINE merges #-}
