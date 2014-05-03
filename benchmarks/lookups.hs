{-# OPTIONS_GHC -fno-warn-orphans #-}
module Main where

import Data.Foldable as F
import Data.Map as M
import Data.Maybe
import Data.Vector.Map as V
import Data.Vector.Map.Ephemeral as E
import Control.DeepSeq
import Control.Monad.Random
import Control.Monad
import Criterion.Config
import Criterion.Main

instance NFData (V.Map k v)
instance NFData (E.Map k v)

buildV :: Int -> V.Map Int Int
buildV n = F.foldl' (flip (join V.insert)) V.empty $ take n $ randoms (mkStdGen 1)

buildE :: Int -> E.Map Int Int
buildE n = F.foldl' (flip (join E.insert)) E.empty $ take n $ randoms (mkStdGen 1)

buildM :: Int -> M.Map Int Int
buildM n = F.foldl' (flip (join M.insert)) M.empty $ take n $ randoms (mkStdGen 1)

lookupV :: V.Map Int Int -> Int -> Int
lookupV m n = F.foldl' (+) 0 $ catMaybes $ fmap (`V.lookup` m) $ take n $ randoms (mkStdGen 1)

lookupE :: E.Map Int Int -> Int -> Int
lookupE m n = F.foldl' (+) 0 $ catMaybes $ fmap (`E.lookup` m) $ take n $ randoms (mkStdGen 1)

lookupM :: M.Map Int Int -> Int -> Int
lookupM m n = F.foldl' (+) 0 $ catMaybes $ fmap (`M.lookup` m) $ take n $ randoms (mkStdGen 1)

main :: IO ()
main = do
    nfIO (return v10)
    nfIO (return e10)
    nfIO (return m10)
    nfIO (return v100)
    nfIO (return e100)
    nfIO (return m100)
    nfIO (return v1000)
    nfIO (return e1000)
    nfIO (return m1000)
    defaultMainWith defaultConfig { cfgSamples = ljust 10 } (return ())
      [ bench "Persistent COLA lookup 10k from 10k"  $ nf (lookupV v10)   10000
      , bench "Ephemeral  COLA lookup 10k from 10k"  $ nf (lookupE e10)   10000
      , bench "Data.Map        lookup 10k from 10k"  $ nf (lookupM m10)   10000
      , bench "Persistent COLA lookup 10k from 100k" $ nf (lookupV v100) 10000
      , bench "Ephemeral  COLA lookup 10k from 100k" $ nf (lookupE e100) 10000
      , bench "Data.Map        lookup 10k from 100k" $ nf (lookupM m100) 10000
      , bench "Persistent COLA lookup 10k from 1m"   $ nf (lookupV v1000) 10000
      , bench "Ephemeral  COLA lookup 10k from 1m"   $ nf (lookupE e1000) 10000
      , bench "Data.Map        lookup 10k from 1m"   $ nf (lookupM m1000) 10000
      ]
  where
    v10   = buildV 10000
    e10   = buildE 10000
    m10   = buildM 10000
    v100  = buildV 100000
    e100  = buildE 100000
    m100  = buildM 100000
    v1000 = buildV 1000000
    e1000 = buildE 1000000
    m1000 = buildM 1000000
