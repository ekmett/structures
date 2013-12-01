{-# OPTIONS_GHC -fno-warn-orphans #-}
module Main where

import Data.Foldable as F
import Data.Map as M
import Data.Maybe
import Data.Vector.Map as V
import Data.Vector.Map.PersistentlyAmortized as P
import Control.DeepSeq
import Control.Monad.Random
import Control.Monad
import Criterion.Config
import Criterion.Main

instance NFData (V.Map k v)
instance NFData (P.Map k v)

buildV :: Int -> V.Map Int Int
buildV n = F.foldl' (flip (join V.insert)) V.empty $ take n $ randoms (mkStdGen 1)

buildP :: Int -> P.Map Int Int
buildP n = F.foldl' (flip (join P.insert)) P.empty $ take n $ randoms (mkStdGen 1)

buildM :: Int -> M.Map Int Int
buildM n = F.foldl' (flip (join M.insert)) M.empty $ take n $ randoms (mkStdGen 1)

lookupV :: V.Map Int Int -> Int -> Int
lookupV m n = F.foldl' (+) 0 $ catMaybes $ fmap (`V.lookup` m) $ take n $ randoms (mkStdGen 1)

lookupP :: P.Map Int Int -> Int -> Int
lookupP m n = F.foldl' (+) 0 $ catMaybes $ fmap (`P.lookup` m) $ take n $ randoms (mkStdGen 1)

lookupM :: M.Map Int Int -> Int -> Int
lookupM m n = F.foldl' (+) 0 $ catMaybes $ fmap (`M.lookup` m) $ take n $ randoms (mkStdGen 1)

main :: IO ()
main = do
    nfIO (return v10)
    nfIO (return p10)
    nfIO (return m10)
    nfIO (return v100)
    nfIO (return p100)
    nfIO (return m100)
    nfIO (return v1000)
    nfIO (return p1000)
    nfIO (return m1000)
    defaultMainWith defaultConfig { cfgSamples = ljust 10 } (return ())
      [ bench "COLA lookup 10k from 10k"      $ nf (lookupV v10)   10000
      , bench "COLA.PA lookup 10k from 10k"   $ nf (lookupP p10)   10000
      , bench "Data.Map lookup 10k from 10k"  $ nf (lookupM m10)   10000
      , bench "COLA lookup 10k from 100k"     $ nf (lookupV v100)  10000
      , bench "COLA.PA lookup 10k from 100k"  $ nf (lookupP p100)  10000
      , bench "Data.Map lookup 10k from 100k" $ nf (lookupM m100)  10000
      , bench "COLA lookup 10k from 1m"       $ nf (lookupV v1000) 10000
      , bench "COLA.PA lookup 10k from 1m"    $ nf (lookupP p1000) 10000
      , bench "Data.Map lookup 10k from 1m"   $ nf (lookupM m1000) 10000
      ]
  where
    v10   = buildV 10000
    p10   = buildP 10000
    m10   = buildM 10000
    v100  = buildV 100000
    p100  = buildP 100000
    m100  = buildM 100000
    v1000 = buildV 1000000
    p1000 = buildP 1000000
    m1000 = buildM 1000000
