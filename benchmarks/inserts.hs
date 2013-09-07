module Main where

import Data.Foldable as F
import Data.Vector.Map as V
import Data.Map as M
import Control.Monad.Random
import Control.Monad
import Criterion.Main
import Control.DeepSeq

instance NFData (V.Map k v)

buildV :: Int -> V.Map Int Int
buildV n = F.foldl' (flip (join V.insert)) V.empty $ take n $ randoms (mkStdGen 1)

build4 :: Int -> V.Map Int Int
build4 n = F.foldl' (flip (join V.insert4)) V.empty $ take n $ randoms (mkStdGen 1)

buildM :: Int -> M.Map Int Int
buildM n = F.foldl' (flip (join M.insert)) M.empty $ take n $ randoms (mkStdGen 1)

main :: IO ()
main = defaultMain
  [ bench "COLA insert 10k"      $ nf buildV 10000
  , bench "COLA insert4 10k"     $ nf build4 10000
  , bench "Data.Map insert 10k"  $ nf buildM 10000
  , bench "COLA insert 100k"     $ nf buildV 100000
  , bench "COLA insert4 100k"    $ nf build4 100000
  , bench "Data.Map insert 100k" $ nf buildM 100000
  , bench "COLA insert 1m"       $ nf buildV 1000000
  , bench "COLA insert4 1m"      $ nf build4 1000000
  , bench "Data.Map insert 1m"   $ nf buildM 1000000
  ]
