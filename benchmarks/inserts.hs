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

fromListV :: Int -> V.Map Int Int
fromListV n = V.fromList $ Prelude.map (\x -> (x,x)) $ take n $ randoms (mkStdGen 1)

fromAsc :: Int -> V.Map Int Int
fromAsc n = V.fromList $ Prelude.map (\x -> (x,x)) [0..n]

buildM :: Int -> M.Map Int Int
buildM n = F.foldl' (flip (join M.insert)) M.empty $ take n $ randoms (mkStdGen 1)

main :: IO ()
main = defaultMain
  [ bench "COLA insert 10k"      $ nf buildV    10000
  , bench "COLA fromAsc 10k"     $ nf fromAsc   10000
  , bench "COLA fromList 10k"    $ nf fromListV 10000
  , bench "Data.Map insert 10k"  $ nf buildM    10000
  , bench "COLA insert 100k"     $ nf buildV    100000
  , bench "COLA fromAsc 100k"    $ nf fromAsc   100000
  , bench "COLA fromList 100k"   $ nf fromListV 100000
  , bench "Data.Map insert 100k" $ nf buildM    100000
  , bench "COLA insert 1m"       $ nf buildV    1000000
  , bench "COLA fromAsc 1m"      $ nf fromAsc   1000000
  , bench "COLA fromList 1m"     $ nf fromListV 1000000
  , bench "Data.Map insert 1m"   $ nf buildM    1000000
  ]
