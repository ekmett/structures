{-# OPTIONS_GHC -fno-warn-orphans #-}
module Main where

import Data.Foldable as F
import Data.Map as M
import Data.Vector.Map as V
import Control.DeepSeq
import Control.Monad.Random
import Control.Monad
import Criterion.Config
import Criterion.Main

instance NFData (V.Map k v)

buildV :: Int -> V.Map Int Int
buildV n = F.foldl' (flip (join V.insert)) V.empty $ take n $ randoms (mkStdGen 1)

build4 :: Int -> V.Map Int Int
build4 n = F.foldl' (flip (join V.insert4)) V.empty $ take n $ randoms (mkStdGen 1)

-- build6 :: Int -> V.Map Int Int
-- build6 n = F.foldl' (flip (join V.insert6)) V.empty $ take n $ randoms (mkStdGen 1)

fromListV :: Int -> V.Map Int Int
fromListV n = V.fromList $ Prelude.map (\x -> (x,x)) $ take n $ randoms (mkStdGen 1)

fromAsc :: Int -> V.Map Int Int
fromAsc n = V.fromDistinctAscList $ Prelude.map (\x -> (x,x)) [0..n]

buildM :: Int -> M.Map Int Int
buildM n = F.foldl' (flip (join M.insert)) M.empty $ take n $ randoms (mkStdGen 1)

main :: IO ()
main = defaultMainWith defaultConfig { cfgSamples = ljust 10 } (return ())
  [ bench "COLA insert 10k"               $ nf buildV    10000
  , bench "COLA insert4 10k"     $ nf build4 10000
--  , bench "COLA insert6 10k"     $ nf build6 10000
  , bench "COLA fromDistinctAscList 10k"  $ nf fromAsc   10000
  , bench "COLA fromList 10k"             $ nf fromListV 10000
  , bench "Data.Map insert 10k"           $ nf buildM    10000
  , bench "COLA insert 100k"              $ nf buildV    100000
  , bench "COLA insert4 100k"    $ nf build4 100000
--  , bench "COLA insert6 100k"    $ nf build6 100000
  , bench "COLA fromDistinctAscList 100k" $ nf fromAsc   100000
  , bench "COLA fromList 100k"            $ nf fromListV 100000
  , bench "Data.Map insert 100k"          $ nf buildM    100000
  , bench "COLA insert 1m"                $ nf buildV    1000000
  , bench "COLA insert4 1m"      $ nf build4 1000000
--  , bench "COLA insert6 1m"      $ nf build6 1000000
  , bench "COLA fromDistinctAscList 1m"   $ nf fromAsc   1000000
  , bench "COLA fromList 1m"              $ nf fromListV 1000000
  , bench "Data.Map insert 1m"            $ nf buildM    1000000
  ]
