{-# OPTIONS_GHC -fno-warn-orphans #-}
module Main where

import Data.Foldable as F
import Data.Map as M
import Data.HashMap.Strict as H
import Data.Vector.Map as V
import Control.DeepSeq
import Control.Monad.Random
import Control.Monad
import Criterion.Config
import Criterion.Main

instance NFData (V.Map k v)

buildV :: Int -> V.Map Int Int
buildV n = F.foldl' (flip (join V.insert)) V.empty $ take n $ randoms (mkStdGen 1)

fromListV :: Int -> V.Map Int Int
fromListV n = V.fromList $ Prelude.map (\x -> (x,x)) $ take n $ randoms (mkStdGen 1)

buildM :: Int -> M.Map Int Int
buildM n = F.foldl' (flip (join M.insert)) M.empty $ take n $ randoms (mkStdGen 1)

buildH :: Int -> H.HashMap Int Int
buildH n = F.foldl' (flip (join H.insert)) H.empty $ take n $ randoms (mkStdGen 1)

main :: IO ()
main = defaultMainWith defaultConfig { cfgSamples = ljust 10 } (return ())
  [ bench "COLA insert 10k"               $ nf buildV    10000
  , bench "COLA fromList 10k"             $ nf fromListV 10000
  , bench "Data.Map insert 10k"           $ nf buildM 10000
  , bench "Data.HashMap insert 10k"       $ nf buildH 10000
  , bench "COLA insert 100k"              $ nf buildV 100000
  , bench "COLA fromList 100k"            $ nf fromListV 100000
  , bench "Data.Map insert 100k"          $ nf buildM 100000
  , bench "Data.HashMap insert 100k"      $ nf buildH 100000
  , bench "COLA insert 1m"                $ nf buildV 1000000
  , bench "COLA fromList 1m"              $ nf fromListV 1000000
  , bench "Data.Map insert 1m"            $ nf buildM 1000000
  , bench "Data.HashMap insert 1m"        $ nf buildH 1000000
  ]
