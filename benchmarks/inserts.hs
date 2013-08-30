module Main where

import Data.Vector.Map as V
import Data.Map as M
import Control.Monad.Random
import Control.Monad
import Criterion.Main
import Control.DeepSeq

instance NFData (V.Map k v)

buildV :: Int -> V.Map Int Int
buildV n = Prelude.foldr (join V.insert) V.empty $ take n $ randoms (mkStdGen 1)

buildM :: Int -> M.Map Int Int
buildM n = Prelude.foldr (join M.insert) M.empty $ take n $ randoms (mkStdGen 1)

main :: IO ()
main = defaultMain
  [ bench "COLA insert 10k"  $ nf buildV 10000
  , bench "Data.Map insert 10k" $ nf buildM 10000
  ]
