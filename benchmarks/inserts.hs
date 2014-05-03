{-# OPTIONS_GHC -fno-warn-orphans #-}
module Main where

import Data.Foldable as F
import Data.Map as M
import Data.HashMap.Strict as H
import Data.Vector.Map.Ephemeral as V
import Data.Vector.Map.Deamortized as D
import Data.Vector.Map as O
import Control.DeepSeq
import Control.Monad.Random
import Control.Monad
import Criterion.Config
import Criterion.Main

instance NFData (V.Map k v)
instance NFData (D.Map k v)
instance NFData (O.Map k v)

buildV :: Int -> V.Map Int Int
buildV n = F.foldl' (flip (join V.insert)) V.empty $ take n $ randoms (mkStdGen 1)

buildD :: Int -> D.Map Int Int
buildD n = F.foldl' (flip (join D.insert)) D.empty $ take n $ randoms (mkStdGen 1)

buildO :: Int -> O.Map Int Int
buildO n = F.foldl' (flip (join O.insert)) O.empty $ take n $ randoms (mkStdGen 1)

buildM :: Int -> M.Map Int Int
buildM n = F.foldl' (flip (join M.insert)) M.empty $ take n $ randoms (mkStdGen 1)

buildH :: Int -> H.HashMap Int Int
buildH n = F.foldl' (flip (join H.insert)) H.empty $ take n $ randoms (mkStdGen 1)

main :: IO ()
main = defaultMainWith defaultConfig { cfgSamples = ljust 10 } (return ())
  [ bench "Ephemeral insert 10k"     $ nf buildV 10000
  , bench "Deamortized insert 10k"   $ nf buildD 10000
  , bench "Persistent insert 10k"    $ nf buildO 10000
  , bench "Data.Map insert 10k"      $ nf buildM 10000
  , bench "Data.HashMap insert 10k"  $ nf buildH 10000
  , bench "Ephemeral insert 100k"    $ nf buildV 100000
  , bench "Deamortized insert 100k"  $ nf buildD 100000
  , bench "Persistent insert 100k"   $ nf buildO 100000
  , bench "Data.Map insert 100k"     $ nf buildM 100000
  , bench "Data.HashMap insert 100k" $ nf buildH 100000
  , bench "Ephemeral insert 1m"      $ nf buildV 1000000
  , bench "Deamortized insert 1m"    $ nf buildD 1000000
  , bench "Persistent insert 1m"     $ nf buildO 1000000
  , bench "Data.Map insert 1m"       $ nf buildM 1000000
  , bench "Data.HashMap insert 1m"   $ nf buildH 1000000
  ]
