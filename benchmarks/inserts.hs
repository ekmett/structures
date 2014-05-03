{-# OPTIONS_GHC -fno-warn-orphans #-}
module Main where

import Data.Foldable as F
import Data.Map as M
import Data.HashMap.Strict as H
import Data.Vector.Map.Ephemeral as V
import Data.Vector.Map.Persistent as P
import Data.Vector.Map.Tuned as T
import Data.Vector.Map as O
import Control.DeepSeq
import Control.Monad.Random
import Control.Monad
import Criterion.Config
import Criterion.Main

instance NFData (V.Map k v)
instance NFData (O.Map k v)
instance NFData (P.Map k v)
instance NFData (T.Map k v)

buildP :: Int -> P.Map Int Int
buildP n = F.foldl' (flip (join P.insert)) P.empty $ take n $ randoms (mkStdGen 1)

buildT :: Int -> T.Map Int Int
buildT n = F.foldl' (flip (join T.insert)) T.empty $ take n $ randoms (mkStdGen 1)

buildV :: Int -> V.Map Int Int
buildV n = F.foldl' (flip (join V.insert)) V.empty $ take n $ randoms (mkStdGen 1)

fromListV :: Int -> V.Map Int Int
fromListV n = V.fromList $ Prelude.map (\x -> (x,x)) $ take n $ randoms (mkStdGen 1)

buildO :: Int -> O.Map Int Int
buildO n = F.foldl' (flip (join O.insert)) O.empty $ take n $ randoms (mkStdGen 1)

fromListO :: Int -> O.Map Int Int
fromListO n = O.fromList $ Prelude.map (\x -> (x,x)) $ take n $ randoms (mkStdGen 1)

buildM :: Int -> M.Map Int Int
buildM n = F.foldl' (flip (join M.insert)) M.empty $ take n $ randoms (mkStdGen 1)

buildH :: Int -> H.HashMap Int Int
buildH n = F.foldl' (flip (join H.insert)) H.empty $ take n $ randoms (mkStdGen 1)

main :: IO ()
main = defaultMainWith defaultConfig { cfgSamples = ljust 10 } (return ())
  [ bench "Ephemeral insert 10k"     $ nf buildV 10000
  , bench "Persistent insert 10k"    $ nf buildP 10000
  , bench "Tuned insert 10k"         $ nf buildT 10000
  , bench "Data.Map insert 10k"      $ nf buildM 10000
  , bench "Data.HashMap insert 10k"  $ nf buildH 10000
  , bench "WC insert 10k"            $ nf buildO 10000
  , bench "Ephemeral insert 100k"    $ nf buildV 100000
  , bench "Persistent insert 100k"   $ nf buildP 100000
  , bench "Tuned insert 100k"        $ nf buildT 100000
  , bench "Data.Map insert 100k"     $ nf buildM 100000
  , bench "Data.HashMap insert 100k" $ nf buildH 100000
  , bench "Worstcase insert 100k"    $ nf buildO 100000
  , bench "Ephemeral insert 1m"      $ nf buildV 1000000
  , bench "Persistent insert 1m"     $ nf buildP 1000000
  , bench "Tuned insert 1m"          $ nf buildT 1000000
  , bench "Data.Map insert 1m"       $ nf buildM 1000000
  , bench "Data.HashMap insert 1m"   $ nf buildH 1000000
  , bench "Overmars insert 1m"       $ nf buildO 1000000
  ]
