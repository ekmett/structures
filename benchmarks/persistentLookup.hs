{-# OPTIONS_GHC -fno-warn-orphans #-}
module Main where

import Data.Foldable as F
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

insertLookupV :: V.Map Int Int -> Int -> Int -> Int
insertLookupV m n p = F.foldl' (+) 0 $ catMaybes $ fmap look $ take n $ randoms (mkStdGen 1)
  where
    look i = V.lookup i $ F.foldl' (flip (join V.insert)) m [1..p + i `mod`2]

insertLookupP :: P.Map Int Int -> Int -> Int -> Int
insertLookupP m n p = F.foldl' (+) 0 $ catMaybes $ fmap look $ take n $ randoms (mkStdGen 1)
  where
    look i = P.lookup i $ F.foldl' (flip (join P.insert)) m [1..p + i `mod`2]

main :: IO ()
main = do
    nfIO (return v12)
    nfIO (return p12)
    nfIO (return v14)
    nfIO (return p14)
    defaultMainWith defaultConfig { cfgSamples = ljust 10 } (return ())
      [ bench "COLA insertLookup from 2^12"     $ nf (insertLookupV v12 (2^12)) 20
      , bench "COLA.PA insertLookup from 2^12"  $ nf (insertLookupP p12 (2^12)) 20
      , bench "COLA insertLookup from 2^14"     $ nf (insertLookupV v14 (2^12)) 20
      , bench "COLA.PA insertLookup from 2^14"  $ nf (insertLookupP p14 (2^12)) 20
      ]
  where
    v12  = buildV $ 2^12 - 10
    p12  = buildP $ 2^12 - 10
    v14  = buildV $ 2^14 - 10
    p14  = buildP $ 2^14 - 10
