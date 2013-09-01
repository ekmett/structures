{-# LANGUAGE TemplateHaskell #-}
module Main
       ( main  -- :: IO ()
       ) where
import Prelude as Prelude
import Data.List as List
import Data.Maybe
import Data.Function
import Control.Monad
import Control.DeepSeq
import Debug.Trace

import Test.Tasty
import Test.Tasty.TH
import Test.Tasty.QuickCheck as QC

import Data.Vector.Map as V

--------------------------------------------------------------------------------

prop_null :: Int -> Bool
prop_null x =
  V.null V.empty == True &&
  V.null (V.insert x () V.empty) == False

prop_emptyLookup :: Int -> Bool
prop_emptyLookup k = (V.lookup k (V.empty :: UMap)) == Nothing

prop_insertLookup :: Int -> Bool
prop_insertLookup k = V.lookup k (V.insert k () V.empty) /= Nothing

-- We need a working Eq instance
--prop_fromList :: [(Int, Int)] -> Bool
--prop_fromList xs = V.fromList xs == foldr (\(k,v) -> V.insert k v) V.empty xs

prop_lookupMany :: [(Int,Int)] -> Property
prop_lookupMany xs = (List.length xs > 0 && List.length xs < 50) ==> prop
  where
    prop = all (\(x,_) -> isJust $ V.lookup x ls) xs'

    ls  = V.fromList xs'
    xs' = List.nubBy ((==) `on` fst) xs

--------------------------------------------------------------------------------

main :: IO ()
main = $(defaultMainGenerator)
