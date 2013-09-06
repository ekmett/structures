import Control.Monad (join)
import Criterion.Main
import qualified Data.Map as M
import qualified Data.Vector.Map as V
import qualified Data.Vector.Unboxed as U
import qualified Data.Vector.Generic as G
import qualified Data.Vector.Generic.Mutable as GM
import System.Random.MWC (withSystemRandom, GenIO, Variate(..))

randVec :: (U.Unbox a, Variate a) => Int -> GenIO -> IO (U.Vector a)
randVec n g = GM.replicateM n (uniform g) >>= G.unsafeFreeze

randVecStd :: (U.Unbox a, Variate a) => Int -> IO (U.Vector a)
randVecStd = withSystemRandom . randVec

insertAll :: U.Unbox a => (a -> a -> t -> t) -> t -> U.Vector a -> t
insertAll f e = U.foldl' (flip $ join f) e

sumAll :: (U.Unbox a, U.Unbox b, Num b) => (a -> b) -> U.Vector a -> b
sumAll f = U.sum . U.map f

main :: IO ()
main = do ns <- randVecStd 1000 :: IO (U.Vector Int)
          -- print (vinsert ns)
          -- print (V.shape (vinsert ns))
          putStrLn $ if sumAll (vget (vinsert ns)) ns ==
                        sumAll (minsert ns M.!) ns
                     then "We are sane"
                     else "We are insane, man!"
          defaultMain [ bench "Map insertion" $
                          whnf minsert ns 
                      , bench "VMap insertion" $
                          whnf vinsert ns
                      , bench "map sum" $
                          whnf (sumAll (minsert ns M.!)) ns
                      , bench "vmap sum" $
                          whnf (sumAll (vget (vinsert ns))) ns]
  where minsert = insertAll M.insert M.empty
        vinsert = insertAll V.insert V.empty
        vget m k = case V.lookup k m of
                     Nothing -> error $ "Missing "++show k
                     Just x -> x
