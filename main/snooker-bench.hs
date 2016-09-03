{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

import           Control.Monad.Morph (MFunctor(..))
import           Control.Monad.Trans.Resource (ResourceT, runResourceT)
import           Control.Monad.IO.Class (MonadIO(..))

import           Data.AffineSpace ((.-.))
import           Data.ByteString (ByteString)
import           Data.Conduit ((=$=), ($$+-), runConduit, newResumableSource)
import           Data.Conduit (Sink)
import           Data.Conduit.Binary (sourceFile, sinkFile)
import qualified Data.Conduit.List as Conduit
import qualified Data.List as List
import qualified Data.Text as T
import qualified Data.Text.IO as T
import           Data.Thyme.Clock (getCurrentTime, toSeconds)
import qualified Data.Vector.Generic as Generic
import qualified Data.Vector.Unboxed as Unboxed
import           Data.Void (Void)

import           P

import           Snooker.Conduit
import           Snooker.Data
import           Snooker.Segmented
import           Snooker.Writable

import           System.IO (IO, FilePath, IOMode(..))
import           System.IO (print, withFile, hFileSize)
import           System.Random.MWC (createSystemRandom)
import           System.Random.MWC.Distributions (uniformShuffle)

import           Text.Printf (printf)

import           X.Control.Monad.Trans.Either (EitherT, runEitherT)


type BenchBlock =
  Block (Unboxed.Vector ()) (Segmented ByteString)

data BenchError =
    ReadError !(SnookerError Void WritableError)
    deriving (Eq, Ord, Show)

type BenchIO =
  EitherT BenchError (ResourceT IO)

embiggen :: Int -> BenchBlock -> IO BenchBlock
embiggen factor (Block n0 ks0 vs0) = do
  let
    n =
      factor * n0

    ks =
      Generic.concat $ List.replicate factor ks0

    vs1 =
      Generic.concat $ List.replicate factor $ bytesOfSegmented vs0

  gen <- createSystemRandom
  vs <- uniformShuffle vs1 gen

  pure . Block n ks $ segmentedOfBytes vs

blockSize :: BenchBlock -> Int
blockSize (Block _ _ vs) =
  Unboxed.sum $ segmentedLengths vs

readSequence :: FilePath -> Sink BenchBlock BenchIO a -> BenchIO a
readSequence path sink = do
  (_, blocks) <-
    firstT ReadError .
    decodeBlocks nullWritable segmentedBytesWritable .
    newResumableSource $
    sourceFile path
  hoist (firstT ReadError) blocks $$+- sink

writeSequence :: FilePath -> [BenchBlock] -> BenchIO ()
writeSequence path blocks =
  runConduit $
    Conduit.sourceList blocks =$=
    encodeBlocks nullWritable segmentedBytesWritable (Metadata []) =$=
    sinkFile path

bench :: MonadIO m => Text -> [(Text, Text, m Double)] -> m a -> m a
bench title stats io = do
  time0 <- liftIO getCurrentTime
  x <- io
  time1 <- liftIO getCurrentTime

  let
    secs :: Double
    secs =
      toSeconds $ time1 .-. time0

  liftIO $ T.putStrLn ""
  liftIO $ T.putStrLn title
  liftIO $ T.putStrLn $ T.replicate (T.length title) "="

  forM_ stats $ \(name, units, getValue) -> do
    value <- getValue
    liftIO $ printf "%s = %.2f %s\n" (T.unpack name) (value / secs) (T.unpack units)

  liftIO $ printf "runnning time = %.2f seconds\n" secs

  return x

getFileSize :: MonadIO m => FilePath -> m Double
getFileSize path =
  let
    fromBytes b =
      b / (1024 * 1024)
  in
    liftM (fromBytes . fromIntegral) .  liftIO $
    withFile path ReadMode hFileSize

mainE :: IO (Either BenchError ())
mainE =
  runResourceT . runEitherT $ do
    [original] <- readSequence "data/expression-2014-06-02" Conduit.consume

    let
      trials =
        [ (10, 50000)
        , (25, 20000)
        , (50, 10000)
        , (100, 5000)
        , (200, 2500) ]

    for_ trials $ \(xblock, nblocks) -> do
      block <- liftIO $ embiggen xblock original

      let
        path =
          "bench.seq"

        blockKiB =
          fromIntegral (blockSize block) / 1024

        sizeMiB =
          fromIntegral nblocks * (blockKiB / 1024)

      bench
        (T.pack $ printf "Write %d x %.2f KiB blocks (%.2f MiB)" nblocks blockKiB sizeMiB)
        [ ("uncompressed", "MiB/s", pure sizeMiB)
        , ("compressed", "MiB/s", getFileSize path) ] $
          writeSequence path (List.replicate nblocks block)

      bench
        (T.pack $ printf "Read %d x %.2f KiB blocks (%.2f MiB)" nblocks blockKiB sizeMiB)
        [ ("uncompressed", "MiB/s", pure sizeMiB)
        , ("compressed", "MiB/s", getFileSize path) ] $ do
          !_ <-
            readSequence path $
              Conduit.map (fromIntegral . blockSize) =$=
              Conduit.fold (+) (0 :: Int64)
          pure ()


main :: IO ()
main =
  either print pure =<< mainE
