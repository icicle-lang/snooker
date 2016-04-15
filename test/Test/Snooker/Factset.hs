{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# OPTIONS_GHC -fno-warn-missing-signatures #-}
module Test.Snooker.Factset where

import           Control.Monad.Trans.Resource (ResourceT, runResourceT)

import           Crypto.Hash (Digest, MD5, digestFromByteString)

import           Data.Binary.Get (runGetOrFail)
import qualified Data.ByteString as B
import qualified Data.ByteString.Base16 as Base16
import qualified Data.ByteString.Lazy as L
import           Data.Conduit (($$+-), (=$=), newResumableSource)
import           Data.Conduit.Binary (sourceFile)
import qualified Data.Conduit.Combinators as Conduit

import           Disorder.Core.IO

import           P

import           Snooker.Codec
import           Snooker.Conduit
import           Snooker.Data

import           System.IO (IO)

import           Test.QuickCheck (Property, quickCheckAll, once, conjoin)
import           Test.QuickCheck ((.&&.), (===))
import           Test.QuickCheck.Instances ()

import           Test.Snooker.Util

import           X.Control.Monad.Trans.Either (EitherT, runEitherT)


fileSync :: Maybe (Digest MD5)
fileSync =
  digestFromByteString . fst $
    Base16.decode "b48b79e329914cd3d0ff793a86801dc7"

nullWritable :: ClassName
nullWritable =
  ClassName "org.apache.hadoop.io.NullWritable"

bytesWritable :: ClassName
bytesWritable =
  ClassName "org.apache.hadoop.io.BytesWritable"

prop_read_header =
  once . testIO $ do
    lbs <- L.readFile "data/mackerel-2014-01-01"
    case runGetOrFail getHeader lbs of
      Left (_, _, msg) ->
        fail msg
      Right (_, _, hdr) ->
        return . conjoin $ [
            headerKeyType hdr === nullWritable
          , headerValueType hdr === bytesWritable
          , headerMetadata hdr === []
          , Just (headerSync hdr) === fileSync
          ]

testEitherResource :: EitherT SnookerError (ResourceT IO) Property -> Property
testEitherResource =
  testIO . runResourceT . fmap (squashRender renderSnookerError) . runEitherT

prop_blocks =
  once . testEitherResource $ do

    let
      path =
        "data/expression-2014-06-02"

      sumFile f = do
        (_, blocks) <-
          decodeEncodedBlocks . newResumableSource $ sourceFile path
        blocks $$+- Conduit.map f =$= Conduit.sum

      sumBytes f =
        sumFile (B.length . f)

    records <- sumFile encodedCount
    keySizeBytes <- sumBytes encodedKeySizes
    keyBytes <- sumBytes encodedKeys
    valueSizeBytes <- sumBytes encodedValueSizes
    valueBytes <- sumBytes encodedValues

    return $
      records === 20 .&&.
      keySizeBytes === 20 .&&.
      keyBytes === 0 .&&.
      valueSizeBytes === 20 .&&.
      valueBytes === 737


return []
tests =
  $quickCheckAll
