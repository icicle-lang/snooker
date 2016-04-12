{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# OPTIONS_GHC -fno-warn-missing-signatures #-}
module Test.Snooker.Factset where

import           Crypto.Hash (Digest, MD5, digestFromByteString)

import           Data.Binary.Get (runGetOrFail)
import qualified Data.ByteString.Base16 as Base16
import qualified Data.ByteString.Lazy as L

import           Disorder.Core.IO

import           Snooker.Header

import           P

import           Test.QuickCheck (quickCheckAll, once, conjoin, (===))
import           Test.QuickCheck.Instances ()


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

prop_sample_file =
  once . testIO $ do
    lbs <- L.readFile "data/mackerel-2014-01-01"
    case runGetOrFail getSeqHeader lbs of
      Left (_, _, msg) ->
        fail msg
      Right (_, _, hdr) ->
        return . conjoin $ [
            seqKeyType hdr === nullWritable
          , seqValueType hdr === bytesWritable
          , seqMetadata hdr === []
          , Just (seqSync hdr) === fileSync
          ]

return []
tests =
  $quickCheckAll
