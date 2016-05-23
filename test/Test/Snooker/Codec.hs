{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# OPTIONS_GHC -fno-warn-missing-signatures #-}
module Test.Snooker.Codec where

import qualified Data.ByteString as B

import           Disorder.Core.Tripping (tripping)

import           Snooker.Codec
import           Snooker.Writable

import           P

import           Test.Snooker.Arbitrary (ArbitraryMD5(..))
import           Test.Snooker.Util

import           Test.QuickCheck (forAllProperties, quickCheckWithResult)
import           Test.QuickCheck (stdArgs, maxSuccess)
import           Test.QuickCheck.Instances ()


prop_header_tripping =
  binaryTripping bHeader getHeader

prop_compressed_block_tripping (ArbitraryMD5 md5) =
  binaryTripping (bCompressedBlock md5) (getCompressedBlock md5)

prop_compress_bytes_tripping =
  tripping compressHadoopChunks decompressChunks

prop_compress_bytes_tripping0 n =
  let
    word32 =
      4

    -- add 'n' empty chunks to the end of the compressed bytes this is
    -- perfectly legal and sometimes appears in sequence files even though it
    -- is redundant.
    emptyBlocks =
      B.replicate (n * word32) 0

    compress0 =
      (<> emptyBlocks) . compressChunk
  in
    tripping compress0 decompressChunks

prop_compress_block_tripping =
  tripping compressBlock decompressBlock

prop_null_bytes_block =
  let
    kc = nullWritable
    vc = bytesWritable
  in
    tripping (encodeBlock kc vc) (decodeBlock kc vc)

return []
tests =
  $forAllProperties $ quickCheckWithResult (stdArgs {maxSuccess = 1000})
