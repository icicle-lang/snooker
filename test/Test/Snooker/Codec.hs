{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# OPTIONS_GHC -fno-warn-missing-signatures #-}
module Test.Snooker.Codec where

import qualified Data.ByteString as B

import           Disorder.Core.Tripping (tripping)

import           Snooker.Codec
import qualified Snooker.Codec.Snappy as Snappy
import           Snooker.Writable

import           P

import           Test.Snooker.Arbitrary (ArbitraryMD5(..))
import           Test.Snooker.Util

import           Test.QuickCheck (conjoin, (===), getNonNegative)
import           Test.QuickCheck (forAllProperties, quickCheckWithResult)
import           Test.QuickCheck (forAllShrink, arbitrary, shrink, scale)
import           Test.QuickCheck (stdArgs, maxSuccess, counterexample)
import           Test.QuickCheck.Instances ()
import           Test.QuickCheck.Property (property, failed)


prop_header_tripping =
  binaryTripping bHeader getHeader

prop_compressed_block_tripping (ArbitraryMD5 md5) =
  binaryTripping (bCompressedBlock md5) (getCompressedBlock md5)

prop_compress_bytes_tripping =
  tripping Snappy.compress Snappy.decompress

prop_compress_many_bytes_tripping =
  forAllShrink (scale (* 10000) (getNonNegative <$> arbitrary)) shrink $ \n ->
    case Snappy.decompress . Snappy.compress $ B.replicate n 0 of
      Left err ->
        counterexample ("Failed to roundtrip " <> show n <> " bytes") .
        counterexample (show err) $
        property failed
      Right bs ->
        conjoin [
            B.length bs === n
          , counterexample "Corruption detected, expected every byte to be == 0" $
            property $ B.all (== 0) bs
          ]

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
      (<> emptyBlocks) . Snappy.compress
  in
    tripping compress0 Snappy.decompress

prop_compress_block_tripping =
  tripping compressBlock (decompressBlock Snappy.decompress)

prop_null_bytes_block =
  let
    kc = nullWritable
    vc = bytesWritable
  in
    tripping (encodeBlock kc vc) (decodeBlock kc vc)

return []
tests =
  $forAllProperties $ quickCheckWithResult (stdArgs {maxSuccess = 100})
