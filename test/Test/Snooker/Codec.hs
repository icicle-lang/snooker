{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# OPTIONS_GHC -fno-warn-missing-signatures #-}
module Test.Snooker.Codec where

import           Disorder.Core.Tripping (tripping)

import           Snooker.Codec

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

prop_compress_tripping =
  tripping compressBlock decompressBlock

return []
tests =
  $forAllProperties $ quickCheckWithResult (stdArgs {maxSuccess = 1000})
