{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# OPTIONS_GHC -fno-warn-missing-signatures #-}
module Test.Snooker.Writable where

import qualified Data.Vector.Generic as Generic

import           Disorder.Jack (Property, gamble, tripping, arbitrary)
import           Disorder.Jack (forAllProperties, quickCheckWithResult, stdArgs, maxSuccess)

import           Snooker.Segmented
import           Snooker.Writable

import           P

import           Test.Snooker.Arbitrary ()

import           Test.QuickCheck.Instances ()


writableVectorTripping ::
  Eq x =>
  Eq a =>
  Show x =>
  Show a =>
  (a -> Int) ->
  WritableCodec x a  ->
  a ->
  Property
writableVectorTripping lengthOf writable =
  let
    encode xs =
      (lengthOf xs, writableEncode writable xs)

    decode (n, (sbs, vbs)) =
      writableDecode writable n sbs vbs
  in
    tripping encode decode

prop_null_tripping :: Property
prop_null_tripping =
  gamble arbitrary $
    writableVectorTripping Generic.length nullWritable

prop_bytes_tripping :: Property
prop_bytes_tripping =
  gamble arbitrary $
    writableVectorTripping Generic.length bytesWritable

prop_segmented_bytes_tripping :: Property
prop_segmented_bytes_tripping =
  gamble (segmentedOfBytes <$> arbitrary) $
    writableVectorTripping segmentedLength segmentedBytesWritable

prop_vint_tripping :: Property
prop_vint_tripping =
  gamble arbitrary $
    writableVectorTripping Generic.length vLongWritable

return []
tests =
  $forAllProperties $ quickCheckWithResult (stdArgs {maxSuccess = 1000})
