{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# OPTIONS_GHC -fno-warn-missing-signatures #-}
module Test.Snooker.Writable where

import qualified Data.Vector.Generic as Generic

import           Disorder.Core.Tripping (tripping)

import           Snooker.Writable

import           P

import           Test.Snooker.Arbitrary ()

import           Test.QuickCheck (Arbitrary, Property)
import           Test.QuickCheck (forAllProperties, quickCheckWithResult)
import           Test.QuickCheck (stdArgs, maxSuccess)
import           Test.QuickCheck.Instances ()


writableTripping ::
  Generic.Vector v a =>
  Arbitrary (v a) =>
  Eq e =>
  Eq (v a) =>
  Show e =>
  Show (v a) =>
  WritableCodec e v a  ->
  v a ->
  Property
writableTripping writable =
  let
    encode xs =
      (Generic.length xs, writableEncode writable xs)

    decode (n, (sbs, vbs)) =
      writableDecode writable n sbs vbs
  in
    tripping encode decode

prop_null_tripping =
  writableTripping nullWritable

prop_bytes_tripping =
  writableTripping bytesWritable

return []
tests =
  $forAllProperties $ quickCheckWithResult (stdArgs {maxSuccess = 1000})
