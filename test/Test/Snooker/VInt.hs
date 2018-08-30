{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# OPTIONS_GHC -fno-warn-missing-signatures #-}
module Test.Snooker.VInt where

import           Disorder.Jack (Property, gamble, sizedBounded, (===))
import           Disorder.Jack (forAllProperties, quickCheckWithResult, stdArgs, maxSuccess)

import qualified Data.ByteString.Lazy as ByteString
import qualified Data.ByteString.Builder as Builder

import           Snooker.VInt

import           P

import           Test.Snooker.Util


prop_vint :: Property
prop_vint =
  gamble sizedBounded $
    binaryTripping bVInt getVInt

prop_vint64 :: Property
prop_vint64 =
  gamble sizedBounded $
    binaryTripping bVInt64 getVInt64

prop_v_int_sizes_accurate :: Property
prop_v_int_sizes_accurate =
  gamble sizedBounded $ \i ->
    (ByteString.length . Builder.toLazyByteString $ bVInt64 i) === vInt64Size i

return []
tests =
  $forAllProperties $ quickCheckWithResult (stdArgs {maxSuccess = 10000})
