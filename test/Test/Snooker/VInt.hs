{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# OPTIONS_GHC -fno-warn-missing-signatures #-}
module Test.Snooker.VInt where

import           Disorder.Jack (Property, gamble, sizedBounded)
import           Disorder.Jack (forAllProperties, quickCheckWithResult, stdArgs, maxSuccess)

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

return []
tests =
  $forAllProperties $ quickCheckWithResult (stdArgs {maxSuccess = 10000})
