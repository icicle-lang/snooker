{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# OPTIONS_GHC -fno-warn-missing-signatures #-}
module Test.Snooker.VInt where

import           Snooker.VInt

import           P

import           Test.Snooker.Util

import           Test.QuickCheck (forAllProperties, quickCheckWithResult)
import           Test.QuickCheck (stdArgs, maxSuccess)
import           Test.QuickCheck.Instances ()


prop_vint =
  binaryTripping bVInt getVInt

prop_vint64 =
  binaryTripping bVInt64 getVInt64

return []
tests =
  $forAllProperties $ quickCheckWithResult (stdArgs {maxSuccess = 10000})
