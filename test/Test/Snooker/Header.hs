{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# OPTIONS_GHC -fno-warn-missing-signatures #-}
module Test.Snooker.Header where

import           Snooker.Header

import           P

import           Test.Snooker.Arbitrary ()
import           Test.Snooker.Util

import           Test.QuickCheck (forAllProperties, quickCheckWithResult)
import           Test.QuickCheck (stdArgs, maxSuccess)
import           Test.QuickCheck.Instances ()


prop_header_tripping =
  binaryTripping bSeqHeader getSeqHeader

return []
tests =
  $forAllProperties $ quickCheckWithResult (stdArgs {maxSuccess = 1000})
