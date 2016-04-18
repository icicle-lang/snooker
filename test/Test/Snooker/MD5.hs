{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# OPTIONS_GHC -fno-warn-missing-signatures #-}
module Test.Snooker.MD5 where

import           Snooker.MD5

import           P

import           Test.QuickCheck (quickCheckAll, once)
import           Test.QuickCheck.Instances ()


prop_shitty_totality =
  once $
    show randomMD5 /= ""

return []
tests =
  $quickCheckAll
