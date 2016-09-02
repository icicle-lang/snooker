{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Snooker.Segmented where

import qualified Data.Vector as Boxed

import           Disorder.Jack (Property, quickCheckAll, gamble)
import           Disorder.Jack (tripping, arbitrary, listOf)

import           P

import           Snooker.Segmented

import           System.IO (IO)

import           Test.QuickCheck.Instances ()


prop_roundtrip_bytes :: Property
prop_roundtrip_bytes =
  gamble (Boxed.fromList <$> listOf arbitrary) $
    tripping segmentedOfBytes (Just . bytesOfSegmented)

return []
tests :: IO Bool
tests =
  $quickCheckAll
