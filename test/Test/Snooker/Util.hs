{-# LANGUAGE NoImplicitPrelude #-}
module Test.Snooker.Util (
    binaryTripping
  ) where

import           Data.Binary.Get (Get)
import qualified Data.Binary.Get as Binary
import           Data.ByteString.Builder (Builder)
import qualified Data.ByteString.Builder as Builder

import           Disorder.Core.Tripping (tripping)

import           P

import           Test.QuickCheck (Property)


binaryTripping :: (Show a, Eq a) => (a -> Builder) -> Get a -> a -> Property
binaryTripping encode decode =
  let
    third (_, _, x) = x
  in
    tripping
      (Builder.toLazyByteString . encode)
      (bimap third third . Binary.runGetOrFail decode)
