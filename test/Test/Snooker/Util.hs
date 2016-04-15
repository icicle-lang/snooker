{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
module Test.Snooker.Util (
    binaryTripping
  , squashRender
  ) where

import           Data.Binary.Get (Get)
import qualified Data.Binary.Get as Binary
import           Data.ByteString.Builder (Builder)
import qualified Data.ByteString.Builder as Builder
import qualified Data.Text as T

import           Disorder.Core.Tripping (tripping)

import           P

import           Test.QuickCheck (Property)
import           Test.QuickCheck.Property (counterexample, failed)


binaryTripping :: (Show a, Eq a) => (a -> Builder) -> Get a -> a -> Property
binaryTripping encode decode =
  let
    third (_, _, x) = x
  in
    tripping
      (Builder.toLazyByteString . encode)
      (bimap third third . Binary.runGetOrFail decode)

squashRender :: Show x => (x -> Text) -> Either x Property -> Property
squashRender render = \case
  Left err ->
    counterexample (show err) $
    counterexample (T.unpack $ render err) $
    failed
  Right prop ->
    prop
