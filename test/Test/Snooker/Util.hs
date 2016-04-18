{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
module Test.Snooker.Util (
    binaryTripping
  , squashRender
  , renderSnookerError'
  , testEitherResource
  , testEitherIO
  ) where

import           Control.Monad.Trans.Resource (ResourceT, runResourceT)

import           Data.Binary.Get (Get)
import qualified Data.Binary.Get as Binary
import           Data.ByteString.Builder (Builder)
import qualified Data.ByteString.Builder as Builder
import qualified Data.Text as T

import           Disorder.Core.Tripping (tripping)
import           Disorder.Core.IO (testIO)

import           P

import           Snooker.Conduit

import           System.IO (IO)

import           Test.QuickCheck (Property)
import           Test.QuickCheck.Property (counterexample, failed)

import           X.Control.Monad.Trans.Either (EitherT, runEitherT)


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

renderSnookerError' :: (Show ek, Show ev) => SnookerError ek ev -> Text
renderSnookerError' =
  renderSnookerError (T.pack . show) (T.pack . show)

testEitherResource :: (Show ek, Show ev) => EitherT (SnookerError ek ev) (ResourceT IO) Property -> Property
testEitherResource =
  testIO . runResourceT . fmap (squashRender renderSnookerError') . runEitherT

testEitherIO :: (Show ek, Show ev) => EitherT (SnookerError ek ev) IO Property -> Property
testEitherIO =
  testIO . fmap (squashRender renderSnookerError') . runEitherT
