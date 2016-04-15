{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Snooker.Binary (
    runGet

  , BinaryError(..)
  , renderBinaryError
  ) where

import           Data.Binary.Get (Get)
import qualified Data.Binary.Get as Binary
import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString.Lazy as Lazy
import qualified Data.Text as T

import           P


data BinaryError =
    BinaryError !Text
  | BinaryDidNotConsumeAll !Int64
    deriving (Eq, Ord, Show)

renderBinaryError :: BinaryError -> Text
renderBinaryError = \case
  BinaryError msg ->
    msg
  BinaryDidNotConsumeAll remaining ->
    "did not consume all bytes, " <>
    T.pack (show remaining) <> " bytes remaining"

runGet :: Get a -> Lazy.ByteString -> Either BinaryError a
runGet g lbs =
  case Binary.runGetOrFail g lbs of
    Left (_, _, msg) ->
      Left . BinaryError $ T.pack msg
    Right ("", _, x) ->
      Right x
    Right (bs, _, _) ->
      Left . BinaryDidNotConsumeAll $ L.length bs
