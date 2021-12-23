{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Snooker.Codec.BZip2 (
    bzip2Codec
  , decompress
) where

import           Control.Exception (SomeException, Exception (..), SomeAsyncException (..), tryJust, evaluate)

import qualified Codec.Compression.BZip as BZip

import qualified Data.ByteString as Strict
import qualified Data.ByteString.Lazy as Lazy
import qualified Data.Text as Text

import           Snooker.Binary
import           Snooker.Data

import qualified System.IO.Unsafe as Unsafe

import           P


overLazy :: (Lazy.ByteString -> Lazy.ByteString) -> Strict.ByteString -> Strict.ByteString
overLazy f =
  Lazy.toStrict . f . Lazy.fromStrict


bzip2Codec :: ClassName
bzip2Codec =
  ClassName "org.apache.hadoop.io.compress.BZip2Codec"


decompress :: Strict.ByteString -> Either BinaryError Strict.ByteString
decompress compressed =
  Unsafe.unsafePerformIO $
    tryJust
      (\(e :: SomeException) ->
        if isSyncException e then
          Just $ BinaryError ("Failed to decompress bzip2: " <> Text.pack (displayException e))
        else
          Nothing
      )
      $ evaluate (overLazy BZip.decompress compressed)

-- | Check if the given exception is synchronous
--
-- @since 0.1.0.0
isSyncException :: Exception e => e -> Bool
isSyncException e =
  case fromException (toException e) of
    Just (SomeAsyncException _) -> False
    Nothing -> True
