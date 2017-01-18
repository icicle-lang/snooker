{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Snooker.Compression (
    CompressionCodec(..)
  , compressionCodecs
  , getCompressionCodec

  , snappyCodec
  , zlibCodec
  ) where

import           Control.Exception (handle)

--import           Codec.Zlib (ZlibException)
--import qualified Codec.Zlib as Zlib

import           Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as Lazy
import           Data.Conduit (Conduit, (=$=), runConduit)
import qualified Data.Conduit.List as Conduit
import qualified Data.Conduit.Zlib as Zlib
import           Data.Map (Map)
import qualified Data.Map as Map
import           Data.Streaming.Zlib (ZlibException)

import           P

import qualified Prelude as Savage

import           Snooker.Data

import qualified Snapper

import           System.IO (IO)
import           System.IO.Unsafe (unsafePerformIO)


data CompressionCodec =
  CompressionCodec {
      compressionClass :: !ClassName
    , compressionCompress :: ByteString -> ByteString
    , compressionDecompress :: ByteString -> Maybe ByteString
    }

snappyCodec :: CompressionCodec
snappyCodec =
  CompressionCodec {
      compressionClass =
        ClassName "org.apache.hadoop.io.compress.SnappyCodec"

    , compressionCompress =
        Snapper.compress

    , compressionDecompress =
        Snapper.decompress
    }

{-# WARNING zlibCodec "Do not use 'zlibCodec' in production code" #-}
zlibCodec :: CompressionCodec
zlibCodec =
  CompressionCodec {
      compressionClass =
        ClassName "org.apache.hadoop.io.compress.TestDecompressor"

    , compressionCompress =
        zlibCompress

    , compressionDecompress =
        zlibDecompress
    }

--zlibCompress :: ByteString -> ByteString
--zlibCompress input =
--  unsafePerformIO $ do
--    inflate <- Zlib.initInflate Zlib.defaultWindowBits
--    Zlib.feedInflate input

zlibCompress :: ByteString -> ByteString
zlibCompress input =
  case withZlibConduit (Zlib.compress (-1) Zlib.defaultWindowBits) input of
    Left err ->
      Savage.error $
        "Snooker.Compression.zlibCompress: zlib compression failed: " <> show err <> "\n" <>
        "This is a bug, please report it: https://github.com/ambiata/snooker"
    Right output ->
      output

zlibDecompress :: ByteString -> Maybe ByteString
zlibDecompress input =
  case withZlibConduit (Zlib.decompress Zlib.defaultWindowBits) input of
    Left _ ->
      Nothing
    Right output ->
      Just output

withZlibConduit ::
  Conduit ByteString IO ByteString ->
  ByteString ->
  Either ZlibException ByteString
withZlibConduit conduit input =
  let
    hushException =
      handle (pure . Left)

    fromChunks =
      Right . Lazy.toStrict . Lazy.fromChunks
  in
    unsafePerformIO . hushException . fmap fromChunks . runConduit $
      Conduit.sourceList [input] =$=
      conduit =$=
      Conduit.consume

compressionCodecs :: Map ClassName CompressionCodec
compressionCodecs =
  Map.fromList [
      (compressionClass snappyCodec, snappyCodec)
    ]

getCompressionCodec :: ClassName -> Maybe CompressionCodec
getCompressionCodec name =
  Map.lookup name compressionCodecs
