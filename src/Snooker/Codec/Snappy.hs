{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Snooker.Codec.Snappy (
    snappyCodec

  , decompress
  , compress
) where

import qualified Data.Binary.Get as Binary
import qualified Data.ByteString as B
import qualified Data.ByteString as Strict
import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString.Lazy as Lazy
import qualified Data.ByteString.Internal as B

import           Snooker.Storable

import           P

import qualified Snapper

import           Snooker.Binary
import           Snooker.Data


snappyCodec :: ClassName
snappyCodec =
  ClassName "org.apache.hadoop.io.compress.SnappyCodec"

--
-- Reads the following block structure:
--
--   Block {
--       chunks :: [Chunk]
--     }
--
--   Chunk {
--       uncompressedSize :: Word32
--     , compressedParts :: [Part]
--     }
--
--   Part {
--       compressedSize :: Word32
--     , compressedBytes :: ByteString
--     }
--
-- Would be nice if this was over strict ByteString instead. This could be done
-- using ByteString.createN byteSwap32 and peekByteOff, but perhaps it's a bit
-- complicated and not worth it.
--
decompressChunksLazy :: Lazy.ByteString -> Either BinaryError Lazy.ByteString
decompressChunksLazy lbs =
  let
    getPart = do
      compressedSize <- Binary.getWord32be
      if compressedSize == 0 then
        pure B.empty
      else do
        compressedBytes <- Binary.getByteString $ fromIntegral compressedSize
        case Snapper.decompress compressedBytes of
          Nothing ->
            fail "could not decompress chunk"
          Just bs ->
            pure bs

    getParts remaining =
      if remaining == 0 then
        return []
      else do
        bs <- getPart
        bss <- getParts (remaining - B.length bs)
        return $ bs : bss

    getChunk = do
      uncompressedSize <- Binary.getWord32be
      getParts $ fromIntegral uncompressedSize

    getChunks = do
      done <- Binary.isEmpty
      if done then
        return []
      else do
        bss <- getChunk
        bsss <- getChunks
        return $ bss <> bsss
  in
    second L.fromChunks $ runGet getChunks lbs
{-# INLINE decompressChunksLazy #-}

decompress :: Strict.ByteString -> Either BinaryError Strict.ByteString
decompress =
  fmap L.toStrict . decompressChunksLazy . L.fromStrict


-- | When Hadoop is decoding snappy compressed chunks, it will accept any size
--   of compressed data, then it will split the **compressed** data in to 64KiB
--   chunks and try to decompress each in turn.
--
--   It assumes that snappy is some kind of magical compression box that allows
--   you to split the compressed input on any byte boundary.
--
hadoopMaximumChunkSize :: Int
hadoopMaximumChunkSize =
  64 * 1024

splitChunks :: Int -> Strict.ByteString -> [Strict.ByteString]
splitChunks size bs =
  case B.splitAt size bs of
    (hd, tl) ->
      if B.null tl then
        [hd]
      else
        hd : splitChunks size tl
{-# INLINE splitChunks #-}


compressChunk :: Strict.ByteString -> Strict.ByteString
compressChunk uncompressed =
  let
    compressed =
      Snapper.compress uncompressed

    uncompressedSize =
      B.length uncompressed

    compressedSize =
      B.length compressed
  in
    case uncompressedSize of
      0 ->
        B.pack [0x0, 0x0, 0x0, 0x0]
      _ ->
        B.unsafeCreate (4 + 4 + compressedSize) $ \ptr -> do
          pokeWord32be ptr 0 (fromIntegral uncompressedSize)
          pokeWord32be ptr 4 (fromIntegral compressedSize)
          pokeByteString ptr 8 compressed
{-# INLINE compressChunk #-}

compress :: Strict.ByteString -> Strict.ByteString
compress =
  B.concat . fmap compressChunk . splitChunks hadoopMaximumChunkSize
