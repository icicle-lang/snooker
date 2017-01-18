{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Snooker.Codec (
    getHeader
  , getCompressedBlock
  , decompressChunks
  , decompressBlock
  , decodeBlock

  , bHeader
  , bCompressedBlock
  , compressChunk
  , compressHadoopChunks
  , compressBlock
  , encodeBlock

  , DecodeError(..)
  , renderDecodeError
  ) where

import           Crypto.Hash (Digest, MD5, digestFromByteString)

import           Data.Binary.Get (Get)
import qualified Data.Binary.Get as Binary
import           Data.ByteArray (convert)
import qualified Data.ByteString as B
import qualified Data.ByteString as Strict
import qualified Data.ByteString.Base16 as Base16
import           Data.ByteString.Builder (Builder)
import qualified Data.ByteString.Builder as Builder
import qualified Data.ByteString.Char8 as Char8
import           Data.ByteString.Internal (ByteString(..))
import qualified Data.ByteString.Internal as B
import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString.Lazy as Lazy
import qualified Data.Text.Encoding as T
import           Data.Word (Word8)

import           P

import           Snooker.Binary
import           Snooker.Compression
import           Snooker.Data
import           Snooker.Storable
import           Snooker.VInt
import           Snooker.Writable

import           Text.Printf (printf)


data DecodeError xk xv =
    KeyDecodeError !xk
  | ValueDecodeError !xv
    deriving (Eq, Ord, Show)

renderDecodeError :: (xk -> Text) -> (xv -> Text) -> DecodeError xk xv -> Text
renderDecodeError renderKeyError renderValueError = \case
  KeyDecodeError err ->
    "failed to decode keys: " <> renderKeyError err
  ValueDecodeError err ->
    "failed to decode values: " <> renderValueError err

getVIntPrefixedBytes :: Get ByteString
getVIntPrefixedBytes =
  Binary.getByteString =<< getVInt
{-# INLINE getVIntPrefixedBytes #-}

bVIntPrefixedBytes :: ByteString -> Builder
bVIntPrefixedBytes bs =
  bVInt (B.length bs) <> Builder.byteString bs
{-# INLINE bVIntPrefixedBytes #-}

getTextWritable :: Get Text
getTextWritable =
  T.decodeUtf8 <$> getVIntPrefixedBytes
{-# INLINE getTextWritable #-}

bTextWritable :: Text -> Builder
bTextWritable =
  bVIntPrefixedBytes . T.encodeUtf8
{-# INLINE bTextWritable #-}

getClassName :: Get ClassName
getClassName =
  ClassName <$> getTextWritable
{-# INLINE getClassName #-}

bClassName :: ClassName -> Builder
bClassName =
  bTextWritable . unClassName
{-# INLINE bClassName #-}

getBool :: Get Bool
getBool =
  (/= 0) <$> Binary.getWord8
{-# INLINE getBool #-}

bBool :: Bool -> Builder
bBool = \case
  False ->
    Builder.word8 0
  True ->
    Builder.word8 1
{-# INLINE bBool #-}

getMD5 :: Get (Digest MD5)
getMD5 = do
  bs <- Binary.getByteString 16
  case digestFromByteString bs of
    Nothing ->
      fail $ "invalid MD5 hash: " <> Char8.unpack (Base16.encode bs)
    Just md5 ->
      pure md5
{-# INLINE getMD5 #-}

bMD5 :: Digest MD5 -> Builder
bMD5 =
  Builder.byteString . convert
{-# INLINE bMD5 #-}

getMetadata :: Get Metadata
getMetadata = do
  n <- fromIntegral <$> Binary.getWord32be
  fmap Metadata . replicateM n $
    (,) <$> getTextWritable <*> getTextWritable

bMetadata :: Metadata -> Builder
bMetadata (Metadata xs) =
  let
    kv (k, v) =
      bTextWritable k <>
      bTextWritable v
  in
    Builder.word32BE (fromIntegral $ length xs) <>
    mconcat (fmap kv xs)

headerVersion :: Word8
headerVersion =
  6

getHeader :: Get Header
getHeader = do
  magic <- Binary.getByteString 3

  when (magic /= "SEQ") $
    fail "not a sequence file"

  version <- Binary.getWord8

  when (version /= headerVersion) $
    fail $ "unknown version: " <> show version

  keyType <- getClassName
  valueType <- getClassName

  compression <- getBool
  blockCompression <- getBool

  unless (compression && blockCompression) $
    fail "only block compressed files supported"

  compressionType <- getClassName

  metadata <- getMetadata
  sync <- getMD5

  return $ Header keyType valueType compressionType metadata sync

bHeader :: Header -> Builder
bHeader h =
  let
    compression = True
    blockCompression = True
  in
    Builder.byteString "SEQ" <>
    Builder.word8 headerVersion <>
    bClassName (headerKeyType h) <>
    bClassName (headerValueType h) <>
    bBool compression <>
    bBool blockCompression <>
    bClassName (headerCompressionType h) <>
    bMetadata (headerMetadata h) <>
    bMD5 (headerSync h)

getCompressedBlock :: Digest MD5 -> Get CompressedBlock
getCompressedBlock expectedMarker = do
  escape <- Binary.getWord32le
  when (escape /= 0xffffffff) . fail $
    "file corrupt, expected to find sync escape " <>
    "<0xffffffff> but was " <> printf "<0x%08x>" escape

  marker <- getMD5
  when (expectedMarker /= marker) . fail $
    "file corrupt, expected to find sync marker " <>
    "<" <> show expectedMarker <> "> but was <" <> show marker <> ">"

  CompressedBlock
    <$> getVInt
    <*> getVIntPrefixedBytes
    <*> getVIntPrefixedBytes
    <*> getVIntPrefixedBytes
    <*> getVIntPrefixedBytes

bCompressedBlock :: Digest MD5 -> CompressedBlock -> Builder
bCompressedBlock marker b =
  if compressedCount b == 0 then
    mempty
  else
    Builder.word32LE 0xffffffff <>
    bMD5 marker <>
    bVInt (compressedCount b) <>
    bVIntPrefixedBytes (compressedKeySizes b) <>
    bVIntPrefixedBytes (compressedKeys b) <>
    bVIntPrefixedBytes (compressedValueSizes b) <>
    bVIntPrefixedBytes (compressedValues b)

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
decompressChunksLazy :: CompressionCodec -> Lazy.ByteString -> Either BinaryError Lazy.ByteString
decompressChunksLazy codec lbs =
  let
    getPart = do
      compressedSize <- Binary.getWord32be
      if compressedSize == 0 then
        pure B.empty
      else do
        compressedBytes <- Binary.getByteString $ fromIntegral compressedSize
        case compressionDecompress codec compressedBytes of
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

decompressChunks :: CompressionCodec -> Strict.ByteString -> Either BinaryError Strict.ByteString
decompressChunks codec =
  fmap L.toStrict . decompressChunksLazy codec . L.fromStrict

compressChunk :: CompressionCodec -> Strict.ByteString -> Strict.ByteString
compressChunk codec uncompressed =
  let
    compressed =
      compressionCompress codec uncompressed

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

compressHadoopChunks :: CompressionCodec -> Strict.ByteString -> Strict.ByteString
compressHadoopChunks codec =
  B.concat . fmap (compressChunk codec) . splitChunks hadoopMaximumChunkSize

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

decompressBlock :: CompressionCodec -> CompressedBlock -> Either BinaryError EncodedBlock
decompressBlock codec b =
  EncodedBlock
    <$> pure (compressedCount b)
    <*> (decompressChunks codec $ compressedKeySizes b)
    <*> (decompressChunks codec $ compressedKeys b)
    <*> (decompressChunks codec $ compressedValueSizes b)
    <*> (decompressChunks codec $ compressedValues b)

compressBlock :: CompressionCodec -> EncodedBlock -> CompressedBlock
compressBlock codec b =
  CompressedBlock
    (encodedCount b)
    (compressHadoopChunks codec $ encodedKeySizes b)
    (compressHadoopChunks codec $ encodedKeys b)
    (compressHadoopChunks codec $ encodedValueSizes b)
    (compressHadoopChunks codec $ encodedValues b)

decodeBlock ::
  WritableCodec xk ks ->
  WritableCodec xv vs ->
  EncodedBlock ->
  Either (DecodeError xk xv) (Block ks vs)
decodeBlock keyCodec valueCodec b = do
  keys <- first KeyDecodeError $
    writableDecode keyCodec (encodedCount b) (encodedKeySizes b) (encodedKeys b)

  values <- first ValueDecodeError $
    writableDecode valueCodec (encodedCount b) (encodedValueSizes b) (encodedValues b)

  return $ Block (encodedCount b) keys values
{-# INLINE decodeBlock #-}

encodeBlock ::
  WritableCodec xk ks ->
  WritableCodec xv vs ->
  Block ks vs ->
  EncodedBlock
encodeBlock keyCodec valueCodec b =
  let
    (keySizes, keys) =
      writableEncode keyCodec $ blockKeys b

    (valueSizes, values) =
      writableEncode valueCodec $ blockValues b
  in
    EncodedBlock (blockCount b) keySizes keys valueSizes values
{-# INLINE encodeBlock #-}
