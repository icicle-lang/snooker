{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Snooker.Codec (
    getHeader
  , getCompressedBlock
  , decompressBlock
  , decodeBlock

  , bHeader
  , bCompressedBlock
  , compressBlock
  , encodeBlock

  , DecodeError(..)
  , renderDecodeError
  ) where

import           Crypto.Hash (Digest, MD5, digestFromByteString)

import           Data.Binary.Get (Get)
import qualified Data.Binary.Get as Binary
import           Data.ByteArray (convert)
import           Data.ByteString.Internal (ByteString(..))
import qualified Data.ByteString as B
import qualified Data.ByteString as Strict
import qualified Data.ByteString.Base16 as Base16
import           Data.ByteString.Builder (Builder)
import qualified Data.ByteString.Builder as Builder
import qualified Data.ByteString.Builder.Extra as Builder
import qualified Data.ByteString.Char8 as Char8
import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString.Lazy as Lazy
import qualified Data.Text.Encoding as T
import           Data.Word (Word8)

import           P

import qualified Snapper

import           Snooker.Binary
import           Snooker.Data
import           Snooker.VInt
import           Snooker.Writable

import           Text.Printf (printf)


data DecodeError ek ev =
    KeyDecodeError !ek
  | ValueDecodeError !ev
    deriving (Eq, Ord, Show)

renderDecodeError :: (ek -> Text) -> (ev -> Text) -> DecodeError ek ev -> Text
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
  n <- fromIntegral <$> Binary.getWord32le
  fmap Metadata . replicateM n $
    (,) <$> getTextWritable <*> getTextWritable

bMetadata :: Metadata -> Builder
bMetadata (Metadata xs) =
  let
    kv (k, v) =
      bTextWritable k <>
      bTextWritable v
  in
    Builder.word32LE (fromIntegral $ length xs) <>
    mconcat (fmap kv xs)

headerVersion :: Word8
headerVersion =
  6

snappyCodec :: ClassName
snappyCodec =
  ClassName "org.apache.hadoop.io.compress.SnappyCodec"

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

  unless (compressionType == snappyCodec) $
    fail "only snappy compressed files supported"

  metadata <- getMetadata
  sync <- getMD5

  return $ Header keyType valueType metadata sync

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
    bClassName snappyCodec <>
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
  Builder.word32LE 0xffffffff <>
  bMD5 marker <>
  bVInt (compressedCount b) <>
  bVIntPrefixedBytes (compressedKeySizes b) <>
  bVIntPrefixedBytes (compressedKeys b) <>
  bVIntPrefixedBytes (compressedValueSizes b) <>
  bVIntPrefixedBytes (compressedValues b)

-- TODO make strict using ByteString.createN byteSwap32 and peekByteOff
decompressLazy :: Lazy.ByteString -> Either BinaryError Lazy.ByteString
decompressLazy lbs =
  let
    getChunk = do
      compressedSize <- Binary.getWord32be
      compressedBytes <- Binary.getByteString $ fromIntegral compressedSize
      case Snapper.decompress compressedBytes of
        Nothing ->
          fail "could not decompress chunk"
        Just bs ->
          pure bs

    getChunks remaining =
      if remaining == 0 then
        return []
      else do
        bs <- getChunk
        bss <- getChunks (remaining - B.length bs)
        return $ bs : bss

    get = do
      decompressedSize <- Binary.getWord32be
      getChunks $ fromIntegral decompressedSize
  in
    second L.fromChunks $ runGet get lbs

decompressStrict :: Strict.ByteString -> Either BinaryError Strict.ByteString
decompressStrict =
  fmap L.toStrict . decompressLazy . L.fromStrict

compressStrict :: Strict.ByteString -> Strict.ByteString
compressStrict uncompressed =
  let
    compressed =
      Snapper.compress uncompressed

    uncompressedSize =
      B.length uncompressed

    compressedSize =
      B.length compressed

    builderSize =
      4 + 4 + compressedSize

    fromBuilder =
      L.toStrict .
      Builder.toLazyByteStringWith (Builder.untrimmedStrategy builderSize 0) L.empty
  in
    case uncompressedSize of
      0 ->
        B.pack [0x0, 0x0, 0x0, 0x0]
      _ ->
        fromBuilder $
          Builder.word32BE (fromIntegral uncompressedSize) <>
          Builder.word32BE (fromIntegral compressedSize) <>
          Builder.byteString compressed

decompressBlock :: CompressedBlock -> Either BinaryError EncodedBlock
decompressBlock b =
  EncodedBlock
    <$> pure (compressedCount b)
    <*> (decompressStrict $ compressedKeySizes b)
    <*> (decompressStrict $ compressedKeys b)
    <*> (decompressStrict $ compressedValueSizes b)
    <*> (decompressStrict $ compressedValues b)

compressBlock :: EncodedBlock -> CompressedBlock
compressBlock b =
  CompressedBlock
    (encodedCount b)
    (compressStrict $ encodedKeySizes b)
    (compressStrict $ encodedKeys b)
    (compressStrict $ encodedValueSizes b)
    (compressStrict $ encodedValues b)

decodeBlock ::
  WritableCodec ek vk k ->
  WritableCodec ev vv v ->
  EncodedBlock ->
  Either (DecodeError ek ev) (Block vk vv k v)
decodeBlock keyCodec valueCodec b = do
  keys <- first KeyDecodeError $
    writableDecode keyCodec (encodedCount b) (encodedKeySizes b) (encodedKeys b)

  values <- first ValueDecodeError $
    writableDecode valueCodec (encodedCount b) (encodedValueSizes b) (encodedValues b)

  return $ Block (encodedCount b) keys values

encodeBlock ::
  WritableCodec ek vk k ->
  WritableCodec ev vv v ->
  Block vk vv k v ->
  EncodedBlock
encodeBlock keyCodec valueCodec b =
  let
    (keySizes, keys) =
      writableEncode keyCodec $ blockKeys b

    (valueSizes, values) =
      writableEncode valueCodec $ blockValues b
  in
    EncodedBlock (blockCount b) keySizes keys valueSizes values
