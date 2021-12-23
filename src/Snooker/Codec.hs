{-# LANGUAGE DoAndIfThenElse #-}
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

  , selectDecompressor
  , Decompressor
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
import qualified Data.ByteString.Builder.Prim as Prim
import qualified Data.ByteString.Char8 as Char8
import           Data.ByteString.Internal (ByteString(..))
import qualified Data.Text.Encoding as T
import           Data.Word (Word8, Word32)

import           P

import qualified Snooker.Codec.Snappy as Snappy
import qualified Snooker.Codec.BZip2 as BZip2

import           Snooker.Binary
import           Snooker.Data
import           Snooker.VInt
import           Snooker.Writable

import           Text.Printf (printf)

type Decompressor =
  Strict.ByteString -> Either BinaryError Strict.ByteString

data DecodeError xk xv =
    KeyDecodeError !xk
  | ValueDecodeError !xv
  | UnsupportedDecompressor ClassName
    deriving (Eq, Ord, Show)

selectDecompressor :: ClassName -> Maybe Decompressor
selectDecompressor klass
  | klass == Snappy.snappyCodec = Just Snappy.decompress
  | klass == BZip2.bzip2Codec  = Just BZip2.decompress
  | otherwise = Nothing

renderDecodeError :: (xk -> Text) -> (xv -> Text) -> DecodeError xk xv -> Text
renderDecodeError renderKeyError renderValueError = \case
  KeyDecodeError err ->
    "failed to decode keys: " <> renderKeyError err
  ValueDecodeError err ->
    "failed to decode values: " <> renderValueError err
  UnsupportedDecompressor err ->
    "Failed to gather a decomprresion codec for: " <> unClassName err

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
  -- SYNC_ESCAPE is the same regardless of endianess
  escape <- Binary.getWord32host
  when (escape /= syncEscape) . fail $
    "file corrupt, expected to find sync escape " <>
    printf "<0x%08x>" syncEscape <> " but was " <>
    printf "<0x%08x>" escape

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
  -- SYNC_ESCAPE is the same regardless of endianess
  Prim.primFixed Prim.word32Host syncEscape <>
  bMD5 marker <>
  bVInt (compressedCount b) <>
  bVIntPrefixedBytes (compressedKeySizes b) <>
  bVIntPrefixedBytes (compressedKeys b) <>
  bVIntPrefixedBytes (compressedValueSizes b) <>
  bVIntPrefixedBytes (compressedValues b)

-- | The @SYNC_ESCAPE@ constant from @org.apache.hadoop.io.SequenceFile@
syncEscape :: Word32
syncEscape =
  0xffffffff
{-# INLINE syncEscape #-}


decompressBlock :: Decompressor ->  CompressedBlock -> Either BinaryError EncodedBlock
decompressBlock decompressor b =
  EncodedBlock (compressedCount b)
    <$> decompressor (compressedKeySizes b)
    <*> decompressor (compressedKeys b)
    <*> decompressor (compressedValueSizes b)
    <*> decompressor (compressedValues b)

compressBlock :: EncodedBlock -> CompressedBlock
compressBlock b =
  CompressedBlock
    (encodedCount b)
    (Snappy.compress $ encodedKeySizes b)
    (Snappy.compress $ encodedKeys b)
    (Snappy.compress $ encodedValueSizes b)
    (Snappy.compress $ encodedValues b)

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
