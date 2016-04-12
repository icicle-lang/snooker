{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Snooker.Header where

import           Crypto.Hash (Digest, MD5, digestFromByteString)

import           Data.Binary.Get (Get)
import qualified Data.Binary.Get as Binary
import           Data.ByteArray (convert)
import           Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Base16 as Base16
import           Data.ByteString.Builder (Builder)
import qualified Data.ByteString.Builder as Builder
import qualified Data.ByteString.Char8 as Char8
import qualified Data.Text.Encoding as T
import           Data.Word (Word8)

import           GHC.Generics (Generic)

import           P

import           Snooker.VInt

import           X.Text.Show (gshowsPrec)

------------------------------------------------------------------------

newtype ClassName =
  ClassName {
      unClassName :: Text
    } deriving (Eq, Ord, Generic)

instance Show ClassName where
  showsPrec =
    gshowsPrec

data SeqHeader =
  SeqHeader {
      seqKeyType :: !ClassName
    , seqValueType :: !ClassName
    , seqMetadata :: ![(Text, Text)]
    , seqSync :: !(Digest MD5)
    } deriving (Eq, Ord, Show, Generic)

------------------------------------------------------------------------

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

getMetadata :: Get [(Text, Text)]
getMetadata = do
  n <- fromIntegral <$> Binary.getWord32le
  replicateM n $
    (,) <$> getTextWritable <*> getTextWritable

bMetadata :: [(Text, Text)] -> Builder
bMetadata xs =
  let
    kv (k, v) =
      bTextWritable k <>
      bTextWritable v
  in
    Builder.word32LE (fromIntegral $ length xs) <>
    mconcat (fmap kv xs)

seqVersion :: Word8
seqVersion =
  6

snappyCodec :: ClassName
snappyCodec =
  ClassName "org.apache.hadoop.io.compress.SnappyCodec"

getSeqHeader :: Get SeqHeader
getSeqHeader = do
  magic <- Binary.getByteString 3

  when (magic /= "SEQ") $
    fail "not a sequence file"

  version <- Binary.getWord8

  when (version /= seqVersion) $
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

  return $ SeqHeader keyType valueType metadata sync

bSeqHeader :: SeqHeader -> Builder
bSeqHeader h =
  let
    compression = True
    blockCompression = True
  in
    Builder.byteString "SEQ" <>
    Builder.word8 seqVersion <>
    bClassName (seqKeyType h) <>
    bClassName (seqValueType h) <>
    bBool compression <>
    bBool blockCompression <>
    bClassName snappyCodec <>
    bMetadata (seqMetadata h) <>
    bMD5 (seqSync h)
