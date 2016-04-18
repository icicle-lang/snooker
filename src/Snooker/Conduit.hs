{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Snooker.Conduit (
    BinaryError(..)
  , SnookerError(..)
  , renderSnookerError

  , decodeCompressedBlocks
  , encodeCompressedBlocks

  , decodeEncodedBlocks
  , encodeEncodedBlocks

  , decodeBlocks
  , encodeBlocks
  ) where

import           Control.Monad.Base (MonadBase)
import           Control.Monad.Morph (MFunctor(..))
import           Control.Monad.Primitive (PrimMonad)
import           Control.Monad.Trans.Class (lift)

import           Crypto.Hash (Digest, MD5)

import           Data.Binary.Get (Get, Decoder(..))
import           Data.Binary.Get (runGetIncremental, pushChunk)
import           Data.ByteString (ByteString)
import qualified Data.ByteString as B
import           Data.Conduit ((=$=), ($$++), ($=+))
import           Data.Conduit (Conduit, ResumableSource, Sink)
import           Data.Conduit (yield, await, leftover)
import           Data.Conduit.ByteString.Builder (builderToByteString)
import qualified Data.Conduit.List as Conduit
import qualified Data.Text as T

import           P

import           Snooker.Binary
import           Snooker.Codec
import           Snooker.Data
import           Snooker.Writable

import           X.Control.Monad.Trans.Either (EitherT, hoistEither, left)


data SnookerError ek ev =
    CorruptHeader !BinaryError
  | CorruptCompressedBlock !BinaryError
  | CorruptCompression !BinaryError
  | CorruptRecords !(DecodeError ek ev)
  | UnexpectedKeyType !ClassName !ClassName
  | UnexpectedValueType !ClassName !ClassName
    deriving (Eq, Ord, Show)

renderSnookerError :: (ek -> Text) -> (ev -> Text) -> SnookerError ek ev -> Text
renderSnookerError renderKeyError renderValueError = \case
  CorruptHeader err ->
    "Sequence file header was corrupt: " <>
    renderBinaryError err
  CorruptCompressedBlock err ->
    "Sequence file compressed block was corrupt: " <>
    renderBinaryError err
  CorruptCompression err ->
    "Sequence file compression was corrupt: " <>
    renderBinaryError err
  CorruptRecords err ->
    "Sequence file records were corrupt: " <>
    renderDecodeError renderKeyError renderValueError err
  UnexpectedKeyType expected actual ->
    "Sequence file had an unexpected key type:" <>
    "\n  expected <" <> unClassName expected <> ">" <>
    "\n  but was  <" <> unClassName actual <> ">"
  UnexpectedValueType expected actual ->
    "Sequence file had an unexpected value type:" <>
    "\n  expected <" <> unClassName expected <> ">" <>
    "\n  but was  <" <> unClassName actual <> ">"

sinkGet :: Monad m => Get a -> Sink ByteString m (Either BinaryError a)
sinkGet g =
  let
    feed = \case
      Fail _ _ e ->
        return . Left . BinaryError $ T.pack e
      Partial k ->
        feed . k =<< await
      Done bs _ v -> do
        unless (B.null bs) $
          leftover bs
        return $ Right v
  in
    feed $ runGetIncremental g

conduitGet :: Monad m => Get a -> Conduit ByteString (EitherT BinaryError m) a
conduitGet g =
  let
    next = do
      mbs <- await
      case mbs of
        Nothing ->
          return ()
        Just bs ->
          feed $ pushChunk (runGetIncremental g) bs

    feed = \case
      Fail _ _ e ->
        lift . left . BinaryError $ T.pack e
      Partial k -> do
        mx <- await
        feed $ k mx
      Done bs _ v -> do
        yield v
        if B.null bs then
          next
        else
          feed $ pushChunk (runGetIncremental g) bs
  in
    next

sinkHeader :: Monad m => Sink ByteString m (Either (SnookerError ek ev) Header)
sinkHeader =
  fmap (first CorruptHeader) $
    sinkGet getHeader

conduitDecodeCompressedBlock ::
  Functor m =>
  Monad m =>
  Digest MD5 ->
  Conduit ByteString (EitherT (SnookerError ek ev) m) CompressedBlock
conduitDecodeCompressedBlock marker =
  hoist (firstT CorruptCompressedBlock) . conduitGet $
    getCompressedBlock marker

conduitDecompressBlock ::
  Monad m =>
  Conduit CompressedBlock (EitherT (SnookerError ek ev) m) EncodedBlock
conduitDecompressBlock =
  Conduit.mapM (hoistEither . first CorruptCompression . decompressBlock)

conduitDecodeBlock ::
  Monad m =>
  WritableCodec ek vk k ->
  WritableCodec ev vv v ->
  Conduit EncodedBlock (EitherT (SnookerError ek ev) m) (Block vk vv k v)
conduitDecodeBlock keyCodec valueCodec =
  Conduit.mapM (hoistEither . first CorruptRecords . decodeBlock keyCodec valueCodec)

decodeCompressedBlocks ::
  Functor m =>
  Monad m =>
  ResumableSource m ByteString ->
  EitherT (SnookerError ek ev) m
    (Header, ResumableSource (EitherT (SnookerError ek ev) m) CompressedBlock)
decodeCompressedBlocks src0 = do
  (src1, eheader) <- lift $ src0 $$++ sinkHeader
  header <- hoistEither eheader
  let
    blocks =
      hoist lift src1 $=+ conduitDecodeCompressedBlock (headerSync header)
  return (header, blocks)

encodeCompressedBlocks ::
  MonadBase base m =>
  PrimMonad base =>
  Header ->
  Conduit CompressedBlock m ByteString
encodeCompressedBlocks header =
  let
    builders = do
      yield (bHeader header)
      Conduit.map . bCompressedBlock $ headerSync header
  in
    builders =$= builderToByteString

decodeEncodedBlocks ::
  Functor m =>
  Monad m =>
  ResumableSource m ByteString ->
  EitherT (SnookerError ek ev) m
    (Header, ResumableSource (EitherT (SnookerError ek ev) m) EncodedBlock)
decodeEncodedBlocks =
  secondT (second ($=+ conduitDecompressBlock)) . decodeCompressedBlocks

encodeEncodedBlocks ::
  MonadBase base m =>
  PrimMonad base =>
  Header ->
  Conduit EncodedBlock m ByteString
encodeEncodedBlocks header =
  Conduit.map compressBlock =$= encodeCompressedBlocks header

decodeBlocks ::
  Functor m =>
  Monad m =>
  WritableCodec ek vk k ->
  WritableCodec ev vv v ->
  ResumableSource m ByteString ->
  EitherT (SnookerError ek ev) m
    (Metadata, ResumableSource (EitherT (SnookerError ek ev) m) (Block vk vv k v))
decodeBlocks keyCodec valueCodec file = do
  (header, encoded) <- decodeEncodedBlocks file

  unless (headerKeyType header == writableClass keyCodec) . left $
    UnexpectedKeyType (headerKeyType header) (writableClass keyCodec)

  unless (headerValueType header == writableClass valueCodec) . left $
    UnexpectedValueType (headerValueType header) (writableClass valueCodec)

  let
    metadata =
      headerMetadata header

    blocks =
      encoded $=+ conduitDecodeBlock keyCodec valueCodec

  return (metadata, blocks)

encodeBlocks ::
  MonadBase base m =>
  PrimMonad base =>
  WritableCodec ek vk k ->
  WritableCodec ev vv v ->
  Digest MD5 ->
  Metadata ->
  Conduit (Block vk vv k v) m ByteString
encodeBlocks keyCodec valueCodec sync metadata =
  let
    header =
      Header
        (writableClass keyCodec)
        (writableClass valueCodec)
        metadata
        sync
  in
    Conduit.map (encodeBlock keyCodec valueCodec) =$= encodeEncodedBlocks header
