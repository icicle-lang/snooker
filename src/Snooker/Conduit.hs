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
import           Snooker.Compression
import           Snooker.Data
import           Snooker.MD5
import           Snooker.Writable

import           X.Control.Monad.Trans.Either (EitherT, hoistEither, left)


data SnookerError xk xv =
    CorruptHeader !BinaryError
  | CorruptCompressedBlock !BinaryError
  | CorruptCompression !BinaryError
  | CorruptRecords !(DecodeError xk xv)
  | UnexpectedKeyType !ClassName !ClassName
  | UnexpectedValueType !ClassName !ClassName
    deriving (Eq, Ord, Show)

renderSnookerError :: (xk -> Text) -> (xv -> Text) -> SnookerError xk xv -> Text
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
{-# INLINE sinkGet #-}

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
{-# INLINE conduitGet #-}

sinkHeader :: Monad m => Sink ByteString m (Either (SnookerError xk xv) Header)
sinkHeader =
  fmap (first CorruptHeader) $
    sinkGet getHeader
{-# INLINE sinkHeader #-}

conduitDecodeCompressedBlock ::
  Monad m =>
  Digest MD5 ->
  Conduit ByteString (EitherT (SnookerError xk xv) m) CompressedBlock
conduitDecodeCompressedBlock marker =
  hoist (firstT CorruptCompressedBlock) . conduitGet $
    getCompressedBlock marker
{-# INLINE conduitDecodeCompressedBlock #-}

conduitDecompressBlock ::
  Monad m =>
  CompressionCodec ->
  Conduit CompressedBlock (EitherT (SnookerError xk xv) m) EncodedBlock
conduitDecompressBlock codec =
  Conduit.mapM (hoistEither . first CorruptCompression . decompressBlock codec)
{-# INLINE conduitDecompressBlock #-}

conduitDecodeBlock ::
  Monad m =>
  WritableCodec xk ks ->
  WritableCodec xv vs ->
  Conduit EncodedBlock (EitherT (SnookerError xk xv) m) (Block ks vs)
conduitDecodeBlock keyCodec valueCodec =
  Conduit.mapM (hoistEither . first CorruptRecords . decodeBlock keyCodec valueCodec)
{-# INLINE conduitDecodeBlock #-}

decodeCompressedBlocks ::
  Monad m =>
  ResumableSource m ByteString ->
  EitherT (SnookerError xk xv) m
    (Header, ResumableSource (EitherT (SnookerError xk xv) m) CompressedBlock)
decodeCompressedBlocks src0 = do
  (src1, eheader) <- lift $ src0 $$++ sinkHeader
  header <- hoistEither eheader
  let
    blocks =
      hoist lift src1 $=+ conduitDecodeCompressedBlock (headerSync header)
  return (header, blocks)
{-# INLINE decodeCompressedBlocks #-}

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
{-# INLINE encodeCompressedBlocks #-}

decodeEncodedBlocks ::
  Monad m =>
  CompressionCodec ->
  ResumableSource m ByteString ->
  EitherT (SnookerError xk xv) m
    (Header, ResumableSource (EitherT (SnookerError xk xv) m) EncodedBlock)
decodeEncodedBlocks codec =
  secondT (second ($=+ conduitDecompressBlock codec)) . decodeCompressedBlocks
{-# INLINE decodeEncodedBlocks #-}

encodeEncodedBlocks ::
  MonadBase base m =>
  PrimMonad base =>
  CompressionCodec ->
  Header ->
  Conduit EncodedBlock m ByteString
encodeEncodedBlocks codec header =
  Conduit.map (compressBlock codec) =$= encodeCompressedBlocks header
{-# INLINE encodeEncodedBlocks #-}

decodeBlocks ::
  Monad m =>
  WritableCodec xk ks ->
  WritableCodec xv vs ->
  CompressionCodec ->
  ResumableSource m ByteString ->
  EitherT (SnookerError xk xv) m
    (Metadata, ResumableSource (EitherT (SnookerError xk xv) m) (Block ks vs))
decodeBlocks keyCodec valueCodec compressionCodec file = do
  (header, encoded) <- decodeEncodedBlocks compressionCodec file

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
{-# INLINE decodeBlocks #-}

encodeBlocks ::
  MonadBase base m =>
  PrimMonad base =>
  WritableCodec xk ks ->
  WritableCodec xv vs ->
  CompressionCodec ->
  Metadata ->
  Conduit (Block ks vs) m ByteString
encodeBlocks =
  encodeBlocks' randomMD5
{-# INLINE encodeBlocks #-}

encodeBlocks' ::
  MonadBase base m =>
  PrimMonad base =>
  Digest MD5 ->
  WritableCodec xk ks ->
  WritableCodec xv vs ->
  CompressionCodec ->
  Metadata ->
  Conduit (Block ks vs) m ByteString
encodeBlocks' sync keyCodec valueCodec compressionCodec metadata =
  let
    header =
      Header
        (writableClass keyCodec)
        (writableClass valueCodec)
        (compressionClass compressionCodec)
        metadata
        sync
  in
    Conduit.map (encodeBlock keyCodec valueCodec) =$= encodeEncodedBlocks compressionCodec header
{-# INLINE encodeBlocks' #-}
