{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Snooker.Streaming where
--    BinaryError(..)
--  , SnookerError(..)
--  , renderSnookerError
--
--  , decodeCompressedBlocks
--  , encodeCompressedBlocks
--
--  , decodeEncodedBlocks
--  , encodeEncodedBlocks
--
--  , decodeBlocks
--  , encodeBlocks
--  ) where

import           Control.Monad.Base (MonadBase)
import           Control.Monad.IO.Class (MonadIO(..))
import           Control.Monad.Morph (MFunctor(..))
import           Control.Monad.Primitive (PrimMonad)
import           Control.Monad.Trans.Class (lift)

import           Crypto.Hash (Digest, MD5)

import           Data.Binary.Get (Get, Decoder(..))
import           Data.Binary.Get (runGetIncremental, pushChunk)
import qualified Data.ByteString as B
import           Data.ByteString.Builder (Builder)
import           Data.ByteString.Streaming (ByteString)
import qualified Data.ByteString.Streaming as BS
import qualified Data.Text as T

import           P

import           Snooker.Binary
import           Snooker.Conduit (SnookerError(..))
import           Snooker.Codec
import           Snooker.Data
import           Snooker.MD5
import           Snooker.Writable

import           Streaming (Stream, Of(..))
import qualified Streaming.Prelude as S

import           X.Control.Monad.Trans.Either (EitherT, hoistEither, left)


runGetOnce ::
  Monad m =>
  Get a ->
  ByteString m () ->
  EitherT BinaryError m (a, ByteString m ())
runGetOnce g sbs0 =
  let
    feed sbs = \case
      Fail _ _ msg ->
        hoistEither . Left . BinaryError $ T.pack msg
      Partial k -> do
        mbs <- lift $ BS.unconsChunk sbs
        case mbs of
          Nothing ->
            feed BS.empty $ k Nothing
          Just (bs, sbs') ->
            feed sbs' $ k (Just bs)
      Done bs _ x ->
        hoistEither $ Right (x, BS.consChunk bs sbs)
  in
    feed sbs0 (runGetIncremental g)

runGetAll ::
  Monad m =>
  Get a ->
  ByteString m () ->
  Stream (Of a) (EitherT BinaryError m) ()
runGetAll g sbs0 =
  let
    next sbs = do
      mbs <- lift . lift $ BS.unconsChunk sbs
      case mbs of
        Nothing ->
          return ()
        Just (bs, sbs') ->
          feed sbs' $ pushChunk (runGetIncremental g) bs

    feed sbs = \case
      Fail _ _ e ->
        lift . left . BinaryError $ T.pack e
      Partial k -> do
        mbs <- lift . lift $ BS.unconsChunk sbs
        case mbs of
          Nothing ->
            feed BS.empty $ k Nothing
          Just (bs, sbs') ->
            feed sbs' $ k (Just bs)
      Done bs _ v -> do
        S.yield v
        if B.null bs then
          next sbs
        else
          feed sbs $ pushChunk (runGetIncremental g) bs
  in
    next sbs0

runBuilders :: MonadIO m => Stream (Of Builder) m () -> ByteString m ()
runBuilders =
  BS.concat . S.subst BS.toStreamingByteString

decodeBlocks ::
  Functor m =>
  Monad m =>
  WritableCodec ek vk k ->
  WritableCodec ev vv v ->
  ByteString m () ->
  EitherT (SnookerError ek ev) m
    (Metadata, Stream (Of (Block vk vv k v)) (EitherT (SnookerError ek ev) m) ())
decodeBlocks keyCodec valueCodec bs0 = do
  (header, bs1) <- firstT CorruptHeader $ runGetOnce getHeader bs0

  unless (headerKeyType header == writableClass keyCodec) . left $
    UnexpectedKeyType (headerKeyType header) (writableClass keyCodec)

  unless (headerValueType header == writableClass valueCodec) . left $
    UnexpectedValueType (headerValueType header) (writableClass valueCodec)

  let
    metadata =
      headerMetadata header

    sync =
      headerSync header

    decode =
      S.mapM (hoistEither . first CorruptRecords . decodeBlock keyCodec valueCodec) .
      S.mapM (hoistEither . first CorruptCompression . decompressBlock) .
      hoist (firstT CorruptCompressedBlock) .
      runGetAll (getCompressedBlock sync)

  return (metadata, decode bs1)

encodeBlocks ::
  MonadIO m =>
  MonadBase base m =>
  PrimMonad base =>
  WritableCodec ek vk k ->
  WritableCodec ev vv v ->
  Metadata ->
  Stream (Of (Block vk vv k v)) m () ->
  ByteString m ()
encodeBlocks =
  encodeBlocks' randomMD5

encodeBlocks' ::
  MonadIO m =>
  MonadBase base m =>
  PrimMonad base =>
  Digest MD5 ->
  WritableCodec ek vk k ->
  WritableCodec ev vv v ->
  Metadata ->
  Stream (Of (Block vk vv k v)) m () ->
  ByteString m ()
encodeBlocks' sync keyCodec valueCodec metadata blocks =
  let
    header =
      Header
        (writableClass keyCodec)
        (writableClass valueCodec)
        metadata
        sync

    compressBlocks =
      S.map (bCompressedBlock sync) .
      S.map compressBlock .
      S.map (encodeBlock keyCodec valueCodec)
  in
    runBuilders $
      bHeader header `S.cons`
      compressBlocks blocks
