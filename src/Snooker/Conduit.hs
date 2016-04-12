{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Snooker.Conduit (
    BinaryError(..)
  , SnookerError(..)
  , renderSnookerError

  , decodeCompressedBlocks
  ) where

import           Control.Monad.Morph (MFunctor(..))
import           Control.Monad.Trans.Class (lift)

import           Crypto.Hash (Digest, MD5)

import           Data.Binary.Get (Get, Decoder(..))
import           Data.Binary.Get (runGetIncremental, pushChunk)
import           Data.Conduit (Conduit, ResumableSource, Sink)
import           Data.Conduit (($$++), ($=+))
import           Data.Conduit (yield, await, leftover)
import           Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.Text as T

import           P

import           Snooker.Codec
import           Snooker.Data

import           X.Control.Monad.Trans.Either (EitherT, hoistEither, left)


data BinaryError =
    BinaryError !Text
    deriving (Eq, Ord, Show)

data SnookerError =
    HeaderError !BinaryError
  | BlockError !BinaryError
    deriving (Eq, Ord, Show)

renderSnookerError :: SnookerError -> Text
renderSnookerError =
  T.pack . show

sinkGet :: Monad m => Get a -> Sink ByteString m (Either BinaryError a)
sinkGet g =
  let
    feed = \case
      Fail _ _ e ->
        return . Left . BinaryError $ T.pack e
      Partial k ->
        feed . k =<< await
      Done bs _ v -> do
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

sinkHeader :: Monad m => Sink ByteString m (Either SnookerError Header)
sinkHeader =
  fmap (first HeaderError) $
    sinkGet getHeader

conduitCompressedBlock ::
  Functor m =>
  Monad m =>
  Digest MD5 ->
  Conduit ByteString (EitherT SnookerError m) CompressedBlock
conduitCompressedBlock marker =
  hoist (firstT BlockError) . conduitGet $
    getCompressedBlock marker

decodeCompressedBlocks ::
  Functor m =>
  Monad m =>
  ResumableSource m ByteString ->
  EitherT SnookerError m (Header, ResumableSource (EitherT SnookerError m) CompressedBlock)
decodeCompressedBlocks src0 = do
  (src1, eheader) <- lift $ src0 $$++ sinkHeader
  header <- hoistEither eheader
  let
    blocks =
      hoist lift src1 $=+ conduitCompressedBlock (headerSync header)
  return (header, blocks)
