{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Snooker.Writable (
    WritableCodec(..)
  , WritableError(..)

  , nullWritable
  , bytesWritable

  , genericNullWritable
  , genericBytesWritable

  , segmentedBytesWritable
  ) where

import qualified Data.ByteString as B
import           Data.ByteString.Internal (ByteString(..))
import qualified Data.ByteString.Internal as B
import qualified Data.ByteString.Lazy as L
import qualified Data.Vector as Boxed
import qualified Data.Vector.Generic as Generic
import qualified Data.Vector.Unboxed as Unboxed
import           Data.Void (Void)

import           P

import           Snooker.Binary
import           Snooker.Data
import           Snooker.Segmented
import           Snooker.Storable
import           Snooker.VInt

import qualified X.Data.ByteString.Unsafe as B


data WritableError =
    WritableBinaryError !BinaryError
  | WritableSizesMismatch !Int !Int
    deriving (Eq, Ord, Show)

data WritableCodec x a =
  WritableCodec {
      writableClass :: ClassName
    , writableEncode :: a -> (ByteString, ByteString)
    , writableDecode :: Int -> ByteString -> ByteString -> Either x a
    }

genericNullWritable :: Generic.Vector v () => WritableCodec Void (v ())
genericNullWritable =
  let
    encodeSizes xs =
      B.unsafeCreate (Generic.length xs) $ \ptr -> do
        _ <- B.memset ptr 0 (fromIntegral $ Generic.length xs)
        pure ()

    encode xs =
      (encodeSizes xs, B.empty)

    decode n _ _ =
      Right $ Generic.replicate n ()
  in
    WritableCodec (ClassName "org.apache.hadoop.io.NullWritable") encode decode

nullWritable :: WritableCodec Void (Unboxed.Vector ())
nullWritable =
  genericNullWritable

genericBytesWritable ::
  Generic.Vector v ByteString =>
  WritableCodec WritableError (v ByteString)
genericBytesWritable =
  let
    sizesSize ss =
      maxSizeVInt * Generic.length ss

    bytesSize =
      Generic.foldl' (\n bs -> n + 4 + (B.length bs)) 0

    encodeSizes ss =
      {-# SCC encodeSizes #-}
      B.unsafeCreateUptoN (sizesSize ss) $ \ptr ->
        let
          go !n bs = do
            !sz <- pokeVInt ptr n (B.length bs + 4)
            pure $! n + sz
        in
          Generic.foldM go 0 ss

    encodeBytes bss =
      {-# SCC encodeBytes #-}
      B.unsafeCreate (bytesSize bss) $ \ptr ->
        let
          go !n bs = do
            pokeWord32be ptr n (fromIntegral $ B.length bs)
            pokeByteString ptr (n + 4) bs
            pure $! n + 4 + B.length bs
        in
          Generic.foldM_ go 0 bss

    encode xs =
      (encodeSizes xs, encodeBytes xs)

    decode n sbs vbs = do
      sizes <-
        {-# SCC decodeSizes #-}
        first WritableBinaryError .
        runGet (Unboxed.replicateM n getVInt) $
        L.fromStrict sbs

      let
        expected = B.length vbs
        actual = Unboxed.sum sizes

      when (actual /= expected) $
        Left $ WritableSizesMismatch expected actual

      return $
        {-# SCC unsafeSplit #-}
        B.unsafeSplits (B.drop 4) vbs sizes
  in
    WritableCodec (ClassName "org.apache.hadoop.io.BytesWritable") encode decode

bytesWritable :: WritableCodec WritableError (Boxed.Vector ByteString)
bytesWritable =
  genericBytesWritable

segmentedBytesWritable :: WritableCodec WritableError (Segmented ByteString)
segmentedBytesWritable =
  let
    sizesSize bss =
      maxSizeVInt * segmentedLength bss

    bytesSize bss =
      Unboxed.foldl' (\n len -> n + 4 + len) 0 $
      segmentedLengths bss

    encodeSizes bss =
      {-# SCC seg_encodeSizes #-}
      B.unsafeCreateUptoN (sizesSize bss) $ \ptr ->
        let
          go !n len = do
            !sz <- pokeVInt ptr n (len + 4)
            pure $! n + sz
        in
          Unboxed.foldM go 0 $
          segmentedLengths bss

    encodeBytes bss =
      {-# SCC seg_encodeBytes #-}
      B.unsafeCreate (bytesSize bss) $ \ptr ->
        let
          go !n bs = do
            pokeWord32be ptr n (fromIntegral $ B.length bs)
            pokeByteString ptr (n + 4) bs
            pure $! n + 4 + B.length bs
        in
          Boxed.foldM_ go 0 $
          bytesOfSegmented bss

    encode xs =
      (encodeSizes xs, encodeBytes xs)

    decode n sbs vbs = do
      lengths0 <-
        {-# SCC seg_decodeSizes #-}
        first WritableBinaryError .
        runGet (Unboxed.replicateM n getVInt) $
        L.fromStrict sbs

      let
        expected =
          B.length vbs

        actual =
          Unboxed.sum lengths0

      when (actual /= expected) $
        Left $ WritableSizesMismatch expected actual

      let
        offsets =
          Unboxed.prescanl' (\acc len -> acc + len) 4 lengths0

        lengths =
          Unboxed.map (subtract 4) lengths0

      return $
        Segmented offsets lengths vbs
  in
    WritableCodec (ClassName "org.apache.hadoop.io.BytesWritable") encode decode
