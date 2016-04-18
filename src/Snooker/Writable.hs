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
  ) where

import           Control.Monad.ST (runST)

import qualified Data.ByteString as B
import qualified Data.ByteString.Builder as Builder
import           Data.ByteString.Internal (ByteString(..))
import qualified Data.ByteString.Internal as B
import qualified Data.ByteString.Lazy as L
import           Data.Mutable (newRef, writeRef, readRef, asPRef)
import qualified Data.Vector as Boxed
import qualified Data.Vector.Generic as Generic
import qualified Data.Vector.Unboxed as Unboxed
import           Data.Void (Void)

import           P

import           Snooker.Binary
import           Snooker.Data
import           Snooker.VInt


data WritableError =
    WritableBinaryError !BinaryError
  | WritableSizesMismatch !Int !Int
    deriving (Eq, Ord, Show)

data WritableCodec e v a =
  WritableCodec {
      writableClass :: ClassName
    , writableEncode :: v a -> (ByteString, ByteString)
    , writableDecode :: Int -> ByteString -> ByteString -> Either e (v a)
    }

unsafeSplit :: Generic.Vector v a => (ByteString -> a) -> ByteString -> Unboxed.Vector Int -> v a
unsafeSplit f bs sizes =
  runST $ do
    offRef <- asPRef <$> newRef 0

    Generic.generateM (Unboxed.length sizes) $ \idx -> do
      let
        len =
          Unboxed.unsafeIndex sizes idx

      off <- readRef offRef
      writeRef offRef $ off + len

      return . f $ unsafeSlice off len bs

unsafeSlice :: Int -> Int -> ByteString -> ByteString
unsafeSlice off len (PS ptr poff _) =
  PS ptr (poff + off) len
{-# INLINE unsafeSlice #-}

genericNullWritable :: Generic.Vector v () => WritableCodec Void v ()
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

nullWritable :: WritableCodec Void Unboxed.Vector ()
nullWritable =
  genericNullWritable

genericBytesWritable :: Generic.Vector v ByteString => WritableCodec WritableError v ByteString
genericBytesWritable =
  let
    encodeSizes =
      L.toStrict .
      Builder.toLazyByteString .
      mconcat .
      fmap (bVInt . (+ 4) . B.length) .
      Generic.toList

    encodeBytes bs =
      Builder.word32BE (fromIntegral $ B.length bs) <>
      Builder.byteString bs

    encodeAllBytes =
      L.toStrict .
      Builder.toLazyByteString .
      mconcat .
      fmap encodeBytes .
      Generic.toList

    encode xs =
      (encodeSizes xs, encodeAllBytes xs)

    decode n sbs vbs = do
      sizes <-
        first WritableBinaryError .
        runGet (Unboxed.replicateM n getVInt) $
        L.fromStrict sbs

      let
        expected = B.length vbs
        actual = Unboxed.sum sizes

      when (actual /= expected) $
        Left $ WritableSizesMismatch expected actual

      return $
        unsafeSplit (B.drop 4) vbs sizes
  in
    WritableCodec (ClassName "org.apache.hadoop.io.BytesWritable") encode decode

bytesWritable :: WritableCodec WritableError Boxed.Vector ByteString
bytesWritable =
  genericBytesWritable
