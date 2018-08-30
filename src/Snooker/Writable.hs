{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Snooker.Writable (
    WritableCodec(..)
  , WritableError(..)

  , nullWritable
  , genericNullWritable

  , bytesWritable
  , boxedBytesWritable
  , segmentedBytesWritable

  , vLongWritable
  ) where

import           Anemone.Foreign.VInt (encodeVIntArray, decodeVIntArray)

import qualified Data.ByteString as B
import           Data.ByteString.Internal (ByteString(..))
import qualified Data.ByteString.Internal as B
import qualified Data.Vector as Boxed
import qualified Data.Vector.Generic as Generic
import qualified Data.Vector.Storable as Storable
import qualified Data.Vector.Unboxed as Unboxed
import           Data.Void (Void)

import           Foreign.ForeignPtr (withForeignPtr)
import           Foreign.Ptr (plusPtr)
import           Foreign.Storable (Storable(..))

import           P

import           Snooker.Binary
import           Snooker.Data
import           Snooker.Segmented
import           Snooker.Storable
import           Snooker.VInt

import           System.IO.Unsafe (unsafePerformIO)

import qualified X.Data.ByteString.Unsafe as B


data WritableError =
    WritableBinaryError !BinaryError
  | WritableVIntError
  | WritableVIntBytesLeftover !Int
  | WritableSizesMismatch !Int !Int
    deriving (Eq, Ord, Show)

data WritableCodec x a =
  WritableCodec {
      writableClass :: ClassName
    , writableEncode :: a -> (ByteString, ByteString)
    , writableDecode :: Int -> ByteString -> ByteString -> Either x a
    }

------------------------------------------------------------------------

genericNullWritable :: Generic.Vector v () => WritableCodec Void (v ())
genericNullWritable =
  let
    encodeSizes xs =
      B.unsafeCreate (Generic.length xs) $ \ptr -> do
        _ <- B.memset ptr 0 (fromIntegral $ Generic.length xs)
        pure ()
    {-# INLINE encodeSizes #-}

    encode xs =
      (encodeSizes xs, B.empty)
    {-# INLINE encode #-}

    decode n _ _ =
      Right $ Generic.replicate n ()
    {-# INLINE decode #-}
  in
    WritableCodec (ClassName "org.apache.hadoop.io.NullWritable") encode decode
{-# INLINE genericNullWritable #-}

nullWritable :: WritableCodec Void (Unboxed.Vector ())
nullWritable =
  genericNullWritable
{-# INLINE nullWritable #-}

------------------------------------------------------------------------

encodeBoxedValues :: Boxed.Vector ByteString -> ByteString
encodeBoxedValues bss =
  {-# SCC encodeBoxedValues #-}
  let
    !n_total =
      Boxed.foldl' (\n bs -> n + 4 + (B.length bs)) 0 bss
  in
    B.unsafeCreate n_total $ \ptr ->
      let
        go !n bs = do
          pokeWord32be ptr n (fromIntegral $ B.length bs)
          pokeByteString ptr (n + 4) bs
          pure $! n + 4 + B.length bs
      in
        Generic.foldM_ go 0 bss
{-# INLINE encodeBoxedValues #-}

encodeBoxedSizes :: Boxed.Vector ByteString -> ByteString
encodeBoxedSizes bss =
  {-# SCC encodeBoxedSizes #-}
  let
    mkSize bs =
      fromIntegral $! B.length bs + 4
  in
    encodeVIntArray .
    Storable.convert $
    Boxed.map mkSize bss
{-# INLINE encodeBoxedSizes #-}

encodeBoxedBytes :: Boxed.Vector ByteString -> (ByteString, ByteString)
encodeBoxedBytes xs =
  (encodeBoxedSizes xs, encodeBoxedValues xs)
{-# INLINE encodeBoxedBytes #-}

decodeBoxedBytes :: Int -> ByteString -> ByteString -> Either WritableError (Boxed.Vector ByteString)
decodeBoxedBytes n sizes values =
  {-# SCC decodeBoxedBytes #-}
  case decodeVIntArray n sizes of
    Nothing ->
      Left WritableVIntError
    Just (lengths0, leftover) ->
      if not $ B.null leftover then
        Left . WritableVIntBytesLeftover $ B.length leftover
      else
        let
          !actual =
            fromIntegral $
            Storable.sum lengths0

          !expected =
            B.length values

          !lengths =
            Unboxed.map fromIntegral $
            Unboxed.convert lengths0
        in
          if actual /= expected then do
            Left $! WritableSizesMismatch expected actual
          else
            {-# SCC decodeGenericBytes_unsafeSplits #-}
            Right $! B.unsafeSplits (B.drop 4) values lengths
{-# INLINE decodeBoxedBytes #-}

boxedBytesWritable :: WritableCodec WritableError (Boxed.Vector ByteString)
boxedBytesWritable =
  WritableCodec
    (ClassName "org.apache.hadoop.io.BytesWritable")
    encodeBoxedBytes
    decodeBoxedBytes
{-# INLINE boxedBytesWritable #-}

bytesWritable :: WritableCodec WritableError (Boxed.Vector ByteString)
bytesWritable =
  boxedBytesWritable
{-# INLINE bytesWritable #-}

------------------------------------------------------------------------

data OffLen =
  OffLen !Int64 !Int64

instance Storable OffLen where
  sizeOf _ =
    sizeOf (0 :: Int64) +
    sizeOf (0 :: Int64)
  {-# INLINE sizeOf #-}

  alignment _ =
    alignment (0 :: Int64)
  {-# INLINE alignment #-}

  peek ptr = do
    off <- peekByteOff ptr 0
    len <- peekByteOff ptr 8
    pure $! OffLen off len
  {-# INLINE peek #-}

  poke ptr (OffLen off len) = do
    pokeByteOff ptr 0 off
    pokeByteOff ptr 8 len
  {-# INLINE poke #-}

encodeSegmentedValues :: Segmented ByteString -> ByteString
encodeSegmentedValues (Segmented offsets lengths (PS fp_in off_in _)) =
  {-# SCC encodeSegmentedValues #-}
  unsafePerformIO . withForeignPtr fp_in $ \ptr_in0 ->
    let
      !n_total =
        Storable.foldl' (\n len -> n + 4 + len) 0 lengths

      !ptr_in =
        ptr_in0 `plusPtr` off_in
    in
      B.create (fromIntegral n_total) $ \ptr_out ->
        let
          go !n (OffLen off len) = do
            pokeWord32be ptr_out n $! fromIntegral len

            B.memcpy
              (ptr_out `plusPtr` n `plusPtr` 4)
              (ptr_in `plusPtr` fromIntegral off)
              (fromIntegral len)

            pure $! n + 4 + fromIntegral len
        in
          Storable.foldM_ go 0 $
          Storable.zipWith OffLen offsets lengths
{-# INLINE encodeSegmentedValues #-}

encodeSegmentedSizes :: Segmented ByteString -> ByteString
encodeSegmentedSizes (Segmented _ lengths _) =
  {-# SCC encodeSegmentedSizes #-}
  let
    mkSize !n =
      fromIntegral $! n + 4
  in
    encodeVIntArray $
    Storable.map mkSize lengths
{-# INLINE encodeSegmentedSizes #-}

encodeSegmentedBytes  :: Segmented ByteString -> (ByteString, ByteString)
encodeSegmentedBytes xs =
  (encodeSegmentedSizes xs, encodeSegmentedValues xs)
{-# INLINE encodeSegmentedBytes #-}

decodeSegmentedBytes :: Int -> ByteString -> ByteString -> Either WritableError (Segmented ByteString)
decodeSegmentedBytes n sizes values =
  {-# SCC decodeSegmentedBytes #-}
  case decodeVIntArray n sizes of
    Nothing ->
      Left WritableVIntError
    Just (lengths0, leftover) ->
      if not $ B.null leftover then
        Left . WritableVIntBytesLeftover $ B.length leftover
      else
        let
          !actual =
            fromIntegral $
            Storable.sum lengths0

          !expected =
            B.length values

          !offsets =
            Storable.prescanl' (\acc len -> acc + len) 4 lengths0

          !lengths =
            Storable.map (fromIntegral . subtract 4) lengths0
        in
          if actual /= expected then do
            Left $! WritableSizesMismatch expected actual
          else
            Right $! Segmented offsets lengths values
{-# INLINE decodeSegmentedBytes #-}

segmentedBytesWritable :: WritableCodec WritableError (Segmented ByteString)
segmentedBytesWritable =
  WritableCodec
    (ClassName "org.apache.hadoop.io.BytesWritable")
    encodeSegmentedBytes
    decodeSegmentedBytes
{-# INLINE segmentedBytesWritable #-}

------------------------------------------------------------------------

vLongWritable :: WritableCodec WritableError (Storable.Vector Int64)
vLongWritable =
  let
    encodeSizes iss =
      encodeVIntArray $
        Storable.map vInt64Size iss
    {-# INLINE encodeSizes #-}

    encode xs =
      (encodeSizes xs, encodeVIntArray xs)
    {-# INLINE encode #-}

    decode n _sizes is =
      case decodeVIntArray n is of
        Nothing ->
          Left WritableVIntError
        Just (iss, leftover) ->
          if not $ B.null leftover then
            Left . WritableVIntBytesLeftover $ B.length leftover
          else
            pure iss
    {-# INLINE decode #-}
  in
    WritableCodec (ClassName "org.apache.hadoop.io.VLongWritable") encode decode
{-# INLINE vLongWritable #-}

-----------------------------------------------------------------------
