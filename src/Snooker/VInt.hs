{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE NoImplicitPrelude #-}
module Snooker.VInt (
    maxSizeVInt
  , getVInt
  , getVInt64
  , bVInt
  , bVInt64
  , vInt64Size
  ) where

import           Data.Binary.Get (Get)
import qualified Data.Binary.Get as Binary
import           Data.Bits ((.&.), (.|.), complement, shiftL, shiftR, bit, countLeadingZeros)
import qualified Data.ByteString as B
import           Data.ByteString.Builder (Builder)
import qualified Data.ByteString.Builder as Builder
import           Data.Word (Word8)

import           P


maxSizeVInt :: Int
maxSizeVInt =
  9
{-# INLINE maxSizeVInt #-}

b11110000 :: Int8
b11110000 =
  fromIntegral (0xf0 :: Word8)
{-# INLINE b11110000 #-}

b10000000 :: Int8
b10000000 =
  fromIntegral (0x80 :: Word8)
{-# INLINE b10000000 #-}

b00000111 :: Int8
b00000111 =
  fromIntegral (0x07 :: Word8)
{-# INLINE b00000111 #-}

b00001000 :: Int8
b00001000 =
  fromIntegral (0x08 :: Word8)
{-# INLINE b00001000 #-}

vintSingle :: Int8 -> Bool
vintSingle firstByte =
  (firstByte .&. b11110000) /= b10000000
{-# INLINE vintSingle #-}

vintRemaining :: Int8 -> Int
vintRemaining firstByte =
  fromIntegral (complement firstByte .&. b00000111) + 1
{-# INLINE vintRemaining #-}

vintNegative :: Int8 -> Bool
vintNegative firstByte =
  (complement firstByte .&. b00001000) /= 0
{-# INLINE vintNegative #-}

getVInt64 :: Get Int64
getVInt64 = do
  firstByte <- fromIntegral <$!> Binary.getWord8

  if vintSingle firstByte then
    pure $! fromIntegral firstByte
  else do
    let
      go :: Int64 -> Word8 -> Int64
      go !x b =
        (x `shiftL` 8) .|. fromIntegral b
      {-# INLINE go #-}

    !x <- B.foldl' go 0 <$!> Binary.getByteString (vintRemaining firstByte)

    if vintNegative firstByte then
      pure $! complement x
    else
      pure x
{-# INLINE getVInt64 #-}

getVInt :: Get Int
getVInt =
  fromIntegral <$> getVInt64
{-# INLINE getVInt #-}

bVInt64 :: Int64 -> Builder
bVInt64 v =
  if v >= -112 && v <= 127 then
    Builder.word8 $ fromIntegral v
  else
    let
      (base, value) =
        if v >= 0 then
          (-113, v)
        else
          (-121, complement v)
    in
      if value < bit 8 then
        Builder.word8 base <>
        Builder.word8 (fromIntegral value)
      else if value < bit 16 then
        Builder.word8 (base - 1) <>
        Builder.word8 (fromIntegral $ value `shiftR` 8) <>
        Builder.word8 (fromIntegral value)
      else if value < bit 24 then
        Builder.word8 (base - 2) <>
        Builder.word8 (fromIntegral $ value `shiftR` 16) <>
        Builder.word8 (fromIntegral $ value `shiftR` 8) <>
        Builder.word8 (fromIntegral value)
      else if value < bit 32 then
        Builder.word8 (base - 3) <>
        Builder.word32BE (fromIntegral $ value)
      else if value < bit 40 then
        Builder.word8 (base - 4) <>
        Builder.word32BE (fromIntegral $ value `shiftR` 8) <>
        Builder.word8 (fromIntegral value)
      else if value < bit 48 then
        Builder.word8 (base - 5) <>
        Builder.word32BE (fromIntegral $ value `shiftR` 16) <>
        Builder.word8 (fromIntegral $ value `shiftR` 8) <>
        Builder.word8 (fromIntegral value)
      else if value < bit 56 then
        Builder.word8 (base - 6) <>
        Builder.word32BE (fromIntegral $ value `shiftR` 24) <>
        Builder.word8 (fromIntegral $ value `shiftR` 16) <>
        Builder.word8 (fromIntegral $ value `shiftR` 8) <>
        Builder.word8 (fromIntegral value)
      else
        Builder.word8 (base - 7) <>
        Builder.word64BE (fromIntegral value)

bVInt :: Int -> Builder
bVInt =
  bVInt64 . fromIntegral
{-# INLINE bVInt #-}

vInt64Size :: Int64 -> Int64
vInt64Size v =
  if v >= -112 && v <= 127 then
    1
  else
    let
      v' =
        if v >= 0 then
          v
        else
          complement v
      dataBits =
        64 - countLeadingZeros v'
    in
      (fromIntegral dataBits + 7) `div` 8 + 1;
