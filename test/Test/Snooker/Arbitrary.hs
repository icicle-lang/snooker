{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Test.Snooker.Arbitrary (
    ArbitraryMD5(..)
  ) where

import           Crypto.Hash (Digest, MD5, digestFromByteString)

import qualified Data.ByteString as B
import qualified Data.Vector.Generic as Generic

import           Disorder.Core.Gen (genFromMaybe)
import           Disorder.Corpus

import           Snooker.Data

import           P

import           Test.QuickCheck (Arbitrary(..), Gen)
import           Test.QuickCheck (vectorOf, listOf, shrinkList, genericShrink)
import           Test.QuickCheck.Instances ()


newtype ArbitraryMD5 =
  ArbitraryMD5 {
      unArbitraryMD5 :: Digest MD5
    } deriving (Eq, Ord, Show)

genMD5 :: Gen (Digest MD5)
genMD5 =
  genFromMaybe $ digestFromByteString . B.pack <$> vectorOf 16 arbitrary

shrinkMD5 :: Digest MD5 -> [Digest MD5]
shrinkMD5 =
  case digestFromByteString $ B.replicate 16 0x0 of
    Nothing ->
      const []
    Just zero ->
      \x ->
        if x == zero then
          []
        else
          [zero]

genClassName :: Gen ClassName
genClassName =
  ClassName . unWater <$> arbitrary

shrinkClassName :: ClassName -> [ClassName]
shrinkClassName =
  fmap (ClassName . unWater) . shrink . (Water . unClassName)

genMetadata :: Gen Metadata
genMetadata =
  fmap Metadata . listOf $ do
    k <- unMuppet <$> arbitrary
    v <- unBoat <$> arbitrary
    return (k, v)

shrinkMetadata :: Metadata -> [Metadata]
shrinkMetadata =
  let
    go (k0, v0) =
      [ (k, v0) | k <- fmap unMuppet . shrink $ Muppet k0 ] <>
      [ (k0, v) | v <- fmap unBoat . shrink $ Boat v0 ]
  in
    fmap Metadata . shrinkList go . unMetadata

genHeader :: Gen Header
genHeader =
  Header
    <$> genClassName
    <*> genClassName
    <*> genMetadata
    <*> genMD5

shrinkHeader :: Header -> [Header]
shrinkHeader (Header k0 v0 m0 s0) =
  [ Header k v0 m0 s0 | k <- shrinkClassName k0 ] <>
  [ Header k0 v m0 s0 | v <- shrinkClassName v0 ] <>
  [ Header k0 v0 m s0 | m <- shrinkMetadata m0 ] <>
  [ Header k0 v0 m0 s | s <- shrinkMD5 s0 ]

genCompressedBlock :: Gen CompressedBlock
genCompressedBlock =
  CompressedBlock
    <$> arbitrary
    <*> arbitrary
    <*> arbitrary
    <*> arbitrary
    <*> arbitrary

shrinkCompressedBlock :: CompressedBlock -> [CompressedBlock]
shrinkCompressedBlock =
  genericShrink

genEncodedBlock :: Gen EncodedBlock
genEncodedBlock =
  EncodedBlock
    <$> arbitrary
    <*> arbitrary
    <*> arbitrary
    <*> arbitrary
    <*> arbitrary

shrinkEncodedBlock :: EncodedBlock -> [EncodedBlock]
shrinkEncodedBlock =
  genericShrink

genBlock ::
  Generic.Vector vk k =>
  Generic.Vector vv v =>
  Arbitrary k =>
  Arbitrary v =>
  Gen (Block vk vv k v)
genBlock = do
  ks <- listOf arbitrary
  vs <- vectorOf (length ks) arbitrary
  pure $
    Block (length ks) (Generic.fromList ks) (Generic.fromList vs)

shrinkBlock ::
  Generic.Vector vk k =>
  Generic.Vector vv v =>
  Arbitrary (vk k) =>
  Arbitrary (vv v) =>
  Arbitrary k =>
  Arbitrary v =>
  Block vk vv k v ->
  [Block vk vv k v]
shrinkBlock =
  let
    fixup (Block len ks vs) =
      let
        n = len `min` Generic.length ks `min` Generic.length vs
      in
        Block n (Generic.take n ks) (Generic.take n vs)
  in
    fmap fixup . genericShrink

instance Arbitrary ArbitraryMD5 where
  arbitrary =
    ArbitraryMD5 <$> genMD5
  shrink =
    fmap ArbitraryMD5 . shrinkMD5 . unArbitraryMD5

instance Arbitrary Metadata where
  arbitrary =
    genMetadata
  shrink =
    shrinkMetadata

instance Arbitrary Header where
  arbitrary =
    genHeader
  shrink =
    shrinkHeader

instance Arbitrary CompressedBlock where
  arbitrary =
    genCompressedBlock
  shrink =
    shrinkCompressedBlock

instance Arbitrary EncodedBlock where
  arbitrary =
    genEncodedBlock
  shrink =
    shrinkEncodedBlock

instance
  ( Generic.Vector vk k
  , Generic.Vector vv v
  , Arbitrary (vk k)
  , Arbitrary (vv v)
  , Arbitrary k
  , Arbitrary v
  ) => Arbitrary (Block vk vv k v) where
  arbitrary =
    genBlock
  shrink =
    shrinkBlock
