{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Test.Snooker.Arbitrary () where

import           Crypto.Hash (Digest, MD5, digestFromByteString)

import qualified Data.ByteString as B

import           Disorder.Core.Gen (genFromMaybe)
import           Disorder.Corpus

import           Snooker.Header

import           P

import           Test.QuickCheck (Arbitrary(..), Gen, vectorOf, listOf, shrinkList)
import           Test.QuickCheck.Instances ()


genMD5 :: Gen (Digest MD5)
genMD5 =
  genFromMaybe $ digestFromByteString . B.pack <$> vectorOf 16 arbitrary

shrinkMD5 :: Digest MD5 -> [Digest MD5]
shrinkMD5 =
  case digestFromByteString $ B.replicate 16 0x0 of
    Nothing ->
      const []
    Just zero ->
      \md5 ->
        if md5 == zero then
          []
        else
          [zero]

genClassName :: Gen ClassName
genClassName =
  ClassName . unWater <$> arbitrary

shrinkClassName :: ClassName -> [ClassName]
shrinkClassName =
  fmap (ClassName . unWater) . shrink . (Water . unClassName)

genMetadata :: Gen [(Text, Text)]
genMetadata =
  listOf $ do
    k <- unMuppet <$> arbitrary
    v <- unBoat <$> arbitrary
    return (k, v)

shrinkMetadata :: [(Text, Text)] -> [[(Text, Text)]]
shrinkMetadata =
  let
    go (k0, v0) =
      [ (k, v0) | k <- fmap unMuppet . shrink $ Muppet k0 ] <>
      [ (k0, v) | v <- fmap unBoat . shrink $ Boat v0 ]
  in
    shrinkList go

genSeqHeader :: Gen SeqHeader
genSeqHeader =
  SeqHeader
    <$> genClassName
    <*> genClassName
    <*> genMetadata
    <*> genMD5

shrinkSeqHeader :: SeqHeader -> [SeqHeader]
shrinkSeqHeader (SeqHeader k0 v0 m0 s0) =
  [ SeqHeader k v0 m0 s0 | k <- shrinkClassName k0 ] <>
  [ SeqHeader k0 v m0 s0 | v <- shrinkClassName v0 ] <>
  [ SeqHeader k0 v0 m s0 | m <- shrinkMetadata m0 ] <>
  [ SeqHeader k0 v0 m0 s | s <- shrinkMD5 s0 ]

instance Arbitrary SeqHeader where
  arbitrary =
    genSeqHeader
  shrink =
    shrinkSeqHeader
