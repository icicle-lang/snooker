{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE NoImplicitPrelude #-}
module Snooker.Data (
    ClassName(..)
  , Metadata(..)
  , Header(..)
  , Block(..)
  , EncodedBlock(..)
  , CompressedBlock(..)
  ) where

import           Crypto.Hash (Digest, MD5)

import           Data.ByteString (ByteString)

import           GHC.Generics (Generic)

import           P

import           X.Text.Show (gshowsPrec)


newtype ClassName =
  ClassName {
      unClassName :: Text
    } deriving (Eq, Ord, Generic)

instance Show ClassName where
  showsPrec =
    gshowsPrec

newtype Metadata =
  Metadata {
      unMetadata :: [(Text, Text)]
    } deriving (Eq, Ord, Generic)

instance Show Metadata where
  showsPrec =
    gshowsPrec

data Header =
  Header {
      headerKeyType :: !ClassName
    , headerValueType :: !ClassName
    , headerMetadata :: !Metadata
    , headerSync :: !(Digest MD5)
    } deriving (Eq, Ord, Show, Generic)

data Block vk vv k v =
  Block {
      blockCount :: !Int
    , blockKeys :: !(vk k)
    , blockValues :: !(vv v)
    } deriving (Eq, Ord, Show, Generic)

data EncodedBlock =
  EncodedBlock {
      encodedCount :: !Int
    , encodedKeySizes :: !ByteString
    , encodedKeys :: !ByteString
    , encodedValueSizes :: !ByteString
    , encodedValues :: !ByteString
    } deriving (Eq, Ord, Show, Generic)

data CompressedBlock =
  CompressedBlock {
      compressedCount :: !Int
    , compressedKeySizes :: !ByteString
    , compressedKeys :: !ByteString
    , compressedValueSizes :: !ByteString
    , compressedValues :: !ByteString
    } deriving (Eq, Ord, Show, Generic)
