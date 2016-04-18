{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE NoImplicitPrelude #-}
module Snooker.Data (
    ClassName(..)
  , Metadata(..)
  , Header(..)

  , Block
  , mkBlock
  , blockCount
  , blockKeys
  , blockValues

  , EncodedBlock(..)
  , CompressedBlock(..)
  ) where

import           Crypto.Hash (Digest, MD5)

import           Data.ByteString (ByteString)
import qualified Data.Vector.Generic as Generic

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
      blockKeys :: !(vk k)
    , blockValues :: !(vv v)
    } deriving (Eq, Ord, Show, Generic)

-- | Get the number of records in a 'Block'.
blockCount :: Generic.Vector vk k => Block vk vv k v -> Int
blockCount =
  Generic.length . blockKeys

-- | Create a 'Block', must have the same number of keys and values.
mkBlock :: (Generic.Vector vk k, Generic.Vector vv v) => vk k -> vv v -> Maybe (Block vk vv k v)
mkBlock ks vs =
  if Generic.length ks == Generic.length vs then
    Just $ Block ks vs
  else
    Nothing

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
