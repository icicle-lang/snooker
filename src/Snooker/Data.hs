{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
module Snooker.Data (
    ClassName(..)
  , Metadata(..)
  , Header(..)
  , Block(..)
  , EncodedBlock(..)
  , CompressedBlock(..)

  -- * Quasi-quoter for 'Digest MD5'
  , md5
  ) where

import           Crypto.Hash (Digest, MD5, digestFromByteString)

import           Data.ByteString (ByteString)
import qualified Data.ByteString.Base16 as Base16
import qualified Data.ByteString.Char8 as Char8
import           Data.String (String)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T

import           GHC.Generics (Generic)

import           Language.Haskell.TH (Lit(..), litE, varE, appE)
import           Language.Haskell.TH.Quote (QuasiQuoter)

import           P
import qualified Prelude

import           X.Text.Show (gshowsPrec)

import           X.Language.Haskell.TH (qparse)


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

unsafeMD5 :: String -> Digest MD5
unsafeMD5 s =
  case digestFromByteString (Char8.pack s) of
    Nothing ->
      Prelude.error "Snooker.Data.md5: the impossible happened, received invalid MD5 hash at runtime"
    Just x ->
      x

md5 :: QuasiQuoter
md5 =
  qparse $ \s ->
    let
      bs16 =
        fst . Base16.decode . T.encodeUtf8 . T.strip $ T.pack s

      s16 =
        litE . StringL $ Char8.unpack bs16
    in
      case digestFromByteString bs16 of
        Nothing ->
          fail $ "Not an MD5 hash: " <> s
        Just (_ :: Digest MD5) ->
          varE 'unsafeMD5 `appE` s16
