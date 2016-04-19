{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NoImplicitPrelude #-}
module Snooker.MD5 (
    randomMD5
  ) where

import           Crypto.Hash (Digest, MD5, digestFromByteString)

import           Data.ByteString (ByteString)

import           P
import qualified Prelude


-- | Used in sequence files so that you can seek into the middle of a file and
--   then synchronise with record starts and ends by scanning for this value.
randomMD5 :: Digest MD5
randomMD5 =
  let
    -- chosen by fair dice roll
    magic :: ByteString
    magic =
      "ambiata-big-data"
  in
    case digestFromByteString magic of
      Nothing ->
        Prelude.error "Snooker.MD5.randomMD5: the guy who sold me this function guaranteed it was total"
      Just md5 ->
        md5
