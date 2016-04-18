{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NoImplicitPrelude #-}
module Snooker.MD5 (
    randomMD5
  ) where

import           Crypto.Hash (Digest, MD5, digestFromByteString)

import qualified Data.ByteString.Base16 as Base16

import           P
import qualified Prelude


randomMD5 :: Digest MD5
randomMD5 =
  let
    -- chosen by fair dice roll
    magic =
      "1BADdeadC0DEfaceFEEDbeefF00Dcafe"
  in
    case digestFromByteString . fst $ Base16.decode magic of
      Nothing ->
        Prelude.error "Snooker.MD5.randomMD5: the guy who sold me this function guaranteed it was total"
      Just md5 ->
        md5
