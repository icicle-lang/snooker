{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE NoImplicitPrelude #-}
module Snooker.MD5 (
    randomMD5
  ) where

import           Crypto.Hash (Digest, MD5, hash)

import           Control.Monad.IO.Class (MonadIO(..))

import qualified Data.ByteString.Lazy as L
import qualified Data.UUID as UUID
import           Data.UUID.V4 (nextRandom)

import           P


-- | Generate a random MD5, Hadoop style:
--
--   https://github.com/apache/hadoop/blob/513ec3de194f705ca342de16829e1f85be227e7f/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/io/SequenceFile.java#L847
--
randomMD5 :: MonadIO m => m (Digest MD5)
randomMD5 =
  liftIO $ liftM (hash . L.toStrict . UUID.toByteString) nextRandom
