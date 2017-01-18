{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}
{-# OPTIONS_GHC -fno-warn-missing-signatures #-}
{-# OPTIONS_GHC -fno-warn-deprecations #-}
module Test.Snooker.Hadoop where

import           Control.Monad.Morph (MFunctor(..))
import           Control.Monad.IO.Class (liftIO)
import           Control.Monad.Trans.Class (lift)

import           Data.ByteString (ByteString)
import           Data.Conduit ((=$=), ($$+-), runConduit, newResumableSource)
import           Data.Conduit (ResumableSource)
import qualified Data.Conduit.Binary as Conduit
import qualified Data.Conduit.List as Conduit
import qualified Data.List as List

import           Disorder.Core.IO (testIO)

import           Snooker.Compression
import           Snooker.Conduit
import           Snooker.Data
import           Snooker.Writable

import           System.Exit (ExitCode(..))
import           System.IO (IO)
import           System.IO.Temp (withSystemTempFile)
import           System.Process (system)

import           P

import           Test.QuickCheck ((==>), (===), (.&&.), property)
import           Test.QuickCheck (forAllProperties, quickCheckWithResult)
import           Test.QuickCheck (stdArgs, maxSuccess)
import           Test.QuickCheck.Instances ()

import           Test.Snooker.Arbitrary ()
import           Test.Snooker.Util

import           X.Control.Monad.Trans.Either (pattern EitherT, runEitherT)


prop_null_bytes_tripping metadata blocks =
  --not (null blocks) ==>
  --all ((> 0) . blockCount) blocks ==>
  let
    src :: ResumableSource IO ByteString
    src =
      newResumableSource $
        Conduit.sourceList blocks =$=
        encodeBlocks bytesWritable bytesWritable zlibCodec metadata
  in
    testIO . withSystemTempFile "snooker-" $ \path handle -> do
      liftIO $ src $$+- Conduit.sinkHandle handle
      code <- system . List.unwords $ [
             "HADOOP_CLASSPATH=dist/build/java"
           , "./bin/hadoop"
           , "SequenceCheck"
           , "2"
           , path
           ]
      -- system "sleep 100000"
        --   "./bin/hadoop", "fs"
        -- , "-D", "log4j.logger.org.apache.hadoop=DEBUG"
        -- , "-text", path
        -- , ">/dev/null"
        -- ]
      pure $
        code === ExitSuccess

return []
tests =
  $forAllProperties $ quickCheckWithResult (stdArgs {maxSuccess = 100})
