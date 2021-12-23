{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}
{-# OPTIONS_GHC -fno-warn-missing-signatures #-}
module Test.Snooker.Conduit where

import           Control.Monad.Morph (MFunctor(..))
import           Control.Monad.Trans.Class (lift)
import           Control.Monad.Trans.Either (EitherT)

import           Data.ByteString (ByteString)
import           Data.Conduit ((=$=), ($$+-), runConduit, newResumableSource)
import           Data.Conduit (ResumableSource)
import qualified Data.Conduit.List as Conduit
import           Data.Void (Void)

import           Snooker.Conduit
import           Snooker.Writable

import           System.IO (IO)

import           P

import           Test.QuickCheck (Property)
import           Test.QuickCheck ((===), (.&&.))
import           Test.QuickCheck (forAllProperties, quickCheckWithResult)
import           Test.QuickCheck (stdArgs, maxSuccess)
import           Test.QuickCheck.Instances ()

import           Test.Snooker.Arbitrary ()
import           Test.Snooker.Util



prop_null_bytes_tripping metadata0 blocks0 =
  let
    src0 :: ResumableSource IO ByteString
    src0 =
      newResumableSource $
        Conduit.sourceList blocks0 =$= encodeBlocks nullWritable bytesWritable metadata0
  in
    testEitherIO $ do
      (metadata, src) <- decodeBlocks nullWritable bytesWritable src0
      blocks <- runConduit (hoist lift src $$+- Conduit.consume)
      return $
        metadata0 === metadata .&&.
        blocks0 === blocks

prop_compressed_block_tripping header0 blocks0 =
  let
    src0 :: ResumableSource IO ByteString
    src0 =
      newResumableSource $
        Conduit.sourceList blocks0 =$= encodeCompressedBlocks header0

    go :: EitherT (SnookerError Void Void) IO Property
    go = do
      (header, src) <- decodeCompressedBlocks src0
      blocks <- runConduit (hoist lift src $$+- Conduit.consume)
      return $
        header0 === header .&&.
        blocks0 === blocks
  in
    testEitherIO go

return []
tests =
  $forAllProperties $ quickCheckWithResult (stdArgs {maxSuccess = 100})
