{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}
{-# OPTIONS_GHC -fno-warn-missing-signatures #-}
module Test.Snooker.Conduit where

import           Control.Monad.Morph (MFunctor(..))
import           Control.Monad.Trans.Class (lift)
import           Control.Monad.ST (ST, runST)

import           Data.ByteString (ByteString)
import           Data.Conduit ((=$=), ($$+-), runConduit, newResumableSource)
import           Data.Conduit (ResumableSource)
import qualified Data.Conduit.List as Conduit
import qualified Data.Text as T
import           Data.Void (Void)

import           Snooker.Conduit
import           Snooker.Writable

import           P

import           Test.QuickCheck (Property)
import           Test.QuickCheck ((===), (.&&.))
import           Test.QuickCheck (forAllProperties, quickCheckWithResult)
import           Test.QuickCheck (stdArgs, maxSuccess)
import           Test.QuickCheck.Instances ()

import           Test.Snooker.Arbitrary (ArbitraryMD5(..))
import           Test.Snooker.Util

import           X.Control.Monad.Trans.Either (EitherT, runEitherT)


testEitherST :: (Show ek, Show ev) => (forall s. EitherT (SnookerError ek ev) (ST s) Property) -> Property
testEitherST x =
  let
    renderError =
      renderSnookerError (T.pack . show) (T.pack . show)
  in
    runST $ fmap (squashRender renderError) $ runEitherT x

testEitherST' :: (forall s. EitherT (SnookerError Void Void) (ST s) Property) -> Property
testEitherST' =
  testEitherST

prop_null_bytes_tripping (ArbitraryMD5 sync) metadata0 blocks0 =
  let
    src0 :: ResumableSource (ST s) ByteString
    src0 =
      newResumableSource $
        Conduit.sourceList blocks0 =$= encodeBlocks nullWritable bytesWritable sync metadata0
  in
    testEitherST $ do
      (metadata, src) <- decodeBlocks nullWritable bytesWritable src0
      blocks <- runConduit (hoist lift src $$+- Conduit.consume)
      return $
        metadata0 === metadata .&&.
        blocks0 === blocks

prop_compressed_block_tripping header0 blocks0 =
  let
    src0 :: ResumableSource (ST s) ByteString
    src0 =
      newResumableSource $
        Conduit.sourceList blocks0 =$= encodeCompressedBlocks header0
  in
    testEitherST' $ do
      (header, src) <- decodeCompressedBlocks src0
      blocks <- runConduit (hoist lift src $$+- Conduit.consume)
      return $
        header0 === header .&&.
        blocks0 === blocks


return []
tests =
  $forAllProperties $ quickCheckWithResult (stdArgs {maxSuccess = 100})
