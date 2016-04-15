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
import           Data.Conduit (ResumableSource)
import           Data.Conduit ((=$=), ($$+-), runConduit, newResumableSource)
import qualified Data.Conduit.List as Conduit

import           Snooker.Conduit

import           P

import           Test.QuickCheck (Property)
import           Test.QuickCheck ((===), (.&&.))
import           Test.QuickCheck (forAllProperties, quickCheckWithResult)
import           Test.QuickCheck (stdArgs, maxSuccess)
import           Test.QuickCheck.Instances ()

import           Test.Snooker.Arbitrary ()
import           Test.Snooker.Util

import           X.Control.Monad.Trans.Either (EitherT, runEitherT)


testEitherST :: (forall s. EitherT SnookerError (ST s) Property) -> Property
testEitherST x =
  runST $ fmap (squashRender renderSnookerError) $ runEitherT x

prop_block_tripping header0 blocks0 =
  let
    src0 :: ResumableSource (ST s) ByteString
    src0 =
      newResumableSource $
        Conduit.sourceList blocks0 =$= encodeCompressedBlocks header0
  in
    testEitherST $ do
      (header, src) <- decodeCompressedBlocks src0
      blocks <- runConduit (hoist lift src $$+- Conduit.consume)
      return $
        header0 === header .&&.
        blocks0 === blocks

return []
tests =
  $forAllProperties $ quickCheckWithResult (stdArgs {maxSuccess = 100})
