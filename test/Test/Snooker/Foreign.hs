{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Snooker.Foreign where

import           Anemone.Foreign.VInt (encodeVIntArray, decodeVIntArray)

import qualified Data.ByteString.Builder as Builder
import qualified Data.ByteString.Lazy as Lazy
import qualified Data.Vector.Storable as Storable

import           Disorder.Core (ExpectedTestSpeed(..), disorderCheckEnvAll)
import           Disorder.Jack (Property, gamble)
import           Disorder.Jack (tripping, listOfN, sizedBounded)

import           P

import           Snooker.Binary
import           Snooker.VInt

import           System.IO (IO)

import           Test.QuickCheck.Instances ()


prop_roundtrip_vint64_hask_to_c :: Property
prop_roundtrip_vint64_hask_to_c =
  gamble (Storable.fromList <$> listOfN 0 10000 sizedBounded) $ \xs ->
    tripping
      (Lazy.toStrict . Builder.toLazyByteString . mconcat . fmap bVInt64 . Storable.toList)
      (fmap fst . decodeVIntArray (Storable.length xs))
      xs

prop_roundtrip_vint64_c_to_hask :: Property
prop_roundtrip_vint64_c_to_hask =
  gamble (Storable.fromList <$> listOfN 0 10000 sizedBounded) $ \xs ->
    tripping
      encodeVIntArray
      (runGet (Storable.replicateM (Storable.length xs) getVInt64) . Lazy.fromStrict)
      xs

return []
tests :: IO Bool
tests =
  $disorderCheckEnvAll TestRunMore
