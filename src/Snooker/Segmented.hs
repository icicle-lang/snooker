{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE NoImplicitPrelude #-}
module Snooker.Segmented (
    Segmented(..)
  , segmentedLength
  , segmentedOfBytes
  , bytesOfSegmented
  ) where

import qualified Data.ByteString as B
import           Data.ByteString.Internal (ByteString(..))
import qualified Data.Vector as Boxed
import qualified Data.Vector.Storable as Storable

import           P

data Segmented a =
  Segmented {
      segmentedOffsets :: !(Storable.Vector Int64)
    , segmentedLengths :: !(Storable.Vector Int64)
    , segmentedValues :: !a
    } deriving (Show)

instance Eq (Segmented ByteString) where
  (==) xss yss =
    bytesOfSegmented xss ==
    bytesOfSegmented yss

segmentedLength :: Segmented a -> Int
segmentedLength =
  Storable.length . segmentedLengths
{-# INLINE segmentedLength #-}

segmentedOfBytes :: Boxed.Vector ByteString -> Segmented ByteString
segmentedOfBytes bss =
  let
    !lengths =
      Storable.convert $
      Boxed.map (fromIntegral . B.length) bss

    !offsets =
      Storable.prescanl' (+) 0 lengths
  in
    -- TODO don't use list
    Segmented offsets lengths (B.concat $ Boxed.toList bss)
{-# INLINE segmentedOfBytes #-}

bytesOfSegmented :: Segmented ByteString -> Boxed.Vector ByteString
bytesOfSegmented (Segmented offs lens (PS ptr off0 _)) =
  let
    loop !off !len =
      PS ptr (off0 + fromIntegral off) (fromIntegral len)
  in
    Boxed.zipWith loop (Boxed.convert offs) (Boxed.convert lens)
{-# INLINE bytesOfSegmented #-}
