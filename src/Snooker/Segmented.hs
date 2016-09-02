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
import qualified Data.Vector.Unboxed as Unboxed

import           P

data Segmented a =
  Segmented {
      segmentedOffsets :: !(Unboxed.Vector Int)
    , segmentedLengths :: !(Unboxed.Vector Int)
    , segmentedValues :: !a
    } deriving (Show)

instance Eq (Segmented ByteString) where
  (==) xss yss =
    bytesOfSegmented xss ==
    bytesOfSegmented yss

segmentedLength :: Segmented a -> Int
segmentedLength =
  Unboxed.length . segmentedLengths
{-# INLINE segmentedLength #-} 

segmentedOfBytes :: Boxed.Vector ByteString -> Segmented ByteString
segmentedOfBytes bss =
  let
    loop (off0, len0) bs =
      let
        len =
          B.length bs
      in
        (off0 + len0, len)

    (offsets, lengths) =
      Unboxed.unzip $
      Unboxed.convert $
      Boxed.postscanl' loop (0, 0) bss
  in
    -- TODO don't use list
    Segmented offsets lengths . B.concat $ Boxed.toList bss
{-# INLINE segmentedOfBytes #-}

bytesOfSegmented :: Segmented ByteString -> Boxed.Vector ByteString
bytesOfSegmented (Segmented offs lens (PS ptr off0 _)) =
  let
    loop !off !len =
      PS ptr (off0 + off) len
  in
    Boxed.zipWith loop (Boxed.convert offs) (Boxed.convert lens)
{-# INLINE bytesOfSegmented #-}
