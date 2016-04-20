{-# LANGUAGE NoImplicitPrelude #-}
module Snooker.Storable (
    pokeWord8
  , pokeWord32be
  , pokeWord64be
  , pokeByteString
  ) where

import           Data.ByteString.Internal (ByteString(..))
import qualified Data.ByteString.Internal as B
import           Data.Word

import           Foreign.Ptr (Ptr, castPtr, plusPtr)
import           Foreign.ForeignPtr (withForeignPtr)
import           Foreign.Storable (pokeByteOff)

import           P

import           System.IO (IO)


pokeWord8 :: Ptr x -> Int -> Word8 -> IO ()
pokeWord8 p off x =
  pokeByteOff p off x
{-# INLINE pokeWord8 #-}

pokeWord32be :: Ptr x -> Int -> Word32 -> IO ()
pokeWord32be p off x =
  pokeByteOff p off (byteSwap32 x)
{-# INLINE pokeWord32be #-}

pokeWord64be :: Ptr x -> Int -> Word64 -> IO ()
pokeWord64be p off x =
  pokeByteOff p off (byteSwap64 x)
{-# INLINE pokeWord64be #-}

pokeByteString :: Ptr x -> Int -> ByteString -> IO ()
pokeByteString p off (PS xfp xoff xlen) =
  withForeignPtr xfp $ \xptr -> do
    B.memcpy (castPtr p `plusPtr` off) (xptr `plusPtr` xoff) xlen
{-# INLINE pokeByteString #-}
