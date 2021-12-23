{-# LANGUAGE NoImplicitPrelude #-}
module Snooker.Storable (
    pokeWord8
  , pokeWord32be
  , pokeWord64be
  , pokeByteString

  , peekWord8be
  , peekWord32be
  , peekWord64be
  ) where

import           Data.ByteString.Internal (ByteString(..))
import qualified Data.ByteString.Internal as B
import           Data.Word

import           Foreign.Ptr (Ptr, castPtr, plusPtr)
import           Foreign.ForeignPtr (withForeignPtr)
import           Foreign.Storable (pokeByteOff, peekByteOff)

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

peekWord8be :: Ptr x -> Int -> IO Word8
peekWord8be p off =
  peekByteOff p off
{-# INLINE peekWord8be #-}

peekWord32be :: Ptr x -> Int -> IO Word32
peekWord32be p off =
  byteSwap32 <$> peekByteOff p off
{-# INLINE peekWord32be #-}

peekWord64be :: Ptr x -> Int -> IO Word64
peekWord64be p off =
  byteSwap64 <$> peekByteOff p off
{-# INLINE peekWord64be #-}

pokeByteString :: Ptr x -> Int -> ByteString -> IO ()
pokeByteString p off (PS xfp xoff xlen) =
  withForeignPtr xfp $ \xptr -> do
    B.memcpy (castPtr p `plusPtr` off) (xptr `plusPtr` xoff) xlen
{-# INLINE pokeByteString #-}
