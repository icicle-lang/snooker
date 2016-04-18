snooker
=======

Haskell library for reading and writing the Hadoop SequenceFile format.

```
Ivory -> Scrivello -> Billiard Balls -> Snooker
```

![Elephants playing snooker](img/elephant-snooker.png)

## Usage

There are two main functions for reading and writing sequence files:

```hs
module Snooker.Conduit where

decodeBlocks ::
  Functor m =>
  Monad m =>
  Generic.Vector vk k =>
  Generic.Vector vv v =>
  WritableCodec ek vk k ->
  WritableCodec ev vv v ->
  ResumableSource m ByteString ->
  EitherT (SnookerError ek ev) m
    (Metadata, ResumableSource (EitherT (SnookerError ek ev) m) (Block vk vv k v))

encodeBlocks ::
  MonadIO m =>
  MonadBase base m =>
  PrimMonad base =>
  Generic.Vector vk k =>
  WritableCodec ek vk k ->
  WritableCodec ev vv v ->
  Metadata ->
  Conduit (Block vk vv k v) m ByteString
```

Both `decodeBlocks` and `encodeBlocks` take a `WritableCodec` which
describes how the keys and values should be encoded/decoded. Currently
only `NullWritable` and `BytesWritable` are supported:

```hs
module Snooker.Writable where

data WritableCodec e v a =
  WritableCodec {
      writableClass :: ClassName
    , writableEncode :: v a -> (ByteString, ByteString)
    , writableDecode :: Int -> ByteString -> ByteString -> Either e (v a)
    }

nullWritable :: WritableCodec Void Unboxed.Vector ()

bytesWritable :: WritableCodec WritableError Boxed.Vector ByteString
```

To encode a file, connect `encodeBlocks` to a conduit source and sink:

```hs
import Snooker.Conduit (encodeBlocks)

writeSequence :: FilePath -> [Block Unboxed.Vector Boxed.Vector () ByteString] -> IO ()
writeSequence path blocks =
  runConduit $
    Conduit.sourceList blocks =$=
    encodeBlocks nullWritable bytesWritable metadata0 =$=
    Conduit.sinkFile path
```

To decode a file, connect `decodeBlock` to a conduit source and sink:

```hs
import Snooker.Conduit (decodeBlocks)

readSequence ::
  FilePath ->
  IO (Either (SnookerError Void WritableError) [Block Unboxed.Vector Boxed.Vector () ByteString])
readSequence path = do
  runResourceT . runEitherT $ do
    (metadata, blocks) <-
      decodeBlocks nullWritable bytesWritable .
      Conduit.newResumableSource $
      Conduit.sourceFile path
    blocks $$+- Conduit.consume
```
