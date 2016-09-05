import           Disorder.Core.Main

import qualified Test.Snooker.Codec
import qualified Test.Snooker.Conduit
import qualified Test.Snooker.Foreign
import qualified Test.Snooker.MD5
import qualified Test.Snooker.Segmented
import qualified Test.Snooker.VInt
import qualified Test.Snooker.Writable


main :: IO ()
main =
  disorderMain [
      Test.Snooker.Codec.tests
    , Test.Snooker.Conduit.tests
    , Test.Snooker.Foreign.tests
    , Test.Snooker.MD5.tests
    , Test.Snooker.Segmented.tests
    , Test.Snooker.VInt.tests
    , Test.Snooker.Writable.tests
    ]
