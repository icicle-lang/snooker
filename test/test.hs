import           Disorder.Core.Main

import qualified Test.Snooker.Codec
import qualified Test.Snooker.VInt


main :: IO ()
main =
  disorderMain [
      Test.Snooker.Codec.tests
    , Test.Snooker.VInt.tests
    ]
