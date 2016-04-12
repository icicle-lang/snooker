import           Disorder.Core.Main

import qualified Test.Snooker.Header
import qualified Test.Snooker.VInt


main :: IO ()
main =
  disorderMain [
      Test.Snooker.Header.tests
    , Test.Snooker.VInt.tests
    ]
