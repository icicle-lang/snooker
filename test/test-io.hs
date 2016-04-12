import           Disorder.Core.Main

import qualified Test.Snooker.Factset


main :: IO ()
main =
  disorderMain [
      Test.Snooker.Factset.tests
    ]
