module Main where

import Lib
import Data.List
import System.Environment (getArgs)

main :: IO ()
main = do
  args <- getArgs
  let cfile = "config/ses_credentials.json"
  cred <- getCredentials cfile
  let
    addresses = filter (elem '@') args
    mFrom = head addresses
    mTo = tail addresses
    attachment =  safeHead $ filter (notElem '@') args
  sendEmail mFrom mTo attachment cred
