module Main where

import Lib
import Data.List
import System.Environment (getArgs)

main :: IO ()
main = do
  args <- getArgs
  let cfile = safeHead $ filter (any (`elem` "json")) args
  cred <- getCredentials cfile
  let
    addresses = filter (elem '@') args
    mFrom = head addresses
    mTo = tail addresses
    attachment =  safeHead $ filter (/= cfile) $filter (notElem '@') args
  sendEmail mFrom mTo attachment cred
