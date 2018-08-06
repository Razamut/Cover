module Main where

import System.Environment (getArgs)
import Processor

safeHead :: [String] -> String
safeHead [] = []
safeHead (x:_) = x


main :: IO ()
main = do
  args <- getArgs
  let loanTypes = concat $ filter (any (`elem` ["ld", "usd"]) ) [[x] | x <- args]
      dbPath = safeHead [xs | xs <- args, xs `notElem` loanTypes ]
  mapM_ (runGetDebtors dbPath oPath "loans.sql" "collections.sql") loanTypes
