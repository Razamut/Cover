{-# LANGUAGE DeriveGeneric #-}
module LoanDueDates where

import Data.Dates
import Data.Text (Text)
import GHC.Generics (Generic)
import Data.Csv as DC hiding (lookup)
import Data.Maybe (fromMaybe)
import Text.CSV
import Data.List
import Data.Char (toLower)
import Data.Either
import Database.HDBC
import Database.HDBC.Sqlite3
import System.Environment (getArgs)
import qualified Data.ByteString.Lazy as BSL
import UtilityFunctions (queryDatabase, readIntegerColumn, readStringColumn,
    readDoubleColumn, addMonthsToDateTime, computeMonthsToAdd, convertStringToDateTime,
    combineTuplesFromLists,getDebtorsRecords, getNamesForUserIDs, findUserIDsForLoans,
    idsActualColnMinusExpectedColn,idsCollections, nextDueAmt, getColnRecords,
    getLoanRecords)

{-
--loans schema
["id INTEGER","loan_id TEXT","loan_type TEXT","amt_requested TEXT","loan_amt_usd REAL","loan_amt_ld REAL","rate REAL","interest_ld REAL","interest_usd REAL","duration_month REAL","status TEXT","take_out_dt TEXT","loan_plus_intr_ld REAL","loan_plus_intr_usd REAL"]
--collections schema
["id INTEGER","loan_id TEXT","loan_plus_intr_usd REAL","loan_plus_intr_ld REAL","payment_dt TEXT","payment_amt_usd REAL","payment_amt_ld REAL"]
--person schema
["id INTEGER","first_name TEXT","last_name TEXT","middle_name TEXT","id_number TEXT","id_type TEXT","age REAL","gender TEXT","ph_numbers REAL","email TEXT"]
-}

data Person = Person { id :: Integer, first_name :: String , last_name :: String,
    loan_id :: String, take_out_date:: String, owed :: Double, next_due_date::String, next_due_amt::Double}
    deriving (Generic, Show)

instance FromNamedRecord Person
instance ToNamedRecord Person
instance DefaultOrdered Person

convertToPerson :: (Integer, String, String, String, String, Double, String, Double) -> Person
convertToPerson (a, b, c, d, e, f, g, h) = Person a b c d e f g h

getDebtors :: String -> IO (Either String [Person])
getDebtors loanType = do
  loanRecords <- getLoanRecords "loans.sql" loanType
  collectionRecords <- getColnRecords "collections.sql" loanType
  case loanRecords of
    Right loanRecs -> do
      case collectionRecords of
        Right colnRecs -> do
          let
              loansExpectedCollections = map (\(_,loanId,_,rate,loan_amt) ->
                                           (loanId, loan_amt + (rate/100.0) * loan_amt)  ) loanRecs
              loansNextExpectedPayment = map (\(_,loanId,_,rate,loan_amt) ->
                                           (loanId, nextDueAmt rate (loan_amt + (rate/100.0) * loan_amt) ) ) loanRecs
              loanIDs = nub $ map (\(x,_) -> x ) colnRecs
              loansCollections = idsCollections loanIDs colnRecs
              loansTotalCollections = map (\(a, b) -> (a, sum b)) loansCollections
              loansOutstanding = filter  (\(_, y) -> y < 0) $  idsActualColnMinusExpectedColn  loansTotalCollections loansExpectedCollections
              userIDsLoanIDsTakeOutDates = nub $ map (\(a,b,c,_,_) -> (a, b, c)) loanRecs
              usersLoansTakeOutDatesAmts = findUserIDsForLoans loansOutstanding userIDsLoanIDsTakeOutDates
              usersToQuery = map (\(a, _, _, _) -> a) usersLoansTakeOutDatesAmts
          usersAndIDs <- getNamesForUserIDs usersToQuery
          let debtorsRecs = fromMaybe [] $ getDebtorsRecords $ combineTuplesFromLists usersAndIDs usersLoansTakeOutDatesAmts
              takeOutDates = map (\(_, _, _, _, e, _) -> convertStringToDateTime e ) debtorsRecs
              numberOfMonthsToAdd = map computeMonthsToAdd takeOutDates
          listOfMaybeDueDates <- sequenceA $ zipWith addMonthsToDateTime numberOfMonthsToAdd takeOutDates
          defaultDate <- getCurrentDateTime
          let dueDates = map (\dt -> fst $ break (==',') $ show $ fromMaybe defaultDate dt) listOfMaybeDueDates
              debtorsWithDueDates :: [(Integer, String, String, String, String, Double, String)]
              debtorsWithDueDates = zipWith (\(a, b, c, d, e, f) x -> (a, b, c, d, e, f, x) ) debtorsRecs dueDates
              dueAmts = map (\(a, b, c, d, e, f, x) -> fromMaybe 0.0 $ lookup d loansNextExpectedPayment) debtorsWithDueDates
              debtorsWithDueAmts :: [(Integer, String, String, String, String, Double, String, Double)]
              debtorsWithDueAmts = zipWith (\(a, b, c, d, e, f, x) y -> (a, b, c, d, e, f, x, minimum [(-f), y])) debtorsWithDueDates dueAmts
              debtors = map convertToPerson debtorsWithDueAmts
          return $ Right debtors
        Left err -> return $ Left err
    Left err -> return $ Left err

saveDebtorsRecordsToCSV :: String -> [Person] -> IO ()
saveDebtorsRecordsToCSV fileName persons = BSL.writeFile fileName $ encodeDefaultOrderedByName persons

runGetDebtors :: [String] -> IO ()
runGetDebtors [] = return ()
runGetDebtors (x:xs) = do
  debtors <- getDebtors x
  case debtors of
    Right records -> do
      saveDebtorsRecordsToCSV (x ++ "_debtors.csv") records
      putStrLn $ "created " ++ x ++ "_debtors.csv file"
    Left err -> putStrLn err
  runGetDebtors xs

main :: IO ()
main = getArgs >>= runGetDebtors
