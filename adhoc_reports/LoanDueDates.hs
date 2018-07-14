{-# LANGUAGE DeriveGeneric #-}

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
    combineTuplesFromLists, getNamesForUserIDs, findUserIDsForLoans,
    idsActualColnMinusExpectedColn,idsCollections, nextDueAmt, getColnRecords,
    getLoanRecords)


{-
--loans schema
["user_id INTEGER","loan_id TEXT","loan_type TEXT","amt_requested TEXT","loan_amt_usd REAL","loan_amt_ld REAL","rate REAL","interest_ld REAL","interest_usd REAL","duration_month REAL","status TEXT","take_out_dt TEXT","loan_plus_intr_ld REAL","loan_plus_intr_usd REAL"]
--collections schema
["user_id INTEGER","loan_id TEXT","loan_plus_intr_usd REAL","loan_plus_intr_ld REAL","payment_dt TEXT","payment_amt_usd REAL","payment_amt_ld REAL"]
--users schema
["user_id INTEGER","first_name TEXT","last_name TEXT","middle_name TEXT","id_number TEXT","id_type TEXT","age REAL","gender TEXT","ph_numbers REAL","email TEXT"]
-}

data User = User { user_id :: Integer, first_name :: String , last_name :: String,
    loan_id :: String, take_out_date:: String, debt :: Double, next_due_date::String, next_due_amt::Double}
    deriving (Generic, Show)

instance FromNamedRecord User
instance ToNamedRecord User
instance DefaultOrdered User

convertToPerson :: (Integer, String, String, String, String, Double, String, Double) -> User
convertToPerson (a, b, c, d, e, f, g, h) = User a b c d e f g h

getLoansExpectedCollections :: [(Integer, String, String, Double, Double)] -> [(String, Double)]
getLoansExpectedCollections = map (\(_,loanId,_,rate,loan_amt) ->
                             (loanId, loan_amt + (rate/100.0) * loan_amt)  )

getLoansNextExpectedPayment :: [(Integer, String, String, Double, Double)] -> [(String, Double)]
getLoansNextExpectedPayment = map (\(_,loanId,_,rate,loan_amt) ->
                             (loanId, nextDueAmt rate (loan_amt + (rate/100.0) * loan_amt) ) )

getDebtorsWithDueDates :: [(Integer, String, String, String, String, Double)]
                       -> [String]
                       -> [(Integer, String, String, String, String, Double, String)]
getDebtorsWithDueDates debtorsRecs dueDates =
  zipWith (\(a, b, c, d, e, f) x -> (a, b, c, d, e, f, x) ) debtorsRecs dueDates

getDebtorsWithDueAmts :: [(Integer, String, String, String, String, Double, String)]
                   -> [Double]
                   -> [(Integer, String, String, String, String, Double, String, Double)]
getDebtorsWithDueAmts debtorsWithDueDates dueAmts =
  zipWith (\(a, b, c, d, e, f, x) y -> (a, b, c, d, e, f, x, minimum [(-f), y])) debtorsWithDueDates dueAmts

getDebtorsRecords :: [(Integer, String, String, Double, Double)]
                  -> [(String, Double)]
                  -> IO [ (Integer, String, String, String, String, Double, String, Double) ]
getDebtorsRecords loanRecs colnRecs = do
  let
      loansExpectedCollections = getLoansExpectedCollections loanRecs
      loansNextExpectedPayment = getLoansNextExpectedPayment loanRecs
      loanIDs = nub $ map (\(x,_) -> x ) colnRecs
      loansCollections = idsCollections loanIDs colnRecs
      loansTotalCollections = map (\(a, b) -> (a, sum b)) loansCollections
      loansOutstanding = filter  (\(_, y) -> y < 0) $  idsActualColnMinusExpectedColn  loansTotalCollections loansExpectedCollections
      userIDsLoanIDsTakeOutDates = nub $ map (\(a,b,c,_,_) -> (a, b, c)) loanRecs
      usersLoansTakeOutDatesAmts = findUserIDsForLoans loansOutstanding userIDsLoanIDsTakeOutDates
      usersToQuery = map (\(a, _, _, _) -> a) usersLoansTakeOutDatesAmts
  usersAndIDs <- getNamesForUserIDs usersToQuery
  let debtorsRecs = fromMaybe [] $ sequenceA $ filter (\x -> x /= Nothing) $ combineTuplesFromLists usersAndIDs usersLoansTakeOutDatesAmts
      takeOutDates = map (\(_, _, _, _, e, _) -> convertStringToDateTime e ) debtorsRecs
      numberOfMonthsToAdd = map computeMonthsToAdd takeOutDates
  listOfMaybeDueDates <- sequenceA $ zipWith addMonthsToDateTime numberOfMonthsToAdd takeOutDates
  defaultDate <- getCurrentDateTime
  let dueDates = map (\dt -> fst $ break (==',') $ show $ fromMaybe defaultDate dt) listOfMaybeDueDates
      debtorsWithDueDates = getDebtorsWithDueDates debtorsRecs dueDates
      dueAmts = map (\(a, b, c, d, e, f, x) -> fromMaybe 0.0 $ lookup d loansNextExpectedPayment) debtorsWithDueDates
      debtorsWithDueAmts = getDebtorsWithDueAmts debtorsWithDueDates dueAmts
  return debtorsWithDueAmts

getDebtors :: String -> IO (Either String [User])
getDebtors loanType = do
  loanRecords <- getLoanRecords "data/loans.sql" loanType
  collectionRecords <- getColnRecords "data/collections.sql" loanType
  case loanRecords of
    Right loanRecs -> do
      case collectionRecords of
        Right colnRecs -> do
          debtorsWithDueAmts <- getDebtorsRecords loanRecs colnRecs
          let debtors = map convertToPerson debtorsWithDueAmts
          return $ Right debtors
        Left err -> return $ Left err
    Left err -> return $ Left err

saveDebtorsRecordsToCSV :: String -> [User] -> IO ()
saveDebtorsRecordsToCSV fileName persons = BSL.writeFile fileName $ encodeDefaultOrderedByName persons

runGetDebtors :: String -> IO ()
runGetDebtors [] = return ()
runGetDebtors xs = do
  debtors <- getDebtors xs
  case debtors of
    Right records -> do
      saveDebtorsRecordsToCSV ("data/" ++ xs ++ "_debtors.csv") records
      putStrLn $ "created " ++ xs ++ "_debtors.csv file"
    Left err -> putStrLn err

main :: IO ()
main = do
  loanTypes <- getArgs
  mapM_ runGetDebtors loanTypes
