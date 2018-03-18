{-# LANGUAGE DeriveGeneric #-}
module LoanDueDates where

import Data.Text    (Text)
import GHC.Generics (Generic)
import Data.Csv as DC
import Data.Maybe (fromMaybe)
import Text.CSV
import Data.List
import Data.Char (toLower)
import Data.Either
import Database.HDBC
import Database.HDBC.Sqlite3
import CreateDatabases (queryDatabase)
import LoanSummary (readIntegerColumn, readStringColumn, readDoubleColumn)
import qualified Data.ByteString.Lazy as BSL


{-
--loans schema
["id INTEGER","loan_id TEXT","loan_type TEXT","amt_requested TEXT","loan_amt_usd REAL","loan_amt_ld REAL","rate REAL","interest_ld REAL","interest_usd REAL","duration_month REAL","status TEXT","take_out_dt TEXT","loan_plus_intr_ld REAL","loan_plus_intr_usd REAL"]
--collections schema
["id INTEGER","loan_id TEXT","loan_plus_intr_usd REAL","loan_plus_intr_ld REAL","payment_dt TEXT","payment_amt_usd REAL","payment_amt_ld REAL"]
--person schema
["id INTEGER","first_name TEXT","last_name TEXT","middle_name TEXT","id_number TEXT","id_type TEXT","age REAL","gender TEXT","ph_numbers REAL","email TEXT"]
-}

getExpectedColnRecords :: Either String [(Integer, String, String, Double, Double)] -> Either String [(String, Double)]
getExpectedColnRecords  loanRecords = case loanRecords of
  Right records -> Right $ map (\(_,a,_,b,c) -> (a, c + (b/100.0) * c)  ) records
  Left err -> Left err

getColnRecordsFromQuery :: String -> String -> IO [(String, Double)]
getColnRecordsFromQuery dbName query = do
  sqlExpectedColnRecords <- queryDatabase dbName query
  let expectedColnRecords = zip (readStringColumn sqlExpectedColnRecords 0) (readDoubleColumn sqlExpectedColnRecords 1)
  return expectedColnRecords

getColnRecords :: String -> String -> IO ( Either String [(String, Double)] )
getColnRecords dbName loanType = case map toLower loanType of
  "ld" -> do
            let query = "SELECT loan_id, payment_amt_ld FROM collections WHERE typeof(payment_amt_ld)=\"real\" AND payment_amt_ld > 0"
            records <- getColnRecordsFromQuery dbName query
            return $ Right records
  "usd" -> do
             let query = "SELECT loan_id, payment_amt_usd FROM collections WHERE typeof(payment_amt_usd)=\"real\" AND payment_amt_usd > 0"
             records <- getColnRecordsFromQuery dbName query
             return $ Right records
  _   -> do
           return $ Left "Provide a valid loan type. Valid loan types are LD or USD."


getLoanRecordsFromQuery :: String -> String -> IO [(Integer, String, String, Double, Double)]
getLoanRecordsFromQuery dbName query = do
  sqlLoanRecords <- queryDatabase dbName query
  let loanRecords = zip5 (readIntegerColumn sqlLoanRecords 0) (readStringColumn sqlLoanRecords 1) (readStringColumn sqlLoanRecords 2) (readDoubleColumn sqlLoanRecords 3) (readDoubleColumn sqlLoanRecords 4)
  return loanRecords

getLoanRecords :: String -> String -> IO ( Either String [(Integer, String, String, Double, Double)] )
getLoanRecords dbName loanType = case map toLower loanType of
  "ld" -> do
            let query = "SELECT id, loan_id, take_out_dt, rate, loan_amt_ld FROM loans WHERE status = 'approved' AND loan_amt_ld > 0"
            records <- getLoanRecordsFromQuery dbName query
            return $ Right records
  "usd" -> do
             let query = "SELECT id, loan_id, take_out_dt, rate, loan_amt_usd FROM loans WHERE status = 'approved' AND loan_amt_usd > 0"
             records <- getLoanRecordsFromQuery dbName query
             return $ Right records
  _   -> do
           return $ Left "Provide a valid loan type. Valid loan types are LD or USD."


getDebtorsRecords :: [Maybe (Integer, String, String, String, Double)] -> Maybe [(Integer, String, String, String, Double)]
getDebtorsRecords ys = sequenceA $ filter (\x -> x /= Nothing) ys

combineTuplesFromLists :: [(Integer, String, String)] -> [(Integer, String, Double)] -> [Maybe (Integer, String, String, String, Double)]
combineTuplesFromLists xs ys = nub $ [combineTuples x y | x <- xs, y <- ys]

combineTuples :: (Integer, String, String) -> (Integer, String, Double) -> Maybe (Integer, String, String, String, Double)
combineTuples (a, b, c) (x,y,z) | a == x = Just (a,b,c,y,z)
                                | otherwise = Nothing

data Person = Person { id :: Integer, first_name :: String , last_name :: String,
    loan_id :: String, owed :: Double }
    deriving (Generic, Show)

instance FromNamedRecord Person
instance ToNamedRecord Person
instance DefaultOrdered Person

convertToPerson :: (Integer, String, String, String, Double) -> Person
convertToPerson (a, b, c, d, e) = Person a b c d e

getNamesForUserIDs :: [Integer] -> IO [(Integer, String, String)]
getNamesForUserIDs userIDs = do
  let stringyIDs = map show userIDs
  let query = "SELECT id, first_name, last_name FROM person WHERE id IN " ++ " (" ++ (intercalate ", " stringyIDs) ++ ")"
  sqlQueryResult <- queryDatabase "person.sql" query
  let queryResult = zip3 (readIntegerColumn sqlQueryResult 0) (readStringColumn sqlQueryResult 1) (readStringColumn sqlQueryResult 2)
  return queryResult

findUserIDsForLoans :: [(String, Double)] -> [(Integer, String)] -> [(Integer, String, Double)]
findUserIDsForLoans outstandingLoans userIDsLoanIDs = map (\x -> findUserIDForLoan x userIDsLoanIDs) outstandingLoans

findUserIDForLoan :: (String, Double) -> [(Integer, String)] -> (Integer, String, Double)
findUserIDForLoan loanIDamtPair userIDsLoanIDs = userIDloanIDamtTriple
  where
    theUserID = fst $ head $ filter (\(_, b) -> b == fst loanIDamtPair) userIDsLoanIDs
    userIDloanIDamtTriple = (theUserID, fst loanIDamtPair, snd loanIDamtPair)

idCollections :: String -> [(String, Double)] -> [(String, [Double])]
idCollections loanID idExpectedCollections  = [(loanID, map (\(_, b) -> b) $ filter (\(a, _) -> a == loanID) idExpectedCollections) ]

idsCollections :: [String] -> [(String, Double)] -> [(String, [Double])]
idsCollections ids idExpectedCollections = concatMap (\id -> idCollections id idExpectedCollections ) ids

idActualColnMinusExpectedColn :: String -> [(String, Double)] -> [(String, Double)] -> [(String, Double)]
idActualColnMinusExpectedColn id actuals expectations = [(id, actual - expectation)]
  where
    actual  = case filter (\(a,_) -> a == id) actuals of
                [] -> 0.0
                x:xs -> snd $ head $ filter (\(a,_) -> a == id) actuals
    expectation = snd $ head $ filter (\(a, _) -> a == id) expectations

idsActualColnMinusExpectedColn :: [(String, Double)] -> [(String, Double)] -> [(String, Double)]
idsActualColnMinusExpectedColn actuals expectations =
  concatMap (\id -> idActualColnMinusExpectedColn id actuals expectations) ids
  where
    ids = nub $ map (\(x,_) -> x ) expectations

getDebtors :: String -> IO (Either String [Person])
getDebtors loanType = do
  loanRecords <- getLoanRecords "loans.sql" loanType
  collectionRecords <- getColnRecords "collections.sql" loanType
  case loanRecords of
    Right loanRecs -> do
      case collectionRecords of
        Right colnRecs -> do
          let loansExpectedCollections = map (\(_,a,_,b,c) -> (a, c + (b/100.0) * c)  ) loanRecs
          let loanIDs = nub $ map (\(x,_) -> x ) colnRecs
          let loansCollections = idsCollections loanIDs colnRecs
          let loansTotalCollections = map (\(a, b) -> (a, sum b)) loansCollections
          let loansOutstanding = filter  (\(_, y) -> y < 0) $  idsActualColnMinusExpectedColn  loansTotalCollections loansExpectedCollections
          let userIDsLoanIDs = nub $ map (\(a,b,_,_,_) -> (a, b)) loanRecs
          let usersLoansAmts = findUserIDsForLoans loansOutstanding userIDsLoanIDs
          let usersToQuery = map (\(a, _, _) -> a) usersLoansAmts
          usersAndIDs <- getNamesForUserIDs usersToQuery
          let debtorsRecs = fromMaybe [] $ getDebtorsRecords $ combineTuplesFromLists usersAndIDs usersLoansAmts
          let debtors = map convertToPerson debtorsRecs
          return $ Right debtors
        Left err -> return $ Left err
    Left err -> return $ Left err

saveRecordsToCSV :: String -> [Person] -> IO ()
saveRecordsToCSV fileName persons = BSL.writeFile fileName $ encodeDefaultOrderedByName persons

main :: IO ()
main = do
  usdDebtors <- getDebtors "USD"
  case usdDebtors of
    Right usdDebts -> do
      saveRecordsToCSV "usd_debtors.csv" usdDebts
      putStrLn "created usd_debtors.csv file"
    Left err -> putStrLn err
  ldDebtors <- getDebtors "LD"
  case ldDebtors of
    Right ldDebts -> do
      saveRecordsToCSV "ld_debtors.csv" ldDebts
      putStrLn "created ld_debtors.csv file"
    Left err -> putStrLn err
