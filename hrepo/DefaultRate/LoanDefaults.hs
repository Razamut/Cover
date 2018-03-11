{-# LANGUAGE DeriveGeneric #-}
module LoanDefaults where

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


getColnRecordsWithDatesFromQuery :: String -> String -> IO [(String, Double, String)]
getColnRecordsWithDatesFromQuery dbName query = do
  sqlExpectedColnRecords <- queryDatabase dbName query
  let expectedColnRecords = zip3 (readStringColumn sqlExpectedColnRecords 0) (readDoubleColumn sqlExpectedColnRecords 1) (readStringColumn sqlExpectedColnRecords 2)
  return expectedColnRecords

getColnRecordsWithDates :: String -> String -> IO ( Either String [(String, Double, String)] )
getColnRecordsWithDates dbName loanType = case map toLower loanType of
  "ld" -> do
            let query = "SELECT loan_id, payment_amt_ld, payment_dt FROM collections WHERE payment_amt_ld > 0 AND typeof(payment_amt_ld)=\"real\""
            records <- getColnRecordsWithDatesFromQuery dbName query
            return $ Right records
  "usd" -> do
             let query = "SELECT loan_id, payment_amt_usd, payment_dt FROM collections WHERE payment_amt_usd > 0 AND typeof(payment_amt_usd)=\"real\""
             records <- getColnRecordsWithDatesFromQuery dbName query
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


getDebtorsRecordsWithDates :: [Maybe (Integer, String, String, String, Double, String, String)] -> Maybe [(Integer, String, String, String, Double, String, String)]
getDebtorsRecordsWithDates ys = sequenceA $ filter (\x -> x /= Nothing) ys

combineTuplesFromListsWithDates :: [(Integer, String, String)] -> [(Integer, String, Double, String, String)] -> [Maybe (Integer, String, String, String, Double, String, String)]
combineTuplesFromListsWithDates xs ys = nub $ [combineTuplesWithDates x y | x <- xs, y <- ys]

combineTuplesWithDates :: (Integer, String, String) -> (Integer, String, Double, String, String) -> Maybe (Integer, String, String, String, Double, String, String)
combineTuplesWithDates (a, b, c) (x,y,z,v,w) | a == x = Just (a,b,c,y,z,v,w)
                                | otherwise = Nothing


data PersonWithDates = PersonWithDates { id :: Integer, first_name :: String , last_name :: String,
    loan_id :: String, owed :: Double, loan_received :: String, last_payment :: String }
    deriving (Generic, Show)

instance FromNamedRecord PersonWithDates
instance ToNamedRecord PersonWithDates
instance DefaultOrdered PersonWithDates

convertToPersonWithDates :: (Integer, String, String, String, Double, String, String) -> PersonWithDates
convertToPersonWithDates (a, b, c, d, e, f, g) = PersonWithDates a b c d e f g

getNamesForUserIDs :: [Integer] -> IO [(Integer, String, String)]
getNamesForUserIDs userIDs = do
  let stringyIDs = map show userIDs
  let query = "SELECT id, first_name, last_name FROM person WHERE id IN " ++ " (" ++ (intercalate ", " stringyIDs) ++ ")"
  sqlQueryResult <- queryDatabase "person.sql" query
  let queryResult = zip3 (readIntegerColumn sqlQueryResult 0) (readStringColumn sqlQueryResult 1) (readStringColumn sqlQueryResult 2)
  return queryResult

findUserIDsForLoansWithDates :: [(String, Double, String, String)] -> [(Integer, String)] -> [(Integer, String, Double, String, String)]
findUserIDsForLoansWithDates outstandingLoans userIDsLoanIDs = map (\x -> findUserIDForLoanWithDates x userIDsLoanIDs) outstandingLoans

findUserIDForLoanWithDates :: (String, Double, String, String) -> [(Integer, String)] -> (Integer, String, Double, String, String)
findUserIDForLoanWithDates loanIDamtWithDates userIDsLoanIDs = userIDloanIDamtWithDates
  where
    theUserID = fst $ head $ filter (\(_, b) -> b == (\(x,_,_,_) -> x) loanIDamtWithDates) userIDsLoanIDs
    userIDloanIDamtWithDates = (\ a0 (a1,a2,a3,a4) -> (a0,a1,a2,a3,a4)) theUserID loanIDamtWithDates


idCollectionsWithDates :: String -> [(String, Double, String)] -> [(String, [Double], [String])]
idCollectionsWithDates loanID idExpectedCollections  = [(loanID, map (\(_, b, _) -> b) filteredId, map (\(_, _, c) -> c) filteredId) ]
  where
    filteredId = filter (\(a, _, _) -> a == loanID) idExpectedCollections

idsCollectionsWithDates :: [String] -> [(String, Double,String)] -> [(String, [Double], [String])]
idsCollectionsWithDates ids idExpectedCollections = concatMap (\id -> idCollectionsWithDates id idExpectedCollections ) ids

first :: (a, b, c) -> a
first (x, _, _) = x

second :: (a, b, c) -> b
second (_, x, _) = x

third :: (a, b, c) -> c
third (_, _, x) = x

idActualColnMinusExpectedColnWithDates :: String -> [(String, Double, String)] -> [(String, Double, String)] -> [(String, Double, String, String)]
idActualColnMinusExpectedColnWithDates id actuals expectations = [(id, actual - expectation, loanreceived, lastpaymentdate)]
  where
    actual  = case filter (\(a,_,_) -> a == id) actuals of
                [] -> 0.0
                x:xs -> second $ head $ filter (\(a,_,_) -> a == id) actuals
    expectation = second $ head $ filter (\(a,_,_) -> a == id) expectations
    loanreceived = third $ head $ filter (\(a,_,_) -> a == id) expectations
    lastpaymentdate = case filter (\(a,_,_) -> a == id) actuals of
                [] -> "No Payments Recieved"
                x:xs -> third $ head $ filter (\(a,_,_) -> a == id) actuals

idsActualColnMinusExpectedColnWithDates :: [(String, Double, String)] -> [(String, Double, String)] -> [(String, Double, String, String)]
idsActualColnMinusExpectedColnWithDates actuals expectations =
  concatMap (\id -> idActualColnMinusExpectedColnWithDates id actuals expectations) ids
  where
    ids = nub $ map (\(x,_,_) -> x ) expectations


getDefaulters :: String -> IO ( Either String [PersonWithDates] )
getDefaulters loanType = do
  loanRecords <- getLoanRecords "loans.sql" loanType
  collectionRecords <- getColnRecordsWithDates "collections.sql" loanType
  case loanRecords of
    Right loanRecs -> do
      case collectionRecords of
        Right colnRecs -> do
          let loansExpectedCollectionsWithDates = map (\(_,a,d,b,c) -> (a, c + (b/100.0) * c, d)  ) loanRecs
          let loanIDs = nub $ map (\(x,_,_) -> x ) colnRecs
          let loansCollectionsWithDates = idsCollectionsWithDates loanIDs colnRecs
          let loansTotalCollectionsWithDates = map (\(a, b, c) -> (a, sum b, maximum c)) loansCollectionsWithDates
          let loansOutstandingWithDates = filter  (\(_, y, _,_) -> y < 0) $  idsActualColnMinusExpectedColnWithDates  loansTotalCollectionsWithDates loansExpectedCollectionsWithDates
          let userIDsLoanIDs = nub $ map (\(a,b,_,_,_) -> (a, b)) loanRecs
          let usersLoansAmtsWithDates = findUserIDsForLoansWithDates loansOutstandingWithDates userIDsLoanIDs
          let usersToQuery = map (\(a, _, _,_,_) -> a) usersLoansAmtsWithDates
          usersAndIDs <- getNamesForUserIDs usersToQuery
          let debtorsRecs = fromMaybe [] $ getDebtorsRecordsWithDates $ combineTuplesFromListsWithDates usersAndIDs usersLoansAmtsWithDates
          let debtors = map convertToPersonWithDates debtorsRecs
          return $ Right debtors
        Left err -> return $ Left err
    Left err -> return $ Left err

saveRecordsToCSV :: String -> [PersonWithDates] -> IO ()
saveRecordsToCSV fileName persons = BSL.writeFile fileName $ encodeDefaultOrderedByName persons

main :: IO ()
main = do
  usdDefaulters <- getDefaulters "USD"
  case usdDefaulters of
    Right usdDebts -> do
      saveRecordsToCSV "usd_defaulters.csv" usdDebts
      putStrLn "created usd_defaulters.csv file"
    Left err -> putStrLn err
  ldDefaulters <- getDefaulters "LD"
  case ldDefaulters of
    Right ldDebts -> do
      saveRecordsToCSV "ld_defaulters.csv" ldDebts
      putStrLn "created ld_defaulters.csv file"
    Left err -> putStrLn err
