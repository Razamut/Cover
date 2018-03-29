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
import Data.Time
import Data.Time.Clock
import Data.Time.Calendar


{-
--loans schema
["id INTEGER","loan_id TEXT","loan_type TEXT","amt_requested TEXT","loan_amt_usd REAL","loan_amt_ld REAL","rate REAL","interest_ld REAL","interest_usd REAL","duration_month REAL","status TEXT","take_out_dt TEXT","loan_plus_intr_ld REAL","loan_plus_intr_usd REAL"]
--collections schema
["id INTEGER","loan_id TEXT","loan_plus_intr_usd REAL","loan_plus_intr_ld REAL","payment_dt TEXT","payment_amt_usd REAL","payment_amt_ld REAL"]
--person schema
["id INTEGER","first_name TEXT","last_name TEXT","middle_name TEXT","id_number TEXT","id_type TEXT","age REAL","gender TEXT","ph_numbers REAL","email TEXT"]
-}

defaultDaysB :: Integer
defaultDaysB = 180

defaultDaysP :: Integer
defaultDaysP = 90

getExpectedColnRecords :: Either String [(Integer, String, String, Double, Double)] -> Either String [(String, Double)]
getExpectedColnRecords  loanRecords = case loanRecords of
  Right records -> Right $ map (\(_,a,_,b,c) -> (a, c + (b/100.0) * c)  ) records
  Left err -> Left err

getNumberOfDaysSinceToday :: String -> IO(Integer)
getNumberOfDaysSinceToday date = do
  today <- getCurrentTime
  let now = utctDay today
  let prev = parseTimeOrError True defaultTimeLocale "%Y-%-m-%-d" date :: Day
  return $ diffDays now prev

getDiffDays :: String -> Day -> Integer
getDiffDays sday1 day2 = numdays
  where
    day1 = parseTimeOrError True defaultTimeLocale "%Y-%-m-%-d" sday1 :: Day
    numdays = diffDays day2 day1

getColnRecordsWithDatesFromQuery :: String -> String -> IO [(String, Double, String)]
getColnRecordsWithDatesFromQuery dbName query = do
  sqlExpectedColnRecords <- queryDatabase dbName query
  let expectedColnRecords = zip3 (readStringColumn sqlExpectedColnRecords 0) (readDoubleColumn sqlExpectedColnRecords 1) (readStringColumn sqlExpectedColnRecords 2)
  return expectedColnRecords

getColnRecordsWithDates :: String -> String -> IO ( Either String [(String, Double, String)] )
getColnRecordsWithDates dbName loanType = case map toLower loanType of
  "ld" -> do
            let query = "SELECT loan_id, payment_amt_ld, payment_dt FROM collections WHERE CAST(payment_amt_ld  AS DOUBLE) > 0"
            records <- getColnRecordsWithDatesFromQuery dbName query
            return $ Right records
  "usd" -> do
             let query = "SELECT loan_id, payment_amt_usd, payment_dt FROM collections WHERE CAST(payment_amt_usd  AS DOUBLE) > 0"
             records <- getColnRecordsWithDatesFromQuery dbName query
             return $ Right records
  _   -> do
           return $ Left "Provide a valid loan type. Valid loan types are LD or USD."



getLoanRecordsFromQuery :: String -> String -> IO [(Integer, String, String, String, Double, Double)]
getLoanRecordsFromQuery dbName query = do
  sqlLoanRecords <- queryDatabase dbName query
  let loanRecords = zip6 (readIntegerColumn sqlLoanRecords 0) (readStringColumn sqlLoanRecords 1) (readStringColumn sqlLoanRecords 2) (readStringColumn sqlLoanRecords 3) (readDoubleColumn sqlLoanRecords 4) (readDoubleColumn sqlLoanRecords 5)
  return loanRecords

getLoanRecords :: String -> String -> IO ( Either String [(Integer, String, String, String, Double, Double)] )
getLoanRecords dbName loanType = case map toLower loanType of
  "ld" -> do
            let query = "SELECT id, loan_id, take_out_dt, loan_type, rate, loan_amt_ld FROM loans WHERE status = 'approved' AND loan_amt_ld > 0"
            records <- getLoanRecordsFromQuery dbName query
            return $ Right records
  "usd" -> do
             let query = "SELECT id, loan_id, take_out_dt, loan_type, rate, loan_amt_usd FROM loans WHERE status = 'approved' AND loan_amt_usd > 0"
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
    loan_id :: String, owed :: Double, loan_received :: String, last_payment :: String, days_last_payment :: Integer, loan_type :: String, in_default :: String}
    deriving (Generic, Show)

instance FromNamedRecord PersonWithDates
instance ToNamedRecord PersonWithDates
instance DefaultOrdered PersonWithDates

checkDefault :: String -> Integer -> String
checkDefault lt days
  | lt == "b" && days >= defaultDaysB = "DEFAULTED"
  | lt == "p" && days >= defaultDaysP = "DEFAULTED"
  | otherwise = "GOOD"

convertToPersonWithDates :: Day -> [(String, String)] -> (Integer, String, String, String, Double, String, String) -> PersonWithDates
convertToPersonWithDates today loanidstype (a, b, c, id, e, f, g) = PersonWithDates a b c id e f g daysLastPayment loantype inDefault
  where
    daysLastPayment = getDiffDays g today
    loantype = snd $ head $ filter (\(x, _) -> x==id) loanidstype
    inDefault = checkDefault loantype daysLastPayment 
    

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
                [] -> loanreceived
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
          let loansExpectedCollectionsWithDates = map (\(_,a,d,_,b,c) -> (a, c + (b/100.0) * c, d)  ) loanRecs
          let loanIDs = nub $ map (\(x,_,_) -> x ) colnRecs
          let loansCollectionsWithDates = idsCollectionsWithDates loanIDs colnRecs
          let loansTotalCollectionsWithDates = map (\(a, b, c) -> (a, sum b, maximum c)) loansCollectionsWithDates
          let loansOutstandingWithDates = filter  (\(_, y, _,_) -> y < 0) $  idsActualColnMinusExpectedColnWithDates  loansTotalCollectionsWithDates loansExpectedCollectionsWithDates
          let userIDsLoanIDs = nub $ map (\(a,b,_,_,_,_) -> (a, b)) loanRecs
          let usersLoansAmtsWithDates = findUserIDsForLoansWithDates loansOutstandingWithDates userIDsLoanIDs
          let usersToQuery = map (\(a, _, _,_,_) -> a) usersLoansAmtsWithDates
          usersAndIDs <- getNamesForUserIDs usersToQuery
          curday <- getCurrentTime
          let today = utctDay curday
          let debtorsRecs = fromMaybe [] $ getDebtorsRecordsWithDates $ combineTuplesFromListsWithDates usersAndIDs usersLoansAmtsWithDates
          let loanIdsloanTypes = map (\(_,id,_,lt,_,_) -> (id, lt)) loanRecs
          let debtors = map (convertToPersonWithDates today loanIdsloanTypes) debtorsRecs
          let numLoans = fromInteger (genericLength loanIDs)
          let numDefaults = fromInteger $ genericLength $ filter (\(PersonWithDates{in_default=d}) -> d == "DEFAULTED") debtors
          putStrLn $ "The percentage of defaulted " ++ loanType ++ " loans: " ++ (show $ 100*numDefaults/numLoans)
          putStrLn $ "The number of " ++ loanType ++ " loans are: " ++ show numLoans
          putStrLn $ "The number of " ++ loanType ++ " defaulted loans are: " ++ show numDefaults
          
          
          return $ Right debtors
        Left err -> return $ Left err
    Left err -> return $ Left err

saveRecordsToCSV :: String -> [PersonWithDates] -> IO ()
saveRecordsToCSV fileName persons = BSL.writeFile fileName $ encodeDefaultOrderedByName persons

getCollectionsChooseDefault :: String -> PersonWithDates -> Double
getCollectionsChooseDefault defaulted (PersonWithDates{owed = a, in_default=b}) 
  | b == defaulted = a
  | otherwise = 0.0

main :: IO ()
main = do
  usdDefaulters <- getDefaulters "USD"
  case usdDefaulters of
    Right usdDebts -> do
      saveRecordsToCSV "usd_defaulters.csv" usdDebts
      let goodCollects = map (getCollectionsChooseDefault "GOOD") usdDebts
      putStrLn $ "The amount of usd loans owed which are not defaulted: $" ++ show (-(sum goodCollects))
      putStrLn "created usd_defaulters.csv file"
    Left err -> putStrLn err
  ldDefaulters <- getDefaulters "LD"
  case ldDefaulters of
    Right ldDebts -> do
      saveRecordsToCSV "ld_defaulters.csv" ldDebts
      let goodCollects = map (getCollectionsChooseDefault "GOOD") ldDebts
      putStrLn $ "The amount of ld loans owed which are not defaulted: $" ++ show  (round ((-(sum goodCollects)) / 115.0))
      putStrLn "created ld_defaulters.csv file"
    Left err -> putStrLn err

  case usdDefaulters of
    Right usdDebts -> do
      case ldDefaulters of
        Right ldDebts -> do
          let numDefaultsUSD = fromInteger $ genericLength $ filter (\(PersonWithDates{in_default=d}) -> d == "DEFAULTED") usdDebts
          let numDefaultsLD = fromInteger $ genericLength $ filter (\(PersonWithDates{in_default=d}) -> d == "DEFAULTED") ldDebts
          let query = "SELECT id, loan_id, take_out_dt, loan_type, rate, loan_amt_ld FROM loans WHERE status = 'approved' AND loan_amt_ld > 0"
          records <- getLoanRecordsFromQuery "loans.sql" query
          let numLoansAll = fromInteger $ genericLength records
          putStrLn $ "The percentage of defaulted loans: " ++ (show $ 100*(numDefaultsUSD+numDefaultsLD)/numLoansAll)
          putStrLn $ "The number of loans are: " ++ show numLoansAll
          putStrLn $ "The number of defaulted loans are: " ++ show (numDefaultsUSD + numDefaultsLD)
