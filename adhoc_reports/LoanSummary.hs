module LoanSummary where

import Data.List
import Data.Char (toLower)
import Text.CSV
import Data.Either
import Database.HDBC
import Database.HDBC.Sqlite3
import UtilityFunctions (queryDatabase, readDoubleColumn, readStringColumn, readIntegerColumn)

{-
--loans schema
["id INTEGER","loan_id TEXT","loan_type TEXT","amt_requested TEXT","loan_amt_usd REAL","loan_amt_ld REAL","rate REAL","interest_ld REAL","interest_usd REAL","duration_month REAL","status TEXT","take_out_dt TEXT","loan_plus_intr_ld REAL","loan_plus_intr_usd REAL"]
--collections schema
["id INTEGER","loan_id TEXT","loan_plus_intr_usd REAL","loan_plus_intr_ld REAL","payment_dt TEXT","payment_amt_usd REAL","payment_amt_ld REAL"]
plotList [PNG "gnu_line.png", Title "line", YRange (0.0, 20.0)] (zip [1..] values)
-}

getLoanRecordsFromQuery :: String -> String -> IO [(String, Double, Double)]
getLoanRecordsFromQuery dbName query = do
  sqlLoanRecords <- queryDatabase dbName query
  let loanRecords = zip3 (readStringColumn sqlLoanRecords 0) (readDoubleColumn sqlLoanRecords 1) (readDoubleColumn sqlLoanRecords 2)
  return loanRecords

getLoanRecords :: String -> String -> IO ( Either String [(String, Double, Double)] )
getLoanRecords dbName loanType = case map toLower loanType of
  "ld" -> do
            let query = "SELECT loan_type, rate, loan_amt_ld FROM loans WHERE status = 'approved' AND CAST(loan_amt_ld AS DOUBLE) > 0.0"
            records <- getLoanRecordsFromQuery dbName query
            return $ Right records
  "usd" -> do
             let query = "SELECT loan_type, rate, loan_amt_usd FROM loans WHERE status = 'approved' AND CAST(loan_amt_usd AS DOUBLE) > 0.0"
             records <- getLoanRecordsFromQuery dbName query
             return $ Right records
  _   -> do
           return $ Left "Provide a valid loan type. Valid loan types are LD or USD."

getLoansDisbursed' :: [(String, Double, Double)] -> (Double, Double, Double, Double)
getLoansDisbursed' records = (totalLoans, totalPersonalLoans, totalBusinessLoans, totalOtherLoans)
  where
    loanTypeAmt = map (\(a, _, c) -> (a, c)) records
    totalLoans = sum $ map (\(_, b) -> b) loanTypeAmt
    totalPersonalLoans = sum $ map (\(_, b) -> b) $ filter (\(a, b) -> a == "p") loanTypeAmt
    totalBusinessLoans = sum $ map (\(_, b) -> b) $ filter (\(a, b) -> a == "b") loanTypeAmt
    totalOtherLoans = sum $ map (\(_, b) -> b) $ filter (\(a, b) -> and [a /= "p", a/="b"] ) loanTypeAmt

getLoansDisbursed :: String -> IO ()
getLoansDisbursed loanType = do
  loanRecords <- getLoanRecords "loans.sql" loanType
  case loanRecords of
    Right records -> do
      let disbursedRecord = getLoansDisbursed' records
      putStrLn $ "total loans disbursed = " ++ show ((\(a,_,_,_) -> a) disbursedRecord)
      putStrLn $ "personal loans disbursed = " ++ show ((\(_,b,_,_) -> b) disbursedRecord)
      putStrLn $ "business loans disbursed = " ++ show ((\(_,_,c,_) -> c) disbursedRecord)
      putStrLn $ "other loans disbursed = " ++ show ((\(_,_,_,d) -> d) disbursedRecord)
    Left err -> putStrLn err

getTotalDisbursed :: Double -> IO Double
getTotalDisbursed exchangeRate = do
  ldLoanRecords <- getLoanRecords "loans.sql" "LD"
  usdLoanRecords <- getLoanRecords "loans.sql" "USD"
  let usdTotalDisbursed = (\(a,_,_,_) -> a) $ getLoansDisbursed' $ (\(Right x) -> x) usdLoanRecords
      ldTotalDisbursed = (\(a,_,_,_) -> a) $ getLoansDisbursed' $ (\(Right x) -> x) ldLoanRecords
  return $ ldTotalDisbursed / exchangeRate + usdTotalDisbursed

getExpectedCollection' :: [(String, Double, Double)] -> Double
getExpectedCollection' records = totalExpectedCollections
  where
    loanTypeExpectedCollections = map (\(a, b, c) -> (a, c + b / 100.0 * c)) records
    totalExpectedCollections = sum $ map (\(_, b) -> b) loanTypeExpectedCollections

getExpectedCollection :: String -> IO (Either String Double)
getExpectedCollection loanType = do
  loanRecords <- getLoanRecords "loans.sql" loanType
  case loanRecords of
    Right records -> return $ Right (getExpectedCollection' records)
    Left err -> return $ Left err

getTotalExpectedCollection :: Double -> IO Double
getTotalExpectedCollection exchangeRate = do
  ldExpectedCollection <- getExpectedCollection "LD"
  usdExpectedCollection <- getExpectedCollection "USD"
  let totalExpected = ( ((\(Right x) -> x) ldExpectedCollection) / exchangeRate  ) + ( (\(Right x) -> x) usdExpectedCollection )
  return totalExpected

getTotalCollectionRecords :: Double -> IO (Double, Double, Double)
getTotalCollectionRecords xchangeRate = do
  sqlCollectionRecords <- queryDatabase "collections.sql" "SELECT payment_amt_ld, payment_amt_usd FROM collections WHERE CAST(payment_amt_ld AS DOUBLE) > 0.0 OR CAST(payment_amt_usd AS DOUBLE) > 0.0"
  let collectionRecords = zip (readDoubleColumn sqlCollectionRecords 0) (readDoubleColumn sqlCollectionRecords 1)
      ldTotalCollections = sum $ map (\(a,_) -> a) collectionRecords
      usdTotalCollections = sum $ map (\(_, b) -> b) collectionRecords
      totalCollection = ldTotalCollections / xchangeRate + usdTotalCollections
  return (totalCollection, ldTotalCollections, usdTotalCollections)

main :: IO ()
main = do
  putStrLn "LD PORTFOLIO :"
  getLoansDisbursed "LD"
  putStrLn "USD PORTFOLIO :"
  getLoansDisbursed "USD"
  totalDisbursed <- getTotalDisbursed 115.0
  putStrLn $ "Total (LD + USD) Disbursed Loans = " ++ show totalDisbursed
  putStrLn "LD Expected Collections:"
  ldExpected <- getExpectedCollection "LD"
  case ldExpected of
    Right value -> putStrLn $ "expected collection = " ++ show value
    Left err -> putStrLn err
  putStrLn "USD Expected Collections:"
  usdExpected <- getExpectedCollection "USD"
  case usdExpected of
    Right value -> putStrLn $ "expected collection = " ++ show value
    Left err -> putStrLn err
  totalExpected <- getTotalExpectedCollection 115.0
  putStrLn $ "total expected collection = " ++ show totalExpected
  collectionRecord <- getTotalCollectionRecords 115.0
  putStrLn $ "total collection = " ++ show ((\(a,_,_) -> a) collectionRecord)
  putStrLn $ "LD collections = " ++ show ((\(_,b,_) -> b) collectionRecord)
  putStrLn $ "USD collections = " ++ show ((\(_,_,c) -> c) collectionRecord)
  putStrLn "total outstanding : "
  let totalCollection = ((\(a,_,_) -> a) collectionRecord)
      totalOutstanding = totalExpected - totalCollection
  print $ totalOutstanding
