module LoanSummary where

import Data.List
import Text.CSV
import Data.Either
import Database.HDBC
import Database.HDBC.Sqlite3
import CreateDatabases

{-
--loans schema
["id INTEGER","loan_id TEXT","loan_type TEXT","amt_requested TEXT","loan_amt_usd REAL","loan_amt_ld REAL","rate REAL","interest_ld REAL","interest_usd REAL","duration_month REAL","status TEXT","take_out_dt TEXT","loan_plus_intr_ld REAL","loan_plus_intr_usd REAL"]
--collections schema
["id INTEGER","loan_id TEXT","loan_plus_intr_usd REAL","loan_plus_intr_ld REAL","payment_dt TEXT","payment_amt_usd REAL","payment_amt_ld REAL"]


sqlLDloanRecords <- queryDatabase "loans.sql" "SELECT loan_type, rate, loan_amt_ld FROM loans WHERE status = 'approved' "
ldLoanRecords = zip3 (readStringColumn sqlLDloanRecords 0) (readDoubleColumn sqlLDloanRecords 1) (readDoubleColumn sqlLDloanRecords 2)
ldLoanTypeAmt = map (\(a, _, c) -> (a, c)) ldLoanRecords
ldTotalLoans = sum $ map (\(_, b) -> b) ldLoanTypeAmt
ldTotalPersonalLoans = sum $ map (\(_, b) -> b) $ filter (\(a, b) -> a == "p") ldLoanTypeAmt
ldTotalBusinessLoans = sum $ map (\(_, b) -> b) $ filter (\(a, b) -> a == "b") ldLoanTypeAmt

ldLoanTypeExpectedCollections = map (\(a, b, c) -> (a, c + b / 100.0 * c)) ldLoanRecords
ldTotalExpectedCollections = sum $ map (\(_, b) -> b) ldLoanTypeExpectedCollections

sqlUSDloanRecords <- queryDatabase "loans.sql" "SELECT loan_type, rate, loan_amt_usd FROM loans WHERE status = 'approved' "
usdLoanRecords = zip3 (readStringColumn sqlUSDloanRecords 0) (readDoubleColumn sqlUSDloanRecords 1) (readDoubleColumn sqlUSDloanRecords 2)
usdLoanTypeAmt = map (\(a, _, c) -> (a, c)) usdLoanRecords
usdTotalLoans = sum $ map (\(_, b) -> b) usdLoanTypeAmt
usdTotalPersonalLoans = sum $ map (\(_, b) -> b) $ filter (\(a, b) -> a == "p") usdLoanTypeAmt
usdTotalBusinessLoans = sum $ map (\(_, b) -> b) $ filter (\(a, b) -> a == "b") usdLoanTypeAmt

usdLoanTypeExpectedCollections = map (\(a, b, c) -> (a, c + b / 100.0 * c)) usdLoanRecords
usdTotalExpectedCollections = sum $ map (\(_, b) -> b) usdLoanTypeExpectedCollections

xchangeRate  = 115 :: Double
totalExpectedCollections = ldTotalExpectedCollections / xchangeRate + usdTotalExpectedCollections

totalDisbursedLoans = ldTotalLoans / xchangeRate + usdTotalLoans
totalPersonalLoans = ldTotalPersonalLoans / xchangeRate + usdTotalPersonalLoans
totalBusinessLoans = ldTotalBusinessLoans / xchangeRate + usdTotalBusinessLoans


sqlCollectionRecords <- queryDatabase "collections.sql" "SELECT payment_amt_ld, payment_amt_usd FROM collections"
collectionRecords = zip (readDoubleColumn sqlCollectionRecords 0) (readDoubleColumn sqlCollectionRecords 1)
ldTotalCollections = sum $ map (\(a,_) -> a) collectionRecords
usdTotalCollections = sum $ map (\(_, b) -> b) collectionRecords
totalCollection = ldTotalCollections / xchangeRate + usdTotalCollections

totalOutstanding = totalExpectedCollections - totalCollection

plotList [PNG "gnu_line.png", Title "line", YRange (0.0, 20.0)] (zip [1..] values)



-}

--read the sqlValues from SQLite3 DB and convert to Haskell values

readIntegerColumn :: [[SqlValue]]  -> Integer -> [Integer]
readIntegerColumn sqlResult index =
  map (\row -> fromSql $ genericIndex row index :: Integer) sqlResult

readDoubleColumn :: [[SqlValue]] -> Integer -> [Double]
readDoubleColumn sqlResult index =
  map (\row -> safeConvertToDouble $ genericIndex row index) sqlResult

readStringColumn :: [[SqlValue]] -> Integer -> [String]
readStringColumn sqlResult index =
  map (\row -> fromSql $ genericIndex row index :: String) sqlResult

safeConvertToDouble :: SqlValue -> Double
safeConvertToDouble value =
  case value of
    SqlDouble x  ->  x
    _                    ->  0.0
