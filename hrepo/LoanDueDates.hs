module LoanDueDates where

import Data.List
import Text.CSV
import Data.Either
import Database.HDBC
import Database.HDBC.Sqlite3
import CreateDatabases
import LoanSummary

{-
--loans schema
["id INTEGER","loan_id TEXT","loan_type TEXT","amt_requested TEXT","loan_amt_usd REAL","loan_amt_ld REAL","rate REAL","interest_ld REAL","interest_usd REAL","duration_month REAL","status TEXT","take_out_dt TEXT","loan_plus_intr_ld REAL","loan_plus_intr_usd REAL"]
--collections schema
["id INTEGER","loan_id TEXT","loan_plus_intr_usd REAL","loan_plus_intr_ld REAL","payment_dt TEXT","payment_amt_usd REAL","payment_amt_ld REAL"]

---LD loans
sqlLDloanRecords <- queryDatabase "loans.sql" "SELECT id, loan_id, take_out_dt, rate, loan_amt_ld FROM loans WHERE status = 'approved' "
ldLoanRecords = zip5 (readIntegerColumn sqlLDloanRecords 0) (readStringColumn sqlLDloanRecords 1) (readStringColumn sqlLDloanRecords 2) (readDoubleColumn sqlLDloanRecords 3) (readDoubleColumn sqlLDloanRecords 4)
validLDloanRecords = filter (\(_,_,_,_,x) -> x > 0) ldLoanRecords
ldLoansExpectedCollections = map (\(_,a,_,b,c) -> (a, c + (b/100.0) * c)  ) validLDloanRecords
ldTotalExpectedCollections = sum $ map (\(_, b) -> b) ldLoansExpectedCollections

sqlLDloanCollectionRecords <- queryDatabase "collections.sql" "SELECT loan_id, payment_amt_ld FROM collections;"
ldLoanCollectionRecords = zip (readStringColumn sqlLDloanCollectionRecords 0) (readDoubleColumn sqlLDloanCollectionRecords 1)
ldLoanIDs = nub $ map (\(x,_) -> x ) $ filter (\(_,x) -> x > 0) ldLoanCollectionRecords
ldLoansCollections = idsCollections ldLoanIDs ldLoanCollectionRecords
ldLoansTotalCollections = map (\(a, b) -> (a, sum b)) ldLoansCollections
ldTotalCollections = sum $ map (\(_,b) -> b) ldLoansTotalCollections

ldLoansOutstanding = filter  (\(_, y) -> y < 0) $  idsActualColnMinusExpectedColn  ldLoansTotalCollections ldLoansExpectedCollections
ldLoansPaidOff = filter (\(_,y) -> y >= 0) $ idsActualColnMinusExpectedColn  ldLoansTotalCollections ldLoansExpectedCollections
ldTotalOutstanding = (-1) * (sum $ map (\(_,b) -> b) ldLoansOutstanding )


--USD loans

sqlUSDloanRecords <- queryDatabase "loans.sql" "SELECT id, loan_id, take_out_dt, rate, loan_amt_usd FROM loans WHERE status = 'approved' "
usdLoanRecords = zip5 (readIntegerColumn sqlUSDloanRecords 0) (readStringColumn sqlUSDloanRecords 1) (readStringColumn sqlUSDloanRecords 2) (readDoubleColumn sqlUSDloanRecords 3) (readDoubleColumn sqlUSDloanRecords 4)
validUSDloanRecords = filter (\(_,_,_,_,x) -> x > 0) usdLoanRecords
usdLoansExpectedCollections = map (\(_,a,_,b,c) -> (a, c + (b/100.0) * c)  ) validUSDloanRecords
usdTotalExpectedCollections = sum $ map (\(_, b) -> b) usdLoansExpectedCollections

sqlUSDloanCollectionRecords <- queryDatabase "collections.sql" "SELECT loan_id, payment_amt_usd FROM collections;"
usdLoanCollectionRecords = zip (readStringColumn sqlUSDloanCollectionRecords 0) (readDoubleColumn sqlUSDloanCollectionRecords 1)
usdLoanIDs = nub $ map (\(x,_) -> x ) $ filter (\(_,x) -> x > 0) usdLoanCollectionRecords
usdLoansCollections = idsCollections usdLoanIDs usdLoanCollectionRecords
usdLoansTotalCollections = map (\(a, b) -> (a, sum b)) usdLoansCollections
usdTotalCollections = sum $ map (\(_, b) -> b) usdLoansTotalCollections

usdLoansOutstanding = filter  (\(_, y) -> y < 0) $  idsActualColnMinusExpectedColn  usdLoansTotalCollections usdLoansExpectedCollections
usdLoansPaidOff = filter (\(_,y) -> y >= 0) $ idsActualColnMinusExpectedColn  usdLoansTotalCollections usdLoansExpectedCollections
usdTotalOutstanding = (-1) * ( sum $ map (\(_,b) -> b) usdLoansOutstanding )

xchangeRate  = 115 :: Double
totalExpectedCollections = ldTotalExpectedCollections / xchangeRate + usdTotalExpectedCollections
totalCollection = ldTotalCollections / xchangeRate + usdTotalCollections
totalOutstanding = totalExpectedCollections - totalCollection
-}

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
