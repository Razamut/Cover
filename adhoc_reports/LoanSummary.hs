import Data.List
import Data.Char (toLower, isSpace)
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

dbPath = "db" :: FilePath

getLoanRecordsFromQuery :: FilePath -> String -> String -> IO ( Either String [(String, Double, Double)] )
getLoanRecordsFromQuery iPath fileName query = do
  sqlLoanRecords <- queryDatabase iPath fileName query
  case sqlLoanRecords of
    Right sqlRecs -> do
      let loanRecords = zip3 (readStringColumn sqlRecs 0) (readDoubleColumn sqlRecs 1) (readDoubleColumn sqlRecs 2)
      return $ Right loanRecords
    Left err -> do return $ Left err

getLoanQueryFromLoanType :: String -> Either String String
getLoanQueryFromLoanType loanType = case map toLower loanType of
  "ld" -> Right "SELECT loan_type, rate, loan_amt_ld FROM loans WHERE status = 'approved' AND CAST(loan_amt_ld AS DOUBLE) > 0.0"
  "usd" -> Right "SELECT loan_type, rate, loan_amt_usd FROM loans WHERE status = 'approved' AND CAST(loan_amt_usd AS DOUBLE) > 0.0"
  _  -> Left "Provide a valid loan type. Valid loan types are LD or USD."

getLoanRecords :: FilePath -> String -> String -> IO ( Either String [(String, Double, Double)] )
getLoanRecords iPath fileName loanType = case getLoanQueryFromLoanType loanType of
  Right query -> do
                 records <- getLoanRecordsFromQuery iPath fileName query
                 return records
  Left err    -> do
                 return $ Left err


trim :: String -> String
trim = dropWhileEnd isSpace . dropWhile isSpace

lowerTrim :: String -> String
lowerTrim = map toLower . trim

getLoansDisbursed' :: [(String, Double, Double)] -> (Double, Double, Double, Double)
getLoansDisbursed' records = (totalLoans, totalPersonalLoans, totalBusinessLoans, totalOtherLoans)
  where
    loanTypeAmt = map (\(a, _, c) -> (a, c)) records
    totalLoans = sum $ map (\(_, b) -> b) loanTypeAmt
    totalPersonalLoans = sum $ map (\(_, b) -> b) $ filter (\(a, b) -> lowerTrim a == "p") loanTypeAmt
    totalBusinessLoans = sum $ map (\(_, b) -> b) $ filter (\(a, b) -> lowerTrim a == "b") loanTypeAmt
    totalOtherLoans = sum $ map (\(_, b) -> b) $ filter (\(a, b) -> and [lowerTrim a /= "p", lowerTrim a /= "b"] ) loanTypeAmt

getLoansDisbursed :: FilePath -> String -> String -> IO ()
getLoansDisbursed iPath fileName loanType = do
  loanRecords <- getLoanRecords iPath fileName loanType  --"loans.sql" loanType
  case loanRecords of
    Right records -> do
      let disbursedRecord = getLoansDisbursed' records
      putStrLn $ "total loans disbursed = " ++ show ((\(a,_,_,_) -> a) disbursedRecord)
      putStrLn $ "personal loans disbursed = " ++ show ((\(_,b,_,_) -> b) disbursedRecord)
      putStrLn $ "business loans disbursed = " ++ show ((\(_,_,c,_) -> c) disbursedRecord)
      putStrLn $ "other loans disbursed = " ++ show ((\(_,_,_,d) -> d) disbursedRecord)
    Left err -> putStrLn err

getTotalDisbursed :: FilePath -> String -> Double -> IO Double
getTotalDisbursed iPath fileName exchangeRate = do
  ldLoanRecords <- getLoanRecords iPath fileName "LD"
  usdLoanRecords <- getLoanRecords iPath fileName "USD"
  let usdTotalDisbursed = (\(a,_,_,_) -> a) $ getLoansDisbursed' $ (\(Right x) -> x) usdLoanRecords
      ldTotalDisbursed = (\(a,_,_,_) -> a) $ getLoansDisbursed' $ (\(Right x) -> x) ldLoanRecords
  return $ ldTotalDisbursed / exchangeRate + usdTotalDisbursed

getExpectedCollection' :: [(String, Double, Double)] -> Double
getExpectedCollection' records = totalExpectedCollections
  where
    loanTypeExpectedCollections = map (\(a, b, c) -> (a, c + b / 100.0 * c)) records
    totalExpectedCollections = sum $ map (\(_, b) -> b) loanTypeExpectedCollections

getExpectedCollection :: FilePath -> String -> String -> IO (Either String Double)
getExpectedCollection iPath fileName loanType = do
  loanRecords <- getLoanRecords iPath fileName loanType
  case loanRecords of
    Right records -> return $ Right (getExpectedCollection' records)
    Left err -> return $ Left err

getTotalExpectedCollection :: FilePath -> String -> Double -> IO Double
getTotalExpectedCollection iPath fileName exchangeRate = do
  ldExpectedCollection <- getExpectedCollection iPath fileName "LD"
  usdExpectedCollection <- getExpectedCollection iPath fileName "USD"
  let totalExpected = ( ((\(Right x) -> x) ldExpectedCollection) / exchangeRate  ) + ( (\(Right x) -> x) usdExpectedCollection )
  return totalExpected

getTotalCollectionRecords :: FilePath -> String -> Double -> IO (Either String (Double, Double, Double) )
getTotalCollectionRecords iPath fileName xchangeRate = do
  sqlCollectionRecords <- queryDatabase iPath fileName "SELECT payment_amt_ld, payment_amt_usd FROM collections WHERE CAST(payment_amt_ld AS DOUBLE) > 0.0 OR CAST(payment_amt_usd AS DOUBLE) > 0.0"
  case sqlCollectionRecords of
    Right sqlRecs -> do
      let collectionRecords = zip (readDoubleColumn sqlRecs 0) (readDoubleColumn sqlRecs 1)
          ldTotalCollections = sum $ map (\(a,_) -> a) collectionRecords
          usdTotalCollections = sum $ map (\(_, b) -> b) collectionRecords
          totalCollection = ldTotalCollections / xchangeRate + usdTotalCollections
      return $ Right (totalCollection, ldTotalCollections, usdTotalCollections)
    Left err -> do return $ Left err

xchangeRate :: Double
xchangeRate = 115.0
loanFile = "loans.sql" :: String
collectionsFile = "collections.sql" :: String

main :: IO ()
main = do
  putStrLn "LD PORTFOLIO :"
  getLoansDisbursed dbPath loanFile "LD"
  putStrLn "USD PORTFOLIO :"
  getLoansDisbursed dbPath loanFile "USD"
  totalDisbursed <- getTotalDisbursed dbPath loanFile xchangeRate
  putStrLn $ "Total (LD + USD) Disbursed Loans = " ++ show totalDisbursed
  putStrLn "LD Expected Collections:"
  ldExpected <- getExpectedCollection dbPath loanFile "LD"
  case ldExpected of
    Right value -> putStrLn $ "expected collection = " ++ show value
    Left err -> putStrLn err
  putStrLn "USD Expected Collections:"
  usdExpected <- getExpectedCollection dbPath loanFile "USD"
  case usdExpected of
    Right value -> putStrLn $ "expected collection = " ++ show value
    Left err -> putStrLn err
  totalExpected <- getTotalExpectedCollection dbPath loanFile xchangeRate
  putStrLn $ "total expected collection = " ++ show totalExpected
  collectionRecord <- getTotalCollectionRecords dbPath collectionsFile xchangeRate
  case collectionRecord of
    Right collRec -> do
      putStrLn $ "total collection = " ++ show ((\(a,_,_) -> a) collRec)
      putStrLn $ "LD collections = " ++ show ((\(_,b,_) -> b) collRec)
      putStrLn $ "USD collections = " ++ show ((\(_,_,c) -> c) collRec)
      putStrLn "total outstanding : "
      let totalCollection = ((\(a,_,_) -> a) collRec)
          totalOutstanding = totalExpected - totalCollection
      print $ totalOutstanding
    Left err -> putStrLn err
