module UtilityFunctions where

import Data.Dates
import Data.Char (toLower)
import Data.List 
import Database.HDBC
import Database.HDBC.Sqlite3
import CreateDatabases (queryDatabase)
import LoanSummary (readIntegerColumn, readStringColumn, readDoubleColumn)

getDateTimeFromString :: String -> (Int, Int, Int, Int, Int, Int)
getDateTimeFromString xs = (year, month, day, hour, minute, second)
  where
    firstSplit = splitAt 4 xs
    year = read $ fst firstSplit
    sndSplit = break (== '-') $ tail $ snd firstSplit
    month = read $ fst sndSplit
    day = read $ tail $ snd sndSplit
    hour   = 0
    minute = 0
    second = 0

convertStringToDateTime :: String -> Maybe DateTime
convertStringToDateTime xs = case length xs of
  10 -> Just dateTime
  _  -> Nothing
  where
    (year,month,day,hour,minute,second) = getDateTimeFromString xs
    dateTime = DateTime year month day hour minute second

computeMonthsToAdd :: Maybe DateTime -> IO (Integer)
computeMonthsToAdd takeOutDate = case takeOutDate of
    Just properDate -> do
      currentDateTime <- getCurrentDateTime
      let daysDiff = datesDifference properDate currentDateTime
          exactMonthsCount =  daysDiff `div` 30
          excessDays =  daysDiff `mod` 30
      if excessDays > 0 then
        return $ exactMonthsCount + 1
      else
        return exactMonthsCount
    _ -> return 0

addMonthsToDateTime :: IO Integer -> Maybe DateTime -> IO (Maybe DateTime)
addMonthsToDateTime ioInt dt = do
  n <- ioInt
  case dt of
    Just dateTime -> return $ Just $ addInterval dateTime $ Days $ n * 30
    _             -> return $ Nothing


getLoanTakeOutDatesAsStrings :: [String] -> IO [(Integer, String)]
getLoanTakeOutDatesAsStrings loanIds = do
  let query = "SELECT loan_id, take_out_dt FROM loans WHERE loan_id IN " ++ " (" ++ (intercalate ", " loanIds) ++ ")"
  sqlQueryResult <- queryDatabase "loans.sql" query
  let queryResult = zip (readIntegerColumn sqlQueryResult 0) (readStringColumn sqlQueryResult 1)
  return queryResult


nextDueAmt :: Double -> Double -> Double
nextDueAmt rate totalLoan = if rate == 15.0 then
                              totalLoan
                            else
                              totalLoan / 3.0

combineTuplesFromLists :: [(Integer, String, String)] -> [(Integer, String, String, Double)] -> [Maybe (Integer, String, String, String, String, Double)]
combineTuplesFromLists xs ys = nub $ [combineTuples x y | x <- xs, y <- ys]

combineTuples :: (Integer, String, String) -> (Integer, String, String, Double) -> Maybe (Integer, String, String, String, String, Double)
combineTuples (a, b, c) (w,x,y,z) | a == w = Just (a,b,c,x,y,z)
                              | otherwise = Nothing

getLoanRecords :: String -> String -> IO ( Either String [(Integer, String, String, Double, Double)] )
getLoanRecords dbName loanType = case map toLower loanType of
  "ld" -> do
          let query = "SELECT id, loan_id, take_out_dt, rate, loan_amt_ld FROM loans WHERE status = 'approved' AND CAST(loan_amt_ld AS DOUBLE) > 0.0"
          records <- getLoanRecordsFromQuery dbName query
          return $ Right records
  "usd" -> do
           let query = "SELECT id, loan_id, take_out_dt, rate, loan_amt_usd FROM loans WHERE status = 'approved' AND CAST(loan_amt_usd AS DOUBLE) > 0.0"
           records <- getLoanRecordsFromQuery dbName query
           return $ Right records
  _   -> do
         return $ Left "Provide a valid loan type. Valid loan types are LD or USD."

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
          let query = "SELECT loan_id, payment_amt_ld FROM collections WHERE CAST(payment_amt_ld AS DOUBLE) > 0.0"
          records <- getColnRecordsFromQuery dbName query
          return $ Right records
  "usd" -> do
           let query = "SELECT loan_id, payment_amt_usd FROM collections WHERE CAST(payment_amt_usd AS DOUBLE) > 0.0"
           records <- getColnRecordsFromQuery dbName query
           return $ Right records
  _   -> do
         return $ Left "Provide a valid loan type. Valid loan types are LD or USD."


getLoanRecordsFromQuery :: String -> String -> IO [(Integer, String, String, Double, Double)]
getLoanRecordsFromQuery dbName query = do
  sqlLoanRecords <- queryDatabase dbName query
  let loanRecords = zip5 (readIntegerColumn sqlLoanRecords 0) (readStringColumn sqlLoanRecords 1) (readStringColumn sqlLoanRecords 2) (readDoubleColumn sqlLoanRecords 3) (readDoubleColumn sqlLoanRecords 4)
  return loanRecords

getDebtorsRecords :: [Maybe (Integer, String, String, String, String, Double)] ->
                    Maybe [(Integer, String, String, String, String, Double)]
getDebtorsRecords ys = sequenceA $ filter (\x -> x /= Nothing) ys


getNamesForUserIDs :: [Integer] -> IO [(Integer, String, String)]
getNamesForUserIDs userIDs = do
  let stringyIDs = map show userIDs
      query = "SELECT id, first_name, last_name FROM person WHERE id IN " ++ " (" ++ (intercalate ", " stringyIDs) ++ ")"
  sqlQueryResult <- queryDatabase "person.sql" query
  let queryResult = zip3 (readIntegerColumn sqlQueryResult 0) (readStringColumn sqlQueryResult 1) (readStringColumn sqlQueryResult 2)
  return queryResult

findUserIDsForLoans :: [(String, Double)] -> [(Integer, String, String)] -> [(Integer, String, String, Double)]
findUserIDsForLoans outstandingLoans userIDsLoanIDsTakeOutDates = map (\x -> findUserIDForLoan x userIDsLoanIDsTakeOutDates) outstandingLoans

findUserIDForLoan :: (String, Double) -> [(Integer, String, String)] -> (Integer, String, String, Double)
findUserIDForLoan loanIDamtPair userIDsLoanIDsTakeOutDates = userIDloanIDTakeOutDateAmt
  where
    theRecord = head $ filter (\(_, b, _) -> b == fst loanIDamtPair) userIDsLoanIDsTakeOutDates
    theUserID = (\(a,_,_) -> a) theRecord
    takeOutDate = (\(_,_,c) -> c) theRecord
    userIDloanIDTakeOutDateAmt = (theUserID, fst loanIDamtPair, takeOutDate, snd loanIDamtPair)

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
