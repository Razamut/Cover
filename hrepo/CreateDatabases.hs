module CreateDatabases where

import Data.List
import Text.CSV
import Data.Either
import Database.HDBC
import Database.HDBC.Sqlite3


{-
-- drop last column in collections table and store as database table
eitherCSV <- dropLastColumnInCSVFile "collections.csv"
either (\err -> ["error"]) (\csv -> head csv) eitherCSV
csv = either (\err -> [["error"]]) (\csv -> csv) eitherCSV
convertCSVtoSQL "collections" "collections.sql" ["id INTEGER","loan_id TEXT","loan_plus_intr_usd REAL","loan_plus_intr_ld REAL","payment_dt TEXT","payment_amt_usd REAL","payment_amt_ld REAL"] csv

-- save the loans table in database
convertCSVFiletoSQL "loans.csv" "loans.sql" "loans" ["id INTEGER","loan_id TEXT","loan_type TEXT","amt_requested TEXT","loan_amt_usd REAL","loan_amt_ld REAL","rate REAL","interest_ld REAL","interest_usd REAL","duration_month REAL","status TEXT","take_out_dt TEXT","loan_plus_intr_ld REAL","loan_plus_intr_usd REAL"]

--save the connections table in database
convertCSVFiletoSQL "connections.csv" "connections.sql" "connections" ["id INTEGER","employment TEXT","employer_name TEXT","occupation TEXT","monthly_salary TEXT","mobile_type TEXT","marital_status TEXT","spouse_first_name TEXT","spouse_last_name TEXT","spouse_age TEXT","spouse_employement_status TEXT","home_address TEXT","business_address TEXT","employer_address TEXT","business_description TEXT","fb_acct TEXT","housing_type TEXT","rent_amt REAL","family_size TEXT","n_kids INTEGER","kids_sch_type TEXT","spouse_employed TEXT"]

queryDatabase "connections.sql" "SELECT COUNT(*) FROM connections WHERE spouse_age > 24;"

--save the person table into database
convertCSVFiletoSQL "person.csv" "person.sql" "person" ["id INTEGER","first_name TEXT","last_name TEXT","middle_name TEXT","id_number TEXT","id_type TEXT","age REAL","gender TEXT","ph_numbers REAL","email TEXT"]

-}


fieldsInCSVFile :: FilePath -> IO ([String])
fieldsInCSVFile inFileName = do
  input <- readFile inFileName
  let records = parseCSV inFileName input
  case records of
    Left error -> return $ ["parse error"]
    Right csv  -> return $ head csv


dropLastColumnInCSV :: CSV -> CSV
dropLastColumnInCSV csv = map (take n) csv
  where
    n = length (head csv) - 1

dropLastColumnInCSVFile :: FilePath -> IO (Either String CSV)
dropLastColumnInCSVFile inFileName = do
  input <- readFile inFileName
  let records = parseCSV inFileName input
  case records of
    Left error -> return $ Left "parse error"
    Right csv  -> return $ Right $ dropLastColumnInCSV csv

convertCSVFiletoSQL :: String -> String -> String -> [String] -> IO ()
convertCSVFiletoSQL inFileName outFileName tableName fields = do
  --open and read the CSV File
  input <- readFile inFileName
  let records = parseCSV inFileName input
  either handleCSVError convertTool records
  where
    convertTool = convertCSVtoSQL tableName outFileName fields
    handleCSVError csv = putStrLn "This does not appear to be a CSV file."

convertCSVtoSQL :: String -> FilePath -> [String] -> CSV -> IO ()
convertCSVtoSQL tableName outFileName fields records =
  if nfieldsInFile == nfieldsInFields then do
    --open connection
    conn <- connectSqlite3 outFileName
    --create a new table
    run conn createStatement []
    --load contents of CSV file into table
    stmt <- prepare conn insertStatement
    executeMany stmt (tail (filter (\record -> nfieldsInFile == length record ) sqlRecords))
    --commit changes
    commit conn
    --close the connection
    disconnect conn
    --report success
    putStrLn "Successful"
  else
     putStrLn "The number of fields differ from that in CSV"
  where
    nfieldsInFile = length $ head records
    nfieldsInFields = length fields
    createStatement = "CREATE TABLE " ++
                      tableName ++
                      " (" ++ (intercalate ", " fields) ++ ")"
    insertStatement = "INSERT INTO " ++
                      tableName ++ " VALUES (" ++
                      intercalate ", " (replicate nfieldsInFile "?") ++ ")"
    sqlRecords = map (map toSql) records


queryDatabase :: FilePath -> String -> IO [[SqlValue]]
queryDatabase databaseFile sqlQuery = do
  conn <- connectSqlite3 databaseFile
  result <- quickQuery' conn sqlQuery []
  disconnect conn
  return result
