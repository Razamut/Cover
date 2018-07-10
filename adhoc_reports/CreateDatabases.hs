module CreateDatabases where

import Text.CSV
import Data.List
import Data.Either
import Database.HDBC
import Database.HDBC.Sqlite3
import System.FilePath.Posix ((</>))
import UtilityFunctions (queryDatabase)

dirPath = "data" :: String

convertCSVFiletoSQL :: String -> String -> String -> [String] -> IO ()
convertCSVFiletoSQL inFileName outFileName tableName fields = do
  let inPath = dirPath </> inFileName
  input <- readFile inPath
  let records = parseCSV inPath input
  either handleCSVError convertTool records
  where
    outPath = dirPath </> outFileName
    -- outTable = dirPath </> tableName
    convertTool = convertCSVtoSQL tableName outPath fields
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

main :: IO ()
main = do
  -- eitherCSV <- dropLastColumnInCSVFile "collections.csv"
  -- let csv = either (\err -> [["error"]]) (\csv -> csv) eitherCSV
  convertCSVFiletoSQL "collections.csv" "collections.sql" "collections"
    ["id INTEGER","loan_id TEXT","loan_plus_intr_usd REAL","loan_plus_intr_ld REAL","payment_dt TEXT","payment_amt_usd REAL","payment_amt_ld REAL"]
  convertCSVFiletoSQL "loans.csv" "loans.sql" "loans" ["id INTEGER","loan_id TEXT","loan_type TEXT","amt_requested TEXT","loan_amt_usd REAL","loan_amt_ld REAL","rate REAL","interest_ld REAL","interest_usd REAL","duration_month REAL","status TEXT","take_out_dt TEXT","loan_plus_intr_ld REAL","loan_plus_intr_usd REAL"]
  convertCSVFiletoSQL "connections.csv" "connections.sql" "connections"
    ["id INTEGER","employment TEXT","employer_name TEXT","occupation TEXT","monthly_salary TEXT","mobile_type TEXT","marital_status TEXT","spouse_first_name TEXT","spouse_last_name TEXT","spouse_age TEXT","spouse_employement_status TEXT","home_address TEXT","business_address TEXT","employer_address TEXT","business_description TEXT","fb_acct TEXT","housing_type TEXT","rent_amt REAL","family_size TEXT","n_kids INTEGER","kids_sch_type TEXT","spouse_employed TEXT"]
  convertCSVFiletoSQL "person.csv" "person.sql" "person" ["id INTEGER","first_name TEXT","last_name TEXT","middle_name TEXT","id_number TEXT","id_type TEXT","age REAL","gender TEXT","ph_numbers REAL","email TEXT"]
  -- putStrLn "Successfully created all databases!"
