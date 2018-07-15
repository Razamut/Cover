import Text.CSV
import Data.List
import Data.Either
import Text.Printf
import Database.HDBC
import System.Directory
import Database.HDBC.Sqlite3
import System.FilePath.Posix ((</>))

dataPath = "data" :: FilePath
dbPath = "db" :: FilePath

convertCSVFiletoSQL :: FilePath -> FilePath -> String -> String -> String -> [String] -> IO ()
convertCSVFiletoSQL inputPath outPath inFileName outFileName tableName fields = do
  dirExists <- doesDirectoryExist inputPath
  case dirExists of
    False -> printf "input Directory %s does not exist\n" inputPath
    True -> do
      let inFile = inputPath </> inFileName
      fileExists <- doesPathExist inFile
      case fileExists of
        False -> printf "input File %s does not exist\n" inFile
        True -> do
          input <- readFile inFile
          createDirectoryIfMissing True outPath
          let records = parseCSV inFile input
          either handleCSVError convertTool records
          where
            oPath = outPath </> outFileName
            convertTool = convertCSVtoSQL tableName oPath fields
            handleCSVError csv = putStrLn "This does not appear to be a CSV file."

convertCSVtoSQL :: String -> FilePath -> [String] -> CSV -> IO ()
convertCSVtoSQL tableName outPath fields records = do
  pathExists <- doesPathExist outPath
  case pathExists of
    True -> do
      removeFile outPath
      convertCSVtoSQL tableName outPath fields records
    False ->
      if nfieldsInFile == nfieldsInFields then do
        --open connection
        conn <- connectSqlite3 outPath
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
        printf "Successfully created %s table\n" tableName
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
  convertCSVFiletoSQL dataPath dbPath "collections.csv" "collections.sql" "collections"
    ["user_id INTEGER","loan_id TEXT","loan_plus_intr_usd REAL","loan_plus_intr_ld REAL","payment_dt TEXT","payment_amt_usd REAL","payment_amt_ld REAL"]
  convertCSVFiletoSQL dataPath dbPath "loans.csv" "loans.sql" "loans"
    ["user_id INTEGER","loan_id TEXT","loan_type TEXT","amt_requested TEXT","loan_amt_usd REAL","loan_amt_ld REAL","rate REAL","interest_ld REAL","interest_usd REAL","duration_month REAL","status TEXT","take_out_dt TEXT","loan_plus_intr_ld REAL","loan_plus_intr_usd REAL"]
  convertCSVFiletoSQL dataPath dbPath "connections.csv" "connections.sql" "connections"
    ["user_id INTEGER","employment TEXT","employer_name TEXT","occupation TEXT","monthly_salary TEXT","mobile_type TEXT","marital_status TEXT","spouse_first_name TEXT","spouse_last_name TEXT","spouse_age TEXT","spouse_employement_status TEXT","home_address TEXT","business_address TEXT","employer_address TEXT","business_description TEXT","fb_acct TEXT","housing_type TEXT","rent_amt REAL","family_size TEXT","n_kids INTEGER","kids_sch_type TEXT","spouse_employed TEXT"]
  convertCSVFiletoSQL dataPath dbPath "users.csv" "users.sql" "users"
    ["user_id INTEGER","first_name TEXT","last_name TEXT","middle_name TEXT","id_number TEXT","id_type TEXT","age REAL","gender TEXT","ph_numbers REAL","email TEXT"]
