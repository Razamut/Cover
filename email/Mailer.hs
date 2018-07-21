import Data.Aeson
import Control.Applicative
import Network.Mail.Mime.SES
import Network.Mail.Mime as M
import Network.Mail.SMTP as S
import qualified Data.Text as T
import System.Environment (getArgs)
import qualified Data.Text.Lazy as L
import qualified Data.ByteString.Lazy as B
import qualified Data.ByteString.Char8 as C8
import Network.HTTP.Client.TLS (tlsManagerSettings, newTlsManager, getGlobalManager)


data SesCredentials = SesCredentials
                        {user :: String, pass :: String} deriving (Eq, Show)

instance FromJSON SesCredentials where
  parseJSON (Object v) = SesCredentials
                          <$> (v .: (T.pack "user"))
                          <*> (v .: (T.pack "pass"))

safeHead :: [String] -> String
safeHead [] = []
safeHead (x:_) = x

getCredentials :: IO (Maybe SesCredentials)
getCredentials = do
  raw_creds <- B.readFile "ses_credentials.json"
  let creds = decode raw_creds :: Maybe SesCredentials
  return creds

getAttachment :: FilePath -> IO (Maybe Part)
getAttachment filePath = do
  case filePath of
    [] -> return Nothing
    (x:xs)  -> do
      attachment <- filePart (T.pack "application/iron-coin") filePath
      return $ Just attachment

cc         = []
bcc        = []
subject    = T.pack "adhoc report"
body = S.plainTextPart $ L.pack ("Hello,\n" ++ "\n" ++
  "Please find report attached.\n" ++ "\n" ++
  "Sincerely, \n" ++ "Data Science Team")
html = S.htmlPart L.empty

sendEmail :: String -> [String] -> FilePath -> IO ()
sendEmail eFrom eTo filePath = do
  -- manager <- newTlsManager
  manager <- getGlobalManager
  -- manager <- newManager tlsManagerSettings
  ses_creds <- getCredentials
  case ses_creds of
    Nothing -> putStrLn "NoSesCredentialFound"
    Just cred -> do
      let
        seAccessKey = C8.pack $ user cred
        seSecretKey = C8.pack $ pass cred
        seSessionToken = Nothing
        seRegion = usEast1
        seFrom = C8.pack eFrom
        seTo = fmap C8.pack eTo
        ses = SES seFrom seTo seAccessKey seSecretKey seSessionToken seRegion
        aFrom = Address Nothing $ T.pack eFrom
        aTo = fmap (Address Nothing . T.pack) eTo
      attachment <- getAttachment filePath
      case attachment of
        Nothing -> do
          let sMail = S.simpleMail aFrom aTo  cc bcc subject [html, body]
          renderSendMailSES manager ses sMail
        Just file -> do
          let sMail = S.simpleMail aFrom aTo  cc bcc subject [html, body, file]
          renderSendMailSES manager ses sMail


main :: IO ()
main = do
  args <- getArgs
  let
    addresses = filter (elem '@') args
    mFrom = head addresses
    mTo = tail addresses
    attachment = safeHead $ filter (notElem '@') args
  sendEmail mFrom mTo attachment
