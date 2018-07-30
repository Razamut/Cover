module Lib
    ( sendEmail, getCredentials, safeHead
    ) where

import Data.Aeson
import Control.Applicative
import Network.Mail.Mime.SES
import Network.Mail.Mime as M
import Network.Mail.SMTP as S
import qualified Data.Text as T
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

getCredentials :: FilePath -> IO (Maybe SesCredentials)
getCredentials  = \filePath -> do
  raw_creds <- B.readFile filePath
  let creds = decode raw_creds :: Maybe SesCredentials
  return creds

safeHead :: [String] -> String
safeHead [] = []
safeHead (x:_) = x

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

sendEmail :: String
   -> [String]
   -> FilePath
   -> Maybe SesCredentials
   -> IO ()
sendEmail eFrom eTo filePath  ses_creds  = do
  case ses_creds of
    Nothing -> putStrLn "NoSesCredentialFound"
    Just cred -> do
      manager <- newTlsManager
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
