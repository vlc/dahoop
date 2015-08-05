{-# LANGUAGE DeriveGeneric   #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Dahoop.Internal.Messages
       (terminate, work, getWorkOrTerminate, Announcement(..), reply,
        getReply, generateJobCode, resultsAddress, preloadAddress,
        askAddress, annJobCode, JobCode(..), announcement, finishUp,
        getAnnouncementOrFinishUp, loggingAddress, SlaveId(..))
       where

import Control.Lens
import Data.ByteString     (ByteString)
import Data.Serialize      (Get, Serialize, encode, get, getWord32be, put, putWord32be, runPut)
import Data.UUID           (UUID, fromWords, toWords)
import Data.UUID.V4        (nextRandom)
import GHC.Generics
import Network.HostName

import Dahoop.ZMQ4

data JobCode = JobCode UUID deriving (Eq, Show, Generic)

generateJobCode :: IO JobCode
generateJobCode = JobCode <$> nextRandom

instance Serialize JobCode

instance Serialize UUID where
    put uuid = case toWords uuid of (w0, w1, w2, w3) -> putWord32be w0 >> putWord32be w1 >> putWord32be w2 >> putWord32be w3
    get = fromWords <$> getWord32be <*> getWord32be <*> getWord32be <*> getWord32be

data SlaveId = SlaveId { slaveHostName :: HostName,
                         slavePort     :: Int
                       } deriving (Eq, Show, Generic)

instance Serialize SlaveId

-- ****************************
-- | WORK

-- | Master -> Slave, Tell a slave to stop asking for work
terminate :: JobCode -> ByteString
terminate n = encode (Left n :: Either JobCode ())

-- | Master -> Slave, Deliver work to a slave
work :: (Serialize i, Serialize a) => (i, a) -> ByteString
work = runPut . put . (Right :: (i, a) -> Either JobCode (i, a))

-- | Slave:
getWorkOrTerminate :: (Serialize i, Serialize a) => Get (Either JobCode (i, a))
getWorkOrTerminate = get

-- | Slave -> Master, Respond to a work message with a result
reply :: (Serialize a, Serialize i) => (SlaveId, JobCode, i, a) -> ByteString
reply = runPut . put

-- | Master: Decode reply
getReply :: (Serialize a, Serialize i) => Get (SlaveId, JobCode, i, a)
getReply = get

-- ****************************
-- | ANNOUNCEMENTS

announcement :: Announcement -> ByteString
announcement n = encode (Right n :: Either JobCode Announcement)

finishUp :: JobCode -> ByteString
finishUp n = encode (Left n :: Either JobCode Announcement)

getAnnouncementOrFinishUp :: Get (Either JobCode Announcement)
getAnnouncementOrFinishUp = get

data Announcement = Announcement {
  _annJobCode     :: JobCode,
  _resultsAddress :: Address Connect,
  _askAddress     :: Address Connect,
  _preloadAddress :: Address Connect,
  _loggingAddress :: Address Connect
  } deriving (Generic, Eq, Show)

annJobCode :: Simple Lens Announcement JobCode
annJobCode = lens _annJobCode (\v s -> v { _annJobCode = s})

resultsAddress :: Simple Lens Announcement (Address Connect)
resultsAddress = lens _resultsAddress (\v s -> v { _resultsAddress = s})

askAddress :: Simple Lens Announcement (Address Connect)
askAddress = lens _askAddress (\v s -> v { _askAddress = s})

preloadAddress :: Simple Lens Announcement (Address Connect)
preloadAddress = lens _preloadAddress (\v s -> v { _preloadAddress = s})
                 
loggingAddress :: Simple Lens Announcement (Address Connect)
loggingAddress = lens _loggingAddress (\v s -> v { _loggingAddress = s})

instance Serialize Announcement
