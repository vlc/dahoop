{-# LANGUAGE DeriveGeneric       #-}
module Dahoop.Event where

import Data.Serialize           (Serialize)
import Network.HostName
import GHC.Generics             (Generic)

import Dahoop.Internal.Messages

data MasterEvent c
  = Announcing Announcement
  | Began JobCode
  | WaitingForWorkRequest
  | SentWork WorkId
  | ReceivedResult WorkId Float
  | SentTerminate
  | Finished
  | SentPreload
  | RemoteEvent SlaveId (SlaveLogEntry c) deriving (Eq, Show)

data SlaveEvent
  = AwaitingAnnouncement
  | ReceivedAnnouncement Announcement
  | RequestingPreload
  | ReceivedPreload
  | WaitingForWorkReply
  | StartedUnit WorkId
  | FinishedUnit WorkId
  | FinishedJob Int JobCode deriving (Eq, Show, Generic)

instance Serialize SlaveEvent

data SlaveLogEntry a = DahoopEntry SlaveEvent | UserEntry a deriving (Eq, Show, Generic)

instance (Serialize a) => Serialize (SlaveLogEntry a)

data SlaveId = SlaveId { slaveHostName :: HostName,
                         slavePort     :: Int
                       } deriving (Eq, Show, Generic)

instance Serialize SlaveId
