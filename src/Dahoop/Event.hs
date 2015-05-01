{-# LANGUAGE DeriveGeneric       #-}
module Dahoop.Event where

import Data.Serialize           (Serialize)
import GHC.Generics             (Generic)

import Dahoop.Internal.Messages

data MasterEvent c
  = Announcing Announcement
  | Began JobCode
  | WaitingForWorkRequest
  | SentWork WorkId
  | ReceivedResult WorkId
  | SentTerminate
  | Finished
  | SentPreload
  | RemoteEvent (SlaveLogEntry c) deriving (Eq, Show)

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
