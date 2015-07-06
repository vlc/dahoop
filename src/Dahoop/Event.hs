{-# LANGUAGE DeriveGeneric       #-}
module Dahoop.Event where

import Data.Serialize           (Serialize)
import GHC.Generics             (Generic)

import Dahoop.Internal.Messages

data MasterEvent c r
  = Announcing Announcement
  | Began JobCode
  | WaitingForWorkRequest
  | SentWork SlaveId
  | ReceivedResult SlaveId r Float
  | SentTerminate SlaveId
  | Finished
  | SentPreload SlaveId
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

type MasterEventHandler m l r = MasterEvent l r -> m ()

type SlaveEventHandler = SlaveEvent -> IO ()

data WorkDetails m a b c = WorkDetails { workPreload :: a,
                                         workPayload :: b,
                                         remoteLogger :: c -> m () }
