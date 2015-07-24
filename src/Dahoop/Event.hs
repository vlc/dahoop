{-# LANGUAGE DeriveGeneric       #-}
module Dahoop.Event where

import Data.Serialize           (Serialize)
import GHC.Generics             (Generic)

import Dahoop.Internal.Messages

data MasterEvent i c
  = Announcing Announcement
  | Began JobCode
  | WaitingForWorkRequest
  | SentWork SlaveId i
  | ReceivedResult SlaveId i Float
  | SentTerminate SlaveId
  | Finished
  | SentPreload SlaveId
  | RemoteEvent SlaveId (SlaveLogEntry i c) deriving (Eq, Show)

data SlaveEvent i
  = AwaitingAnnouncement
  | ReceivedAnnouncement Announcement
  | RequestingPreload
  | ReceivedPreload
  | WaitingForWorkReply
  | StartedUnit i
  | FinishedUnit i
  | FinishedJob Int JobCode deriving (Eq, Show, Generic)

instance Serialize i => Serialize (SlaveEvent i)

data SlaveLogEntry i a = DahoopEntry (SlaveEvent i) | UserEntry a deriving (Eq, Show, Generic)

instance (Serialize i, Serialize a) => Serialize (SlaveLogEntry i a)

type MasterEventHandler m i l = MasterEvent i l -> m ()

type SlaveEventHandler i = SlaveEvent i -> IO ()

data WorkDetails m a b c = WorkDetails { workPreload :: a,
                                         workPayload :: b,
                                         remoteLogger :: c -> m () }
