{-# LANGUAGE RankNTypes                #-}
module Dahoop.Single where

import qualified Control.Foldl as L
import Control.Monad (foldM)
import Control.Monad.IO.Class
import Data.List.NonEmpty (NonEmpty)

import qualified Dahoop.Internal.Messages  as M
import           Dahoop.Event
import           Dahoop.ZMQ4

runASingle :: (MonadIO m)
           => MasterEventHandler IO i c
           -> SlaveEventHandler i
           -> a
           -> NonEmpty (i, IO b)
           -> (forall n. (MonadIO n) => WorkDetails n a b c -> n r)
           -> L.FoldM m (i, r) z
           -> m z
runASingle mk sk preload workBuilders workFunction (L.FoldM step first extract) = do
  jobCode <- liftIO M.generateJobCode
  let slaveId = M.SlaveId "single"
      fakeAddress = TCP (IP4' 255 255 255 255) 1234
      fakeAnnouncement = M.Announcement jobCode fakeAddress fakeAddress fakeAddress fakeAddress
      masterLog = liftIO . mk
      slaveLog = liftIO . sk
      clientLog e = masterLog (RemoteEvent slaveId (UserEntry e))
      slaveRemoteLog e = do
        masterLog (RemoteEvent slaveId (DahoopEntry e))
        slaveLog e

  masterLog (Began jobCode)
  masterLog (Announcing fakeAnnouncement)

  slaveLog AwaitingAnnouncement
  slaveLog (ReceivedAnnouncement fakeAnnouncement)

  slaveLog RequestingPreload
  masterLog (SentPreload slaveId)
  slaveLog ReceivedPreload

  initial <- first

  final <- foldM (\state (ix, action) -> do
    work <- liftIO action
    slaveRemoteLog WaitingForWorkReply
    masterLog (SentWork slaveId ix)

    slaveRemoteLog $ StartedUnit ix 0
    result <- workFunction (WorkDetails preload work clientLog)
    slaveRemoteLog $ FinishedUnit ix

    masterLog (ReceivedResult slaveId ix 0.0) -- TODO fake out actual percent complete?

    step state (ix, result)
   ) initial workBuilders

  masterLog (SentTerminate slaveId)
  masterLog Finished

  extract final
