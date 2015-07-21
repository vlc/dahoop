{-# LANGUAGE RankNTypes                #-}
module Dahoop.Single where

import qualified Control.Foldl as L
import Control.Monad (foldM)
import Control.Monad.IO.Class

import qualified Dahoop.Internal.Messages  as M
import           Dahoop.Internal.WorkQueue
import           Dahoop.Event
import           Dahoop.ZMQ4

runASingle :: (MonadIO m)
           => MasterEventHandler m c
           -> SlaveEventHandler
           -> a
           -> [m b]
           -> (forall n. (MonadIO n) => WorkDetails n a b c -> n r)
           -> L.FoldM m r z
           -> m z
runASingle mk sk preload workBuilders workFunction (L.FoldM step first extract) = do
  jobCode <- liftIO M.generateJobCode
  let slaveId = M.SlaveId "single" 1234
  let fakeAddress = TCP (IP4' 255 255 255 255) 1234
  let fakeAnnouncement = M.Announcement jobCode fakeAddress fakeAddress fakeAddress fakeAddress
  let masterLog = mk
  let slaveLog = liftIO . sk
  let clientLog e = masterLog (RemoteEvent slaveId (UserEntry e))
  let slaveRemoteLog e = do
        masterLog (RemoteEvent slaveId (DahoopEntry e))
        slaveLog e

  let workCount = length workBuilders

  masterLog (Began jobCode)
  masterLog (Announcing fakeAnnouncement)

  slaveLog AwaitingAnnouncement
  slaveLog (ReceivedAnnouncement fakeAnnouncement)

  slaveLog RequestingPreload
  masterLog (SentPreload slaveId)
  slaveLog ReceivedPreload

  initial <- first

  final <- foldM (\state (ix, action) -> do
    work <- action
    slaveRemoteLog WaitingForWorkReply
    masterLog (SentWork slaveId)

    slaveRemoteLog $ StartedUnit (WorkId ix)
    result <- workFunction (WorkDetails preload work clientLog)
    slaveRemoteLog $ FinishedUnit (WorkId ix)

    masterLog (ReceivedResult slaveId (fromIntegral ix / fromIntegral workCount))

    step state result
   ) initial (zip [1..] workBuilders)

  masterLog (SentTerminate slaveId)
  masterLog Finished

  extract final
