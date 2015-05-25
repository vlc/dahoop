{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE ConstraintKinds     #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
{-# OPTIONS_GHC -Werror #-}
module Dahoop.Master (
  module Dahoop.Master
) where

import Control.Applicative      ((<*))
import Control.Concurrent       (threadDelay)
import Control.Concurrent.Async (cancel, link)
import Control.Lens             (makeLenses, (^.))
import Control.Monad            (forever, unless, when)
import Control.Monad.IO.Class
import Control.Monad.Trans
import Control.Monad.Trans.Control
import Control.Concurrent.STM
import Data.ByteString          (ByteString)
import Data.List.NonEmpty       (NonEmpty ((:|)))
import Data.Serialize           (runGet, encode, decode, Serialize)

import System.ZMQ4.Monadic      (Pub (..), Pull (..), Sub (..), Receiver, Router (..), Sender)
import Dahoop.ZMQ4.Trans        (ZMQT, Socket, receive, receiveMulti, runZMQT, liftZMQ, send, sendMulti, socket, subscribe, async)

import qualified Dahoop.Internal.Messages  as M
import           Dahoop.Internal.WorkQueue
import           Dahoop.Event
import           Dahoop.Utils
import           Dahoop.ZMQ4

-- TO DO
-- * A heartbeat?
-- * UUID for job codes

type EventHandler m l r = MasterEvent l r -> m ()

-- MASTER

data DistConfig = DistConfig {
                             _resultsPort     :: Int
                             ,_askPort        :: Int
                             ,_preloadPort    :: Int
                             ,_loggingPort    :: Int
                             ,_connectAddress :: Connect
                             ,_slaves         :: [Address Connect]}
makeLenses ''DistConfig

runAMaster :: (Serialize a, Serialize b, Serialize r, Serialize l,
               MonadIO m, MonadBaseControl IO m) =>
              EventHandler m l r -> DistConfig -> a -> [IO b] -> m ()
runAMaster k config preloadData work =
        runZMQT $ do jobCode <- liftIO M.generateJobCode
                     eventQueue <- atomicallyIO $ newTQueue
                     announceThread <- liftZMQ $ async (announce (announcement config jobCode) (config ^. slaves) eventQueue)
                     -- liftIO . link $ announceThread
                     (liftIO . link) =<< liftZMQ (async (preload (config ^. preloadPort) preloadData eventQueue))
                     lift $ k (Began jobCode)
                     theProcess' k (config ^. askPort) jobCode (config ^. resultsPort) (config ^. loggingPort) work eventQueue
                     liftIO (cancel announceThread)
                     broadcastFinished jobCode (config ^. slaves)
                     lift $ k Finished
                     return ()

announcement :: DistConfig
                -> M.JobCode
                -> M.Announcement
announcement v jc =
  M.Announcement
      jc
      (tcpHere (v ^. resultsPort))
      (tcpHere (v ^. askPort))
      (tcpHere (v ^. preloadPort))
      (tcpHere (v ^. loggingPort))
  where tcpHere = TCP (v ^. connectAddress)

announce :: (MonadIO m) => M.Announcement -> [Address Connect] -> TQueue (MasterEvent l r) -> ZMQT s m ()
announce ann ss eventQueue =
  do announceSocket <- socket Pub
     mapM_ (connectM announceSocket) ss
     -- 'Pub' sockets don't queue messages, they broadcast only to things
     -- that have connected.
     -- Need to give some time for the slave connections to complete
     liftIO (threadDelay 500000)
     atomicallyIO $ writeTQueue eventQueue (Announcing ann)
     forever $
       do
          (send announceSocket [] . M.announcement) ann
          -- we wait so that we don't spam more than necessary
          liftIO $ threadDelay 500000

preload :: (MonadIO m, Serialize a) => Int -> a -> TQueue (MasterEvent l r) -> ZMQT s m ()
preload port preloadData eventQueue = do
  s <- returning (socket Router) (`bindM` TCP Wildcard port)
  forever $ do
    replyToReq s (encode preloadData)
    atomicallyIO $ writeTQueue eventQueue (SentPreload)

broadcastFinished :: (MonadIO m) => M.JobCode -> [Address Connect] -> ZMQT z m ()
broadcastFinished n ss =
  do announceSocket <- socket Pub
     mapM_ (connectM announceSocket) ss
     liftIO (threadDelay 500000)
     (send announceSocket [] . M.finishUp) n


-- NOTE: Send and receive must be done using different sockets, as they are used in different threads
theProcess' :: forall m a l r z. (MonadIO m, MonadBaseControl IO m, Serialize a, Serialize l, Serialize r)
            => EventHandler m l r -> Int -> M.JobCode -> Int -> Int -> [IO a] -> TQueue (MasterEvent l r) -> ZMQT z m ()
theProcess' k sendPort jc rport logPort work eventQueue = do
  workQueue <- atomicallyIO $ buildWork work
  liftZMQ $ do
    (liftIO . link) =<< async (dealWork sendPort jc workQueue eventQueue)
    (liftIO . link) =<< async (receiveLogs logPort eventQueue)

  waitForAllResults k rport jc workQueue eventQueue

dealWork :: (MonadIO m, Serialize a) => Int -> M.JobCode -> Work (m a) -> TQueue (MasterEvent l r) -> ZMQT s m ()
dealWork port n workQueue eventQueue =
  do sendSkt <- returning (socket Router)
                          (`bindM` TCP Wildcard port)
     let loop =
           do (request, replyWith) <- replyarama sendSkt
              let Right slaveJc = decode request
              if (slaveJc /= n) then do
                replyWith (M.terminate slaveJc)
                loop
              else do
                item <- (atomicallyIO . start) workQueue
                case item of
                  Just (wid, Repeats _, builder) -> do
                    -- if we've just started repeating, we could return
                    -- the item to the queue (unGetTQueue), tell the client to hold tight
                    -- for a little while, sleep for a bit, then loop.
                    -- this would give time for recently received results to
                    -- get processed, and also give time for slightly slower slaves
                    -- to get their results in
                    thing <- lift builder
                    atomicallyIO $ writeTQueue eventQueue (SentWork)

                    (replyWith . M.work) (wid,thing) >> loop
                  Nothing ->
                    replyWith (M.terminate n)
     loop
     forever (replyToReq sendSkt
                         (M.terminate n) <*
              (atomicallyIO $ writeTQueue eventQueue (SentTerminate)))

receiveLogs :: forall z m l r. (MonadIO m, Serialize l, Serialize r) => Int -> TQueue (MasterEvent l r) -> ZMQT z m ()
receiveLogs logPort eventQueue =
  do logSocket <- returning (socket Sub)
                            (`bindM` TCP Wildcard logPort)
     subscribe logSocket "" -- Subscribe to every incoming message
     forever $
       do result <- receive logSocket
          let Right (slaveid, logEntry) = decode result :: Either String (SlaveId, SlaveLogEntry l)
          atomicallyIO $ writeTQueue eventQueue (RemoteEvent slaveid logEntry)
     return ()

waitForAllResults :: (MonadIO m, Serialize r) => EventHandler m l r -> Int -> M.JobCode -> Work a2 -> TQueue (MasterEvent l r) -> ZMQT z m ()
waitForAllResults k rp jc queue eventQueue =
  do receiveSocket <- returning (socket Pull)
                                (`bindM` TCP Wildcard rp)
     let loop =
           do result <- receive receiveSocket
              let Right (slaveJc, wid, stuff) =
                    runGet M.getReply result
              -- If a slave is sending work for the wrong job code,
              -- it will be killed when it asks for the next bit of work
              when (jc == slaveJc) $ do
                atomicallyIO $ do
                  complete wid queue
                  p <- progress queue
                  writeTQueue eventQueue (ReceivedResult stuff p)
                return ()

              let checkEvents = do
                    event <- atomicallyIO $ tryReadTQueue eventQueue
                    case event of
                      Just e -> lift (k e) >> checkEvents
                      Nothing -> return ()

              checkEvents

              completed <- atomicallyIO $ isComplete queue
              unless completed loop
     loop
     return ()

-- | 0mq Utils

replyToReq :: (MonadIO m) => Socket z Router -> ByteString -> ZMQT z m ()
replyToReq sendSkt m =
  do (_, replyWith) <- replyarama sendSkt
     replyWith m

replyarama :: (MonadIO m, Receiver t, Sender t) => Socket z t -> ZMQT z m (ByteString, ByteString -> ZMQT z m ())
replyarama s =
  do (peer:_:request:_) <- receiveMulti s
     return (request, sendToReq s peer)

sendToReq :: (MonadIO m, Sender t) => Socket z t -> ByteString -> ByteString -> ZMQT z m ()
sendToReq skt peer msg =
  sendMulti skt
            (peer :|
             ["", msg])
