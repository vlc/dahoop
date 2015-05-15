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
import qualified Control.Concurrent.Async.Lifted.Safe as A
import Data.ByteString          (ByteString)
import Data.List.NonEmpty       (NonEmpty ((:|)))
import Data.Serialize           (runGet, encode, decode, Serialize)

import System.ZMQ4.Monadic      (Pub (..), Pull (..), Sub (..), Receiver, Router (..), Sender)
import Dahoop.ZMQ4.Trans        (ZMQT, Socket, receive, receiveMulti, runZMQT, send, sendMulti, socket, subscribe, async)

import qualified Dahoop.Internal.Messages  as M
import           Dahoop.Internal.WorkQueue
import           Dahoop.Event
import           Dahoop.Utils
import           Dahoop.ZMQ4

-- TO DO
-- * A heartbeat?
-- * UUID for job codes

type EventHandler m l = (MasterEvent l -> m ())

-- MASTER

data DistConfig = DistConfig {
                             _resultsPort     :: Int
                             ,_askPort        :: Int
                             ,_preloadPort    :: Int
                             ,_loggingPort    :: Int
                             ,_connectAddress :: Connect
                             ,_slaves         :: [Address Connect]}
makeLenses ''DistConfig

runAMaster :: (Serialize a, Serialize b, Serialize c, Serialize l,
               MonadIO m, MonadBaseControl IO m, A.Forall (A.Pure m)) =>
              EventHandler m l -> DistConfig -> a -> [m b] -> (c -> m ()) -> m ()
runAMaster k config preloadData work f =
        runZMQT $ do jobCode <- liftIO M.generateJobCode
                     announceThread <- async (announce k (announcement config jobCode) (config ^. slaves))
                     -- liftIO . link $ announceThread
                     (liftIO . link) =<< async (preload k (config ^. preloadPort) preloadData)
                     lift $ k (Began jobCode)
                     theProcess' k (config ^. askPort) jobCode (config ^. resultsPort) (config ^. loggingPort) work f
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

announce :: (MonadIO m) =>
               EventHandler m c
            -> M.Announcement
            -> [Address Connect]
            -> ZMQT s m ()
announce k ann ss =
  do announceSocket <- socket Pub
     mapM_ (connectM announceSocket) ss
     -- 'Pub' sockets don't queue messages, they broadcast only to things
     -- that have connected.
     -- Need to give some time for the slave connections to complete
     liftIO (threadDelay 500000)
     lift $ k (Announcing ann)
     forever $
       do
          (send announceSocket [] . M.announcement) ann
          -- we wait so that we don't spam more than necessary
          liftIO $ threadDelay 500000

preload :: (MonadIO m, Serialize a) => EventHandler m c -> Int -> a -> ZMQT s m ()
preload k port preloadData = do s <- returning (socket Router) (`bindM` TCP Wildcard port)
                                forever (replyToReq s (encode preloadData) >> (lift $ k SentPreload))

broadcastFinished :: (MonadIO m) => M.JobCode -> [Address Connect] -> ZMQT z m ()
broadcastFinished n ss =
  do announceSocket <- socket Pub
     mapM_ (connectM announceSocket) ss
     liftIO (threadDelay 500000)
     (send announceSocket [] . M.finishUp) n


-- NOTE: Send and receive must be done using different sockets, as they are used in different threads
theProcess' :: (MonadIO m, MonadBaseControl IO m, A.Forall (A.Pure m),
                Serialize a, Serialize c, Serialize l)  => EventHandler m l -> Int -> M.JobCode -> Int -> Int -> [m a] -> (c -> m ()) -> ZMQT s m ()
theProcess' k sendPort jc rport logPort work yield = do
  queue <- atomicallyIO $ buildWork work
  (liftIO . link) =<< async (dealWork k sendPort jc queue)
  (liftIO . link) =<< async (receiveLogs k logPort)
  waitForAllResults k yield rport jc queue

dealWork :: (MonadIO m, Serialize a) => EventHandler m c -> Int -> M.JobCode -> Work (m a) -> ZMQT s m ()
dealWork k port n queue =
  do sendSkt <- returning (socket Router)
                          (`bindM` TCP Wildcard port)
     let loop =
           do (request, replyWith) <- replyarama sendSkt
              let Right slaveJc = decode request
              if (slaveJc /= n) then do
                replyWith (M.terminate slaveJc)
                loop
              else do
                item <- (atomicallyIO . start) queue
                case item of
                  Just (wid,Repeats _, builder) -> do
                    -- if we've just started repeating, we could return
                    -- the item to the queue (unGetTQueue), tell the client to hold tight
                    -- for a little while, sleep for a bit, then loop.
                    -- this would give time for recently received results to
                    -- get processed, and also give time for slightly slower slaves
                    -- to get their results in
                    thing <- lift builder
                    (replyWith . M.work) (wid,thing) >> loop
                  Nothing ->
                    replyWith (M.terminate n)
     loop
     forever (replyToReq sendSkt
                         (M.terminate n) <*
              lift (k SentTerminate))

receiveLogs :: forall z m c. (MonadIO m, Serialize c) => EventHandler m c -> Int -> ZMQT z m ()
receiveLogs k logPort =
  do logSocket <- returning (socket Sub)
                            (`bindM` TCP Wildcard logPort)
     subscribe logSocket "" -- Subscribe to every incoming message
     forever $
       do result <- receive logSocket
          let Right (slaveid, logEntry) = decode result :: Either String (SlaveId, SlaveLogEntry c)
          lift $ k (RemoteEvent slaveid logEntry)
     return ()

waitForAllResults :: (MonadIO m, Serialize a) => EventHandler m c -> (a -> m ()) -> Int -> M.JobCode -> Work a2 -> ZMQT z m ()
waitForAllResults k f rp jc queue =
  do receiveSocket <- returning (socket Pull)
                                (`bindM` TCP Wildcard rp)
     let loop =
           do result <- receive receiveSocket
              let Right (slaveJc, wid, stuff) =
                    runGet M.getReply result
              -- If a slave is sending work for the wrong job code,
              -- it will be killed when it asks for the next bit of work
              when (jc == slaveJc) $ do
                lift $ k (ReceivedResult wid)
                lift $ f stuff
                atomicallyIO $ complete wid queue
                return ()

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
