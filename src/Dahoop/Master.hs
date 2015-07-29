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

import Control.Concurrent       (threadDelay)
import Control.Concurrent.Async (cancel, link)
import qualified Control.Foldl as L
import Control.Lens             (makeLenses, (^.))
import Control.Monad            (forever)
import Control.Monad.IO.Class
import Control.Monad.Trans
import Control.Monad.Catch
import Control.Concurrent.STM
import Data.ByteString          (ByteString)
import Data.List.NonEmpty       (NonEmpty ((:|)))
import Data.Serialize           (runGet, encode, decode, Serialize)
import Network.HostName

import System.ZMQ4.Monadic      (Pub (..), Pull (..), Sub (..), Receiver, Router (..), Sender, Poll (Sock), Event (In), poll)
import Dahoop.ZMQ4.Trans        (ZMQT, Socket, receive, receiveMulti, runZMQT, liftZMQ, send, sendMulti, socket, subscribe, async)

import qualified Dahoop.Internal.Messages  as M
import           Dahoop.Internal.WorkQueue
import           Dahoop.Event
import           Dahoop.Utils
import           Dahoop.ZMQ4

-- TO DO
-- * A heartbeat?
-- * UUID for job codes

-- MASTER

data DistConfig = DistConfig {
                             _resultsPort     :: Int
                             ,_askPort        :: Int
                             ,_preloadPort    :: Int
                             ,_loggingPort    :: Int
                             ,_announcePort   :: Int
                             }
makeLenses ''DistConfig

runAMaster :: (Serialize a, Serialize b, Serialize r, Serialize l, Ord i, Serialize i,
               MonadIO m, MonadMask m) =>
              MasterEventHandler m i l -> DistConfig -> a -> [(i, m b)] -> L.FoldM m r z -> m z
runAMaster k config preloadData work (L.FoldM step first extract) =
        runZMQT $ do jobCode <- liftIO M.generateJobCode
                     eventQueue <- atomicallyIO newTQueue
                     h <- liftIO getHostName
                     sock <- liftZMQ $ socket Pub
                     bindM sock $ TCP Wildcard (config^.announcePort)
                     announceThread <- liftZMQ $ async (announce sock (announcement config (DNS h) jobCode) eventQueue)
                     -- liftIO . link $ announceThread
                     (liftIO . link) =<< liftZMQ (async (preload (config ^. preloadPort) preloadData eventQueue))
                     lift $ k (Began jobCode)
                     state <- lift first
                     result <- theProcess' k (config ^. askPort) jobCode (config ^. resultsPort) (config ^. loggingPort) work eventQueue (state, step)
                     liftIO (cancel announceThread)
                     broadcastFinished jobCode sock
                     lift $ k Finished
                     lift $ extract result

announcement :: DistConfig
                -> Connect
                -> M.JobCode
                -> M.Announcement
announcement v ourHostname jc =
  M.Announcement
      jc
      (tcpHere (v ^. resultsPort))
      (tcpHere (v ^. askPort))
      (tcpHere (v ^. preloadPort))
      (tcpHere (v ^. loggingPort))
  where tcpHere = TCP ourHostname

announce :: (MonadIO m) => (Socket s Pub) -> M.Announcement -> TQueue (MasterEvent i l) -> ZMQT s m ()
announce announceSocket ann eventQueue =
  do -- 'Pub' sockets don't queue messages, they broadcast only to things
     -- that have connected.
     -- Need to give some time for the slave connections to complete
     liftIO (threadDelay 500000)
     atomicallyIO $ writeTQueue eventQueue (Announcing ann)
     forever $
       do
          (send announceSocket [] . M.announcement) ann
          -- we wait so that we don't spam more than necessary
          liftIO $ threadDelay 500000

preload :: (MonadIO m, Serialize a) => Int -> a -> TQueue (MasterEvent i l) -> ZMQT s m ()
preload port preloadData eventQueue = do
  s <- returning (socket Router) (`bindM` TCP Wildcard port)
  forever $ do
    (request, replyWith) <- replyarama s
    let Right slaveid = decode request :: Either String M.SlaveId
    atomicallyIO $ writeTQueue eventQueue (SentPreload slaveid)
    replyWith (encode preloadData)

broadcastFinished :: (MonadIO m) => M.JobCode -> Socket z Pub -> ZMQT z m ()
broadcastFinished n announceSocket =
  do liftIO (threadDelay 500000)
     (send announceSocket [] . M.finishUp) n


-- NOTE: Send and receive must be done using different sockets, as they are used in different threads
theProcess' :: forall i m a l z x r.
               (Ord i, MonadIO m, MonadMask m, Serialize a, Serialize l, Serialize r, Serialize i)
            => MasterEventHandler m i l
            -> Int
            -> M.JobCode
            -> Int
            -> Int
            -> [(i, m a)]
            -> TQueue (MasterEvent i l)
            -> (x, x -> r -> m x)
            -> ZMQT z m x
theProcess' k sendPort jc rport logPort work eventQueue foldbits = do
    workVar <- atomicallyIO newEmptyTMVar
    liftZMQ $
        do (liftIO . link) =<<
               async (dealWork sendPort jc workVar eventQueue)
           (liftIO . link) =<<
               async (receiveLogs logPort eventQueue)
    waitForAllResults k rport jc work eventQueue workVar foldbits

dealWork :: (MonadIO m, Serialize a, Serialize i) => Int -> M.JobCode -> TMVar (Maybe (i, a)) -> TQueue (MasterEvent i l) -> ZMQT s m ()
dealWork port n workVar eventQueue =
  do sendSkt <- returning (socket Router)
                          (`bindM` TCP Wildcard port)
     let loop =
           do (request, replyWith) <- replyarama sendSkt
              let Right (slaveid, slaveJc) = decode request
              if slaveJc /= n then do
                replyWith (M.terminate slaveJc)
                loop
              else do
                item <- atomicallyIO $ takeTMVar workVar
                case item of
                  Just (wid, thing) -> do
                    -- if we've just started repeating, we could return
                    -- the item to the queue (unGetTQueue), tell the client to hold tight
                    -- for a little while, sleep for a bit, then loop.
                    -- this would give time for recently received results to
                    -- get processed, and also give time for slightly slower slaves
                    -- to get their results in
                    atomicallyIO $ writeTQueue eventQueue (SentWork slaveid wid)
                    (replyWith . M.work) (wid,thing) >> loop
                  Nothing ->
                    replyWith (M.terminate n)
     loop

     forever $ do
       (request, replyWith) <- replyarama sendSkt
       let Right (slaveid, _) = decode request :: Either String (M.SlaveId, M.JobCode)
       atomicallyIO (writeTQueue eventQueue (SentTerminate slaveid))
       replyWith (M.terminate n)

receiveLogs :: forall z m i l. (MonadIO m, Serialize i, Serialize l) => Int -> TQueue (MasterEvent i l) -> ZMQT z m ()
receiveLogs logPort eventQueue =
  do logSocket <- returning (socket Sub)
                            (`bindM` TCP Wildcard logPort)
     subscribe logSocket "" -- Subscribe to every incoming message
     _ <- forever $
       do result <- receive logSocket
          let Right (slaveid, logEntry) = decode result :: Either String (M.SlaveId, SlaveLogEntry i l)
          atomicallyIO $ writeTQueue eventQueue (RemoteEvent slaveid logEntry)
     return ()

waitForAllResults :: (MonadIO m, Serialize r, Serialize i, Ord i)
                  => MasterEventHandler m i l -> Int -> M.JobCode -> [(i, m a)]
                  -> TQueue (MasterEvent i l) -> TMVar (Maybe (i, a)) -> (x, x -> r -> m x) -> ZMQT z m x
waitForAllResults k rp jc work eventQueue workVar (first, step) =
  do receiveSocket <- returning (socket Pull)
                                (`bindM` TCP Wildcard rp)
     queue <- atomicallyIO $ buildWork work
     let loop state =
           do hasReceived <- hasReceivedMessage receiveSocket
              newstate <- if hasReceived then do
                result <- receive receiveSocket
                let Right (slaveid, slaveJc, wid, stuff) =
                      runGet M.getReply result
                -- If a slave is sending work for the wrong job code,
                -- it will be killed when it asks for the next bit of work
                if jc == slaveJc
                  then do
                  atomicallyIO $ do
                    _ <- complete wid queue
                    p <- progress queue
                    writeTQueue eventQueue (ReceivedResult slaveid wid p)
                  lift $ step state stuff
                  else return state
              else
                return state

              slurpTQueue eventQueue (lift . k)

              completed <- atomicallyIO $ isComplete queue

              if completed
               then
                atomicallyIO $ do
                  _ <- tryTakeTMVar workVar
                  putTMVar workVar Nothing
                  return newstate
               else do
                Just (wid, Repeats _, action) <- atomicallyIO $ start queue
                workItem <- lift action
                atomicallyIO $ putTMVar workVar $ Just (wid, workItem)
                loop newstate

     loop first

-- | 0mq Utils

hasReceivedMessage :: MonadIO m => Socket z t -> m Bool
hasReceivedMessage s = 
           do let recievedPoll = Sock s [In] Nothing
              events <- poll 0 [recievedPoll]
              return $! elem In $ head events

slurpTQueue :: MonadIO m => TQueue a -> (a -> m ()) -> m ()
slurpTQueue q f = go
              where go = do
                            event <- atomicallyIO $ tryReadTQueue q
                            case event of
                                Just e -> f e >> go
                                Nothing -> return ()

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
