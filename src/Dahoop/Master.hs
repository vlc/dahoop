{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
{-# OPTIONS_GHC -Werror #-}
module Dahoop.Master (
  module Dahoop.Master
)where

import Control.Applicative      ((<*))
import Control.Concurrent       (threadDelay)
import Control.Concurrent.Async (cancel, link)
import Control.Lens             (makeLenses, (^.))
import Control.Monad            (forever, unless, when)
import Data.ByteString          (ByteString)
import Data.List.NonEmpty       (NonEmpty ((:|)))
import Data.Serialize           (runGet, encode, decode, Serialize)

import System.ZMQ4.Monadic      (Pub (..), Pull (..), Sub (..), Receiver, Router (..), Sender, Socket, ZMQ, async, liftIO,
                                 receive, receiveMulti, runZMQ, send, sendMulti, socket, subscribe)

import qualified Dahoop.Internal.Messages  as M
import           Dahoop.Internal.WorkQueue
import           Dahoop.Event
import           Dahoop.Utils
import           Dahoop.ZMQ4

-- TODO
-- * A heartbeat?
-- * UUID for job codes

-- MASTER

type EventHandler c = MasterEvent c -> IO ()

data DistConfig = DistConfig {
                             _resultsPort     :: Int
                             ,_askPort        :: Int
                             ,_preloadPort    :: Int
                             ,_loggingPort    :: Int
                             ,_connectAddress :: Connect
                             ,_slaves         :: [Address Connect]}
makeLenses ''DistConfig

runAMaster :: (Serialize a, Serialize b, Serialize c, Serialize l) =>
              EventHandler l -> DistConfig -> a -> [IO b] -> (c -> IO ()) -> IO ()
runAMaster k config preloadData work f =
        runZMQ $ do jobCode <- liftIO M.generateJobCode
                    announceThread <- async (announce k (announcement config jobCode) (config ^. slaves))
                    -- liftIO . link $ announceThread
                    (liftIO . link) =<< async (preload k (config ^. preloadPort) preloadData)
                    liftIO $ k (Began jobCode)
                    theProcess' k (config ^. askPort) jobCode (config ^. resultsPort) (config ^. loggingPort) work (liftIO . f)
                    liftIO (cancel announceThread)
                    broadcastFinished jobCode (config ^. slaves)
                    liftIO $ k Finished
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

announce :: EventHandler c
            -> M.Announcement
            -> [Address Connect]
            -> ZMQ s ()
announce k ann ss =
  do announceSocket <- socket Pub
     mapM_ (connectM announceSocket) ss
     -- 'Pub' sockets don't queue messages, they broadcast only to things
     -- that have connected.
     -- Need to give some time for the slave connections to complete
     liftIO (threadDelay 500000)
     liftIO $ k (Announcing ann)
     forever $
       do
          (send announceSocket [] . M.announcement) ann
          -- we wait so that we don't spam more than necessary
          liftIO $ threadDelay 500000

preload :: (Serialize a) => EventHandler c -> Int -> a -> ZMQ s ()
preload k port preloadData = do s <- returning (socket Router) (`bindM` TCP Wildcard port)
                                forever (replyToReq s (encode preloadData) >> liftIO (k SentPreload))

broadcastFinished :: M.JobCode -> [Address Connect] -> ZMQ z ()
broadcastFinished n ss =
  do announceSocket <- socket Pub
     mapM_ (connectM announceSocket) ss
     liftIO (threadDelay 500000)
     (send announceSocket [] . M.finishUp) n


-- NOTE: Send and receive must be done using different sockets, as they are used in different threads
theProcess' :: (Serialize a, Serialize c, Serialize l)  => EventHandler l -> Int -> M.JobCode -> Int -> Int -> [IO a] -> (c -> ZMQ s ()) -> ZMQ s ()
theProcess' k sendPort jc rport logPort work yield = do
  queue <- atomicallyIO $ buildWork work
  (liftIO . link) =<< async (dealWork k sendPort jc queue)
  (liftIO . link) =<< async (receiveLogs k logPort)
  waitForAllResults k yield rport jc queue

dealWork :: (Serialize a) => EventHandler c -> Int -> M.JobCode -> Work (IO a) -> ZMQ s ()
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
                  Just (wid, Repeats _, builder) -> do
                    -- if we've just started repeating, we could return
                    -- the item to the queue (unGetTQueue), tell the client to hold tight
                    -- for a little while, sleep for a bit, then loop.
                    -- this would give time for recently received results to
                    -- get processed, and also give time for slightly slower slaves
                    -- to get their results in
                    thing <- liftIO builder
                    liftIO $ k (SentWork wid)
                    (replyWith . M.work) (wid,thing) >> loop
                  Nothing ->
                    replyWith (M.terminate n)
     loop
     forever (replyToReq sendSkt
                         (M.terminate n) <*
              liftIO (k SentTerminate))

receiveLogs :: forall c z. (Serialize c) => EventHandler c -> Int -> ZMQ z ()
receiveLogs k logPort =
  do logSocket <- returning (socket Sub)
                            (`bindM` TCP Wildcard logPort)
     subscribe logSocket "" -- Subscribe to every incoming message
     forever $
       do result <- receive logSocket
          let Right (slaveid, logEntry) = decode result :: Either String (SlaveId, SlaveLogEntry c)
          liftIO $ k (RemoteEvent slaveid logEntry)
     return ()

waitForAllResults :: Serialize a => EventHandler c -> (a -> ZMQ z a1) -> Int -> M.JobCode -> Work a2 -> ZMQ z ()
waitForAllResults k yield rp jc queue =
  do receiveSocket <- returning (socket Pull)
                                (`bindM` TCP Wildcard rp)
     let loop =
           do result <- receive receiveSocket
              let Right (slaveJc, wid, stuff) =
                    runGet M.getReply result
              -- If a slave is sending work for the wrong job code,
              -- it will be killed when it asks for the next bit of work
              when (jc == slaveJc) $ do
                yield stuff
                p <- atomicallyIO $ do
                  complete wid queue
                  progress queue
                liftIO $ k (ReceivedResult wid p)
                return ()

              completed <- atomicallyIO $ isComplete queue
              unless completed loop
     loop
     return ()

-- | 0mq Utils

replyToReq :: Socket z Router -> ByteString -> ZMQ z ()
replyToReq sendSkt m =
  do (_, replyWith) <- replyarama sendSkt
     replyWith m

replyarama :: (Receiver t, Sender t) => Socket z t -> ZMQ z (ByteString, ByteString -> ZMQ z ())
replyarama s =
  do (peer:_:request:_) <- receiveMulti s
     return (request, sendToReq s peer)

sendToReq :: (Sender t) => Socket z t -> ByteString -> ByteString -> ZMQ z ()
sendToReq skt peer msg =
  sendMulti skt
            (peer :|
             ["", msg])
