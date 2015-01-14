{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
{-# OPTIONS_GHC -Werror #-}
module Dahoop.Master where

import Control.Applicative      ((<*))
import Control.Concurrent       (threadDelay)
import Control.Concurrent.Async (cancel)
import Control.Lens             (makeLenses, (^.))
import Control.Monad            (forever, unless)
import Data.ByteString          (ByteString)
import Data.List.NonEmpty       (NonEmpty ((:|)))
import Data.Serialize           (runGet)
import System.ZMQ4.Monadic      (Pub (..), Pull (..), Receiver, Router (..), Sender, Socket, ZMQ, async, liftIO,
                                 receive, receiveMulti, runZMQ, send, sendMulti, socket)

import qualified Dahoop.Internal.Messages  as M
import           Dahoop.Internal.WorkQueue
import           Dahoop.Utils
import           Dahoop.ZMQ4

-- TODO
-- * A heartbeat?
-- * UUID for job codes

-- MASTER

data Events = Announcing M.Announcement
            | Began M.JobCode
            | WaitingForWorkRequest
            | SentWork WorkId
            | ReceivedResult WorkId
            | SentTerminate
            | Finished
            | SentPreload

type EventHandler = forall s. Events -> ZMQ s ()

data DistConfig = DistConfig {
                             _resultsPort     :: Int
                             ,_askPort        :: Int
                             ,_preloadPort    :: Int
                             ,_connectAddress :: Connect
                             ,_slaves         :: [Address Connect]}
makeLenses ''DistConfig

runAMaster :: EventHandler -> DistConfig -> ByteString -> [ByteString] -> IO ()
runAMaster k config preloadData messages =
        runZMQ $ do jobCode <- liftIO M.generateJobCode
                    announceThread <- async (announce k (announcement config jobCode) (config ^. slaves))
                    _ <- async (preload k (config ^. preloadPort) preloadData)
                    k (Began jobCode)
                    theProcess' k (config ^. askPort) jobCode (config ^. resultsPort) messages
                    liftIO (cancel announceThread)
                    broadcastFinished jobCode (config ^. slaves)
                    k Finished
                    return ()

announcement :: DistConfig -> M.JobCode -> M.Announcement
announcement v jc =
  M.Announcement
      jc
      (tcpHere (v ^. resultsPort))
      (tcpHere (v ^. askPort))
      (tcpHere (v ^. preloadPort))
  where tcpHere = TCP (v ^. connectAddress)

announce :: EventHandler -> M.Announcement -> [Address Connect] -> ZMQ s ()
announce k ann ss =
  do announceSocket <- socket Pub
     mapM_ (connectM announceSocket) ss
     -- 'Pub' sockets don't queue messages, they broadcast only to things
     -- that have connected.
     -- Need to give some time for the slave connections to complete
     liftIO (threadDelay 500000)
     k (Announcing ann)
     forever $
       do
          (send announceSocket [] . M.announcement) ann
          -- we wait so that we don't spam more than necessary
          liftIO $ threadDelay 500000

preload :: EventHandler -> Int -> ByteString -> ZMQ s ()
preload k port preloadData = do s <- returning (socket Router) (`bindM` TCP Wildcard port)
                                forever (replyToReq s preloadData >> k SentPreload)

broadcastFinished :: M.JobCode -> [Address Connect] -> ZMQ z ()
broadcastFinished n ss =
  do announceSocket <- socket Pub
     mapM_ (connectM announceSocket) ss
     liftIO (threadDelay 500000)
     (send announceSocket [] . M.finishUp) n


-- NOTE: Send and receive must be done using different sockets, as they are used in different threads
theProcess' :: EventHandler -> Int -> M.JobCode -> Int -> [ByteString] -> ZMQ s ()
theProcess' k sendPort jc rport ms =
  do queue <- (atomicallyIO . buildWork . annotateWork) ms
     _ <- async (dealWork k sendPort jc queue)
     waitForAllResults k rport queue
  where annotateWork = zip (map WorkId [1 ..])

dealWork :: EventHandler -> Int -> M.JobCode -> Work ByteString -> ZMQ s ()
dealWork k port n queue =
  do sendSkt <- returning (socket Router)
                          (`bindM` TCP Wildcard port)
     let loop =
           do replyWith <- replyarama sendSkt
              item <- (atomicallyIO . start) queue
              case item of
                Just (wid,Repeats _,thing) ->
                  -- if we've just started repeating, we could return
                  -- the item to the queue (unGetTQueue), tell the client to hold tight
                  -- for a little while, sleep for a bit, then loop.
                  -- this would give time for recently received results to
                  -- get processed, and also give time for slightly slower slaves
                  -- to get their results in
                  (replyWith . M.work) (wid,thing) >> loop
                Nothing ->
                  replyWith (M.terminate n)
     loop
     forever (replyToReq sendSkt
                         (M.terminate n) <*
              k SentTerminate)

waitForAllResults :: EventHandler -> Int -> Work ByteString -> ZMQ s ()
waitForAllResults k rp queue =
  do receiveSocket <- returning (socket Pull)
                                (`bindM` TCP Wildcard rp)
     let loop =
           do result <- receive receiveSocket
              let Right (wid,_ :: ByteString) =
                    runGet M.getReply result
              -- TODO What if the result is for the wrong job code?
              k (ReceivedResult wid)
              completed <- atomicallyIO
                             (complete wid queue >>
                              isComplete queue)
              unless completed loop
     loop
     return ()

-- | 0mq Utils

replyToReq :: forall z. Socket z Router -> ByteString -> ZMQ z ()
replyToReq sendSkt m =
  do replyWith <- replyarama sendSkt
     replyWith m

replyarama :: (Receiver t, Sender t) => Socket z t -> ZMQ z (ByteString -> ZMQ z ())
replyarama s =
  do (peer:_) <- receiveMulti s
     return (sendToReq s peer)

sendToReq :: Sender t => Socket z t -> ByteString -> ByteString -> ZMQ z ()
sendToReq skt peer msg =
  sendMulti skt
            (peer :|
             ["",msg])
