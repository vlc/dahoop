{-# LANGUAGE ConstraintKinds           #-}
{-# LANGUAGE DeriveGeneric             #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE MultiWayIf                #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE RankNTypes                #-}
{-# LANGUAGE ScopedTypeVariables       #-}
-- GHC 8.0.1 says the DahoopTask constraint isn't needed on runASlave
-- {-# OPTIONS_GHC -Wno-redundant-constraints #-}
module Dahoop.Slave where


import Control.Concurrent       (threadDelay)
import Control.Concurrent.Async hiding (async)
import Control.Concurrent.STM
import Control.Lens             ((^.))
import Control.Monad            (forever)
import Data.Time
import Control.Monad.Trans
import Data.ByteString          (ByteString)
import Data.Serialize           (runGet, encode, decode)
import Network.HostName
import System.ZMQ4.Monadic      (EventMsg (MonitorStopped), EventType (AllEvents), Pub (Pub), Push (Push), Receiver, Req (Req),
                                 Sender, Sub (Sub))
import Dahoop.ZMQ4.Trans        (ZMQT, ZMQ, Socket, async, monitor, receive, runZMQ, send, socket, subscribe, waitRead)

import Dahoop.Internal.Messages
import Dahoop.Event
import Dahoop.Utils
import Dahoop.ZMQ4

-- TODO
-- * Sending an abort message
-- * Map the TChan to be typed with the ann/finish up message
-- * Run many workers, maybe it'll just work!

-- |
-- A NOTE ABOUT CONCURRENCY
--
-- the 0mq `async` call does reference counting of the Context in use at the time, to make
-- sure the context isn't cleaned up until the threads are done.
-- any async tasks that are expected to run forever (in the context of a job) need to be
-- explicitly cancelled

runASlave :: forall j. DahoopTask j
          => j
          -> SlaveEventHandler (Id j)
          -> (forall m. (MonadIO m) => WorkDetails m (Preload j) (Input j) (Log j) -> m (Result j))
          -> Address Connect
          -> IO ()
runASlave x k workFunction s =
  forever $ runZMQ (do (v,queue) <- announcementsQueue s
                       ann <- waitForAnnouncement k queue
                       h   <- liftIO getHostName
                       let slaveid = SlaveId h
                       Right (preload :: Preload j) <- decode <$> requestPreload slaveid k (ann ^. preloadAddress)
                       worker <- async (do workIn  <- returning (socket Req)  (`connectM` (ann ^. askAddress))
                                           workOut <- returning (socket Push) (`connectM` (ann ^. resultsAddress))
                                           logOut  <- returning (socket Pub)  (`connectM` (ann ^. loggingAddress))
                                           workLoop x slaveid (ann ^. annJobCode) k workIn workOut logOut preload workFunction)
                       waiter <- async (liftIO . waitForDone queue $ ann ^. annJobCode)
                       liftIO $ do _ <- waitAnyCancel [worker,waiter,v]
                                   -- If we don't threadDelay here, STM exceptions happen when we loop
                                   -- around and wait for a new job to do

                                   -- I think it's because 0mq cleanup happens out of band somehow.
                                   threadDelay 500000
                       return ())

waitForAnnouncement :: (MonadIO m) => SlaveEventHandler i -> TChan ByteString -> ZMQT z m Announcement
waitForAnnouncement k queue =
  do liftIO $ k AwaitingAnnouncement
     ann <- atomicallyIO loop
     liftIO $ k (ReceivedAnnouncement ann)
     return ann
  where
        -- There's apparently a thing where subscriptions might see old messages, if you bind with a Sub, and connect
        -- with a Pub. Might be worth putting an age into the announce message, so that clients can disregard old things
        loop =
          do x <- readTChan queue
             let Right jv =
                   runGet getAnnouncementOrFinishUp x
             case jv of
               Right ann -> return ann
               Left _ -> loop

waitForDone :: (MonadIO m) => TChan ByteString -> JobCode -> m ()
waitForDone queue ourJc =
  do let loop =
           do Right j <- runGet getAnnouncementOrFinishUp <$> readTChan queue
              case j of
                Left jc -> if | jc == ourJc -> return ()
                              | otherwise   -> loop
                Right _ -> loop
     atomicallyIO loop

requestPreload :: (MonadIO m) => SlaveId -> SlaveEventHandler i -> Address Connect -> ZMQT z m ByteString
requestPreload slaveid k port =
  do s <- socket Req
     connectM s port
     liftIO $ k RequestingPreload
     send s [] (encode slaveid)
     receive s <*
       liftIO (k ReceivedPreload)

workLoop :: forall m j t t1 t2 z.
            (MonadIO m,
             DahoopTask j,
             Receiver t, Sender t1, Sender t, Sender t2)
            => j
            -> SlaveId
            -> JobCode
            -> SlaveEventHandler (Id j)
            -> Socket z t
            -> Socket z t1
            -> Socket z t2
            -> Preload j
            -> (forall n. (MonadIO n) => WorkDetails n (Preload j) (Input j) (Log j) -> n (Result j))
            -> ZMQT z m ()
workLoop _ slaveid jc k workIn workOut logOut preload f = loop (0 :: Int)
  where loop c =
          do send workIn [] $ encode (slaveid, jc)
             t1 <- liftIO getCurrentTime
             sendDahoopLog WaitingForWorkReply
             input <- waitRead workIn >> receive workIn
             let Right n = runGet getWorkOrTerminate input -- HAHA, parsing never fails
             case n of
               Left z -> sendDahoopLog $ FinishedJob c z
               Right (wid, payload) ->
                 do t2 <- liftIO getCurrentTime
                    sendDahoopLog (StartedUnit wid (realToFrac (diffUTCTime t2 t1)))
                    result <- f (WorkDetails preload payload sendUserLog)
                    send workOut [] . reply $ (slaveid, jc, wid, result)
                    sendDahoopLog (FinishedUnit wid)
                    loop (succ c)
        sendDahoopLog e = liftIO (k e) >> send logOut [] (encode (slaveid, DahoopEntry e :: SlaveLogEntry (Id j) (Log j)))
        sendUserLog   e = send logOut [] (encode (slaveid, UserEntry e :: SlaveLogEntry (Id j) (Log j)))

monitorUntilStopped :: Socket z t -> (Maybe EventMsg -> ZMQ z a) -> ZMQ z (Async (Maybe EventMsg))
monitorUntilStopped skt yield =
  do f <- monitor [AllEvents] skt
     async $
       let loop =
             do v <- liftIO (f True)
                case v of
                  Just (MonitorStopped _ _) ->
                    liftIO (f False) -- This terminates monitoring
                  e -> yield e >> loop
       in loop

-- |
-- >>> let twice z = z >> z
-- >>> twice $ runZMQ $ announcementsQueue 5 >>= \(v,_) ->  liftIO (cancel v >> print ())
-- ()
-- ()
announcementsQueue :: Address Connect -> ZMQ z (Async a, TChan ByteString)
announcementsQueue address =
  do queue <- liftIO newTChanIO
     v <- async $
          do subSocket <- returning (socket Sub) $
                          \s ->
                            do connectM s address
                               subscribe s "" -- Subscribe to all messages
             forever (do waitRead subSocket
                         v <- receive subSocket
                         atomicallyIO (writeTChan queue v))
     return (v,queue)
