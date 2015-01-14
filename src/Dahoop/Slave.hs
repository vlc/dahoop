{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE MultiWayIf          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Dahoop.Slave where

import Control.Applicative      ((<$>), (<*))
import Control.Concurrent       (threadDelay)
import Control.Concurrent.Async hiding (async)
import Control.Concurrent.STM
import Control.Lens             ((^.))
import Control.Monad            (forever)
import Control.Monad.IO.Class   (MonadIO, liftIO)
import Data.ByteString          (ByteString)
import Data.Serialize           (Serialize, runGet)
import System.ZMQ4.Monadic      (EventMsg (MonitorStopped), EventType (AllEvents), Push (Push), Receiver, Req (Req),
                                 Sender, Socket, Sub (Sub), ZMQ, async, monitor, receive, runZMQ, send, socket,
                                 subscribe, waitRead)

import Dahoop.Internal.Messages
import Dahoop.Utils
import Dahoop.ZMQ4

-- TODO
-- * Sending an abort message
-- * Map the TChan to be typed with the ann/finish up message
-- * Run many workers, maybe it'll just work!

data Events
  = AwaitingAnnouncement
  | ReceivedAnnouncement Announcement
  | ReceivedPreload
  | StartedUnit WorkId
  | FinishedUnit WorkId
  | FinishedJob Int
                JobCode

-- |
-- A NOTE ABOUT CONCURRENCY
--
-- the 0mq `async` call does reference counting of the Context in use at the time, to make
-- sure the context isn't cleaned up until the threads are done.
-- any async tasks that are expected to run forever (in the context of a job) need to be
-- explicitly cancelled

zCancel :: Async a -> IO ()
zCancel = cancel

zThreadDelay :: Int -> IO ()
zThreadDelay = threadDelay

type EventHandler = forall s. Events -> ZMQ s ()

runASlave :: (Serialize a,Serialize b)
          => EventHandler -> (ByteString -> a -> IO b) -> Int -> IO ()
runASlave k workerThread s =
  forever $
  runZMQ $
  do (v,queue) <- announcementsQueue s
     ann <- waitForAnnouncement k queue
     preload <- requestPreload k
                               (ann ^. preloadAddress)
     worker <- async (do workIn <- returning (socket Req) (`connectM` (ann ^. askAddress))
                         workOut <- returning (socket Push) (`connectM` (ann ^. resultsAddress))
                         workLoop k workIn workOut (workerThread preload))
     waiter <- async (liftIO . waitForDone queue $ ann ^. annJobCode)
     liftIO $
       do _ <- waitAnyCancel [worker,waiter,v]
          -- If we don't threadDelay here, STM exceptions happen when we loop
          -- around and wait for a new job to do
          -- I think it's because 0mq cleanup happens out of band somehow.
          threadDelay 500000
     return ()

waitForAnnouncement :: EventHandler -> TChan ByteString -> ZMQ z Announcement
waitForAnnouncement k queue =
  do k AwaitingAnnouncement
     ann <- atomicallyIO loop
     k $
       ReceivedAnnouncement ann
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

waitForDone :: (MonadIO m, Functor m) => TChan ByteString -> JobCode -> m ()
waitForDone queue ourJc =
  do let loop =
           do Right j <- runGet getAnnouncementOrFinishUp <$> readTChan queue
              case j of
                Left jc -> if | jc == ourJc -> return ()
                              | otherwise   -> loop
                Right _ -> loop
     atomicallyIO loop
     liftIO (putStrLn "Got Done")

requestPreload :: EventHandler -> Address Connect -> ZMQ z ByteString
requestPreload k port =
  do s <- socket Req
     connectM s port
     send s [] ""
     receive s <*
       k ReceivedPreload

workLoop :: (Serialize s,Serialize a,Receiver t,Sender t1,Sender t)
         => EventHandler
         -> Socket z t
         -> Socket z t1
         -> (a -> IO s)
         -> ZMQ z ()
workLoop k workIn workOut f = loop (0 :: Int)
  where loop c =
          do send workIn [] ""
             liftIO $ putStrLn "Waiting for reply" -- TODO Events
             input <- waitRead workIn >> receive workIn
             let Right n = runGet getWorkOrTerminate input -- HAHA, parsing never fails
             case n of
               Left z -> k $ FinishedJob c z
               Right (wid, a) ->
                 do k $ StartedUnit wid
                    result <- liftIO (f a)
                    send workOut [] . reply $ (wid, result)
                    k $ FinishedUnit wid
                    loop (succ c)

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
announcementsQueue :: Int -> ZMQ z (Async a, TChan ByteString)
announcementsQueue port =
  do queue <- liftIO newTChanIO
     v <- async $
          do subSocket <- returning (socket Sub) $
                          \s ->
                            do bindM s (TCP Wildcard port)
                               subscribe s "" -- Subscribe to all messages
             forever (do waitRead subSocket
                         v <- receive subSocket
                         atomicallyIO (writeTChan queue v))
     return (v,queue)

print' :: (MonadIO m, Show a) => a -> m ()
print' = liftIO . print
