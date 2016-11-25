{-# LANGUAGE ConstraintKinds     #-}
{-# LANGUAGE TypeFamilies     #-}
{-# LANGUAGE AllowAmbiguousTypes     #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
{-# OPTIONS_GHC -Werror #-}
module Dahoop.Master (
  module Dahoop.Master
) where

import qualified Control.Concurrent.Chan.Unagi         as UC
import qualified Control.Concurrent.Chan.Unagi.Bounded as BC

import           Control.Concurrent                    (threadDelay)
import           Control.Concurrent.Async              (cancel, link)
import qualified Control.Concurrent.Async              as A (async, wait)
import qualified Control.Foldl                         as L
import           Control.Lens
import           Control.Monad.Catch
import           Control.Monad.State.Strict
import           Data.ByteString                       (ByteString)
import           Data.List.NonEmpty                    (NonEmpty ((:|)))
import           Data.Serialize                        (decode, encode, runGet)

import           Dahoop.Event
import qualified Dahoop.Internal.Messages              as M
import           Dahoop.Internal.WorkQueue
import           Dahoop.Utils
import           Dahoop.ZMQ4
import           Dahoop.ZMQ4.Trans                     (Socket, ZMQT, async,
                                                        liftZMQ, receive,
                                                        receiveMulti, runZMQT,
                                                        send, sendMulti, socket,
                                                        subscribe, waitRead)
import           System.ZMQ4.Monadic                   (Event (In), Poll (Sock),
                                                        Pub (..), Pull (..),
                                                        Receiver, Router (..),
                                                        Sender, Sub (..), poll)

-- TO DO
-- * A heartbeat?

-- MASTER

data DistConfig = DistConfig
    { _masterAddress :: String
    , _resultsPort :: Int
    , _askPort :: Int
    , _preloadPort :: Int
    , _loggingPort :: Int
    , _announcePort :: Int
    }

type Job i b = (i, IO b)

runAMaster :: forall j m z. (DahoopTask j, MonadIO m, MonadMask m)
           => j
           -> MasterEventHandler IO (Id j) (Log j)
           -> DistConfig
           -> Preload j
           -> NonEmpty (Job (Id j) (Input j))
           -> L.FoldM m (Id j, Result j) z
           -> m z
runAMaster proxy k config preloadData work (L.FoldM step first extract) =
        runZMQT $ do jobCode <- liftIO M.generateJobCode
                     eventQueue <- initEvents

                     -- setup
                     liftIO $ k (Began jobCode)
                     sock <- liftZMQ $ socket Pub
                     bindM sock $ TCP Wildcard (config&_announcePort)
                     announceThread <- liftZMQ $ async (announce sock (announcement config (DNS (_masterAddress config)) jobCode) eventQueue)
                     (liftIO . link) =<< liftZMQ (async (preload proxy (config & _preloadPort) preloadData eventQueue))

                     -- the work
                     initial <- lift first
                     result <- theProcess' proxy k (config & _askPort) jobCode (config & _resultsPort) (config & _loggingPort) work eventQueue (initial, step)

                     -- shutdown
                     liftIO (cancel announceThread)
                     broadcastFinished jobCode sock
                     liftIO $ k Finished

                     -- post shutdown extract
                     lift $ extract result -- this must be ZMQT m z

announcement :: DistConfig
                -> Connect
                -> M.JobCode
                -> M.Announcement
announcement v ourHostname jc =
  M.Announcement
      jc
      (tcpHere (v & _resultsPort))
      (tcpHere (v & _askPort))
      (tcpHere (v & _preloadPort))
      (tcpHere (v & _loggingPort))
  where tcpHere = TCP ourHostname

announce :: (MonadIO m) => Socket s Pub -> M.Announcement -> Events i l -> ZMQT s m ()
announce announceSocket ann eventQueue =
  do -- 'Pub' sockets don't queue messages, they broadcast only to things
     -- that have connected.
     -- Need to give some time for the slave connections to complete
     liftIO (threadDelay 500000)
     writeEvent eventQueue (Announcing ann)
     forever $
       do
          (send announceSocket [] . M.announcement) ann
          -- we wait so that we don't spam more than necessary
          liftIO $ threadDelay 500000

preload :: (DahoopTask t, MonadIO m) => t -> Int -> Preload t -> Events (Id t) (Log t)-> ZMQT s m ()
preload _ port preloadData eventQueue = do
  s <- returning (socket Router) (`bindM` TCP Wildcard port)
  let encoded = encode preloadData
  forever $ do
    (request, replyWith) <- replyarama s
    let Right slaveid = decode request :: Either String M.SlaveId
    writeEvent eventQueue (SentPreload slaveid)
    replyWith encoded

broadcastFinished :: (MonadIO m) => M.JobCode -> Socket z Pub -> ZMQT z m ()
broadcastFinished n announceSocket =
  do liftIO (threadDelay 500000)
     (send announceSocket [] . M.finishUp) n

-- NOTE: Send and receive must be done using different sockets, as they are used in different threads
theProcess' :: forall t m x z. (MonadIO m, DahoopTask t)
            => t
            -> MasterEventHandler IO (Id t) (Log t)
            -> Int
            -> M.JobCode
            -> Int
            -> Int
            -> NonEmpty (Job (Id t) (Input t))
            -> Events (Id t) (Log t)
            -> (x, x -> (Id t , Result t) -> m x)
            -> ZMQT z m x
theProcess' proxy k sendPort jc rport logPort work eventQueue foldbits = do
    workVar <- initOutgoing 1
    queue <- atomicallyIO $ buildWork work
    liftZMQ $
        do asyncLink (dealWork proxy sendPort jc workVar eventQueue queue)
           asyncLink (receiveLogs proxy logPort eventQueue)
           asyncLink $ lift $ slurpEvents eventQueue k
    waitForAllResults proxy rport jc queue eventQueue workVar foldbits

dealWork :: (DahoopTask t,MonadIO m)
         => t
         -> Int
         -> M.JobCode
         -> Outgoing (Id t)
         -> Events (Id t) (Log t)
         -> Work (Id t) (IO (Input t))
         -> ZMQT z m (Result t)
dealWork _ port n workVar eventQueue work =
  do sendSkt <- returning (socket Router)
                          (`bindM` TCP Wildcard port)
     let loop =
           do (request, replyWith) <- replyarama sendSkt
              let Right (slaveid, slaveJc) = decode request
              if slaveJc /= n then do
                replyWith (M.terminate slaveJc)
                loop
              else do let findAndDoNextNotYetDone = do
                            item <- nextOutgoing workVar
                            case item of
                              Just (wid, msg) -> do
                                alreadyDone <- atomicallyIO (itemIsComplete wid work)
                                if alreadyDone
                                  then findAndDoNextNotYetDone
                                  -- if we've just started repeating, we could return
                                  -- the item to the queue, tell the client to hold tight
                                  -- for a little while, sleep for a bit, then loop.
                                  -- this would give time for recently received results to
                                  -- get processed, and also give time for slightly slower slaves
                                  -- to get their results in
                                  else do writeEvent eventQueue (SentWork slaveid wid)
                                          replyWith msg
                                          loop
                              Nothing ->
                                replyWith (M.terminate n)
                      findAndDoNextNotYetDone
     loop

     forever $ do
       (request, replyWith) <- replyarama sendSkt
       let Right (slaveid, _) = decode request :: Either String (M.SlaveId, M.JobCode)
       writeEvent eventQueue (SentTerminate slaveid)
       replyWith (M.terminate n)

receiveLogs :: forall z m t. (MonadIO m, DahoopTask t) => t -> Int -> Events (Id t) (Log t) -> ZMQT z m ()
receiveLogs _ logPort eventQueue =
  do logSocket <- returning (socket Sub)
                            (`bindM` TCP Wildcard logPort)
     subscribe logSocket "" -- Subscribe to every incoming message
     _ <- forever $
       do result <- receive logSocket
          let Right (slaveid, logEntry) = decode result :: Either String (M.SlaveId, SlaveLogEntry (Id t) (Log t))
          writeEvent eventQueue (RemoteEvent slaveid logEntry)
     return ()

waitForAllResults :: (MonadIO m,DahoopTask t)
                  => t
                  -> Int
                  -> M.JobCode
                  -> Work (Id t) (IO (Input t))
                  -> Events (Id t) (Log t)
                  -> Outgoing (Id t)
                  -> (s,s -> (Id t,Result t) -> m s)
                  -> ZMQT z m s
waitForAllResults _ rp jc queue eventQueue workVar (first, step) =
  do receiveSocket <- returning (socket Pull) (`bindM` TCP Wildcard rp)
     let
       populateWork :: IO ()
       populateWork = do
                next  <- atomicallyIO $ start queue
                case next of
                  Nothing -> closeOutgoing workVar
                  Just (wid, _, action) -> do
                    -- I think this can be spawned into a thread.
                    -- Every time we receive a result, we spawn the next bit of enqueuing
                    -- Actually in theory we should probably be one step ahead? so we wait less?

                    -- Other thoughts
                    -- we need a pool of work available to be pulled
                    -- the size of the slots increases on each preload request
                    -- and then increases again when work is received
                    -- an available slot goes away when it is filled
                    workItem <- action
                    let encoded = M.work (wid, workItem)
                    writeOutgoing workVar wid $! encoded
                    populateWork

     workVarA <- liftIO $ A.async populateWork -- TODO, should possibly be linked to this thread?

     let loop =
           do result <- lift $ waitRead receiveSocket >> receive receiveSocket
              let Right (slaveid, slaveJc, wid, stuff) = runGet M.getReply result
                -- If a slave is sending work for the wrong job code,
                -- it will be killed when it asks for the next bit of work
              when (jc == slaveJc) $ do
                    (isNew, p) <- atomicallyIO $
                        (,) <$> complete wid queue <*> progress queue
                    writeEvent eventQueue (ReceivedResult slaveid wid p)
                    when isNew $ do
                        current <- get
                        v <- lift . lift $ step current (wid, stuff)
                        put v
              -- What if this happened on a different thread?
              completed <- atomicallyIO $ isComplete queue
              unless completed loop
     v <- execStateT loop first
     liftIO $ A.wait workVarA
     return $! v

-- | Utils

asyncLink :: ZMQT z IO a -> ZMQT z IO ()
asyncLink f = (liftIO . link) =<< async f

hasReceivedMessage :: MonadIO m => Socket z t -> m Bool
hasReceivedMessage s =
           do let recievedPoll = Sock s [In] Nothing
              events <- poll 0 [recievedPoll]
              return $! elem In $ head events

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
             ["",msg])

type Elem i = Maybe (i, ByteString)
data Outgoing i = Outgoing (BC.InChan (Elem i)) (BC.OutChan (Elem i))

nextOutgoing :: MonadIO m => Outgoing i -> m (Maybe (i, ByteString))
nextOutgoing (Outgoing _ outs) = liftIO $ BC.readChan outs

closeOutgoing :: MonadIO m => Outgoing i -> m ()
closeOutgoing (Outgoing ins _) = liftIO $ BC.writeChan ins Nothing
                                    -- todo, also drain the chan

writeOutgoing :: MonadIO m => Outgoing t -> t -> ByteString -> m ()
writeOutgoing (Outgoing ins _) wid item = liftIO $ BC.writeChan ins $ Just (wid, item) -- should workItem be an async around action here?

initOutgoing :: MonadIO m => Int -> m (Outgoing i)
initOutgoing n = liftIO $ uncurry Outgoing <$> BC.newChan n

data Events i l = Events (UC.InChan (MasterEvent i l)) (UC.OutChan (MasterEvent i l))

-- writeEvent (Events ins _) k = writeChan ins k

initEvents :: MonadIO m => m (Events i l)
initEvents = liftIO $ uncurry Events <$> UC.newChan

writeEvent :: MonadIO m => Events i l -> MasterEvent i l -> m ()
writeEvent (Events ins _) = liftIO . UC.writeChan ins

slurpEvents :: Events i l -> (MasterEvent i l -> IO a) -> IO ()
slurpEvents (Events _ outs) f = forever $ do
  v <- UC.readChan outs
  _ <- f v
  return ()

