{-# LANGUAGE ConstraintKinds     #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
{-# OPTIONS_GHC -Werror #-}
module Dahoop.Master (
  module Dahoop.Master
) where

import qualified Control.Concurrent.Chan.Unagi.Bounded as BC
import qualified Control.Concurrent.Chan.Unagi as UC

import           Control.Concurrent              (threadDelay)
import           Control.Concurrent.Async        (cancel, link)
import qualified Control.Concurrent.Async        as A (async, wait)
import qualified Control.Foldl                   as L
import           Control.Lens
import           Control.Monad.Catch
import           Control.Monad.State.Strict
import           Data.ByteString                 (ByteString)
import           Data.List.NonEmpty              (NonEmpty ((:|)))
import           Data.Serialize                  (Serialize, decode, encode,
                                                  runGet)

import           Dahoop.Event
import qualified Dahoop.Internal.Messages        as M
import           Dahoop.Internal.WorkQueue
import           Dahoop.Utils
import           Dahoop.ZMQ4
import           Dahoop.ZMQ4.Trans               (Socket, ZMQT, async, liftZMQ,
                                                  receive, receiveMulti,
                                                  runZMQT, send, sendMulti,
                                                  socket, subscribe, waitRead)
import           System.ZMQ4.Monadic             (Event (In), Poll (Sock),
                                                  Pub (..), Pull (..), Receiver,
                                                  Router (..), Sender, Sub (..),
                                                  poll)

-- TO DO
-- * A heartbeat?
-- * UUID for job codes

-- MASTER

data DistConfig = DistConfig
    { _masterAddress :: String
    , _resultsPort :: Int
    , _askPort :: Int
    , _preloadPort :: Int
    , _loggingPort :: Int
    , _announcePort :: Int
    }

masterAddress :: Simple Lens DistConfig String
masterAddress = lens _masterAddress (\v s -> v { _masterAddress = s})

resultsPort :: Simple Lens DistConfig Int
resultsPort = lens _resultsPort (\v s -> v { _resultsPort = s})

askPort :: Simple Lens DistConfig Int
askPort = lens _askPort (\v s -> v { _askPort = s})

preloadPort :: Simple Lens DistConfig Int
preloadPort = lens _preloadPort (\v s -> v { _preloadPort = s})

loggingPort :: Simple Lens DistConfig Int
loggingPort = lens _loggingPort (\v s -> v { _loggingPort = s})

announcePort :: Simple Lens DistConfig Int
announcePort = lens _announcePort (\v s -> v { _announcePort = s})

runAMaster :: (Serialize a, Serialize b, Serialize r, Serialize l, Ord i, Serialize i, MonadIO m, MonadMask m)
           => MasterEventHandler IO i l
           -> DistConfig
           -> a
           -> [(i, IO b)]
           -> L.FoldM m r z
           -> m z
runAMaster k config preloadData work (L.FoldM step first extract) =
        runZMQT $ do jobCode <- liftIO M.generateJobCode
                     eventQueue <- initEvents
                     sock <- liftZMQ $ socket Pub
                     bindM sock $ TCP Wildcard (config^.announcePort)
                     announceThread <- liftZMQ $ async (announce sock (announcement config (DNS (config ^. masterAddress)) jobCode) eventQueue)
                     -- liftIO . link $ announceThread
                     (liftIO . link) =<< liftZMQ (async (preload (config ^. preloadPort) preloadData eventQueue))
                     liftIO $ k (Began jobCode)
                     initial <- lift first
                     result <- theProcess' k (config ^. askPort) jobCode (config ^. resultsPort) (config ^. loggingPort) work eventQueue (initial, step)
                     liftIO (cancel announceThread)
                     broadcastFinished jobCode sock
                     liftIO $ k Finished
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

announce :: (MonadIO m) => (Socket s Pub) -> M.Announcement -> Events i l -> ZMQT s m ()
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

preload :: (MonadIO m, Serialize a) => Int -> a -> Events i l -> ZMQT s m ()
preload port preloadData eventQueue = do
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
theProcess' :: forall i m a l z x r.
               (Ord i, MonadIO m, Serialize a, Serialize l, Serialize r, Serialize i)
            => MasterEventHandler IO i l
            -> Int
            -> M.JobCode
            -> Int
            -> Int
            -> [(i, IO a)]
            -> Events i l
            -> (x, x -> r -> m x)
            -> ZMQT z m x
theProcess' k sendPort jc rport logPort work eventQueue foldbits = do
    workVar <- initOutgoing 64
    liftZMQ $
        do asyncLink (dealWork sendPort jc workVar eventQueue)
           asyncLink (receiveLogs logPort eventQueue)
           asyncLink $ lift $ slurpEvents eventQueue k
    waitForAllResults rport jc work eventQueue workVar foldbits

dealWork :: (MonadIO m, Serialize i) => Int -> M.JobCode -> Outgoing i -> Events i l -> ZMQT s m ()
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
                -- I don't think the workVar is really needed, the worqueue could just be taken from directly
                item <- nextOutgoing workVar
                case item of
                  Just (wid, msg) -> do
                    -- if we've just started repeating, we could return
                    -- the item to the queue, tell the client to hold tight
                    -- for a little while, sleep for a bit, then loop.
                    -- this would give time for recently received results to
                    -- get processed, and also give time for slightly slower slaves
                    -- to get their results in
                    writeEvent eventQueue (SentWork slaveid wid)
                    replyWith msg
                    loop
                  Nothing ->
                    replyWith (M.terminate n)
     loop

     forever $ do
       (request, replyWith) <- replyarama sendSkt
       let Right (slaveid, _) = decode request :: Either String (M.SlaveId, M.JobCode)
       writeEvent eventQueue (SentTerminate slaveid)
       replyWith (M.terminate n)

receiveLogs :: forall z m i l. (MonadIO m, Serialize i, Serialize l) => Int -> Events i l -> ZMQT z m ()
receiveLogs logPort eventQueue =
  do logSocket <- returning (socket Sub)
                            (`bindM` TCP Wildcard logPort)
     subscribe logSocket "" -- Subscribe to every incoming message
     _ <- forever $
       do result <- receive logSocket
          let Right (slaveid, logEntry) = decode result :: Either String (M.SlaveId, SlaveLogEntry i l)
          writeEvent eventQueue (RemoteEvent slaveid logEntry)
     return ()

waitForAllResults :: (MonadIO m, Serialize r, Serialize i, Ord i, Serialize a)
                  => Int
                  -> M.JobCode
                  -> [(i, IO a)]
                  -> Events i l
                  -> Outgoing i
                  -> (x, x -> r -> m x)
                  -> ZMQT z m x
waitForAllResults rp jc work eventQueue workVar (first, step) =
  do receiveSocket <- returning (socket Pull) (`bindM` TCP Wildcard rp)
     queue <- atomicallyIO $ buildWork work
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
                    (isNew, p) <- atomicallyIO $ do
                        (,) <$> complete wid queue <*> progress queue
                    writeEvent eventQueue (ReceivedResult slaveid wid p)
                    when isNew $ do
                        current <- get
                        v <- lift . lift $ step current stuff
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
             ["", msg])


---

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
  threadDelay 200000

-- type Outgoing i = TBMQueue (i, ByteString)

-- nextOutgoing :: MonadIO m => Outgoing i -> m (Maybe (i, ByteString))
-- nextOutgoing q = atomicallyIO $ readTBMQueue q

-- closeOutgoing :: MonadIO m => Outgoing i -> m ()
-- closeOutgoing q = atomicallyIO $
--   do closeTBMQueue q
--      drainQueue q

-- writeOutgoing :: MonadIO m => Outgoing t -> t -> ByteString -> m ()
-- writeOutgoing q wid item = atomicallyIO $ writeTBMQueue q (wid, item) -- should workItem be an async around action here?

-- initOutgoing n = atomicallyIO (newTBMQueue n)

-- type Events i l = TQueue (MasterEvent i l)

-- initEvents :: MonadIO m => m (Events i l)
-- initEvents = atomicallyIO newTQueue

-- writeEvent :: MonadIO m => Events i l -> MasterEvent i l -> m ()
-- writeEvent q k = atomicallyIO $ writeTQueue q k

-- slurpEvents :: Events i l -> (MasterEvent i l -> IO a) -> IO ()
-- slurpEvents q f =
--   do forever $ slurpTQueue q f >> threadDelay 200000


-- slurpTQueue :: MonadIO m => TQueue a -> (a -> m x) -> m ()
-- slurpTQueue q f = do
--   events <- atomicallyIO $ readAllFromQueue q
--   mapM_ f events

-- readAllFromQueue :: TQueue a -> STM [a]
-- readAllFromQueue q = go []
--   where go xs = do
--           a <- tryReadTQueue q
--           case a of
--             Just v -> go (v:xs)
--             Nothing -> return xs

-- drainQueue :: TBMQueue a -> STM ()
-- drainQueue q = go
--   where go = do
--           x <- readTBMQueue q
--           case x of
--             Nothing -> return ()
--             Just _ -> go
