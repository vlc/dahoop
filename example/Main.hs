{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -Wall #-}
module Main where

import Control.Monad.Reader
import Control.Concurrent     (threadDelay)
import Control.Concurrent.Async
import System.Environment     (getArgs)
import System.Exit            (exitFailure)
import System.Random

import qualified Control.Foldl as L
import qualified Dahoop as D

main :: IO ()
main =
  do v <- getArgs
     let preload = 1.1 :: Float
         secret = "BOO!"
     flip runReaderT secret $ case v of
       ["master", numWorkUnits, port] -> let config = D.DistConfig 4001 4000 4002 4003 (read port)
                          in D.runAMaster masterHandler config preload (fmap (fmap liftIO) $ makeWorkUnits $ read numWorkUnits) resultFold
       ["slave", numSlaves, port] -> liftIO $ do
          as <- replicateM (read numSlaves) $ async $ D.runASlave slaveHandler workerThread (D.TCP (D.IP4' 127 0 0 1) (read port))
          mapM_ wait as
       _ -> liftIO $
         do putStrLn "USAGE: $0 [slave NUM_WORKERS MASTER_PORT | master NUM_WORK_UNITS PORT]"
            exitFailure

makeWorkUnits :: Int -> [(Int, IO Float)]
makeWorkUnits n = zip [1..] $ replicate n (getStdRandom random)

resultFold :: L.FoldM (ReaderT String IO) Float ()
resultFold = L.FoldM saveFunc (return ()) (const (return ()))
  where saveFunc _ f = do
          s <- ask
          lift (putStrLn $ "The result was " ++ show (f::Float) ++ " and the secret is " ++ s)

masterHandler :: D.MasterEventHandler (ReaderT String IO) Int ()
masterHandler e = case e of
  D.Announcing ann         -> liftIO $ putStrLn $ "Announcing " ++ show ann
  D.Began n                -> liftIO $ putStrLn $ "Job #" ++ show n
  D.WaitingForWorkRequest  -> liftIO $ putStrLn "Waiting for slave"
  D.SentWork sid i         -> liftIO $ putStrLn $ "Sent work to: " ++ show sid ++ ", " ++ show i
  D.ReceivedResult _ i _   -> liftIO $ putStrLn $ "Received " ++ show i
  D.SentTerminate _        -> return ()
  D.Finished               -> liftIO $ putStrLn "Finished"
  D.SentPreload sid        -> liftIO $ putStrLn $ "Sent preload to: " ++ show sid
  D.RemoteEvent slaveid se -> liftIO $ print (slaveid, se)


slaveHandler :: D.SlaveEventHandler Int
slaveHandler e = case e of
  D.AwaitingAnnouncement -> putStrLn "Waiting for job"
  D.ReceivedAnnouncement a -> putStrLn $ "Received " ++ show a
  D.StartedUnit a -> putStrLn $ "Started unit " ++ show a
  D.FinishedUnit a -> putStrLn $ "Finished unit " ++ show a
  D.FinishedJob units job ->
    putStrLn ("Worked " ++ show units ++ " units of job " ++ show job)
  D.ReceivedPreload ->
    putStrLn "Preload arrived"
  D.RequestingPreload -> putStrLn "Requesting payload"
  D.WaitingForWorkReply -> putStrLn "Waiting for work reply"

workerThread :: forall m. (MonadIO m) => D.WorkDetails m Float Float () -> m Float
workerThread (D.WorkDetails preload a _) =
  do liftIO $ print a
     delay <- liftIO $ randomRIO (1,4)
     liftIO $ threadDelay (1000000 * delay)
     return (log a + preload)
