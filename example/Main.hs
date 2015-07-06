{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -Wall #-}
module Main where

import Control.Monad.Reader
import Control.Concurrent     (threadDelay)
import System.Environment     (getArgs)
import System.Exit            (exitFailure)
import System.Random          (randomRIO)

import Dahoop
import qualified Dahoop.Master as M
import qualified Dahoop.Event  as E

import Dahoop.ZMQ4

main :: IO ()
main =
  do v <- getArgs
     let floats = [(1.0 :: Float)..3.0]
         work = map return floats
         preload = 1.1
         secret = "BOO!"
     let (master, slave, single) = dahoop masterHandler slaveHandler preload work workerThread
     flip runReaderT secret $ case v of
       ["master"] -> let config = M.DistConfig 4001 4000 4002 4003 (IP4' 127 0 0 1) someSlaves
                         someSlaves = map f [5000, 5001] where f = TCP (IP4' 127 0 0 1)
                     in master config
       ["slave",ss] -> slave (read ss)
       ["single"] -> single
       _ -> liftIO $
         do putStrLn "USAGE: $0 [slave PORT | master]"
            exitFailure

masterHandler :: E.MasterEventHandler (ReaderT String IO) () Float
masterHandler e = case e of
  E.Announcing ann         -> liftIO $ putStrLn $ "Announcing " ++ show ann
  E.Began n                -> liftIO $ putStrLn $ "Job #" ++ show n
  E.WaitingForWorkRequest  -> liftIO $ putStrLn "Waiting for slave"
  E.SentWork sid           -> liftIO $ putStrLn $ "Sent work to: " ++ show sid
  E.ReceivedResult _ r _   -> saveFunc r
  E.SentTerminate _        -> return ()
  E.Finished               -> liftIO $ putStrLn "Finished"
  E.SentPreload sid        -> liftIO $ putStrLn $ "Sent preload to: " ++ show sid
  E.RemoteEvent slaveid se -> liftIO $ print (slaveid, se)
  where saveFunc f = do
          s <- ask
          lift (putStrLn $ "The result was " ++ show (f::Float) ++ " and the secret is " ++ s)

slaveHandler :: E.SlaveEventHandler
slaveHandler e = liftIO $ case e of
  E.AwaitingAnnouncement -> putStrLn "Waiting for job"
  E.ReceivedAnnouncement a -> putStrLn $ "Received " ++ show a
  E.StartedUnit a -> putStrLn $ "Started unit " ++ show a
  E.FinishedUnit a -> putStrLn $ "Finished unit " ++ show a
  E.FinishedJob units job ->
    putStrLn ("Worked " ++ show units ++ " units of job " ++ show job)
  E.ReceivedPreload ->
    putStrLn "Preload arrived"
  E.RequestingPreload -> putStrLn "Requesting payload"
  E.WaitingForWorkReply -> putStrLn "Waiting for work reply"

workerThread :: forall m. (MonadIO m) => E.WorkDetails m Float Float () -> m Float
workerThread (E.WorkDetails preload a _) =
  do liftIO $ print a
     delay <- liftIO $ randomRIO (1,4)
     liftIO $ threadDelay (1000000 * delay)
     return (log a + preload)
