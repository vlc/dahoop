{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -Wall #-}
module Main where

import Control.Monad.Reader
import Control.Concurrent     (threadDelay)
import Control.Monad.IO.Class (MonadIO, liftIO)
import System.Environment     (getArgs)
import System.Exit            (exitFailure)
import System.Random          (randomRIO)

import qualified Dahoop.Master as M
import qualified Dahoop.Slave  as S
import qualified Dahoop.Event  as E

import Dahoop.ZMQ4

main :: IO ()
main =
  do v <- getArgs
     case v of
       ["master"] -> master
       ["slave",ss] -> slave (read ss)
       _ ->
         do putStrLn "USAGE: $0 [slave PORT | master]"
            exitFailure

master :: IO ()
master =
    let config = M.DistConfig 4001 4000 4002 4003 (IP4' 127 0 0 1) someSlaves
        someSlaves = map f [5000, 5001] where f = TCP (IP4' 127 0 0 1)
        secret = "BOO!"
        floats = [(1.0 :: Float)..3.0]
        work = map return floats
        preloadData = 1.1 :: Float
        saveFunc f = do
                       s <- ask
                       lift (putStrLn $ "The result was " ++ show (f::Float) ++ " and the secret is " ++ s)
      in runReaderT (M.runAMaster k config preloadData work saveFunc) secret
  where k :: M.EventHandler (ReaderT String IO) String
        k e = liftIO $ case e of
                        E.Announcing ann -> putStrLn $ "Announcing " ++ show ann
                        E.Began n -> putStrLn $ "Job #" ++ show n
                        E.WaitingForWorkRequest -> putStrLn "Waiting for slave"
                        E.SentWork a -> putStrLn $ "Sent work " ++ show a
                        E.ReceivedResult a -> putStrLn $ "Received result " ++ show a
                        E.SentTerminate -> return ()
                        E.Finished -> putStrLn "Finished"
                        E.SentPreload -> putStrLn "Sent preload"
                        E.RemoteEvent slaveid e -> print (slaveid, e)

-- SLAVE
slave :: Int -> IO ()
slave = S.runASlave k workerThread
  where workerThread :: forall m. (MonadIO m) => S.WorkDetails m Float Float () -> m Float
        workerThread (S.WorkDetails preload a _) =
          do liftIO $ print a
             delay <- liftIO $ randomRIO (1,4)
             liftIO $ threadDelay (1000000 * delay)
             return (log a + preload)
        k :: S.EventHandler IO
        k e = liftIO $ case e of
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
