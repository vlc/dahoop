{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Main where

import           Control.Concurrent     (threadDelay)
import           Control.Monad.IO.Class (liftIO)
import           Data.ByteString        (ByteString)
import qualified Data.ByteString        as BS (reverse)
import           Data.Monoid            ((<>))
import           System.Environment     (getArgs)
import           System.Exit            (exitFailure)
import           System.Random          (randomRIO)


import qualified Dahoop.Master as M
import qualified Dahoop.Slave  as S

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
    let config = M.DistConfig 4001 4000 4002 (IP4' 127 0 0 1) someSlaves
        someSlaves = map f [5000, 5001] where f = TCP (IP4' 127 0 0 1)
        messages :: [ByteString]
        messages = ["one", "two", "three"]
        preloadData = "abc"
     in M.runAMaster k config preloadData messages
  where k :: M.EventHandler
        k = liftIO . \case
                        M.Announcing ann -> putStrLn $ "Announcing " ++ show ann
                        M.Began n -> putStrLn $ "Job #" ++ show n
                        M.WaitingForWorkRequest -> putStrLn "Waiting for slave"
                        M.SentWork a -> putStrLn $ "Sent work " ++ show a
                        M.ReceivedResult a -> putStrLn $ "Received result " ++ show a
                        M.SentTerminate -> return ()
                        M.Finished -> putStrLn "Finished"
                        M.SentPreload -> putStrLn "Sent preload"

-- SLAVE
slave :: Int -> IO ()
slave = S.runASlave k workerThread
  where workerThread preload a =
          do print a
             threadDelay . (1000000 *) =<< randomRIO (1,4)
             return (BS.reverse a <> preload)
        k :: S.EventHandler
        k =
          liftIO .
          \case
            S.AwaitingAnnouncement -> putStrLn "Waiting for job"
            S.ReceivedAnnouncement a -> putStrLn $ "Received " ++ show a
            S.StartedUnit a -> putStrLn $ "Started unit " ++ show a
            S.FinishedUnit a -> putStrLn $ "Finished unit " ++ show a
            S.FinishedJob units job ->
              putStrLn ("Worked " ++ show units ++ " units of job " ++ show job)
            S.ReceivedPreload ->
              putStrLn "Preload arrived"
