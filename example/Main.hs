{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -Wall #-}
module Main (
  main
  ) where

import Control.Monad.Reader
import Control.Concurrent     (threadDelay)
import Control.Concurrent.Async
import Data.List.NonEmpty     hiding (zip)
import System.Environment     (getArgs)
import System.Exit            (exitFailure)
import System.Random

import qualified Control.Foldl as L
import qualified Dahoop as D

data ExampleJob = ExampleJob

instance D.DahoopTask ExampleJob where
  type Preload ExampleJob = Float
  type Input ExampleJob = Float
  type Result ExampleJob = Float
  type Log ExampleJob = ()
  type Id ExampleJob = Int

main :: IO ()
main =
  do v <- getArgs
     let preload = 1.1 :: Float
         secret = "BOO!"
     flip runReaderT secret $ case v of
       ["master", numWorkUnits, port] ->
         let config = D.DistConfig "192.168.113.248" 4001 4000 4002 4003 (read port)
          in case makeWorkUnits (read numWorkUnits) of
             Nothing -> liftIO $ putStrLn "Must have at least one work unit" >> exitFailure
             Just ws -> D.runAMaster ExampleJob masterHandler config preload (fmap (fmap liftIO) ws) resultFold
       ["slave", numSlaves, host, port] -> liftIO $ do
          as <- replicateM (read numSlaves) $ async $ D.runASlave ExampleJob slaveHandler workerThread (D.TCP (D.DNS host) (read port))
          mapM_ wait as
       _ -> liftIO $
         do putStrLn "USAGE: $0 [slave NUM_WORKERS MASTER_PORT | master NUM_WORK_UNITS PORT]"
            exitFailure

makeWorkUnits :: Int -> Maybe (NonEmpty (Int, IO Float))
makeWorkUnits n = nonEmpty . zip [1..] $ replicate n (threadDelay 500000 >> getStdRandom random)

resultFold :: L.FoldM (ReaderT String IO) (Int, Float) ()
resultFold = L.FoldM saveFunc (return ()) (const (return ()))
  where saveFunc _ (_ix, f) = do
          s <- ask
          lift (putStrLn $ "The result was " ++ show (f::Float) ++ " and the secret is " ++ s)

masterHandler :: D.MasterEventHandler IO Int ()
masterHandler e = case e of
  D.Announcing ann         -> putStrLn $ "Announcing " ++ show ann
  D.Began n                -> putStrLn $ "Job #" ++ show n
  D.WaitingForWorkRequest  -> putStrLn "Waiting for slave"
  D.SentWork sid i         -> putStrLn $ "Sent work to: " ++ show sid ++ ", " ++ show i
  D.ReceivedResult _ i _   -> putStrLn $ "Received " ++ show i
  D.SentTerminate _        -> return ()
  D.Finished               -> putStrLn "Finished"
  D.SentPreload sid        -> putStrLn $ "Sent preload to: " ++ show sid
  D.RemoteEvent slaveid se -> print (slaveid, se)

slaveHandler :: D.SlaveEventHandler Int
slaveHandler = print

workerThread :: forall m. (MonadIO m) => D.WorkDetails m Float Float () -> m Float
workerThread (D.WorkDetails preload a _) =
  do liftIO $ print a
     liftIO $ randomDelay 1 4
     return (log a + preload)

randomDelay :: Int -> Int -> IO ()
randomDelay n m = do
     delay <- randomRIO (n,m)
     threadDelay (1000000 * delay)
