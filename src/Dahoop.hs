{-# LANGUAGE RankNTypes                #-}
{-# LANGUAGE ScopedTypeVariables                #-}
module Dahoop (
  dahoop,
  Dahoop.Master.runAMaster,
  Dahoop.Slave.runASlave,
  Dahoop.Single.runASingle,
  module Dahoop.Event,
  DahoopTask(..),
  Dahoop.Master.DistConfig(..),
  Dahoop.ZMQ4.Address(..),
  Dahoop.ZMQ4.Connect(..),
  Dahoop.ZMQ4.Bind(..),
) where

import Control.Monad.IO.Class
import Control.Monad.Catch
import qualified Control.Foldl as L
import Data.List.NonEmpty

import Dahoop.Event
import Dahoop.Slave
import Dahoop.Master
import Dahoop.Single
import Dahoop.ZMQ4
import Dahoop.Utils

dahoop :: (DahoopTask j,MonadIO m,MonadMask m)
       => j
       -> MasterEventHandler IO (Id j) (Log j)
       -> SlaveEventHandler (Id j)
       -> Preload j
       -> NonEmpty (Job (Id j) (Input j))
       -> (forall n. (MonadIO n) => WorkDetails n (Preload j) (Input j) (Log j) -> n (Result j))
       -> L.FoldM m (Id j,Result j) z
       -> (DistConfig -> m z,Address Connect -> m (),m z)
dahoop x mk sk pre workBuilders workFunction fold =
  (\distConfig -> runAMaster x mk distConfig pre workBuilders fold,
   \ma -> liftIO $ runASlave x sk workFunction ma,
   runASingle x mk sk pre workBuilders workFunction fold)
