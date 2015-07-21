{-# LANGUAGE RankNTypes                #-}
module Dahoop (
  dahoop,
  Dahoop.Master.runAMaster,
  Dahoop.Slave.runASlave,
  Dahoop.Single.runASingle,
  module Dahoop.Event,
  Dahoop.Master.DistConfig(..),
  Dahoop.ZMQ4.Address(..),
  Dahoop.ZMQ4.Connect(..),
  Dahoop.ZMQ4.Bind(..),
) where

import Control.Monad.IO.Class
import Control.Monad.Catch
import qualified Control.Foldl as L
import Data.Serialize

import Dahoop.Event
import Dahoop.Slave
import Dahoop.Master
import Dahoop.Single
import Dahoop.ZMQ4

dahoop :: (Serialize a, Serialize b, Serialize c, Serialize r, MonadIO m, MonadMask m)
          => MasterEventHandler m c
          -> SlaveEventHandler
          -> a
          -> [m b]
          -> (forall m. (MonadIO m) => WorkDetails m a b c -> m r)
          -> L.FoldM m r z
          -> (DistConfig -> m z,
              Int -> m (),
              m z)
dahoop mk sk preload workBuilders workFunction fold =
  (\distConfig -> runAMaster mk distConfig preload workBuilders fold,
   \port -> liftIO $ runASlave sk workFunction port,
   runASingle mk sk preload workBuilders workFunction fold)
