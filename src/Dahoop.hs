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
import Data.Serialize

import Dahoop.Event
import Dahoop.Slave
import Dahoop.Master
import Dahoop.Single
import Dahoop.ZMQ4

dahoop :: (Serialize a, Serialize b, Serialize c, Serialize r, MonadIO m, MonadMask m)
          => MasterEventHandler m c r
          -> SlaveEventHandler
          -> a
          -> [m b]
          -> (forall n. (MonadIO n) => WorkDetails n a b c -> n r)
          -> (DistConfig -> m (),
              Int -> m (),
              m ())
dahoop mk sk pre workBuilders workFunction =
  (\distConfig -> runAMaster mk distConfig pre workBuilders,
   \port -> liftIO $ runASlave sk workFunction port,
   runASingle mk sk pre workBuilders workFunction)