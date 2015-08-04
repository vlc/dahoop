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

dahoop :: (Serialize a, Serialize b, Serialize c, Serialize r, MonadIO m, MonadMask m, Serialize i, Ord i)
          => MasterEventHandler m i c
          -> SlaveEventHandler i
          -> a
          -> [(i, IO b)]
          -> (forall n. (MonadIO n) => WorkDetails n a b c -> n r)
          -> L.FoldM m r z
          -> (DistConfig -> m z,
              Address Connect -> m (),
              m z)
dahoop mk sk pre workBuilders workFunction fold =
  (\distConfig -> runAMaster mk distConfig pre workBuilders fold,
   \ma -> liftIO $ runASlave sk workFunction ma,
   runASingle mk sk pre workBuilders workFunction fold)
