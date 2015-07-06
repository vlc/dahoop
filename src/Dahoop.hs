{-# LANGUAGE RankNTypes                #-}
module Dahoop where

import Control.Monad.IO.Class
import Control.Monad.Catch
import Data.Serialize

import Dahoop.Event
import Dahoop.Slave
import Dahoop.Master
import Dahoop.Single

dahoop :: (Serialize a, Serialize b, Serialize c, Serialize r, MonadIO m, MonadMask m)
          => MasterEventHandler m c r
          -> SlaveEventHandler
          -> a
          -> [m b]
          -> (forall m. (MonadIO m) => WorkDetails m a b c -> m r)
          -> (DistConfig -> m (),
              Int -> m (),
              m ())
dahoop mk sk preload workBuilders workFunction =
  (\distConfig -> runAMaster mk distConfig preload workBuilders,
   \port -> liftIO $ runASlave sk workFunction port,
   runASingle mk sk preload workBuilders workFunction)
