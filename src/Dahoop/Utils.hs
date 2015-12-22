{-# LANGUAGE TypeFamilies     #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE AllowAmbiguousTypes     #-}
module Dahoop.Utils where

import Control.Concurrent.STM
import Control.Monad.IO.Class
import Data.Serialize hiding (Result)

returning :: Monad m => m b -> (b -> m a) -> m b
returning f z =
  do v <- f
     _ <- z v
     return v

atomicallyIO :: MonadIO m => STM a -> m a
atomicallyIO = liftIO . atomically

class (Ord (Id j), Serialize (Input j), Serialize (Result j), Serialize (Log j), Serialize (Id j), Serialize (Preload j)) => DahoopTask j where
  type Preload j
  type Input j
  type Result j
  type Log j
  type Id j
