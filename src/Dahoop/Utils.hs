module Dahoop.Utils where

import Control.Concurrent.STM
import Control.Monad.IO.Class

returning :: Monad m => m b -> (b -> m a) -> m b
returning f z =
  do v <- f
     _ <- z v
     return v

atomicallyIO :: MonadIO m => STM a -> m a
atomicallyIO = liftIO . atomically
