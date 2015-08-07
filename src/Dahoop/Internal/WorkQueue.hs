{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE TupleSections              #-}
module Dahoop.Internal.WorkQueue where

import           Control.Applicative
import Prelude hiding (all)
import           Control.Concurrent.STM
import Control.Monad.Trans.State
import           Control.Lens
import           Data.Foldable
import qualified Data.Map               as M
import           Data.Maybe             (fromMaybe)

-- $setup
-- >>> import Test.QuickCheck

-- | Out work queue
data Work i a =
  Work { _todo :: TQueue (i, History, a)
       , _done :: TVar (M.Map i Int)
       , _size :: Int }
-- invariants: in a fresh queue, all keys in the map are in the queue

-- The state that a work item transition
newtype History = Repeats Int deriving (Eq, Show, Num, Enum)


type M i a m = StateT (Work i a) m

buildWork :: Ord i => [(i, a)] -> STM (Work i a)
buildWork ws = Work <$> initialQueue <*> newTVar initMap <*> pure (length ws)
  where initialQueue =
          do q <- newTQueue
             _ <- traverse (\(wid, a) -> writeTQueue q (wid,Repeats 0,a)) idWork
             return q
        initMap = M.fromList . toList . fmap (\(wid, _) -> (wid, 0)) $ idWork
        idWork = ws -- zip (map WorkId [1..]) ws

-- | Start a work item, returns Nothing if we have nothing to do
-- >>> atomically $ buildWork [] >>= start
-- Nothing
-- >>> atomically $ buildWork [(1, ())] >>= start
-- Just (1,Repeats 0,())
-- >>> atomically $ buildWork [(1, ())] >>= \w -> start w >> start w
-- Just (1,Repeats 1,())
-- >>> atomically $ buildWork [(1, ()), (2, ())] >>= \w -> start w >> start w
-- Just (2,Repeats 0,())
-- >>> atomically $ buildWork [(1, ()), (2, ())] >>= \w -> start w >> start w >> start w
-- Just (1,Repeats 1,())
start :: Ord i => Work i a -> STM (Maybe (i, History, a))
start w = do tryWork <- tryReadTQueue (_todo w)
             -- Completed items are not removed from the queue
             -- So we need to check that we haven't already completed this item
             -- if it has we move on
             doned <- readTVar $ _done w
             case tryWork of
               Nothing -> return Nothing
               Just t -> if M.lookup (t ^. _1) doned > Just 0
                            then start w
                            else writeTQueue (_todo w) (t & _2 %~ succ) >> return (Just t)

-- | Complete a work item, returns the whether it was previously complete
-- >>> atomically $ buildWork [] >>= complete 5
-- True
-- >>> atomically $ buildWork [(1, ())] >>= \w -> complete 1 w >> complete 1 w
-- False
--
-- Completing an item means it won't be started again
-- >>> atomically $ buildWork [(1, ()), (2, ()), (3, ())] >>= \w -> complete 1 w >> complete 2 w >> start w
-- Just (3,Repeats 0,())
complete :: Ord i => i -> Work i a -> STM Bool
complete wid w = do doned <- readTVar $ _done w
                    let next = succ . fromMaybe 0 $ doned ^. at wid
                    -- We cannot remove items from a TQueue, so we track what is done here
                    -- and skip done items in `start`
                    writeTVar (_done w) $ doned & at wid .~ Just next
                    return (next == 1)

-- | Check is the work is all finished
-- >>> atomically $ buildWork [] >>= isComplete
-- True
-- >>> atomically $ buildWork [(1, ())] >>= isComplete
-- False
-- >>> atomically $ buildWork [(1, ())] >>= \w -> complete 1 w >> isComplete w
-- True
isComplete :: Work i a -> STM Bool
isComplete w = do doned <- readTVar $ _done w
                  return $ all (> 0) doned
  -- we are done if all keys in the map are >0

progress :: Work i a -> STM Float
progress w = do donecount <- fmap M.size $ readTVar $ _done w
                return (fromIntegral donecount / fromIntegral (_size w))
