{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE TupleSections              #-}
module Dahoop.Internal.WorkQueue where

import           Control.Applicative
import           Control.Concurrent.STM
import           Control.Lens
import           Control.Monad.Trans.State
import           Data.Foldable
import qualified Data.List.NonEmpty        as NE
import qualified Data.Map                  as M
import           Data.Maybe                (fromMaybe)
import           Prelude                   hiding (all)

-- $setup
-- >>> import Test.QuickCheck
-- >>> import Data.List.NonEmpty
-- >>> let buildWork1 a b = buildWork (a :| b)

-- | Out work queue
data Work i a =
  Work { _todo :: TQueue (i, History, a)
       , _done :: TVar (M.Map i Int)
       , _size :: Int }
-- invariants: in a fresh queue, all keys in the map are in the queue

-- The state that a work item transition
newtype History = Repeats Int deriving (Eq, Show, Num, Enum)

type M i a m = StateT (Work i a) m

buildWork :: Ord i => NE.NonEmpty (i, a) -> STM (Work i a)
buildWork ws = Work <$> initialQueue <*> newTVar initMap <*> pure (NE.length ws)
  where initialQueue =
          do q <- newTQueue
             _ <- traverse (\(wid, a) -> writeTQueue q (wid,Repeats 0,a)) idWork
             return q
        initMap = M.fromList . toList . fmap (\(wid, _) -> (wid, 0)) $ idWork
        idWork = ws -- zip (map WorkId [1..]) ws

-- | Start a work item, returns Nothing if we have nothing to do
-- >>> atomically $ buildWork1 (1, ()) [] >>= start
-- Just (1,Repeats 0,())
-- >>> atomically $ buildWork1 (1, ()) [] >>= \w -> start w >> start w
-- Just (1,Repeats 1,())
-- >>> atomically $ buildWork1 (1, ()) [(2, ())] >>= \w -> start w >> start w
-- Just (2,Repeats 0,())
-- >>> atomically $ buildWork1 (1, ()) [(2, ())] >>= \w -> start w >> start w >> start w
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
-- >>> atomically $ buildWork1 (1, ()) [] >>= \w -> complete 1 w >> complete 1 w
-- False
--
-- Completing an item means it won't be started again
-- >>> atomically $ buildWork1 (1, ()) [(2, ()), (3, ())] >>= \w -> complete 1 w >> complete 2 w >> start w
-- Just (3,Repeats 0,())
complete :: Ord i => i -> Work i a -> STM Bool
complete wid w = do doned <- readTVar $ _done w
                    let next = succ . fromMaybe 0 $ doned ^. at wid
                    -- We cannot remove items from a TQueue, so we track what is done here
                    -- and skip done items in `start`
                    writeTVar (_done w) $ doned & at wid .~ Just next
                    return (next == 1)

-- | Check if a single item is complete
-- >>> atomically $ buildWork1 (1, ()) [] >>= \w -> itemIsComplete 1 w
-- False
-- >>> atomically $ buildWork1 (1, ()) [] >>= \w -> complete 1 w >> itemIsComplete 1 w
-- True
itemIsComplete :: Ord i => i -> Work i a -> STM Bool
itemIsComplete i w =
  do dones <- readTVar $ _done w
     return $ maybe False (>0) (M.lookup i dones)

-- | Check is the work is all finished
-- >>> atomically $ buildWork1 (1, ()) [] >>= isComplete
-- False
-- >>> atomically $ buildWork1 (1, ()) [] >>= \w -> complete 1 w >> isComplete w
-- True
isComplete :: Work i a -> STM Bool
isComplete w = do doned <- readTVar $ _done w
                  return $ all (> 0) doned
  -- we are done if all keys in the map are >0

-- |
-- >>> atomically $ buildWork1 (1, ()) [(2, ())] >>= \w -> complete 1 w >> progress w
-- 0.5
-- >>> atomically $ buildWork1 (1, ()) [(2, ())] >>= \w -> complete 1 w >> complete 2 w >> progress w
-- 1.0
progress :: Work i a -> STM Float
progress w = do
  x <- readTVar (_done w)
  let donecount = lengthOf (traverse . filtered (>0)) x
  return (fromIntegral donecount / fromIntegral (_size w))
