{-# LANGUAGE RankNTypes                         #-}
{-# LANGUAGE FlexibleContexts                   #-}
{-# LANGUAGE ConstraintKinds                    #-}
module Dahoop.ZMQ4.Trans where

import Control.Applicative
import Control.Concurrent.Async (Async)
import Control.Monad
import Control.Monad.Trans
import Control.Monad.Trans.Reader
import Control.Monad.Catch
import Data.Int

import Data.List.NonEmpty (NonEmpty)
import Data.Restricted
import Data.Word
import qualified Data.ByteString.Lazy as Lazy (ByteString)
import Data.ByteString (ByteString)
import System.Posix.Types (Fd)

import qualified Control.Concurrent.Async as A
import           Data.IORef
import qualified Control.Monad.Catch      as C
import qualified System.ZMQ4              as Z
import qualified System.ZMQ4.Internal     as I

data ZMQEnv = ZMQEnv
  { _refcount :: !(IORef Word)
  , _context  :: !Z.Context
  , _sockets  :: !(IORef [I.SocketRepr])
  }

-- | The ZMQ monad is modeled after 'Control.Monad.ST' and encapsulates
-- a 'System.ZMQ4.Context'. It uses the uninstantiated type variable 'z' to
-- distinguish different invoctions of 'runZMQ' and to prevent
-- unintented use of 'Socket's outside their scope. Cf. the paper
-- of John Launchbury and Simon Peyton Jones /Lazy Functional State Threads/.
newtype ZMQT z m a = ZMQT { _unzmqt :: ReaderT ZMQEnv m a }
type ZMQ z a = ZMQT z IO a

-- | The ZMQ socket, parameterised by 'SocketType' and belonging to
-- a particular 'ZMQ' thread.
newtype Socket z t = Socket { _unsocket :: Z.Socket t }

instance I.SocketLike (Socket z) where
    toSocket = _unsocket

instance Monad m => Monad (ZMQT z m) where
    return = ZMQT . return
    (ZMQT m) >>= f = ZMQT $ m >>= _unzmqt . f

instance MonadTrans (ZMQT z) where
  lift = ZMQT . lift

instance MonadIO m => MonadIO (ZMQT z m) where
    liftIO m = ZMQT $! liftIO m

instance MonadThrow m => MonadThrow (ZMQT z m) where
    throwM = ZMQT . C.throwM

instance MonadCatch m => MonadCatch (ZMQT z m) where
    catch (ZMQT m) f = ZMQT $ m `C.catch` (_unzmqt . f)

instance (MonadMask m) => MonadMask (ZMQT z m) where
    mask a = ZMQT . ReaderT $ \env ->
        C.mask $ \restore ->
            let f (ZMQT (ReaderT b)) = ZMQT $ ReaderT (restore . b)
            in runReaderT (_unzmqt (a $ f)) env

    uninterruptibleMask a = ZMQT . ReaderT $ \env ->
        C.uninterruptibleMask $ \restore ->
            let f (ZMQT (ReaderT b)) = ZMQT $ ReaderT (restore . b)
            in runReaderT (_unzmqt (a $ f)) env

instance (Monad m) => Functor (ZMQT z m) where
    fmap = liftM

instance (Monad m) => Applicative (ZMQT z m) where
    pure  = return
    (<*>) = ap

-- | Return the value computed by the given 'ZMQ' monad. Rank-2
-- polymorphism is used to prevent leaking of 'z'.
-- An invocation of 'runZMQ' will internally create a 'System.ZMQ4.Context'
-- and all actions are executed relative to this context. On finish the
-- context will be disposed, but see 'async'.
runZMQT :: (MonadIO m, MonadMask m) => (forall z. ZMQT z m a) -> m a
runZMQT z = bracket make term (runReaderT (_unzmqt z))
  where
    make = liftIO $ ZMQEnv <$> newIORef 1 <*> Z.context <*> newIORef []

runZMQ :: (forall z. ZMQ z a) -> IO a
runZMQ = runZMQT

liftZMQ :: (MonadIO m) => ZMQ z a -> ZMQT z m a
liftZMQ = ZMQT . mapReaderT liftIO . _unzmqt

-- | Run the given 'ZMQ' computation asynchronously, i.e. this function
-- runs the computation in a new thread using 'Control.Concurrent.Async.async'.
-- /N.B./ reference counting is used to prolong the lifetime of the
-- 'System.ZMQ.Context' encapsulated in 'ZMQ' as necessary, e.g.:
--
-- @
-- runZMQ $ do
--     s <- socket Pair
--     async $ do
--         liftIO (threadDelay 10000000)
--         identity s >>= liftIO . print
-- @
--
-- Here, 'runZMQ' will finish before the code section in 'async', but due to
-- reference counting, the 'System.ZMQ4.Context' will only be disposed after
-- 'async' finishes as well.
async :: ZMQ z a -> ZMQ z (Async a)
async z = ZMQT $ do
    e <- ask
    lift $ atomicModifyIORef (_refcount e) $ \n -> (succ n, ())
    lift $ A.async $ (runReaderT (_unzmqt z) e) `finally` term e

ioThreads :: (MonadIO m) => ZMQT z m Word
ioThreads = onContext Z.ioThreads

setIoThreads :: (MonadIO m) => Word -> ZMQT z m ()
setIoThreads = onContext . Z.setIoThreads

maxSockets :: (MonadIO m) => ZMQT z m Word
maxSockets = onContext Z.maxSockets

setMaxSockets :: (MonadIO m) => Word -> ZMQT z m ()
setMaxSockets = onContext . Z.setMaxSockets

socket :: (MonadIO m, Z.SocketType t) => t -> ZMQT z m (Socket z t)
socket t = ZMQT $ do
    c <- asks _context
    s <- asks _sockets
    x <- liftIO $ I.mkSocketRepr t c
    liftIO $ atomicModifyIORef s $ \ss -> (x:ss, ())
    return (Socket (I.Socket x))

version :: (MonadIO m) => ZMQT z m (Int, Int, Int)
version = liftIO $! Z.version

-- -- * Socket operations

close :: (MonadIO m) => Socket z t -> ZMQT z m ()
close = liftIO . Z.close . _unsocket

bind :: (MonadIO m) => Socket z t -> String -> ZMQT z m ()
bind s = liftIO . Z.bind (_unsocket s)

unbind :: (MonadIO m) => Socket z t -> String -> ZMQT z m ()
unbind s = liftIO . Z.unbind (_unsocket s)

connect :: (MonadIO m) => Socket z t -> String -> ZMQT z m ()
connect s = liftIO . Z.connect (_unsocket s)

disconnect :: (MonadIO m) => Socket z t -> String -> ZMQT z m ()
disconnect s = liftIO . Z.disconnect (_unsocket s)

send :: (MonadIO m, Z.Sender t) => Socket z t -> [Z.Flag] -> ByteString -> ZMQT z m ()
send s f = liftIO . Z.send (_unsocket s) f

send' :: (MonadIO m, Z.Sender t) => Socket z t -> [Z.Flag] -> Lazy.ByteString -> ZMQT z m ()
send' s f = liftIO . Z.send' (_unsocket s) f

sendMulti :: (MonadIO m, Z.Sender t) => Socket z t -> NonEmpty ByteString -> ZMQT z m ()
sendMulti s = liftIO . Z.sendMulti (_unsocket s)

receive :: (MonadIO m, Z.Receiver t) => Socket z t -> ZMQT z m ByteString
receive = liftIO . Z.receive . _unsocket

receiveMulti :: (MonadIO m, Z.Receiver t) => Socket z t -> ZMQT z m [ByteString]
receiveMulti = liftIO . Z.receiveMulti . _unsocket

subscribe :: (MonadIO m, Z.Subscriber t) => Socket z t -> ByteString -> ZMQT z m ()
subscribe s = liftIO . Z.subscribe (_unsocket s)

unsubscribe :: (MonadIO m) => Z.Subscriber t => Socket z t -> ByteString -> ZMQT z m ()
unsubscribe s = liftIO . Z.unsubscribe (_unsocket s)

proxy :: (MonadIO m) => Socket z a -> Socket z b -> Maybe (Socket z c) -> ZMQT z m ()
proxy a b c = liftIO $ Z.proxy (_unsocket a) (_unsocket b) (_unsocket <$> c)

monitor :: (MonadIO m) => [Z.EventType] -> Socket z t -> ZMQT z m (Bool -> IO (Maybe Z.EventMsg))
monitor es s = onContext $ \ctx -> Z.monitor es ctx (_unsocket s)

socketMonitor :: (MonadIO m) => [Z.EventType] -> String -> Socket z t -> ZMQT z m ()
socketMonitor es addr s = liftIO $ Z.socketMonitor es addr (_unsocket s)

-- * Socket Options (Read)

affinity :: (MonadIO m) => Socket z t -> ZMQT z m Word64
affinity = liftIO . Z.affinity . _unsocket

backlog :: (MonadIO m) => Socket z t -> ZMQT z m Int
backlog = liftIO . Z.backlog . _unsocket

conflate :: (MonadIO m) => Z.Conflatable t => Socket z t -> ZMQT z m Bool
conflate = liftIO . Z.conflate . _unsocket

curvePublicKey :: (MonadIO m) => Z.KeyFormat f -> Socket z t -> ZMQT z m ByteString
curvePublicKey f = liftIO . Z.curvePublicKey f . _unsocket

curveSecretKey :: (MonadIO m) => Z.KeyFormat f -> Socket z t -> ZMQT z m ByteString
curveSecretKey f = liftIO . Z.curveSecretKey f . _unsocket

curveServerKey :: (MonadIO m) => Z.KeyFormat f -> Socket z t -> ZMQT z m ByteString
curveServerKey f = liftIO . Z.curveServerKey f . _unsocket

delayAttachOnConnect :: (MonadIO m) => Socket z t -> ZMQT z m Bool
delayAttachOnConnect = liftIO . Z.delayAttachOnConnect . _unsocket
{-# DEPRECATED delayAttachOnConnect "Use immediate" #-}

events :: (MonadIO m) => Socket z t -> ZMQT z m [Z.Event]
events = liftIO . Z.events . _unsocket

fileDescriptor :: (MonadIO m) => Socket z t -> ZMQT z m Fd
fileDescriptor = liftIO . Z.fileDescriptor . _unsocket

identity :: (MonadIO m) => Socket z t -> ZMQT z m ByteString
identity = liftIO . Z.identity . _unsocket

immediate :: (MonadIO m) => Socket z t -> ZMQT z m Bool
immediate = liftIO . Z.immediate . _unsocket

ipv4Only :: (MonadIO m) => Socket z t -> ZMQT z m Bool
ipv4Only = liftIO . Z.ipv4Only . _unsocket
{-# DEPRECATED ipv4Only "Use ipv6" #-}

ipv6 :: (MonadIO m) => Socket z t -> ZMQT z m Bool
ipv6 = liftIO . Z.ipv6 . _unsocket

lastEndpoint :: (MonadIO m) => Socket z t -> ZMQT z m String
lastEndpoint = liftIO . Z.lastEndpoint . _unsocket

linger :: (MonadIO m) => Socket z t -> ZMQT z m Int
linger = liftIO . Z.linger . _unsocket

maxMessageSize :: (MonadIO m) => Socket z t -> ZMQT z m Int64
maxMessageSize = liftIO . Z.maxMessageSize . _unsocket

mcastHops :: (MonadIO m) => Socket z t -> ZMQT z m Int
mcastHops = liftIO . Z.mcastHops . _unsocket

mechanism :: (MonadIO m) => Socket z t -> ZMQT z m Z.SecurityMechanism
mechanism = liftIO . Z.mechanism . _unsocket

moreToReceive :: (MonadIO m) => Socket z t -> ZMQT z m Bool
moreToReceive = liftIO . Z.moreToReceive . _unsocket

plainServer :: (MonadIO m) => Socket z t -> ZMQT z m Bool
plainServer = liftIO . Z.plainServer . _unsocket

plainPassword :: (MonadIO m) => Socket z t -> ZMQT z m ByteString
plainPassword = liftIO . Z.plainPassword . _unsocket

plainUserName :: (MonadIO m) => Socket z t -> ZMQT z m ByteString
plainUserName = liftIO . Z.plainUserName . _unsocket

rate :: (MonadIO m) => Socket z t -> ZMQT z m Int
rate = liftIO . Z.rate . _unsocket

receiveBuffer :: (MonadIO m) => Socket z t -> ZMQT z m Int
receiveBuffer = liftIO . Z.receiveBuffer . _unsocket

receiveHighWM :: (MonadIO m) => Socket z t -> ZMQT z m Int
receiveHighWM = liftIO . Z.receiveHighWM . _unsocket

receiveTimeout :: (MonadIO m) => Socket z t -> ZMQT z m Int
receiveTimeout = liftIO . Z.receiveTimeout . _unsocket

reconnectInterval :: (MonadIO m) => Socket z t -> ZMQT z m Int
reconnectInterval = liftIO . Z.reconnectInterval . _unsocket

reconnectIntervalMax :: (MonadIO m) => Socket z t -> ZMQT z m Int
reconnectIntervalMax = liftIO . Z.reconnectIntervalMax . _unsocket

recoveryInterval :: (MonadIO m) => Socket z t -> ZMQT z m Int
recoveryInterval = liftIO . Z.recoveryInterval . _unsocket

sendBuffer :: (MonadIO m) => Socket z t -> ZMQT z m Int
sendBuffer = liftIO . Z.sendBuffer . _unsocket

sendHighWM :: (MonadIO m) => Socket z t -> ZMQT z m Int
sendHighWM = liftIO . Z.sendHighWM . _unsocket

sendTimeout :: (MonadIO m) => Socket z t -> ZMQT z m Int
sendTimeout = liftIO . Z.sendTimeout . _unsocket

tcpKeepAlive :: (MonadIO m) => Socket z t -> ZMQT z m Z.Switch
tcpKeepAlive = liftIO . Z.tcpKeepAlive . _unsocket

tcpKeepAliveCount :: (MonadIO m) => Socket z t -> ZMQT z m Int
tcpKeepAliveCount = liftIO . Z.tcpKeepAliveCount . _unsocket

tcpKeepAliveIdle :: (MonadIO m) => Socket z t -> ZMQT z m Int
tcpKeepAliveIdle = liftIO . Z.tcpKeepAliveIdle . _unsocket

tcpKeepAliveInterval :: (MonadIO m) => Socket z t -> ZMQT z m Int
tcpKeepAliveInterval = liftIO . Z.tcpKeepAliveInterval . _unsocket

zapDomain :: (MonadIO m) => Socket z t -> ZMQT z m ByteString
zapDomain = liftIO . Z.zapDomain . _unsocket

-- * Socket Options (Write)

setAffinity :: (MonadIO m) => Word64 -> Socket z t -> ZMQT z m ()
setAffinity a = liftIO . Z.setAffinity a . _unsocket

setBacklog :: (MonadIO m) => Integral i => Restricted (N0, Int32) i -> Socket z t -> ZMQT z m ()
setBacklog b = liftIO . Z.setBacklog b . _unsocket

setConflate :: (MonadIO m) => Z.Conflatable t => Bool -> Socket z t -> ZMQT z m ()
setConflate i = liftIO . Z.setConflate i . _unsocket

setCurvePublicKey :: (MonadIO m) => Z.KeyFormat f -> Restricted f ByteString -> Socket z t -> ZMQT z m ()
setCurvePublicKey f k = liftIO . Z.setCurvePublicKey f k . _unsocket

setCurveSecretKey :: (MonadIO m) => Z.KeyFormat f -> Restricted f ByteString -> Socket z t -> ZMQT z m ()
setCurveSecretKey f k = liftIO . Z.setCurveSecretKey f k . _unsocket

setCurveServer :: (MonadIO m) => Bool -> Socket z t -> ZMQT z m ()
setCurveServer b = liftIO . Z.setCurveServer b . _unsocket

setCurveServerKey :: (MonadIO m) => Z.KeyFormat f -> Restricted f ByteString -> Socket z t -> ZMQT z m ()
setCurveServerKey f k = liftIO . Z.setCurveServerKey f k . _unsocket

setDelayAttachOnConnect :: (MonadIO m) => Bool -> Socket z t -> ZMQT z m ()
setDelayAttachOnConnect d = liftIO . Z.setDelayAttachOnConnect d . _unsocket
{-# DEPRECATED setDelayAttachOnConnect "Use setImmediate" #-}

setIdentity :: (MonadIO m) => Restricted (N1, N254) ByteString -> Socket z t -> ZMQT z m ()
setIdentity i = liftIO . Z.setIdentity i . _unsocket

setImmediate :: (MonadIO m) => Bool -> Socket z t -> ZMQT z m ()
setImmediate i = liftIO . Z.setImmediate i . _unsocket

setIpv4Only :: (MonadIO m) => Bool -> Socket z t -> ZMQT z m ()
setIpv4Only i = liftIO . Z.setIpv4Only i . _unsocket
{-# DEPRECATED setIpv4Only "Use setIpv6" #-}

setIpv6 :: (MonadIO m) => Bool -> Socket z t -> ZMQT z m ()
setIpv6 i = liftIO . Z.setIpv6 i . _unsocket

setLinger :: (MonadIO m) => Integral i => Restricted (Nneg1, Int32) i -> Socket z t -> ZMQT z m ()
setLinger l = liftIO . Z.setLinger l . _unsocket

setMaxMessageSize :: (MonadIO m) => Integral i => Restricted (Nneg1, Int64) i -> Socket z t -> ZMQT z m ()
setMaxMessageSize s = liftIO . Z.setMaxMessageSize s . _unsocket

setMcastHops :: (MonadIO m) => Integral i => Restricted (N1, Int32) i -> Socket z t -> ZMQT z m ()
setMcastHops k = liftIO . Z.setMcastHops k . _unsocket

setPlainServer :: (MonadIO m) => Bool -> Socket z t -> ZMQT z m ()
setPlainServer b = liftIO . Z.setPlainServer b . _unsocket

setPlainPassword :: (MonadIO m) => Restricted (N1, N254) ByteString -> Socket z t -> ZMQT z m ()
setPlainPassword s = liftIO . Z.setPlainPassword s . _unsocket

setPlainUserName :: (MonadIO m) => Restricted (N1, N254) ByteString -> Socket z t -> ZMQT z m ()
setPlainUserName s = liftIO . Z.setPlainUserName s . _unsocket

setProbeRouter :: (MonadIO m) => Z.SendProbe t => Bool -> Socket z t -> ZMQT z m ()
setProbeRouter b = liftIO . Z.setProbeRouter b . _unsocket

setRate :: (MonadIO m) => Integral i => Restricted (N1, Int32) i -> Socket z t -> ZMQT z m ()
setRate r = liftIO . Z.setRate r . _unsocket

setReceiveBuffer :: (MonadIO m) => Integral i => Restricted (N0, Int32) i -> Socket z t -> ZMQT z m ()
setReceiveBuffer k = liftIO . Z.setReceiveBuffer k . _unsocket

setReceiveHighWM :: (MonadIO m) => Integral i => Restricted (N0, Int32) i -> Socket z t -> ZMQT z m ()
setReceiveHighWM k = liftIO . Z.setReceiveHighWM k . _unsocket

setReceiveTimeout :: (MonadIO m) => Integral i => Restricted (Nneg1, Int32) i -> Socket z t -> ZMQT z m ()
setReceiveTimeout t = liftIO . Z.setReceiveTimeout t . _unsocket

setReconnectInterval :: (MonadIO m) => Integral i => Restricted (N0, Int32) i -> Socket z t -> ZMQT z m ()
setReconnectInterval i = liftIO . Z.setReconnectInterval i . _unsocket

setReconnectIntervalMax :: (MonadIO m) => Integral i => Restricted (N0, Int32) i -> Socket z t -> ZMQT z m ()
setReconnectIntervalMax i = liftIO . Z.setReconnectIntervalMax i . _unsocket

setRecoveryInterval :: (MonadIO m) => Integral i => Restricted (N0, Int32) i -> Socket z t -> ZMQT z m ()
setRecoveryInterval i = liftIO . Z.setRecoveryInterval i . _unsocket

setReqCorrelate :: (MonadIO m) => Bool -> Socket z Z.Req -> ZMQT z m ()
setReqCorrelate b = liftIO . Z.setReqCorrelate b . _unsocket

setReqRelaxed :: (MonadIO m) => Bool -> Socket z Z.Req -> ZMQT z m ()
setReqRelaxed b = liftIO . Z.setReqRelaxed b . _unsocket

setRouterMandatory :: (MonadIO m) => Bool -> Socket z Z.Router -> ZMQT z m ()
setRouterMandatory b = liftIO . Z.setRouterMandatory b . _unsocket

setSendBuffer :: (MonadIO m) => Integral i => Restricted (N0, Int32) i -> Socket z t -> ZMQT z m ()
setSendBuffer i = liftIO . Z.setSendBuffer i . _unsocket

setSendHighWM :: (MonadIO m) => Integral i => Restricted (N0, Int32) i -> Socket z t -> ZMQT z m ()
setSendHighWM i = liftIO . Z.setSendHighWM i . _unsocket

setSendTimeout :: (MonadIO m) => Integral i => Restricted (Nneg1, Int32) i -> Socket z t -> ZMQT z m ()
setSendTimeout i = liftIO . Z.setSendTimeout i . _unsocket

setTcpAcceptFilter :: (MonadIO m) => Maybe ByteString -> Socket z t -> ZMQT z m ()
setTcpAcceptFilter s = liftIO . Z.setTcpAcceptFilter s . _unsocket

setTcpKeepAlive :: (MonadIO m) => Z.Switch -> Socket z t -> ZMQT z m ()
setTcpKeepAlive s = liftIO . Z.setTcpKeepAlive s . _unsocket

setTcpKeepAliveCount :: (MonadIO m) => Integral i => Restricted (Nneg1, Int32) i -> Socket z t -> ZMQT z m ()
setTcpKeepAliveCount c = liftIO . Z.setTcpKeepAliveCount c . _unsocket

setTcpKeepAliveIdle :: (MonadIO m) => Integral i => Restricted (Nneg1, Int32) i -> Socket z t -> ZMQT z m ()
setTcpKeepAliveIdle i = liftIO . Z.setTcpKeepAliveIdle i . _unsocket

setTcpKeepAliveInterval :: (MonadIO m) => Integral i => Restricted (Nneg1, Int32) i -> Socket z t -> ZMQT z m ()
setTcpKeepAliveInterval i = liftIO . Z.setTcpKeepAliveInterval i . _unsocket

setXPubVerbose :: (MonadIO m) => Bool -> Socket z Z.XPub -> ZMQT z m ()
setXPubVerbose b = liftIO . Z.setXPubVerbose b . _unsocket

-- * Low Level Functions

waitRead :: (MonadIO m) => Socket z t -> ZMQT z m ()
waitRead = liftIO . Z.waitRead . _unsocket

waitWrite :: (MonadIO m) => Socket z t -> ZMQT z m ()
waitWrite = liftIO . Z.waitWrite . _unsocket

-- * Internal

onContext :: (MonadIO m) => (Z.Context -> IO a) -> ZMQT z m a
onContext f = ZMQT $! asks _context >>= liftIO . f

term :: (MonadIO m) => ZMQEnv -> m ()
term env = liftIO $ do
    n <- atomicModifyIORef (_refcount env) $ \n -> (pred n, n)
    when (n == 1) $ do
        readIORef (_sockets env) >>= mapM_ close'
        Z.term (_context env)
  where
    close' s = I.closeSock s `catch` (\e -> print (e :: SomeException))
