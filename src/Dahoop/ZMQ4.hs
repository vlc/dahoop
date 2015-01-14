{-# LANGUAGE DeriveGeneric #-}
module Dahoop.ZMQ4 where

import           Data.List           (intercalate)
import           Data.Monoid         ((<>))
import           Data.Serialize      (Serialize)
import           GHC.Generics        (Generic)
import           System.ZMQ4         as Z (Socket, bind, connect, unbind, disconnect)
import qualified System.ZMQ4.Monadic as M (Socket, ZMQ, bind, connect, unbind, disconnect)

-- | An address that a 0mq socket can either bind or connect to, depending on the type of `a`.
--
-- It is expected that the type parameter will either be Bind or Connect, depending on how the
-- socket will be connected.
data Address a = TCP a Int
               | IPC FilePath
               | Inproc String deriving (Generic, Eq, Show)

instance Serialize a => Serialize (Address a)

-- The interfaces that 0mq sockets can bind to
data Bind = IP4 Int Int Int Int
          | Wildcard
          | IFace String deriving (Generic, Eq, Show)

instance Serialize Bind

-- The network addresses that 0mq sockets can connect to
data Connect = DNS String
             | IP4' Int Int Int Int deriving (Generic, Eq, Show)

instance Serialize Connect

-- | Bind a socket to a valid address.
bind :: Socket a -> Address Bind -> IO ()
bind skt = Z.bind skt . addressOf bindInterface

-- | Bind a socket to a valid address, using the monadic API.
bindM :: M.Socket z t -> Address Bind -> M.ZMQ z ()
bindM skt = M.bind skt . addressOf bindInterface

unbind :: Socket a -> Address Bind -> IO ()
unbind skt = Z.unbind skt . addressOf bindInterface

unbindM :: M.Socket z t -> Address Bind -> M.ZMQ z ()
unbindM skt = M.unbind skt . addressOf bindInterface

-- | Connect a socket to a valid address.
connect :: Socket a -> Address Connect -> IO ()
connect skt = Z.connect skt . addressOf connectInterface

-- | Connect a socket to a valid address, using the monadic API.
connectM :: M.Socket z t -> Address Connect -> M.ZMQ z ()
connectM skt = M.connect skt . addressOf connectInterface

disconnect :: Socket a -> Address Connect -> IO ()
disconnect skt = Z.disconnect skt . addressOf connectInterface

disconnectM :: M.Socket z t -> Address Connect -> M.ZMQ z ()
disconnectM skt = M.disconnect skt . addressOf connectInterface

-- Internal functions

addressOf :: (a -> String) -> Address a -> String
addressOf f (TCP iface port) = "tcp://" <> f iface <> ":" <> show port
addressOf _ (IPC filepath) = "ipc://" <> filepath
addressOf _ (Inproc name) = "inproc://" <> name

bindInterface :: Bind -> String
bindInterface (IP4 a b c d) = (intercalate "." . map show) [a,b,c,d]
bindInterface Wildcard = "*"
bindInterface (IFace s) = s


connectInterface :: Connect -> String
connectInterface (DNS t) = t
connectInterface (IP4' a b c d) = (intercalate "." . map show) [a,b,c,d]
