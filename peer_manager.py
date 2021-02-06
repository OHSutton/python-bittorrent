import ipaddress
import socket
import struct

from peer import Peer
import asyncio
from session import Session


REQUEST_TIMEOUT = 60


class PeerManager:
    my_id = '-OH0001-012345678910'
    port = 6888  # Range is 6881-6889

    blacklisted_peers: set[tuple[int, int]] = []
    connected_addrs: set[tuple[int, int]] = []

    peers: dict[str, Peer] = {}
    peer_tasks: dict[str, asyncio.Task] = {}

    session: Session = None
    server = None

    peer_count = 0

    max_connections = 35
    min_connections = 25

    def __init__(self, my_id, session):
        self.my_id = my_id
        self.session = session

    async def _cold_connect_peer(self, ip: int, port: int):
        peer = None
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(socket.inet_ntoa(struct.pack("!I", ip)), port), REQUEST_TIMEOUT)

            peer = Peer(self.my_id, reader, writer)
            if await peer.handshake():
                # Successfully connected to peer
                return peer
        except asyncio.TimeoutError:
            # Log timeouts
            pass
        except Exception as e:
            # Unknown errors # TODO: LOg
            pass
        finally:
            # If here peer terminated
            if peer:
                peer.terminate()
        return None

    async def _start_peer(self, peer: Peer):
        try:
            self.peers[peer.their_id] = peer
            self.peer_count += 1

            await peer.run()
        except:
            pass  # TODO: Log
        finally:
            # If here peer terminated
            peer.terminate()
            del self.peers[peer.their_id]
            del self.peer_tasks[peer.their_id]
            self.connected_addrs.remove((peer.host, peer.port))
            self.peer_count -= 1

    async def handle_conn(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        if self.peer_count == self.max_connections:
            writer.close()
            return

        addr = writer.get_extra_info('peername')  # 0 -> ip (str), 1->port (int)
        addr[0] = ipaddress.IPv4Address(addr[0])  # convert string ip to int.

        if addr in self.blacklisted_peers:
            writer.close()
            return

        peer = Peer(self.my_id, reader, writer)
        if not await peer.handshake():
            return

        self.peer_tasks[peer.their_id] = asyncio.ensure_future(self._start_peer(peer))

    async def start_server(self):
        # Can take ports in the range 6881-6889 so switch ports if exception
        self.server = await asyncio.start_server(
            self.handle_conn, port=self.port)
        self.session.active = True
        await self.server.start_serving()
        self.session.active = False

    def connect_to_peers(self, peer_addrs: list[tuple[int, int]]):
        if self.peer_count >= self.max_connections:
            return
        # Filter out connected and blacklisted peers
        peer_addrs = set(peer_addrs).difference(self.blacklisted_peers).difference(self.connected_addrs)

        if peer_addrs:
            peer_addrs = peer_addrs[:self.max_connections]
            # Connect to peer & handshake
            pending_peers = asyncio.gather(*[self._cold_connect_peer(ip, port) for ip, port in peer_addrs])
            loop = asyncio.get_event_loop()
            peers = loop.run_until_complete(pending_peers)

            for peer_pos in range(len(peers)):
                peer = peers[peer_pos]
                # Failed connection / handshake -> peer == None
                if peer:
                    self.connected_addrs.add((peer.host, peer.port))
                    # Run the peer
                    self.peer_tasks[peer.their_id] = asyncio.ensure_future(self._start_peer(peer))
                else:
                    # Port & ip were bad, ban future connections
                    self.blacklisted_peers.add(peer_addrs[peer_pos])

    async def shutdown(self):
        for task in self.peer_tasks.values():
            task.cancel()
        if self.peer_tasks:
            await asyncio.wait(self.peer_tasks.values())
        self.server.close()
        await self.server.wait_closed()