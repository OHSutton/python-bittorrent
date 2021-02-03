from peer import Peer
import asyncio


class PeerManager:
    my_id = '-OH0001-012345678910'
    port = 6888  # Range is 6881-6889

    peers: dict[str, Peer] = {}
    peer_tasks: dict[str, asyncio.Task] = {}

    server = None
    peer_count = 0

    max_connections = 25  # TODO: Figure out proper max

    def __init__(self, my_id):
        self.my_id = my_id

    def connect_peers(self, peers: list[str]):
        # TODO: Do once tracker implemented. Runs when torrent first started to connect to first lot of peers
        for peer in peers:
            self.peer_tasks[peer.their_id] = asyncio.ensure_future(self._cold_connect_peer(peer))
              # Something lik this

    async def _cold_connect_peer(self):
        # TODO TRY EXCEPT FOR TIMEOUT
        peer = None
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(self._peer.host, self._peer.port), PeerTCPClient.CONNECT_TIMEOUT)

            peer = Peer(self.my_id, reader, writer)
            if await peer.handshake():
                # Successfully connected to peer
                self.peers[peer.their_id] = peer
                self.peer_count += 1

                await peer.run()
        except:
            # Log unknown errors
            pass
        finally:
            # If here peer terminated
            if peer:
                peer.terminate()
            del self.peers[peer.their_id]
            del self.peer_tasks[peer.their_id]

    async def _start_peer(self, peer: Peer):
        try:
            # Successfully connected to peer
            self.peers[peer.their_id] = peer
            self.peer_count += 1

            await peer.run()
        except:
            pass  # Log
        finally:
            # If here peer terminated
            peer.terminate()
            del self.peers[peer.their_id]
            del self.peer_tasks[peer.their_id]

    async def handle_conn(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        if self.peer_count == self.max_connections:
            writer.close()

        peer = Peer(reader, writer, self.my_id)
        if not await peer.handshake():
            return

        self.peer_tasks[peer.their_id] = asyncio.ensure_future(self._start_peer(peer))

    async def start(self):
        # TODO: Will have something like
        self.connect_peers()

        self.server = await asyncio.start_server(
            self.handle_conn, port=self.port)

        await self.server.start_serving()

    async def shutdown(self):
        for task in self.peer_tasks.values():
            task.cancel()
        if self.peer_tasks:
            await asyncio.wait(self.peer_tasks.values())
        self.server.close()
        await self.server.wait_closed()