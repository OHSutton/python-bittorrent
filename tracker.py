import asyncio
import random
import struct
import time
import urllib.parse
from enum import Enum
from typing import Optional

import aiohttp

from utils import bdecode
from session import Session

RESEND_TIMEOUT = 15
REQUEST_TIMEOUT = 60

class TrackerEvent(Enum):
    none = 0
    completed = 1
    started = 2
    stopped = 3


class _UDPProtocol(asyncio.BaseProtocol):
    def __init__(self):
        self.buffer: bytes = b''
        self.con_update: Optional[asyncio.Future] = None
        self.transport = None
        self.exception = None

    def connection_made(self, transport):
        """
        Called when a connection is made.

        The transport argument is the transport representing the connection. The protocol is responsible for storing
        the reference to its transport.
        """
        self.transport = transport

    def connection_lost(self, exc):
        """
        Called when the connection is lost or closed.

        The argument is either an exception object or None. The latter means a regular EOF is received, or the
        connection was aborted or closed by this side of the connection.
        """
        self.exception = exc or EOFError()
        self.set_con_update()

    def datagram_received(self, data, addr):
        """
        Called when a datagram is received. data is a bytes object containing the incoming data. addr is the address
        of the peer sending the data; the exact format depends on the transport.
        """
        self.buffer += data
        self.set_con_update()

    def error_received(self, exc):
        """
        Called when a previous send or receive operation raises an OSError. exc is the OSError instance.

        This method is called in rare conditions, when the transport (e.g. UDP) detects that a datagram could not
        be delivered to its recipient. In many conditions though, undeliverable datagrams will be silently dropped.

        """
        self.exception = exc or EOFError()
        self.set_con_update()

    def set_con_update(self):
        if self.con_update:
            self.con_update.set_result(None)

    async def read(self):
        if not self.buffer:
            self.con_update = asyncio.Future()
            await self.con_update
            self.con_update = None

        if self.exception:
            e = self.exception
            self.exception = None
            # TODO: LOG
            raise e

        data = self.buffer
        self.buffer = b''
        return data


class _UDPTracker:
    protocol_id: int = 0x41727101980  # Magic constant

    connect_action: int = 0
    announce_action: int = 1
    # scrape_action: int = 2  probs wont do scraping

    error_action: int = 3

    protocol = None
    transport = None

    def __init__(self, parsed_announce: urllib.parse.ParseResult, server_port, peer_id, file, session: Session):
        self.file = file
        self.key = random.randint(0, 2 ** 32 - 1)
        self.peer_id = peer_id
        self.session = session
        self.loop = asyncio.get_event_loop()
        self.server_port = server_port
        self.parsed_announce = parsed_announce

    async def _wait_for(self, msg):
        """
        Send message and wait for response.  Request is resent every 15 seconds if no response has been received.  If
        no reply has been received after 60 seconds, stops trying & raises ConnectionAbortedError
        """
        start = time.time()
        resp = b''

        while time.time() - start < REQUEST_TIMEOUT:
            self.transport.sendto(msg)
            try:
                resp = await asyncio.wait_for(self.protocol.read(), RESEND_TIMEOUT)
            except asyncio.TimeoutError:
                # TODO: Log
                # want to resend request every 15 secs
                pass
            except Exception as e:
                # TODO: LOG
                raise e

        if not resp:
            raise ConnectionAbortedError()

        return resp

    async def _connect(self):
        tx_id = random.randint(1, 2 ** 32 - 1)
        msg = struct.pack(">qii", _UDPTracker.protocol_id, _UDPTracker.connect_action, tx_id)
        resp = await self._wait_for(msg)

        if len(resp) < 16:
            raise ValueError()

        action, resp_tx_id, conn_id = struct.unpack(">iiq", resp)

        if tx_id != resp_tx_id or action != self.connect_action:
            raise ValueError()

        return conn_id

    async def announce(self, event: TrackerEvent):
        try:
            self.transport, self.protocol = self.loop.create_datagram_endpoint(
                _UDPProtocol, remote_addr=(self.parsed_announce.hostname, self.parsed_announce.port))

            tx_id = random.randint(1, 2 ** 32 - 1)
            conn_id = await self._connect()
            msg = struct.pack(">q", conn_id,  # connection_id
                              "i", self.announce_action,
                              'i', tx_id,  # tx_id
                              '20s', self.file.info_hash,
                              '20s', self.peer_id,
                              'q', self.file.pieces_completed * self.file.piece_size,  # downloaded
                              # Not really exact but yolo
                              'q', self.file.file_size - self.file.pieces_completed * self.file.piece_size,  # remaining
                              'q', self.session.uploaded,
                              'i', event,
                              'I', 0,  # 0 for unsupplied ip
                              'I', self.key,
                              'i', -1,  # num_want: Number of peers wanted in reply.  -1 for default
                              'H', self.server_port)

            resp = await self._wait_for(msg)
            if len(resp) < 20:
                raise ValueError()

            action, resp_tx_id, interval, leechers, seeders, peers = struct.unpack('>5i', resp[:20])
            if tx_id != resp_tx_id or action != self.announce_action:
                raise ValueError()

            # Remaining part of buffer should be list of peers [(ip, port), (ip, port), ...]
            # ip == 4 bytes,  port == 2 bytes -> each peer is 6 bytes of data
            # Need to ensure remaining buffer only consists of a list of peers.
            if not len(resp[20:]) % 6 == 0:
                raise ValueError()
            peers = list(struct.iter_unpack(">iH", resp[20:]))  # List[(ip: int, port: int)]

            return interval, peers

        finally:
            self.transport.close()


class _HTTPTracker:
    def __init__(self, parsed_announce: urllib.parse.ParseResult, server_port, file, peer_id, session):
        self.server_port = server_port
        self.file = file
        self.announce_url = parsed_announce.geturl()
        self.peer_id = peer_id
        self.session = session
        self.tracker_id = ''

    async def announce(self, event: TrackerEvent):
        params = {
            'info_hash': urllib.parse.quote(self.file.info_hash),
            'peer_id': self.peer_id,
            'port': self.server_port,
            'uploaded': self.session.uploaded,
            'downloaded': self.file.pieces_completed * self.file.piece_size,
            'remaining': self.file.file_size - self.file.pieces_completed * self.file.piece_size,
            'compact': 1,
            'event': event.name,
        }
        if self.tracker_id :
            params['trackerid'] = self.tracker_id

        url = f"{self.announce_url}?{urllib.parse.urlencode(params)}"
        try:
            timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as response:
                    resp_bytes = await response.read()

            data, _ = bdecode(resp_bytes.decode('utf-8'), strict=False)
            if 'failure reason' in data:
                raise ValueError(data['failure reason'])
            elif 'warning message' in data:
                # LOG WARNING MESSAGE
                pass

            if isinstance(data['peers'], dict):
                peers = [(entry['ip'], entry['port']) for entry in data['peers'].items()]
            else:
                # In compact mode peers is string consisting of ip & port only.  I.e.:
                # ip,port,ip,port,...

                data['peers'] = data['peers'].encode('utf-8')
                if not len(data['peers']) % 6 == 0:
                    raise ValueError()  # LOG
                peers = list(struct.iter_unpack("!IH", data['peers']))  # List[(ip: int, port: int)]

            self.tracker_id = data['tracker id']
            return data['interval'], peers

        except asyncio.TimeoutError:
            raise ConnectionAbortedError()  # TODO: Log
        except Exception as e:
            raise e  # TODO: Log


class Announcer:
    interval = 0
    server_port: int = -1
    announce_url = None

    def __init__(self, metainfo, server_port, peer_id, file, session, peer_manager):

        self.announce_url = urllib.parse.urlparse(metainfo['announce'])
        self.server_port = server_port
        self.peer_manager = peer_manager
        self.session = session

        if self.announce_url.scheme == 'udp':
            self.tracker = _UDPTracker(self.announce_url, server_port, peer_id, file, session)
        elif self.announce_url.scheme == 'http':
            self.tracker = _HTTPTracker(self.announce_url, server_port, peer_id, file, session)
        else:
            raise ValueError(f'Invalid announce url: {metainfo["announce"]}')

    async def _connect(self):
        self.interval, peers = await self.tracker.announce(TrackerEvent.started)
        return peers

    async def _announce(self):
        _, peers = await self.tracker.announce(TrackerEvent.none)
        return peers

    async def _disconnect(self):
        await self.tracker.announce(TrackerEvent.stopped)

    async def run(self):
        try:
            peers = await self._connect()
            self.peer_manager.connect_to_peers(peers)

            while self.session.active:
                await asyncio.sleep(self.interval)
                peers = self._announce()
                self.peer_manager.connect_to_peers(peers)

        except ConnectionAbortedError:
            # Terminal error, abort entire program.
            # TODO: LOG
            pass
        except ValueError:
            # Another terminal error: invalid message from tracker
            # TODO: LOG
            pass
        finally:
            await self._disconnect()

