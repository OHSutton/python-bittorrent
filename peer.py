import asyncio
import time

from session import Session
from file import File, BlockRequest
from messages import *
from bitarray import bitarray

# Constants
MAX_BUFFER = 64 * 1024  # 64 kb
DEAD_TIMEOUT = 2 * 60  # 2 minutes
HANDSHAKE_WAIT = 15  # seconds


class Peer:
    my_id: str = ''
    their_id: str = ''
    buffer: bytes = b''

    reader: asyncio.StreamReader = None
    writer: asyncio.StreamWriter = None
    last_response: float = 0

    session: Session = None
    file: File = None

    num_pending = 0
    pending_requests: list[BlockRequest] = []  # Sent requests awaiting response
    completed_requests: asyncio.Queue = []  # Received pieces / expired requests

    _am_choking = True
    _am_interested = False
    _peer_choking = True
    _peer_interested = False

    def __init__(self, my_id: str, reader: asyncio.StreamReader = None, writer: asyncio.StreamWriter = None):
        self.reader = reader
        self.writer = writer
        self.my_id = my_id

    async def handshake(self):
        try:
            msg = Handshake.tobytes(self.file.info_hash, self.my_id)
            self.writer.write(msg)
            self.buffer += await asyncio.wait_for(self.reader.readexactly(Handshake.length), HANDSHAKE_WAIT)
            self.their_id = Handshake.validate(self.buffer, self.file.info_hash)
            self.send_bitfield()
            return True
        except (asyncio.TimeoutError, MessageParsingError):
            return False

    async def run(self):
        """ The main running-loop"""
        while self.session.active:
            try:
                self.buffer += await asyncio.wait_for(self.reader.read(MAX_BUFFER), HANDSHAKE_WAIT)
                self.last_response = time.time()

                while self.buffer:
                    msg, self.buffer = Message.parse_first(self.buffer)
                    self.handle_message(msg)

            except IncompleteMessage:
                # When the end of a message is clipped. The rest might come through on next read
                pass
            except MessageParsingError:
                return  # TODO: LOG
            except asyncio.TimeoutError:
                if not self.connection_alive():
                    return
            except:
                # For any unknown error.  # TODO: LOG
                return
            finally:
                self.refresh()

    def connection_alive(self):
        return time.time() - self.last_response < DEAD_TIMEOUT

    def terminate(self):
        self.writer.close()
        self.session.terminate_peer(self.their_id)
        for req in self.pending_requests:
            req.successful = False
            self.completed_requests.put_nowait(req)

    def handle_message(self, msg: Message):
        if msg.id == MsgID.KeepAlive:
            pass  # Ignore
        elif msg.id == MsgID.Choke:
            self.peer_choking = True
        elif msg.id == MsgID.UnChoke:
            self.peer_choking = False
        elif msg.id == MsgID.Interested:
            self.peer_interested = True
        elif msg.id == MsgID.NotInterested:
            self.peer_interested = False

        elif msg.id == MsgID.Have:
            self.session.add_piece_owner(self.their_id, msg.piece)
        elif msg.id == MsgID.Bitfield:
            bitarr = bitarray(endian='big')
            bitarr.frombytes(msg.bitfield)
            bitarr = bitarr.tolist(as_ints=True)
            self.session.register_bitfield(self.their_id, bitarr)

        elif msg.id == MsgID.Piece:
            req_pos = self.pending_requests.index(BlockRequest(msg.piece, msg.begin, len(msg.block)))
            if req_pos >= 0:
                req = self.pending_requests.pop(req_pos)
                req.data = msg.block
                req.successful = True
                self.completed_requests.put_nowait(req)
            # if req_pos < 0, ignore.  Indicates delayed response to an expired request

        elif msg.id == MsgID.Request:
            # Currently responds to all requests, no specific algo.
            if not self.am_choking and self.file.have_block(msg.piece, msg.begin, msg.block_length):
                block = self.file.get_block(msg.piece, msg.begin, msg.block_length)
                self.send_piece(msg.piece, msg.begin, block)

        elif msg.id == MsgID.Cancel:
            # Not useable atm
            pass
        elif msg.id == MsgID.Port:
            # DHT not supported
            pass

    def refresh(self):
        for req in self.pending_requests[:]:
            if req.expired():
                req.successful = False
                self.completed_requests.put_nowait(req)
                self.pending_requests.remove(req)
                self.num_pending -= 1

    # Send messages

    def send_keep_alive(self):
        msg = KeepAlive()
        self.writer.write(bytes(msg))

    def send_choke(self, choking: bool):
        if choking:
            msg = Choke()
        else:
            msg = UnChoke()
        self.writer.write(bytes(msg))

    def send_interested(self, interested: bool):
        if interested:
            msg = Interested()
        else:
            msg = NotInterested()
        self.writer.write(bytes(msg))

    def send_have(self, piece: int):
        msg = Message.new(MsgID.Have, **{"piece": piece})
        self.writer.write(bytes(msg))

    def send_request(self, req: BlockRequest):
        msg = Message.new(MsgID.Request, **{"piece": req.piece,
                                            "begin": req.begin,
                                            "block_length": req.length})
        req.start()
        self.num_pending += 1
        self.pending_requests.append(req)
        self.writer.write(bytes(msg))

    def send_bitfield(self):
        bitstring = ''.join(str(field) for field in self.file.bitfield)
        bitfield = bitarray(bitstring).tobytes()
        msg = Message.new(MsgID.Bitfield, **{"bitfield": bitfield})
        self.writer.write(bytes(msg))

    def send_piece(self, piece: int, begin: int, block: bytes):
        kwargs = {
            'piece': piece,
            'begin': begin,
            'block': block
        }
        msg = Message.new(MsgID.Piece, **kwargs)
        self.writer.write(bytes(msg))

    @property
    def am_choking(self):
        return self._am_choking

    @am_choking.setter
    def am_choking(self, value):
        self.send_choke(value)
        self._am_choking = value

    @property
    def am_interested(self):
        return self._am_interested

    @am_interested.setter
    def am_interested(self, value):
        self.send_interested(value)
        self._am_interested = value

