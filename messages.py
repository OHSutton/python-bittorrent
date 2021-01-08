import enum
import struct


class MessageParsingError(Exception):
    pass


class IncompleteMessage(Exception):
    pass


""" Base Datatypes for parsing """


class DataType:
    length = 0

    def __init__(self, value=None):
        if value:
            self._value = value

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, x):
        self._value = x

    def __len__(self):
        return self.length

    def __bytes__(self):
        raise NotImplementedError()

    @staticmethod
    def parse(buffer, length=None):
        raise NotImplementedError()


class _Int(DataType):
    length: int = 4  # Length in Bytes

    def __bytes__(self):
        return struct.pack(">I", self.value)

    @staticmethod
    def parse(buffer, length=None):
        #  Kwargs not needed here
        (value,), buffer = struct.unpack(">I", buffer[:_Int.length]), \
                           buffer[_Int.length:]
        return value, buffer


class _Char(DataType):
    length: int = 1  # Length in Bytes

    def __bytes__(self):
        return struct.pack(">B", self.value)

    @staticmethod
    def parse(buffer, length=None):
        #  Kwargs not needed here
        (value,), buffer = struct.unpack(">B", buffer[:_Char.length]), \
                           buffer[_Char.length:]
        return value, buffer


class _Bytes(DataType):
    # Length is variable

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, x):
        self._value = x
        self.length = len(x)

    def __bytes__(self):
        """ Used when we know value and length"""
        return self._value

    @staticmethod
    def parse(buffer, length=None):
        """ Used when we don't know value but given length"""
        #  kwargs needed here
        value, buffer = buffer[:length], buffer[length:]
        return value, buffer


""" Messages """


class Handshake:
    length = 68  # Size of handshake message
    pstr = b"BitTorrent protocol"
    pstrlen = struct.pack(">B", 19)  # 19 = len(pstr)

    @staticmethod
    def tobytes(info_hash: bytes, peer_id: str) -> bytes:
        msg = Handshake.pstrlen + Handshake.pstr
        msg += "\0" * 8  # 8 reserved bytes
        msg += info_hash  # sha1 hash of infokey in metainfo file
        msg += bytes(peer_id, 'ascii')  # this client's id
        return msg

    @staticmethod
    def validate(buffer: bytes, info_hash: bytes) -> str:
        """
        Parses the Peer's handshake msg.  If valud returns peer_id, else
        throws InvalidMessage Exception
        """
        if len(buffer) != Handshake.length:
            raise MessageParsingError()
        pstrlen, buffer = _Char.parse(buffer)
        if pstrlen != Handshake.pstrlen:
            raise MessageParsingError()
        pstr, buffer = buffer[:pstrlen].decode('ascii'), buffer[pstrlen:]
        if pstr != Handshake.pstr or buffer[:20] != info_hash:
            raise MessageParsingError()
        return buffer[20:].decode('ascii')


class MsgID(enum.Enum):
    KeepAlive = None
    Choke = 0
    UnChoke = 1
    Interested = 2
    NotInterested = 3
    Have = 4
    Bitfield = 5
    Request = 6
    Piece = 7
    Cancel = 8
    Port = 9


class Message:
    data: dict[str, DataType]

    @staticmethod
    def new(id, **kwargs):
        msg = MessageMap[id]
        for kw, value in kwargs.items():
            msg.data[kw].value = value
            msg.data['length'].value += len(msg.data[kw])
        return msg

    @staticmethod
    def parse_first(buffer: bytes):
        if len(buffer) < 4:
            raise MessageParsingError()

        length, data = _Int.parse(buffer)
        if length == 0:
            # KeepAlive Message
            return KeepAlive()
        if len(data) < length:
            raise IncompleteMessage()

        id, data = _Char.parse(data)
        msg = MessageMap[id]

        # Kinda inelegant so maybe refactor later.
        # need to remove so it doesn't re-parse it
        del msg.data['length']
        del msg.data['id']
        msg.id = id
        try:
            for kw, dtype in msg.data.items():
                # Length only used for bytes type, so can always pass it & decrement
                value, data = dtype.parse(data, length)
                length -= dtype.length
                setattr(msg, kw, value)
        except:
            raise MessageParsingError()

        return msg, data

    def __bytes__(self) -> bytes:
        msg = b''
        for kw, value in self.data.items():
            msg += bytes(value)
        return msg


class KeepAlive(Message):
    data = {
        'length': _Int(0)
    }


class Choke(Message):
    data = {
        'length': _Int(1),
        'id': _Char(MsgID.Choke),
    }


class UnChoke(Message):
    data = {
        'length': _Int(1),
        'id': _Char(MsgID.UnChoke),
    }


class Interested(Message):
    data = {
        'length': _Int(1),
        'id': _Char(MsgID.Interested),
    }


class NotInterested(Message):
    data = {
        'length': _Int(1),
        'id': _Char(MsgID.NotInterested),
    }


class Have(Message):
    data = {
        'length': _Int(5),
        'id': _Char(MsgID.Have),
        'piece': _Int()
    }


class Bitfield(Message):
    data = {
        'length': _Int(1),
        'id': _Char(MsgID.Bitfield),
        'bitfield': _Bytes(b''),
    }


class Request(Message):
    data = {
        'length': _Int(13),
        'id': _Char(MsgID.Request),
        'piece': _Int(),
        'begin': _Int(),
        'block_length': _Int(),
    }


class Piece(Message):
    data = {
        'length': _Int(13),
        'id': _Char(MsgID.Piece),
        'piece': _Int(),
        'begin': _Int(),
        'block': _Bytes(),
    }


class Cancel(Message):
    data = {
        'length': _Int(13),
        'id': _Char(MsgID.Cancel),
        'piece': _Int(),
        'begin': _Int(),
        'block_length': _Int(),
    }


class Port(Message):
    data = {
        'length': _Int(13),
        'id': _Char(MsgID.Port),
        'port': _Int(),
    }


MessageMap = {
    None: KeepAlive,
    0: Choke,
    1: UnChoke,
    2: Interested,
    3: NotInterested,
    4: Have,
    5: Bitfield,
    6: Request,
    7: Piece,
    8: Cancel,
    9: Port,
}
