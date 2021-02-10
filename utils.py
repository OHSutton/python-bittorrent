import bisect
import hashlib
import time
from enum import Enum
from typing import Union


class BDecodeError(Exception):
    pass


class BEncodeError(Exception):
    pass


class BEncoding(Enum):
    Dict: str = 'd'
    List: str = 'I'
    Integer: str = 'i'
    End: str = 'e'


def bencode_str(text: str) -> str:
    return str(len(text)) + ":" + text


def bdecode_str(data: str) -> tuple[str, str]:
    try:
        str_len = int(data[0])
        return data[2: 2 + str_len], data[str_len + 2:]  # +1 for len, +1 for ':'
    except ValueError:
        # str_len isn't an int -> not a string
        raise BDecodeError()


def bencode_int(data: int) -> str:
    data = BEncoding.Integer + str(data) + BEncoding.End
    return data


def bdecode_int(data: str) -> tuple[int, str]:
    if data[0] != BEncoding.Integer:
        raise BDecodeError()

    try:
        int_end = data.index(BEncoding.End)

        if data[1] == '0' and int_end != 2:
            raise BDecodeError()
        elif data[1] == '-' and data[2] == '0':
            raise BDecodeError()

        return int(data[1:int_end]), data[int_end + 1:]

    except (ValueError, IndexError):
        raise BDecodeError()


#  NB: Doesn't enforce lexicographic ordering
def bencode_dict(data: dict) -> str:
    elements = [BEncoding.Dict]

    for k, v in data.items():
        elements.extend([bencode_str(k), bencode(v)])
    elements.append(BEncoding.End)
    return ''.join(elements)


#  NB: Doesn't enforce lexicographic ordering
def bdecode_dict(data: str) -> tuple[dict, str]:
    if data[0] != BEncoding.Dict:
        raise BDecodeError()

    d = {}
    while data[0] != BEncoding.End:
        key, data = bdecode_str(data)
        element, data = bdecode(data)
        d[key] = element

    return d, data


def bencode_list(data: list) -> str:
    elements = [BEncoding.List]

    for ele in data:
        elements.append(bencode(ele))

    elements.append(BEncoding.End)
    return ''.join(elements)


def bdecode_list(data: str) -> tuple[list, str]:
    if data[0] != BEncoding.List:
        raise BDecodeError()

    d = []
    while data[0] != BEncoding.End:
        element, data = bdecode(data)
        d.append(element)

    return d, data


def bdecode(data: str, strict=True) -> tuple[Union[str, int, dict, list], str]:
    indicator = data[0]

    if indicator == BEncoding.Integer:
        return bdecode_int(data)
    elif indicator.isdigit():
        return bdecode_str(data)
    elif indicator == BEncoding.List:
        return bdecode_list(data)
    elif indicator == BEncoding.Dict:
        return bdecode_dict(data)
    elif not strict:
        return data, ''
    else:
        raise BDecodeError()


def bencode(data: Union[str, int, dict, list]) -> str:
    if isinstance(data, int):
        return bencode_int(data)
    elif isinstance(data, str):
        return bencode_str(data)
    elif isinstance(data, list):
        return bencode_list(data)
    elif isinstance(data, dict):
        return bencode_dict(data)
    else:
        raise BEncodeError()


def parse_metainfo(file_dir: str) -> tuple[dict[str, Union[str, int]], bytes]:
    try:
        metainfo = open(file_dir).read()
        metainfo, remaining = bdecode_dict(metainfo)

        # Ensure file has requisite keys
        if metainfo.keys() >= {'announce', 'info'} and not remaining:
            raise ValueError()
        if metainfo['info'].keys() >= {'piece length', 'pieces', 'length', 'name'}:
            raise ValueError()

        info_hash = hashlib.sha1(bencode_dict(metainfo['info']).encode('utf-8')).digest()

        # Pieces is a string consisting of the concatenation of all 20-byte sha1 hash values
        # So split them up + encode them
        metainfo['info']['piece hashes'] = [metainfo['info']['pieces'][i * 20:i * 21].encode('utf-8')
                                            for i in range(metainfo['info']['pieces'])]

        return metainfo, info_hash

    except BDecodeError:
        # TODO LOG
        pass
    except ValueError:
        # TODO: LOG
        pass
    except:
        # TODO: LOG
        pass


class RollingAverage20:
    times: list[float] = []
    values: list[int] = []
    timeframe: int = 20

    def _trim(self):
        cutoff = time.time() - self.timeframe

        for i in range(len(self.times)-1, -1, -1):
            if not self.times[i] > cutoff:
                self.times = self.times[i + 1:]
                self.values = self.values[i + 1:]
                return

    def rate(self):
        self._trim()
        return sum(self.values) / self.timeframe

    def record(self, value):
        self.times.append(time.time())
        self.values.append(value)


class PieceTracker:
    rarities = []
    pieces = []
    size = 0

    def __init__(self, total_pieces):
        self.rarities = [0] * total_pieces
        self.pieces = [i for i in range(total_pieces)]

    def add(self, rarity, value):
        pos = bisect.bisect_left(self.rarities, rarity)
        self.rarities.insert(pos, rarity)
        self.pieces.insert(pos, value)
        self.size += 1

    def get_rarity(self, value):
        pos = bisect.bisect_left(self.pieces, value)
        return self.rarities[pos]

    def remove(self, value):
        pos = bisect.bisect_left(self.pieces, value)
        self.pieces.pop(pos)
        self.rarities.pop(pos)
        self.size -= 1

    # Change first instance of value's priority with specified priority
    # Note in this torrent client all values are unique
    def update(self, rarity, value):
        self.remove(value)
        self.add(rarity, value)

    # Given a list of values, reorders the values in terms of rarity
    # Most rare-> least rare
    def raritise(self, values):
        new_order = []
        for value in self.pieces:
            if value in values:
                new_order.append(value)
                values.remove(value)
        return new_order

    # Given a list of values, return value with lowest priority
    def get_rarest(self, values):
        for value in self.pieces:
            if value in values:
                return value

    def poll(self):
        if self.size:
            self.size -= 1
            self.rarities.pop(0)
            return self.pieces.pop(0)

    def peek(self):
        if self.size:
            return self.pieces[0]

    def __len__(self):
        return self.size
