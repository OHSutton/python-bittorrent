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


def bencode_str(text: str) -> bytes:
    return (str(len(text)) + ":" + text).encode(encoding='ascii')


def bdecode_str(data: bytes) -> tuple[str, bytes]:
    text = data.decode(encoding='ascii')
    try:
        str_len = int(text[0])
        return text[2: 2 + str_len], data[str_len + 2:]  # +1 for len, +1 for ':'
    except ValueError:
        # str_len isn't an int -> not a string
        raise BDecodeError()


def bencode_int(data: int) -> bytes:
    data = BEncoding.Integer + str(data) + BEncoding.End
    return data.encode(encoding='ascii')


def bdecode_int(data: bytes) -> tuple[int, bytes]:
    decoded = data.decode(encoding='ascii')
    if decoded[0] != BEncoding.Integer:
        raise BDecodeError()

    try:
        int_end = decoded.index(BEncoding.End)

        if decoded[1] == '0' and int_end != 2:
            raise BDecodeError()
        elif decoded[1] == '-' and decoded[2] == '0':
            raise BDecodeError()

    except (ValueError, IndexError):
        raise BDecodeError()


    return int(decoded[1:int_end]), data[int_end + 1:]


#  NB: Doesn't enforce lexicographic ordering
def bencode_dict(data: dict) -> bytes:
    elements = [BEncoding.Dict.encode(encoding='ascii')]

    for k, v in data.items():
        elements.extend([bencode_str(k), bencode(v)])
    elements.append(BEncoding.End.encode(encoding='ascii'))
    return b''.join(elements)


#  NB: Doesn't enforce lexicographic ordering
def bdecode_dict(data: bytes) -> tuple[dict, bytes]:
    decoded = data.decode(encoding='ascii')
    if decoded[0] != BEncoding.Dict:
        raise BDecodeError()

    d = {}
    while decoded[0] != BEncoding.End:
        key, data = bdecode_str(data)
        element, data = bdecode(data)
        d[key] = element

    return d, data


def bencode_list(data: list) -> bytes:
    elements = [BEncoding.List.encode(encoding='ascii')]

    for ele in data:
        elements.append(bencode(ele))

    elements.append(BEncoding.End.encode(encoding='ascii'))
    return b''.join(elements)


def bdecode_list(data: bytes) -> tuple[list, bytes]:
    decoded = data.decode(encoding='ascii')
    if decoded[0] != BEncoding.List:
        raise BDecodeError()

    d = []
    while decoded[0] != BEncoding.End:
        element, data = bdecode(data)
        d.append(element)

    return d, data


def bdecode(data: bytes) -> tuple[Union[str, int, dict, list], bytes]:
    indicator = data.decode(encoding='ascii')[0]

    if indicator == BEncoding.Integer:
        return bdecode_int(data)
    elif indicator.isdigit():
        return bdecode_str(data)
    elif indicator == BEncoding.List:
        return bdecode_list(data)
    elif indicator == BEncoding.Dict:
        return bdecode_dict(data)
    else:
        raise BDecodeError()


def bencode(data: Union[str, int, dict, list]) -> bytes:
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