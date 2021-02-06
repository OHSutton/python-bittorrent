import hashlib
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
