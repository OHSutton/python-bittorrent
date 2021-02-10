import os
import hashlib
import time
import math

BlockSize = 2 ** 14  # 16 Kb


class InvalidHashException(Exception):
    pass


class InvalidBlock(Exception):
    pass


# Requests will expire if not fulfilled within 15 secs
# Might be too low/big
RequestLifespan = 10


class BlockRequest:
    completed_by: str = None
    successful: bool = False
    data: bytes = None

    expiration_time: float = 0.0
    piece: int = 0
    begin: int = 0
    length: int = 0

    def __init__(self, piece: int, begin: int, length: int):
        self.piece = piece
        self.begin = begin
        self.length = length

    def __eq__(self, other):
        if not isinstance(other, BlockRequest):
            return False

        return self.__dict__ == other.__dict__

    def start(self):
        self.expiration_time = time.time() + RequestLifespan

    def reset(self):
        self.expiration_time = 0.0
        self.successful = False
        self.completed_by = ""

    def expired(self):
        return time.time() > self.expiration_time


class Piece:
    piece: int = 0
    total_size: int = 0
    current_size: int = 0
    sha1: bytes = None
    data: bytearray = None

    num_blocks_remaining: int = 0
    remaining_blocks: set[BlockRequest] = set()

    def __init__(self, piece: int, total_size: int, sha1_hash: bytes):
        self.piece = piece
        self.total_size = total_size
        self.data = bytearray(total_size)
        self.sha1 = sha1_hash
        self.generate_requests()

    def reset(self):
        """ Clears data in case of Invalid Hash """
        self.data = bytearray(self.total_size)
        self.remaining_blocks.clear()
        self.current_size = 0
        self.generate_requests()

    def generate_requests(self):
        # Divide piece into blocks
        for offset in range(self.total_size, BlockSize):
            req = BlockRequest(self.piece, offset, min(BlockSize, self.total_size - offset))
            self.remaining_blocks.add(req)
            self.num_blocks_remaining += 1

    def add_block(self, req: BlockRequest):
        if req not in self.remaining_blocks:
            return  # Ignore them, might change later idk
            # TODO: Log Each reset + reason i.e. MalformedPiece, InvalidHash, etc
        self.remaining_blocks.remove(req)
        self.num_blocks_remaining -= 1
        block_len = len(req.data)
        self.data[req.begin:req.begin + block_len] = req.data
        self.current_size += block_len

    def full(self):
        return self.total_size == self.current_size

    def valid_hash(self):
        sha1 = hashlib.sha1(bytes(self.data)).digest()
        return sha1 == self.sha1


class File:
    info_hash = ""
    piece_loc: dict[int, int] = {}  # Maps piece index -> pos in file
    pieces: dict[int, Piece] = {}  # Maps piece piece_index -> Piece
    incomplete_pieces: set[int] = []
    completed_pieces: set[int] = []
    bitfield = None

    piece_count = 0
    total_pieces: int = 0
    pieces_completed: int = 0
    path: str = None
    file_size: int = 0
    piece_size: int = 0

    def __int__(self, path: str, file_size: int, piece_size: int,
                piece_hashes: list[bytes]):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        # Allocate file size
        with open(path, "wb") as f:
            f.truncate(file_size)

        self.piece_size = piece_size
        self.file_size = file_size
        self.piece_count = math.ceil(self.file_size / self.piece_size)
        self.remaining = file_size
        self.path = path
        self.bitfield = [0] * self.piece_count

    def init_pieces(self, piece_size: int, piece_hashes: list[bytes]):
        self.piece_size = piece_size
        self.total_pieces = len(piece_hashes)

        for piece_idx, sha1 in enumerate(piece_hashes):
            self.pieces[piece_idx] = Piece(piece_idx,
                min(piece_size, self.file_size - piece_idx * piece_size), sha1)

            self.piece_loc[piece_idx] = piece_idx * piece_size

    def is_complete(self):
        return self.pieces_completed == self.total_pieces

    def get_block(self, piece_idx: int, offset: int, length: int) -> bytes:
        """ Returns data if the piece is complete, None otherwise"""
        if piece_idx in self.completed_pieces:
            with open(self.path, 'rb') as f:
                f.seek(offset)
                data = f.read(length)
            return data

    def add_block(self, req: BlockRequest):
        """ Returns piece_idx if piece is complete, None otherwise"""
        if req.piece in self.pieces:
            piece = self.pieces[req.piece]
            piece.add_block(req)

            if piece.full():
                if not piece.valid_hash():
                    raise InvalidHashException()

                # Piece is complete & has correct hash -> Write to file
                with open(self.path, "wb") as f:
                    f.seek(self.piece_loc[req.piece])
                    f.write(bytes(piece.data))

                self.completed_pieces.add(req.piece)
                self.incomplete_pieces.remove(req.piece)
                self.bitfield[req.piece] = 1
                self.pieces_completed += 1

    def reset_piece(self, piece: int):
        self.pieces[piece].reset()
        self.bitfield[piece] = 0

    def block_remaining(self, req: BlockRequest):
        if req.piece in self.incomplete_pieces:
            return req in self.pieces[req.piece].remaining_blocks
        return False
