import os
import hashlib


BlockSize = 2 ** 14  # 16 Kb


class Piece:
    piece_size: int = 0
    current_size: int = 0
    sha1: bytes = None
    data: bytearray = None

    blocks: list[tuple[int, int]] = []

    def __init__(self, piece_size: int, sha1_hash: bytes):
        self.piece_size = piece_size
        self.data = bytearray(piece_size)
        self.sha1 = sha1_hash

        # Divide piece into blocks
        for offset in range(piece_size, BlockSize):
            self.blocks.append((offset, min(BlockSize, piece_size - offset)))

    def reset(self):
        """ Clears data in case of Invalid Hash """
        self.data = bytearray(self.piece_size)

    def add_block(self, offset, block):
        block_len = len(block)
        block_pos = self.blocks.index((offset, block_len))
        if block_pos < 0:
            self.reset()
            # TODO: Log Each reset + reason i.e. MalformedPiece, InvalidHash, etc
            return False

        self.blocks.pop(block_pos)
        self.data[offset:offset + block_len] = block
        self.current_size += block_len
        return self.current_size == self.piece_size

    def validate_hash(self):
        sha1 = hashlib.sha1(bytes(self.data)).digest()
        if sha1 == self.sha1:
            return True
        else:
            self.reset()
            # TODO: Log Each reset + reason i.e. MalformedPiece, InvalidHash, etc
            return False


class File:
    piece_loc: dict[int, int] = {}  # Maps piece index -> pos in file
    incomplete_pieces: dict[int, Piece] = {}  # Maps piece index -> Piece
    completed_pieces: list[int] = []

    total_pieces: int = 0
    num_completed: int = 0
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
        self.remaining = file_size
        self.path = path

    def init_pieces(self, piece_size: int, piece_hashes: list[bytes]):
        self.piece_size = piece_size
        self.total_pieces = len(piece_hashes)

        for piece_idx, sha1 in enumerate(piece_hashes):
            self.incomplete_pieces[piece_idx] = Piece(
                min(piece_size, self.file_size - piece_idx * piece_size), sha1)

            self.piece_loc[piece_idx] = piece_idx * piece_size

    def is_complete(self):
        return self.num_completed == self.total_pieces

    def fetch_block(self, piece_idx: int, offset: int, length: int) -> bytes:
        """ Returns data if the piece is complete, None otherwise"""
        if piece_idx in self.completed_pieces:
            with open(self.path, 'rb') as f:
                f.seek(offset)
                data = f.read(length)
            return data

    def add_block(self, piece_idx: int, begin: int, block: bytes) -> int:
        """ Returns piece_idx if piece is complete, None otherwise"""
        if piece_idx in self.completed_pieces:
            piece = self.incomplete_pieces[piece_idx]

            if piece.add_block(begin, block) and piece.validate_hash():
                # Piece is complete & has correct hash -> Write to file
                with open(self.path, "wb") as f:
                    f.seek(self.piece_loc[piece_idx])
                    f.write(bytes(piece.data))

                # Remove data from memory
                del self.incomplete_pieces[piece_idx]
                self.completed_pieces.append(piece_idx)
                self.num_completed += 1
                return piece_idx
