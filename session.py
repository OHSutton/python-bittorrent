from file import File


class Session:
    active: bool = False
    file: File = None
    peer_status: dict[str, bool] = {}
    peer_pieces: dict[str, list[int]] = []


    piece_owners: dict[int, list[str]] = {}
    piece_counts: dict[int, int] = {}
    piece_updated: bool = False

    def __init__(self):

        self.piece_counts = {i: 0 for i in range(self.file.piece_count)}
        pass

    def terminate_peer(self, peer_id: str):
        for piece in self.peer_pieces[peer_id]:
            self.piece_owners[piece].remove(peer_id)
            self.piece_counts[piece] -= 1
        del self.peer_pieces[peer_id]
        del self.peer_status[peer_id]
        self.piece_updated = True

    def new_peer(self, peer_id):
        self.peer_status[peer_id] = True

    def add_piece_owner(self, peer_id: str, piece: int):
        self.piece_owners[piece].append(peer_id)
        self.piece_counts[piece] += 1
        self.piece_updated = True

    def register_bitfield(self, peer_id: str, bitfield: list[int]):
        for piece, have in enumerate(bitfield):
            if have:
                self.piece_owners[piece].append(peer_id)
                self.piece_counts[piece] += 1
        self.piece_updated = True
