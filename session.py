

class Session:
    peer_status: dict[str, bool] = {}
    piece_owners: dict[int, list[str]] = {}
    bitfields: dict[str, list[int]] = {}

    def __init__(self):
        pass

    def terminate_peer(self, peer_id):
        del self.peer_status[peer_id]

    def new_peer(self, peer_id):
        self.peer_status[peer_id] = True

    def add_piece_owner(self, peer_id: str, piece: int):
        self.piece_owners[piece].append(peer_id)

    def register_bitfield(self, peer_id: str, bitfield: bytes):
        self.bitfields[peer_id] = bitfield
