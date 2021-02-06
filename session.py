class Session:
    active: bool = False

    uploaded: int = 0
    num_file_pieces: int = 0
    # Maps piece num -> peer owners
    piece_owners: dict[int, set[str]] = {}
    # Maps piece num -> num peers that own it  (kinda redundant but saves having to repeatedly len() piece_owners)
    piece_counts: dict[int, int] = {}

    piece_updated: bool = False

    def __init__(self, num_file_pieces: int):
        self.num_file_pieces = num_file_pieces
        self.piece_counts = {i: 0 for i in range(num_file_pieces)}
        self.piece_owners = {i: set() for i in range(num_file_pieces)}

    def terminate_peer(self, peer_id: str):
        for piece, owners in self.piece_owners.items():
            if peer_id in owners:
                self.piece_owners[piece].remove(peer_id)
                self.piece_counts[piece] -= 1
                self.piece_updated = True

    def add_piece_owner(self, peer_id: str, piece: int):
        self.piece_owners[piece].add(peer_id)
        self.piece_counts[piece] += 1
        self.piece_updated = True

    def register_bitfield(self, peer_id: str, bitfield: list[int]):
        for piece, have in enumerate(bitfield):
            if have:
                self.add_piece_owner(peer_id, piece)
