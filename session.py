from utils import RollingAverage20, PieceTracker
from file import File


class Session:
    active: bool = False
    file: File = None

    uploaded: int = 0
    piece_tracker: PieceTracker = None
    peer_download_rates: dict[str, RollingAverage20] = {}  # TODO: Hook up to peer

    peers_unchoking: set[str] = set()
    interesting: set[str] = set()

    num_file_pieces: int = 0
    # Maps piece num -> peer owners
    piece_owners: dict[int, set[str]] = {}
    # Maps peer_id-> pieces owned
    owned_pieces: dict[str, set[int]] = {}

    def __init__(self, file: File, piece_tracker: PieceTracker):
        self.piece_owners = {i: set() for i in range(file.total_pieces)}
        self.piece_tracker = piece_tracker
        self.file = file

    def terminate_peer(self, peer_id: str):
        for piece, owners in self.piece_owners.items():
            if peer_id in owners:
                self.piece_owners[piece].remove(peer_id)
                self.piece_tracker.update(self.piece_tracker.get_rarity(piece) - 1, piece)

        del self.peer_download_rates[peer_id]
        del self.owned_pieces[peer_id]

        if peer_id in self.interesting:
            self.interesting.remove(peer_id)
        if peer_id in self.peers_unchoking:
            self.peers_unchoking.remove(peer_id)

    def add_piece_owner(self, peer_id: str, piece: int):
        self.owned_pieces[peer_id].add(piece)
        self.piece_owners[piece].add(peer_id)
        self.piece_tracker.update(self.piece_tracker.get_rarity(piece) + 1, piece)

    def add_peer(self, peer_id: str):
        self.peer_download_rates[peer_id] = RollingAverage20()
        self.owned_pieces[peer_id] = set()

    def am_unchoked(self, peer_id: str):
        self.peers_unchoking.add(peer_id)

    def am_choked(self, peer_id: str):
        if peer_id in self.peers_unchoking:
            self.peers_unchoking.remove(peer_id)

    def register_bitfield(self, peer_id: str, bitfield: list[int]):
        interesting = False
        for piece, have in enumerate(bitfield):
            if have:
                interesting = interesting and self.add_piece_owner(peer_id, piece)
        return interesting