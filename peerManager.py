from peer import Peer
from session import Session

class PeerManager:
    max_peers = 15

    peers: dict[str, Peer] = {}
    peer_count: int = 0
    session: Session = None

    def terminate_peer(self, peer_id: str):
        pass
