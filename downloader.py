from session import Session
from file import File, BlockRequest, InvalidHashException
from peerManager import PeerManager
import time
import asyncio

MaxPeerWait = 100
NoRequestTimeout = 100
MaxPeerRequests = 5


class NoPeersException(Exception):
    pass


class IncompleteDownloadException(Exception):
    pass


class Downloader:
    session: Session = None
    file: File = None
    peer_manager: PeerManager = None

    invalid_requests: list[BlockRequest] = []  # Pieces that no one is seeding
    to_request: list[BlockRequest] = []
    completed_requests: asyncio.Queue = None

    endgame: bool = False

    interesting_peers = []

    def __init__(self, file: File, peer_manager: PeerManager, session: Session, completed_requests: asyncio.Queue):
        self.file = file
        self.peer_manager = peer_manager
        self.session = session
        self.completed_requests = completed_requests
        self.piece_rarity = [0] * self.file.piece_count
        self.create_requests()

    # Assumes we have peers.  Recomputes rarities and reorders request queue
    def refresh_rarities(self):

        combined = self.to_request + self.invalid_requests
        self.invalid_requests, self.to_request = [], []
        raritys = [self.session.piece_counts[req.piece] for req in combined]

        for rarity, req in sorted(zip(raritys, combined), key=lambda x: x[0]):
            if rarity:
                self.to_request.append(req)
            else:
                self.invalid_requests.append(req)  # No seeders

    def set_interesting_peers(self):
        new_peers = []
        for peer, pieces in self.session.peer_pieces:
            for piece in pieces:
                if piece in self.file.incomplete_pieces:
                    self.interesting_peers.append(peer)

        if self.interesting_peers:
            # If already have interesting peers, tell newly added peers we're interested
            for new_peer in set(new_peers).difference(set(self.interesting_peers)):
                self.peer_manager.peers[new_peer].am_interested = True

            # If not interested in old peers, tell them
            for new_peer in set(self.interesting_peers).difference(set(new_peers)):
                self.peer_manager.peers[new_peer].am_interested = False
        self.interesting_peers = new_peers

    def create_requests(self):
        for piece_num, piece in self.file.incomplete_pieces:
            for req in piece.remaining_blocks:
                self.to_request.append(req)

    def purge_dead_peers(self):
        for peer_id, alive in self.session.peer_status:
            if not alive:
                self.session.terminate_peer(peer_id)
                peer = self.peer_manager.peers[peer_id]
                for req in peer.pending_requests:
                    self.to_request.append(req)
                self.peer_manager.terminate_peer(peer_id)
                self.session.piece_updated = True

    def wait_for_peers(self):
        # Waits until a peer connects
        sleeping = 0
        while self.peer_manager.peer_count == 0:
            time.sleep(1)
            sleeping += 1
            if sleeping > MaxPeerWait:
                raise NoPeersException()

    def send_requests(self):
        for peer_id in self.interesting_peers:
            peer = self.peer_manager.peers[peer_id]

            while peer.num_pending < MaxPeerRequests and not peer.peer_choking and self.to_request:
                num_reqs = len(self.to_request)
                for req in self.to_request:
                    if req.piece in self.session.peer_pieces[peer_id]:
                        peer.send_request(req)
                        if not self.endgame:
                            self.to_request.remove(req)
                        break
                if len(self.to_request) == num_reqs:
                    # No suitable pieces
                    break

    def handle_request(self, req: BlockRequest):
        if req.successful:
            try:
                self.file.add_block(req)
                if req in self.to_request:
                    # In endgame requests are kept in self.to_request, so remove it
                    # send_cancels here maybe
                    self.to_request.remove(req)
            except InvalidHashException:
                # Deleting existing piece data and recreate BlockRequests
                self.file.reset_piece(req.piece)
                for req in self.file.incomplete_pieces[req.piece].remaining_blocks:
                    self.to_request.append(req)
        else:
            if req in self.to_request:
                # In endgame requests are kept in self.to_request, so remove it
                self.to_request.remove(req)
            req.reset()
            self.to_request.append(req)

    def shutdown(self):
        self.session.active = False

    async def active(self):
        self.purge_dead_peers()
        self.wait_for_peers()
        self.refresh_rarities()
        self.set_interesting_peers()
        self.send_requests()

        while True:
            try:
                # Need timeout incase no peers left
                req = await asyncio.wait_for(self.completed_requests.get(), timeout=NoRequestTimeout)
                self.handle_request(req)
                if self.file.is_complete():
                    self.shutdown()
                    break

                # self.send_cancels(req)  # Do maybe in future
                self.purge_dead_peers()
                self.wait_for_peers()
                if self.session.piece_updated:
                    self.refresh_rarities()
                    self.set_interesting_peers()
                    self.session.piece_updated = False
                self.send_requests()

            except asyncio.TimeoutError:
                if self.peer_manager.peer_count == 0:
                    self.shutdown()
                    break


