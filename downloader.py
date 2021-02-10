import random

from session import Session
from file import File, Piece, BlockRequest, InvalidHashException
from peer_manager import PeerManager
from utils import PieceTracker
import time
import asyncio

PEER_WAIT = 3
MAX_PEER_WAIT = 100
NoRequestTimeout = 100
MaxPeerRequests = 5


class NoPeersException(Exception):
    pass


class DownloadComplete(Exception):
    pass


class Downloader:
    session: Session = None
    file: File = None
    peer_manager: PeerManager = None

    assigned_pieces: dict[str, int] = {}
    assigned_requests: dict[int, set[BlockRequest]] = {}
    piece_contributors: dict[int, set[str]] = {}

    piece_tracker: PieceTracker = None
    completed_requests: asyncio.Queue = None

    endgame: bool = False

    def __init__(self, file: File, peer_manager: PeerManager, session: Session, completed_requests: asyncio.Queue):
        self.file = file
        self.peer_manager = peer_manager
        self.session = session
        self.completed_requests = completed_requests

        self.assigned_pieces = {i: set() for i in range(self.file.total_pieces)}
        self.piece_contributors = {i: set() for i in range(self.file.total_pieces)}

        self.piece_tracker = PieceTracker()  # TODO: CONSTRUCT

    def handle_request(self, req: BlockRequest):
        if req.successful:
            try:
                self.file.add_block(req)
                self.piece_contributors[req.piece].add(req.completed_by)

            except InvalidHashException:
                # Deleting existing piece data and recreate BlockRequests
                self.file.reset_piece(req.piece)
                # Blacklist any peers who provided data to this piece.  Not 100% accurate, some peers may just have been
                # assigned to this piece.  # TODO: Make this 100% correct i.e. track who has contributed.
                for contributor in self.piece_contributors[req.piece]:
                    peer = self.peer_manager.peers[contributor]
                    self.peer_manager.blacklisted_peers.add((peer.host, peer.port))
                    self.peer_manager.terminate_peer(peer)
                self.piece_contributors[req.piece] = set()
                # Note do not need to unassign the piece/delete assigned requests
        else:
            req.reset()

    def distribute_requests(self):
        # Only want to issue requests to peers who are interesting and have us unchoked.
        for peer_id in self.session.interesting & self.session.peers_unchoking:
            self.issue_requests(peer_id)

    def _assign_piece(self, peer_id: str):
        # Peer doesn't have a piece assigned to it
        candidate_pieces = self.file.incomplete_pieces & self.session.owned_pieces[peer_id]
        if not candidate_pieces:
            # Note: Shouldn't reach here, but need the sanity check
            return False

        if (available_pieces := candidate_pieces - self.assigned_pieces.keys()):
            # Owns pieces that haven't been assigned.
            candidate_pieces = available_pieces
        # Else just have to double up

        # Assign the rarest piece to this peer
        self.assigned_pieces[peer_id] = self.piece_tracker.get_rarest(candidate_pieces)
        return True

    def issue_requests(self, peer_id: str):
        peer = self.peer_manager.peers[peer_id]
        to_request = MaxPeerRequests - peer.num_pending
        if to_request == 0:
            # return if has max pending requests.
            return

        assigned_piece = self.assigned_pieces[peer_id]
        # Piece num can be 0.  No assigned piece when == None.
        if assigned_piece == None or self.file.pieces[assigned_piece].full():
            if not self._assign_piece(peer_id):
                return

        piece = self.file.pieces[assigned_piece]
        unsent_requests = set(piece.remaining_blocks) - self.assigned_requests[assigned_piece]

        if not unsent_requests or self.endgame:
            # If all of this piece's blocks have been requested to other peers, or in endgame mode, then
            # request the piece's remaining unfulfilled block requests.  Want to finish pieces as quickly as
            # possible (though it is inefficient in the former case).
            possible_reqs = piece.remaining_blocks
        else:
            possible_reqs = unsent_requests
        to_request = random.sample(possible_reqs, min(to_request, len(possible_reqs)))

        for req in to_request:
            peer.send_request(req)
            self.assigned_requests[assigned_piece].add(req)

    async def run(self):
        try:
            while self.session.active:
                try:
                    if not self._have_available_peers():
                        await self.wait_for_peers()
                    self.distribute_requests()

                    # Need timeout incase no peers left
                    req = await asyncio.wait_for(self.completed_requests.get(), timeout=NoRequestTimeout)
                    self.handle_request(req)
                    if self.file.is_complete():
                        raise DownloadComplete()

                except asyncio.TimeoutError:
                    continue
                except NoPeersException:
                    # TODO: LOG
                    break
                except Exception as e:
                    # TODO: LOG unknown
                    pass
        finally:
            self.shutdown()

    def _have_available_peers(self):
        # Can download blocks from peers if they are interesting (have pieces we want) and aren't choking us (aren't
        # ignoring our requests).
        return len(self.session.peers_unchoking & self.session.interesting) > 0

    async def wait_for_peers(self):
        # Waits until a peer connects
        time_waited = 0

        while self.session.active:
            if self.peer_manager.peer_count > 0:
                if self._have_available_peers():
                    return

            if time_waited > MAX_PEER_WAIT:
                raise NoPeersException()

            await asyncio.sleep(PEER_WAIT)
            time_waited += PEER_WAIT

    def shutdown(self):
        self.session.active = False