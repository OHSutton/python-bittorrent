# Class that coordinates the distribution of downloaded pieces to other peers.
# Employs optimistic choking to control upload rates
import random

from peer_manager import PeerManager
from peer import Peer
from session import Session
import asyncio


# TODO: Still needs anti-snubbing rules + upload-only seeding (when the download completes).
class Seeder:
    peer_manager: PeerManager = None
    session: Session = None

    current_unchoked: set[Peer] = []
    optimistic_unchoke: Peer = None

    CHOKING_WAIT: int = 10
    OPTIMISTIC_UNCHOKE_RATE: int = 3
    MAX_UNCHOKED: int = 4

    def __init__(self, peer_manager: PeerManager, session: Session):
        self.peer_manager = peer_manager
        self.session = session

    async def run(self):
        iteration = 1

        while self.session.active:
            candidates = list(self.peer_manager.peers.values())
            candidates.sort(key=lambda p: self.session.peer_download_rates[p.their_id].rate(), reverse=True)

            unchoked_count = 0

            unchoke = set()
            #  Unchoke peers which have uploaded the most to us and are interested in downloading from us.
            for candidate in candidates[:]:
                if candidate.peer_interested:
                    unchoke.add(candidate)
                    unchoked_count += 1
                    candidates.remove(candidate)
                    if unchoked_count == self.MAX_UNCHOKED:
                        break

            # If len(unchoke) < 4, make up the difference.
            # Ensures 4 peers unchoked at a time (excluding optimistic unchoke)
            unchoke.update(candidates[:self.MAX_UNCHOKED - unchoked_count])
            candidates = candidates[self.MAX_UNCHOKED - unchoked_count:]
            optim_unchoke = None

            if iteration % self.OPTIMISTIC_UNCHOKE_RATE == 0:
                # Select optimistic unchoke
                if candidates:
                    # TODO: Increase weighting for new peers
                    optim_unchoke = random.choice(candidates)

            for peer in unchoke.union({optim_unchoke}) - self.current_unchoked.union({self.optimistic_unchoke}):
                if peer:
                    peer.am_choking = True

            for peer in self.current_unchoked.union({self.optimistic_unchoke}) - unchoke.union({optim_unchoke}):
                if peer:
                    peer.am_choking = False

            self.optimistic_unchoke = optim_unchoke
            self.current_unchoked = unchoke

            iteration += 1
            await asyncio.sleep(self.CHOKING_WAIT)
