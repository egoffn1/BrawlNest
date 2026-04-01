"""Network service — управление узлами и маршрутизацией."""
from __future__ import annotations
from typing import Dict
from utils.logger import setup_logger
logger = setup_logger(__name__)

class NetworkService:
    def __init__(self, peer):
        self.peer = peer
        self.p2p_node = None

    async def get_status(self) -> Dict:
        nodes = await self.peer.get_all_nodes_with_ping()
        return {
            "current_node": self.peer.node_address,
            "node_id": self.peer.node_id,
            "active_nodes": len(nodes),
            "http_peer_list": nodes,
        }
