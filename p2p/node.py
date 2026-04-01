"""P2P Node — заглушка."""
class P2PNode:
    def __init__(self, node_id, host, port, bootstrap_nodes, max_hops):
        self.node_id = node_id
        self.host = host
        self.port = port

    async def start(self): pass
    async def stop(self): pass
    def get_stats(self): return {"node_id": self.node_id}
