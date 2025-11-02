import aerospike
import logging
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

class GraphDB:
    def __init__(self, client: aerospike.Client, namespace: str = "test"):
        self.client = client
        self.ns = namespace
        self.vertex_set = "vertex"
        self.edge_set = "edge"

    def add_vertex(self, vertex_id: str, vertex_type: str, properties: Dict[str, Any]):
        key = (self.ns, self.vertex_set, vertex_id)
        bins = {
            "type": vertex_type,
            "properties": properties,
            # initialize common edge bins lazily is fine; include some common ones here
            "out_WORKS_AT": [],
            "out_KNOWS": [],
            "in_WORKS_AT": [],
            "in_KNOWS": []
        }
        self.client.put(key, bins)
        logger.info(f"Added vertex: {vertex_type}:{vertex_id}")

    def add_edge(self, from_id: str, to_id: str, edge_type: str, properties: Dict[str, Any]):
        edge_id = f"{from_id}-{edge_type}-{to_id}"
        key = (self.ns, self.edge_set, edge_id)
        bins = {
            "from_id": from_id,
            "to_id": to_id,
            "type": edge_type,
            "properties": properties
        }
        self.client.put(key, bins)

        # Append references to vertices
        from_key = (self.ns, self.vertex_set, from_id)
        out_edges_bin = f"out_{edge_type}"
        ops = [
            {
                "op": aerospike.OP_LIST_APPEND,
                "bin": out_edges_bin,
                "val": to_id
            }
        ]
        self.client.operate(from_key, ops)

        to_key = (self.ns, self.vertex_set, to_id)
        in_edges_bin = f"in_{edge_type}"
        ops = [
            {
                "op": aerospike.OP_LIST_APPEND,
                "bin": in_edges_bin,
                "val": from_id
            }
        ]
        self.client.operate(to_key, ops)

        logger.info(f"Added edge: {edge_type} from {from_id} to {to_id}")

    def get_vertex(self, vertex_id: str) -> Dict[str, Any]:
        key = (self.ns, self.vertex_set, vertex_id)
        (_, _, bins) = self.client.get(key)
        return bins

    def get_neighbors(self, vertex_id: str, edge_type: str, direction: str = "out") -> List[Dict[str, Any]]:
        vertex_key = (self.ns, self.vertex_set, vertex_id)
        (_, _, bins) = self.client.get(vertex_key)
        edge_bin = f"{direction}_{edge_type}"
        neighbor_ids = bins.get(edge_bin, [])

        neighbors = []
        for n_id in neighbor_ids:
            neighbor = self.get_vertex(n_id)
            neighbors.append(neighbor)
        return neighbors
