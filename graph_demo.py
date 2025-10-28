import aerospike
import logging
from typing import Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
config = {
    'hosts': [('localhost', 3000)],
    'policies': {
        'timeout': 5000  # connection timeout in milliseconds
    }
}

def create_graph_operations(client):
    # Create operations with CDT (Complex Data Type) operations
    graph_ns = "test"      # namespace
    vertex_set = "vertex"  # set for vertices
    edge_set = "edge"      # set for edges
    
    def add_vertex(vertex_id: str, vertex_type: str, properties: Dict[str, Any]):
        """Add a vertex to the graph"""
        key = (graph_ns, vertex_set, vertex_id)
        bins = {
            "type": vertex_type,
            "properties": properties,
            "out_WORKS_AT": [],
            "out_KNOWS": [],
            "in_WORKS_AT": [],
            "in_KNOWS": []
        }
        client.put(key, bins)
        logger.info(f"Added vertex: {vertex_type}:{vertex_id}")

    def add_edge(from_id: str, to_id: str, edge_type: str, properties: Dict[str, Any]):
        """Add an edge to the graph"""
        edge_id = f"{from_id}-{edge_type}-{to_id}"
        key = (graph_ns, edge_set, edge_id)
        bins = {
            "from_id": from_id,
            "to_id": to_id,
            "type": edge_type,
            "properties": properties
        }
        client.put(key, bins)
        
        # Add edge reference to vertex
        from_key = (graph_ns, vertex_set, from_id)
        out_edges_bin = f"out_{edge_type}"
        ops = [
            {
                "op": aerospike.OP_LIST_APPEND,
                "bin": out_edges_bin,
                "val": to_id
            }
        ]
        client.operate(from_key, ops)
        
        to_key = (graph_ns, vertex_set, to_id)
        in_edges_bin = f"in_{edge_type}"
        ops = [
            {
                "op": aerospike.OP_LIST_APPEND,
                "bin": in_edges_bin,
                "val": from_id
            }
        ]
        client.operate(to_key, ops)
        
        logger.info(f"Added edge: {edge_type} from {from_id} to {to_id}")

    def get_vertex(vertex_id: str):
        """Get a vertex by ID"""
        key = (graph_ns, vertex_set, vertex_id)
        (_, _, bins) = client.get(key)
        return bins

    def get_neighbors(vertex_id: str, edge_type: str, direction: str = "out"):
        """Get neighboring vertices"""
        vertex_key = (graph_ns, vertex_set, vertex_id)
        (_, _, bins) = client.get(vertex_key)
        
        edge_bin = f"{direction}_{edge_type}"
        neighbor_ids = bins.get(edge_bin, [])
        
        neighbors = []
        for n_id in neighbor_ids:
            neighbor = get_vertex(n_id)
            neighbors.append(neighbor)
        
        return neighbors

    return {
        'add_vertex': add_vertex,
        'add_edge': add_edge,
        'get_vertex': get_vertex,
        'get_neighbors': get_neighbors
    }

def main():
    try:
        # Connect to Aerospike
        client = aerospike.client(config).connect()
        logger.info("Connected to Aerospike")

        # Create graph operations
        graph = create_graph_operations(client)

        # Create sample data
        # People
        graph['add_vertex']('p1', 'person', {
            'name': 'Alice',
            'age': 30,
            'city': 'San Francisco'
        })
        
        graph['add_vertex']('p2', 'person', {
            'name': 'Bob',
            'age': 28,
            'city': 'New York'
        })
        
        graph['add_vertex']('p3', 'person', {
            'name': 'Charlie',
            'age': 35,
            'city': 'San Francisco'
        })

        # Companies
        graph['add_vertex']('c1', 'company', {
            'name': 'Tech Corp',
            'industry': 'Technology',
            'location': 'San Francisco'
        })
        
        graph['add_vertex']('c2', 'company', {
            'name': 'Data Inc',
            'industry': 'Data Analytics',
            'location': 'New York'
        })

        # Relationships
        graph['add_edge']('p1', 'c1', 'WORKS_AT', {
            'role': 'Software Engineer',
            'since': 2020
        })
        
        graph['add_edge']('p2', 'c2', 'WORKS_AT', {
            'role': 'Data Scientist',
            'since': 2021
        })
        
        graph['add_edge']('p3', 'c1', 'WORKS_AT', {
            'role': 'Product Manager',
            'since': 2019
        })

        graph['add_edge']('p1', 'p2', 'KNOWS', {
            'since': 2019,
            'relationship': 'colleague'
        })
        
        graph['add_edge']('p2', 'p3', 'KNOWS', {
            'since': 2020,
            'relationship': 'friend'
        })

        # Query examples
        logger.info("\nFinding where Alice (p1) works:")
        companies = graph['get_neighbors']('p1', 'WORKS_AT')
        for company in companies:
            logger.info(f"Alice works at {company['properties']['name']}")

        logger.info("\nFinding who Bob (p2) knows:")
        friends = graph['get_neighbors']('p2', 'KNOWS')
        for friend in friends:
            logger.info(f"Bob knows {friend['properties']['name']}")

        logger.info("\nFinding employees at Tech Corp (c1):")
        employees = graph['get_neighbors']('c1', 'WORKS_AT', 'in')
        for employee in employees:
            logger.info(f"{employee['properties']['name']} works at Tech Corp")

    except Exception as e:
        logger.error(f"Error: {e}")
        raise e
    finally:
        client.close()
        logger.info("Closed Aerospike connection")

if __name__ == "__main__":
    main()