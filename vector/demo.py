"""
Placeholder for vector capability demos with Aerospike.

Planned next steps:
- Add functions to store and query dense vectors in records
- Demonstrate approximate nearest neighbor search using Aerospike's UDFs or external tools
- Provide helper utilities for vector normalization and indexing
"""

from typing import List


def store_vector_example():
    """Store a sample vector record.
    This is a stub — implement when ready to demo vector capabilities.
    """
    vector = [0.1, 0.2, 0.3]
    # TODO: save to Aerospike with a known bin name like 'vec'
    return vector


def knn_query_stub(vector: List[float], k: int = 5):
    """Stub for KNN query against stored vectors."""
    # TODO: Implement ANN or brute-force scan
    return []
