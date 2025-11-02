# Graph package

This package contains a minimal GraphDB wrapper using the Aerospike Python client.

Files:
- `graph.py` - GraphDB class with basic vertex/edge operations using CDT lists
- `demo.py` - Demo runner that creates sample vertices and edges and performs simple queries
- `vector_demo.py` - Placeholder for upcoming vector capability demos

How to run the demo (from repository root):

```bash
# activate your virtualenv if needed
python graph/demo.py
```

Next steps:
- Implement vector storage and KNN helper functions in `vector_demo.py`
- Add tests and small integration checks
