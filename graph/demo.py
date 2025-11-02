import logging
from typing import Dict, Any
import aerospike
from .graph import GraphDB

logger = logging.getLogger(__name__)

config = {
    'hosts': [('localhost', 3000)],
    'policies': {
        'timeout': 5000
    }
}


def run_demo():
    client = aerospike.client(config).connect()
    logger.info("Connected to Aerospike (graph demo)")

    g = GraphDB(client)

    # Create sample data
    g.add_vertex('p1', 'person', {'name': 'Alice', 'age': 30, 'city': 'San Francisco'})
    g.add_vertex('p2', 'person', {'name': 'Bob', 'age': 28, 'city': 'New York'})
    g.add_vertex('p3', 'person', {'name': 'Charlie', 'age': 35, 'city': 'San Francisco'})

    g.add_vertex('c1', 'company', {'name': 'Tech Corp', 'industry': 'Technology', 'location': 'San Francisco'})
    g.add_vertex('c2', 'company', {'name': 'Data Inc', 'industry': 'Data Analytics', 'location': 'New York'})

    g.add_edge('p1', 'c1', 'WORKS_AT', {'role': 'Software Engineer', 'since': 2020})
    g.add_edge('p2', 'c2', 'WORKS_AT', {'role': 'Data Scientist', 'since': 2021})
    g.add_edge('p3', 'c1', 'WORKS_AT', {'role': 'Product Manager', 'since': 2019})

    g.add_edge('p1', 'p2', 'KNOWS', {'since': 2019, 'relationship': 'colleague'})
    g.add_edge('p2', 'p3', 'KNOWS', {'since': 2020, 'relationship': 'friend'})

    logger.info("\nFinding where Alice (p1) works:")
    companies = g.get_neighbors('p1', 'WORKS_AT')
    for company in companies:
        logger.info(f"Alice works at {company['properties']['name']}")

    logger.info("\nFinding who Bob (p2) knows:")
    friends = g.get_neighbors('p2', 'KNOWS')
    for friend in friends:
        logger.info(f"Bob knows {friend['properties']['name']}")

    logger.info("\nFinding employees at Tech Corp (c1):")
    employees = g.get_neighbors('c1', 'WORKS_AT', 'in')
    for employee in employees:
        logger.info(f"{employee['properties']['name']} works at Tech Corp")

    client.close()
    logger.info("Closed Aerospike connection (graph demo)")


if __name__ == "__main__":
    run_demo()