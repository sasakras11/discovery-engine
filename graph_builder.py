import os
import json
from typing import Dict, Any, List
from neo4j import GraphDatabase

class GraphBuilder:
    def __init__(self, uri: str, user: str, password: str):
        """
        Initialize Neo4j connection
        """
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        """
        Close Neo4j connection
        """
        self.driver.close()

    def create_paper_node(self, paper: Dict[str, Any]):
        """
        Create a node for a scientific paper with its properties
        """
        with self.driver.session() as session:
            session.write_transaction(self._create_paper_node_tx, paper)

    @staticmethod
    def _create_paper_node_tx(tx, paper: Dict[str, Any]):
        """
        Transaction function to create paper node
        """
        query = """
        CREATE (p:Paper {
            id: $id,
            title: $title,
            abstract: $abstract,
            authors: $authors,
            date: $date,
            source: $source,
            categories: $categories
        })
        """
        tx.run(query, **paper)

    def create_author_nodes(self, paper: Dict[str, Any]):
        """
        Create nodes for authors and relationships to papers
        """
        with self.driver.session() as session:
            session.write_transaction(self._create_author_nodes_tx, paper)

    @staticmethod
    def _create_author_nodes_tx(tx, paper: Dict[str, Any]):
        """
        Transaction function to create author nodes and relationships
        """
        query = """
        MATCH (p:Paper {id: $paper_id})
        WITH p
        UNWIND $authors as author
        MERGE (a:Author {name: author})
        CREATE (a)-[:AUTHORED]->(p)
        """
        tx.run(query, paper_id=paper['id'], authors=paper['authors'])

    def create_category_relationships(self, paper: Dict[str, Any]):
        """
        Create relationships between papers and their categories
        """
        with self.driver.session() as session:
            session.write_transaction(self._create_category_relationships_tx, paper)

    @staticmethod
    def _create_category_relationships_tx(tx, paper: Dict[str, Any]):
        """
        Transaction function to create category relationships
        """
        query = """
        MATCH (p:Paper {id: $paper_id})
        WITH p
        UNWIND $categories as category
        MERGE (c:Category {name: category})
        CREATE (p)-[:BELONGS_TO]->(c)
        """
        tx.run(query, paper_id=paper['id'], categories=paper['categories'])

def load_papers(directory: str) -> List[Dict[str, Any]]:
    """
    Load paper data from JSON files in directory
    """
    papers = []
    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            with open(os.path.join(directory, filename), 'r') as f:
                papers.append(json.load(f))
    return papers

def main():
    # Neo4j connection details
    uri = "bolt://localhost:7687"
    user = "neo4j"
    password = "password"  # Change this to your Neo4j password

    # Initialize graph builder
    builder = GraphBuilder(uri, user, password)

    # Data directories to process
    data_dirs = [
        'data/arxiv/comp_bio',
        'data/arxiv/battery_chem',
        'data/pubmed/comp_bio',
        'data/pubmed/battery_chem'
    ]

    try:
        # Process each directory
        for directory in data_dirs:
            if os.path.exists(directory):
                papers = load_papers(directory)
                for paper in papers:
                    builder.create_paper_node(paper)
                    builder.create_author_nodes(paper)
                    builder.create_category_relationships(paper)
    finally:
        builder.close()

if __name__ == '__main__':
    main() 