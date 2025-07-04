#!/usr/bin/env python3

import os
import json
import argparse
import asyncio
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

import arxiv
from pymed import PubMed
import openai
from neo4j import GraphDatabase
from dotenv import load_dotenv
from tqdm import tqdm

# Load environment variables
load_dotenv()

def setup_directories():
    """Create necessary directories for storing data."""
    dirs = [
        "data/arxiv/comp_bio",
        "data/arxiv/battery_chem", 
        "data/pubmed/comp_bio",
        "data/pubmed/battery_chem"
    ]
    for dir_path in dirs:
        Path(dir_path).mkdir(parents=True, exist_ok=True)

def scrape_arxiv(query: str, category: str, limit: int) -> List[Dict[str, Any]]:
    """Scrape papers from arXiv."""
    print(f"Scraping arXiv for {category} (limit: {limit})...")
    
    client = arxiv.Client()
    search = arxiv.Search(
        query=query,
        max_results=limit,
        sort_by=arxiv.SortCriterion.Relevance
    )
    
    papers = []
    dir_path = f"data/arxiv/{category}"
    
    for result in tqdm(client.results(search), desc=f"ArXiv {category}", total=limit):
        # Create filename from arXiv ID
        arxiv_id = result.entry_id.split('/')[-1]
        filename = f"{dir_path}/{arxiv_id}.json"
        
        # Skip if file already exists
        if os.path.exists(filename):
            continue
            
        paper_data = {
            "id": arxiv_id,
            "title": result.title,
            "summary": result.summary,
            "authors": [author.name for author in result.authors],
            "published_date": result.published.isoformat(),
            "source": "arxiv"
        }
        
        # Save to JSON file
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(paper_data, f, indent=2, ensure_ascii=False)
            
        papers.append(paper_data)
    
    return papers

def scrape_pubmed(query: str, category: str, limit: int) -> List[Dict[str, Any]]:
    """Scrape papers from PubMed."""
    print(f"Scraping PubMed for {category} (limit: {limit})...")
    
    pubmed = PubMed(tool="SerendipityEngine", email="researcher@example.com")
    papers = []
    dir_path = f"data/pubmed/{category}"
    
    try:
        results = pubmed.query(query, max_results=limit)
        
        for result in tqdm(results, desc=f"PubMed {category}", total=limit):
            pmid = result.pubmed_id
            filename = f"{dir_path}/PMID-{pmid}.json"
            
            # Skip if file already exists
            if os.path.exists(filename):
                continue
                
            paper_data = {
                "id": f"PMID-{pmid}",
                "title": result.title or "",
                "summary": result.abstract or "",
                "authors": [author for author in result.authors] if result.authors else [],
                "published_date": str(result.publication_date) if result.publication_date else "",
                "source": "pubmed"
            }
            
            # Save to JSON file
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(paper_data, f, indent=2, ensure_ascii=False)
                
            papers.append(paper_data)
            
    except Exception as e:
        print(f"Error scraping PubMed: {e}")
    
    return papers

def extract_triples(papers: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    """Extract knowledge triples from papers using OpenAI."""
    print("Extracting knowledge triples...")
    
    client = openai.OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
    all_triples = []
    
    system_prompt = ("You are a precision knowledge extraction AI. Your task is to read scientific abstracts "
                    "and extract meaningful relationships as Subject-Predicate-Object triples. Focus on relationships "
                    "like 'causes', 'inhibits', 'is_a_type_of', 'is_used_for', 'improves', and 'interacts_with'. "
                    "Output ONLY a valid JSON list of objects, where each object has keys 'subject', 'predicate', and 'object'.")
    
    for paper in tqdm(papers, desc="Extracting triples"):
        try:
            # Prepare content for extraction
            content = f"Title: {paper['title']}\nAbstract: {paper['summary']}"
            
            response = client.chat.completions.create(
                model="gpt-4.1-nano",  # Using gpt-4.1-nano as requested
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": content}
                ],
                temperature=0.1,
                max_tokens=1000
            )
            
            # Parse the response
            response_text = response.choices[0].message.content.strip()
            triples = json.loads(response_text)
            
            # Validate and add triples
            for triple in triples:
                if all(key in triple for key in ['subject', 'predicate', 'object']):
                    all_triples.append(triple)
                    
        except Exception as e:
            print(f"Error extracting triples from paper {paper['id']}: {e}")
            continue
    
    return all_triples

def load_to_neo4j(triples: List[Dict[str, str]]):
    """Load extracted triples into Neo4j database."""
    print("Loading triples to Neo4j...")
    
    uri = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
    
    # Connecting without authentication as per user request.
    driver = GraphDatabase.driver(uri)
    
    def create_triples_batch(tx, batch_triples):
        query = """
        UNWIND $triples as triple
        MERGE (s:Entity {name: triple.subject})
        MERGE (o:Entity {name: triple.object})
        MERGE (s)-[r:RELATED_TO {type: triple.predicate}]->(o)
        ON CREATE SET r.count = 1
        ON MATCH SET r.count = r.count + 1
        """
        tx.run(query, triples=batch_triples)
    
    try:
        with driver.session() as session:
            # Process triples in batches of 100
            batch_size = 100
            for i in tqdm(range(0, len(triples), batch_size), desc="Loading to Neo4j"):
                batch = triples[i:i + batch_size]
                session.execute_write(create_triples_batch, batch)
                
        print(f"Successfully loaded {len(triples)} triples to Neo4j")
        
    except Exception as e:
        print(f"Error loading to Neo4j: {e}")
    finally:
        driver.close()

async def main():
    """Main function to orchestrate the serendipity engine."""
    parser = argparse.ArgumentParser(description="Serendipity Engine: Scientific Paper Knowledge Graph Builder")
    parser.add_argument('--limit', type=int, default=50, 
                       help='Maximum number of articles to download per query (default: 50)')
    args = parser.parse_args()
    
    print("🚀 Starting Serendipity Engine...")
    print(f"Article limit per query: {args.limit}")
    
    # Setup directories
    setup_directories()
    
    # Define search queries
    arxiv_queries = {
        'comp_bio': 'cat:q-bio.QM OR cat:q-bio.GN OR (cs.AI AND biology)',
        'battery_chem': 'ti:battery OR ti:batteries OR abs:electrochemistry'
    }
    
    pubmed_queries = {
        'comp_bio': 'computational biology OR systems biology',
        'battery_chem': 'lithium-ion battery OR solid-state electrolyte'
    }
    
    all_papers = []
    
    # Phase 1: Data Acquisition
    print("\n📚 Phase 1: Data Acquisition")
    
    # Scrape arXiv
    for category, query in arxiv_queries.items():
        papers = scrape_arxiv(query, category, args.limit)
        all_papers.extend(papers)
    
    # Scrape PubMed
    for category, query in pubmed_queries.items():
        papers = scrape_pubmed(query, category, args.limit)
        all_papers.extend(papers)
    
    print(f"Total papers collected: {len(all_papers)}")
    
    # Phase 2: Knowledge Graph Construction
    print("\n🧠 Phase 2: Knowledge Graph Construction")
    
    if all_papers:
        # Extract triples
        triples = extract_triples(all_papers)
        print(f"Extracted {len(triples)} knowledge triples")
        
        # Load to Neo4j
        if triples:
            load_to_neo4j(triples)
    
    print("\n✅ Serendipity Engine completed successfully!")

if __name__ == "__main__":
    asyncio.run(main()) 