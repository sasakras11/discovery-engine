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
       "data/arxiv/structural_bio",
        "data/arxiv/battery_chem", 
        "data/pubmed/structural_bio",
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
            # Handle both single and multiple PMIDs
            pmid_str = result.pubmed_id
            if '\n' in pmid_str:
                pmids = pmid_str.split('\n')
            else:
                pmids = [pmid_str]

            for pmid in pmids:
                if not pmid.strip():
                    continue

                filename = f"{dir_path}/PMID-{pmid.strip()}.json"
                
                # Skip if file already exists
                if os.path.exists(filename):
                    continue
                    
                paper_data = {
                    "id": f"PMID-{pmid.strip()}",
                    "title": result.title or "",
                    "summary": result.abstract or "",
                    "authors": [author['firstname'] + ' ' + author['lastname'] for author in result.authors] if result.authors else [],
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
                    # Add paper ID and metadata for traceability
                    triple['paper_id'] = paper['id']
                    triple['paper_title'] = paper['title']
                    triple['paper_summary'] = paper['summary']
                    # Ensure authors is a list of strings (names only)
                    triple['paper_authors'] = [a if isinstance(a, str) else a.get('firstname', '') + ' ' + a.get('lastname', '') for a in paper.get('authors', [])]
                    triple['paper_published_date'] = paper['published_date']
                    triple['paper_source'] = paper['source']
                    all_triples.append(triple)
                    
        except Exception as e:
            print(f"Error extracting triples from paper {paper['id']}: {e}")
            continue
    
    return all_triples

def load_to_neo4j(triples: List[Dict[str, Any]]):
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
        MERGE (p:Paper {id: triple.paper_id})
        ON CREATE SET
            p.title = triple.paper_title,
            p.summary = triple.paper_summary,
            p.authors = triple.paper_authors,
            p.published_date = triple.paper_published_date,
            p.source = triple.paper_source
        MERGE (s)-[r:RELATED_TO {type: triple.predicate}]->(o)
        ON CREATE SET r.count = 1, r.paper_ids = [triple.paper_id]
        ON MATCH SET r.count = r.count + 1,
                       r.paper_ids = CASE WHEN triple.paper_id IN r.paper_ids THEN r.paper_ids ELSE r.paper_ids + [triple.paper_id] END
        MERGE (s)-[:EXTRACTED_FROM]->(p)
        """
        tx.run(query, triples=batch_triples)
    
    try:
        with driver.session() as session:
            # Process triples in batches of 100
            batch_size = 100
            for i in tqdm(range(0, len(triples), batch_size), desc="Loading to Neo4j"):
                batch = triples[i:i + batch_size]
                # Debug: Print first few triples to see structure
                if i == 0:
                    print(f"Debug: First triple structure: {batch[0] if batch else 'No triples'}")
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
        'structural_bio': "'cat:q-bio.BM' OR 'cat:q-bio.SC' OR 'abs:\"structural biology\"'",
        'battery_chem': 'ti:battery OR ti:batteries OR abs:electrochemistry'
    }
    
    pubmed_queries = {
        'structural_bio': '"structural biology" OR "protein structure" OR "cryo-em"',
        'battery_chem': '"lithium-ion battery" OR "solid-state electrolyte"'
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