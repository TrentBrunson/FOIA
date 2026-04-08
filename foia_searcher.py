# ================================================
# SCRIPT 2: FOIA Logs Semantic + Keyword Searcher
# ================================================
# Run AFTER Script 1. 
# Supports:
#   • Simple keyword search (pandas)
#   • Full semantic search (local embeddings)
#   • Optional Elasticsearch indexing + semantic search

# pip install pandas tqdm sentence-transformers elasticsearch numpy

import pandas as pd
from pathlib import Path
import sys
from tqdm import tqdm
import numpy as np
from datetime import datetime

# ----------------- ELASTICSEARCH (OPTIONAL) -----------------
# If you want Elasticsearch:
#   1. Run Elasticsearch locally (or Docker: docker run -p 9200:9200 -e "discovery.type=single-node" elasticsearch:8.15.0)
#   2. pip install elasticsearch
#   3. Uncomment the ELASTIC block

try:
    from elasticsearch import Elasticsearch
    from sentence_transformers import SentenceTransformer
    ELASTIC_AVAILABLE = True
except ImportError:
    ELASTIC_AVAILABLE = False

def load_all_logs(folder_path: str) -> pd.DataFrame:
    folder = Path(folder_path).expanduser().resolve()
    csv_files = list(folder.glob("**/*foia-log*.csv"))
    print(f"Found {len(csv_files)} log files. Loading...")
    
    dfs = []
    for f in tqdm(csv_files, desc="Loading CSVs"):
        try:
            df = pd.read_csv(f, low_memory=False)
            df["source_file"] = f.name
            dfs.append(df)
        except Exception as e:
            print(f"⚠️  Could not read {f.name}: {e}")
    
    if not dfs:
        print("❌ No valid CSV files found!")
        sys.exit(1)
    
    return pd.concat(dfs, ignore_index=True)

def keyword_search(df: pd.DataFrame, query: str):
    query = query.lower()
    mask = (
        df.astype(str).apply(lambda x: x.str.contains(query, case=False, na=False)).any(axis=1)
    )
    return df[mask]

def semantic_search_local(df: pd.DataFrame, query: str, top_k: int = 50):
    print("🔍 Generating embeddings for semantic search (first run may take a minute)...")
    model = SentenceTransformer("all-MiniLM-L6-v2")
    
    # Combine relevant text columns for embedding
    text_cols = [col for col in df.columns if col in ["Description", "Requester", "Subject", "Company"]]
    df["search_text"] = df[text_cols].fillna("").agg(" | ".join, axis=1)
    
    embeddings = model.encode(df["search_text"].tolist(), show_progress_bar=True)
    query_emb = model.encode(query)
    
    scores = np.dot(embeddings, query_emb)
    top_indices = np.argsort(scores)[::-1][:top_k]
    
    results = df.iloc[top_indices].copy()
    results["semantic_score"] = scores[top_indices]
    return results

def index_to_elasticsearch(df: pd.DataFrame, index_name="sec_foia_logs"):
    if not ELASTIC_AVAILABLE:
        print("❌ Elasticsearch not installed.")
        return
    
    es = Elasticsearch("http://localhost:9200")
    
    # Create index with dense_vector for semantic search
    if not es.indices.exists(index=index_name):
        mapping = {
            "mappings": {
                "properties": {
                    "Description": {"type": "text"},
                    "Requester": {"type": "text"},
                    "search_text": {"type": "text"},
                    "embedding": {
                        "type": "dense_vector",
                        "dims": 384,
                        "index": True,
                        "similarity": "cosine"
                    },
                    "source_file": {"type": "keyword"}
                }
            }
        }
        es.indices.create(index=index_name, body=mapping)
    
    model = SentenceTransformer("all-MiniLM-L6-v2")
    text_col = df["search_text"] if "search_text" in df.columns else df.astype(str).agg(" | ".join, axis=1)
    embeddings = model.encode(text_col.tolist())
    
    for i, row in tqdm(df.iterrows(), total=len(df), desc="Indexing to Elasticsearch"):
        doc = row.to_dict()
        doc["embedding"] = embeddings[i].tolist()
        es.index(index=index_name, id=i, body=doc)
    
    print(f"✅ Indexed {len(df)} records to Elasticsearch index '{index_name}'")

if __name__ == "__main__":
    folder = input("Enter folder containing the downloaded logs: ")
    df = load_all_logs(folder)
    
    print(f"\nTotal records loaded: {len(df):,}")
    print("Columns:", list(df.columns))
    
    while True:
        print("\n" + "="*60)
        print("Search Options:")
        print("1. Keyword search")
        print("2. Semantic search (local embeddings)")
        if ELASTIC_AVAILABLE:
            print("3. Index to Elasticsearch (once only)")
            print("4. Semantic search via Elasticsearch")
        print("0. Quit")
        
        choice = input("\nChoose (0-4): ").strip()
        
        if choice == "0":
            break
        if choice == "1":
            q = input("Enter keyword/phrase: ")
            results = keyword_search(df, q)
        elif choice == "2":
            q = input("Enter semantic query: ")
            results = semantic_search_local(df, q)
        elif choice == "3" and ELASTIC_AVAILABLE:
            index_to_elasticsearch(df)
            continue
        elif choice == "4" and ELASTIC_AVAILABLE:
            q = input("Enter semantic query for Elasticsearch: ")
            # Simple knn search example
            model = SentenceTransformer("all-MiniLM-L6-v2")
            query_vec = model.encode(q).tolist()
            es = Elasticsearch("http://localhost:9200")
            resp = es.search(
                index="sec_foia_logs",
                body={
                    "knn": {
                        "field": "embedding",
                        "query_vector": query_vec,
                        "k": 50,
                        "num_candidates": 100
                    }
                }
            )
            hits = resp["hits"]["hits"]
            results = pd.DataFrame([hit["_source"] for hit in hits])
        else:
            print("Invalid choice.")
            continue
        
        print(f"\nFound {len(results):,} matching records:")
        print(results[["source_file", "Description", "Requester"]].head(20))
        
        export = input("Export results to CSV? (y/n): ").lower() == "y"
        if export:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            results.to_csv(f"foia_search_results_{ts}.csv", index=False)
            print("✅ Exported!")
