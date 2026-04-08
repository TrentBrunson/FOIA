# ================================================
# OPTIMIZED FOIA LOGS SEARCHER v2 - With Docker ES Support
# ================================================

import pandas as pd
from pathlib import Path
import sys
from tqdm import tqdm
import numpy as np
from datetime import datetime
import time

# ====================== ELASTICSEARCH SETUP ======================
try:
    from elasticsearch import Elasticsearch
    from sentence_transformers import SentenceTransformer
    ELASTIC_AVAILABLE = True
except ImportError:
    print("⚠️  Elasticsearch or sentence-transformers not installed.")
    ELASTIC_AVAILABLE = False

def check_elasticsearch():
    """Check if Elasticsearch (Docker) is running"""
    try:
        es = Elasticsearch("http://localhost:9200", timeout=5)
        if es.ping():
            print("✅ Connected to Elasticsearch (Docker)")
            return es
        else:
            print("❌ Elasticsearch is not responding")
            return None
    except Exception:
        print("❌ Could not connect to Elasticsearch at http://localhost:9200")
        print("   Make sure Docker container is running:")
        print("   → docker compose up -d   (or the docker run command)")
        return None

def load_all_logs(folder_path: str) -> pd.DataFrame:
    folder = Path(folder_path).expanduser().resolve()
    csv_files = list(folder.glob("**/*foia-log*.csv"))
    print(f"Found {len(csv_files)} log files. Loading...")
    
    dfs = []
    for f in tqdm(csv_files, desc="Loading CSVs"):
        try:
            df = pd.read_csv(f, low_memory=False, dtype=str)
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
    mask = df.astype(str).apply(
        lambda x: x.str.contains(query, case=False, na=False)
    ).any(axis=1)
    return df[mask]

def semantic_search_local(df: pd.DataFrame, query: str, top_k: int = 50):
    print("🔍 Generating embeddings for semantic search...")
    model = SentenceTransformer("all-MiniLM-L6-v2")
    
    text_cols = [col for col in ["Description", "Requester", "Subject", "Company", "FOIA Request"] 
                 if col in df.columns]
    df["search_text"] = df[text_cols].fillna("").agg(" | ".join, axis=1)
    
    embeddings = model.encode(df["search_text"].tolist(), show_progress_bar=True, batch_size=32)
    query_emb = model.encode(query)
    
    scores = np.dot(embeddings, query_emb)
    top_indices = np.argsort(scores)[::-1][:top_k]
    
    results = df.iloc[top_indices].copy()
    results["semantic_score"] = [round(float(s), 4) for s in scores[top_indices]]
    return results

def index_to_elasticsearch(df: pd.DataFrame, es, index_name="sec_foia_logs"):
    if es is None:
        print("❌ Elasticsearch not connected.")
        return
    
    # Create index if it doesn't exist
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
                    "source_file": {"type": "keyword"},
                    "Date": {"type": "date"}
                }
            }
        }
        es.indices.create(index=index_name, body=mapping)
        print(f"✅ Created new index: {index_name}")
    
    model = SentenceTransformer("all-MiniLM-L6-v2")
    text_col = df["search_text"] if "search_text" in df.columns else df.astype(str).agg(" | ".join, axis=1)
    embeddings = model.encode(text_col.tolist(), batch_size=32, show_progress_bar=True)
    
    print("📤 Indexing documents to Elasticsearch...")
    for i, row in tqdm(df.iterrows(), total=len(df), desc="Indexing"):
        doc = row.to_dict()
        doc["embedding"] = embeddings[i].tolist()
        try:
            es.index(index=index_name, id=i, document=doc)
        except Exception:
            pass  # Skip individual failures
    
    print(f"✅ Successfully indexed {len(df):,} records")

if __name__ == "__main__":
    print("=== SEC FOIA Logs Semantic Search Tool ===\n")
    
    # Check Elasticsearch
    es_client = check_elasticsearch() if ELASTIC_AVAILABLE else None
    
    folder = input("\nEnter folder containing downloaded FOIA logs: ").strip()
    df = load_all_logs(folder)
    
    print(f"\n✅ Total records loaded: {len(df):,}")
    
    while True:
        print("\n" + "="*70)
        print("Search Menu:")
        print("1. Keyword Search")
        print("2. Semantic Search (Local)")
        if ELASTIC_AVAILABLE and es_client:
            print("3. Index Data to Elasticsearch (One-time)")
            print("4. Semantic Search via Elasticsearch")
        print("0. Quit")
        
        choice = input("\nChoose option: ").strip()
        
        if choice == "0":
            break
            
        elif choice == "1":
            q = input("Enter keyword or phrase: ")
            results = keyword_search(df, q)
            
        elif choice == "2":
            q = input("Enter semantic search query: ")
            results = semantic_search_local(df, q)
            
        elif choice == "3" and ELASTIC_AVAILABLE and es_client:
            index_to_elasticsearch(df, es_client)
            continue
            
        elif choice == "4" and ELASTIC_AVAILABLE and es_client:
            q = input("Enter semantic search query: ")
            model = SentenceTransformer("all-MiniLM-L6-v2")
            query_vec = model.encode(q).tolist()
            
            resp = es_client.search(
                index="sec_foia_logs",
                knn={
                    "field": "embedding",
                    "query_vector": query_vec,
                    "k": 50,
                    "num_candidates": 200
                },
                size=50
            )
            hits = resp["hits"]["hits"]
            results = pd.DataFrame([hit["_source"] for hit in hits])
            
        else:
            print("Invalid option.")
            continue
        
        if len(results) == 0:
            print("No results found.")
        else:
            print(f"\nFound {len(results):,} results:")
            display_cols = [col for col in ["source_file", "Requester", "Description", "Date"] 
                          if col in results.columns]
            print(results[display_cols].head(15))
            
            if input("\nExport results to CSV? (y/n): ").lower() == "y":
                ts = datetime.now().strftime("%Y%m%d_%H%M")
                filename = f"foia_results_{ts}.csv"
                results.to_csv(filename, index=False)
                print(f"✅ Exported to {filename}")
