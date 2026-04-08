# FOIA
FOIA DB searches with vector store and elastic

Polite scripts for simple and full semantiv searches.

Run pip install requests beautifulsoup4 tqdm pandas sentence-transformers elasticsearch numpy (Elasticsearch client optional).
Run Script 1 first → choose a folder.
Run Script 2 → point it at that folder and search.

Enhancements:
Concurrent downloads.  Emhanced error handling and retry logic.  Progress tracking and reporting.

### Docker

BASH
Run Elasticsearch 8.x with everything you need for semantic search
docker run -d \
  --name elasticsearch \
  -p 9200:9200 \
  -p 9300:9300 \
  -e "discovery.type=single-node" \
  -e "xpack.security.enabled=false" \
  -e "ES_JAVA_OPTS=-Xms1g -Xmx1g" \
  --restart unless-stopped \
  elasticsearch:8.15.0

  then...
  docker compose up -d

  validation check - running:
  docker logs elasticsearch
  curl -X GET "http://localhost:9200"

  Search script now automatically connects to this Docker setup

