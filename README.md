# RiG
Retrieval over ifcJSON Graphs _for AI-Native CMMS_


### IFC-GraphRAG-CMMS

Goal: Load IFC (ifcJSON) into a property graph, power GraphRAG + LLM, and enable safe CRUD for CMMS.

### Repo layout

## Architecture Overview

```
+-------------------+      +-------------------+      +-------------------+
|   data/raw        | ---> |   ingest/         | ---> |   neo4j/          |
| (IFC, ifcJSON)    |      | (ETL scripts)     |      | (Graph DB Docker) |
+-------------------+      +-------------------+      +-------------------+
				|                        |                           |
				v                        v                           v
+-------------------+      +-------------------+      +-------------------+
|   rag/            | <--- |   graph/          | <--- |   scripts/        |
| (RAG, LLM, FAISS) |      | (Cypher schema)   |      | (Utils, checks)   |
+-------------------+      +-------------------+      +-------------------+
```

- **data/raw**: Source IFC and ifcJSON files (e.g., architectural, mechanical models).
- **ingest/**: Scripts to load and transform ifcJSON into Neo4j property graphs.
- **neo4j/**: Dockerized Neo4j database, with plugins (APOC) and persistent volumes.
- **graph/**: Cypher schema and constraints for graph structure.
- **rag/**: Retrieval-Augmented Generation (RAG) pipeline, embeddings, and FAISS index.
- **scripts/**: Utility scripts (e.g., connectivity checks).

---

## Key Libraries

- **ifcopenshell**: Reads and writes IFC files, used for merging models.
- **neo4j**: Python driver for interacting with the Neo4j graph database.
- **dotenv**: Loads environment variables from `.env` files.
- **sentence-transformers**: Generates text embeddings for semantic search.
- **faiss**: Efficient similarity search and clustering of dense vectors.
- **numpy**: Numerical operations, especially for embeddings.

---

## Main Functions & Scripts

### 1. IFC File Merging
- `data/raw/ifc/sample_hospital/merge_ifc_files.py`
	- Merges architectural and mechanical IFC files using ifcopenshell.
	- Outputs a combined IFC file for unified graph ingestion.

### 2. Ingestion to Neo4j
- `ingest/ifcjson_to_neo4j.py`
	- Loads ifcJSON, extracts entities, properties, and relationships.
	- Maps IFC types to CMMS classes, flattens property sets, and creates nodes/edges in Neo4j.
	- Handles spatial, system, and connectivity relationships.

### 3. Graph Schema
- `graph/schema.cypher`
	- Defines constraints and indexes for efficient querying in Neo4j.

### 4. RAG Pipeline
- `rag/build_index.py`
	- Fetches nodes and context from Neo4j.
	- Builds text “cards” for each node, generates embeddings, and creates a FAISS index.
- `rag/query.py`
	- Uses semantic and lexical search to find relevant nodes for a question.
	- Expands graph neighborhoods and builds evidence for LLM-based answers.
- `rag/answer.py`
	- Loads evidence and prints summary information (e.g., rooms, storeys).

### 5. Utility Scripts
- `scripts/bolt_check.py`
	- Verifies Neo4j connectivity and APOC plugin status.

---

## Example Data Flow

1. **Merge IFC files** (if needed) → `merge_ifc_files.py`
2. **Convert to ifcJSON** (external or via script)
3. **Ingest to Neo4j** → `ifcjson_to_neo4j.py`
4. **Apply schema** → `schema.cypher`
5. **Build RAG index** → `build_index.py`
6. **Query graph** → `query.py`, `answer.py`

---

## Diagram: RAG Pipeline

```
[Neo4j Graph]
		 |
		 v
[build_index.py] --(embeddings)--> [FAISS Index]
		 |
		 v
[query.py] <--- User Question
		 |
		 v
[answer.py] --(evidence)--> Output
```

---

## Explanations

- **ifcopenshell**: Used for IFC file manipulation and merging.
- **neo4j**: All graph operations (nodes, relationships, queries).
- **dotenv**: Centralizes configuration for database and model settings.
- **sentence-transformers**: Powers semantic search and retrieval.
- **faiss**: Enables fast vector similarity search for RAG.
- **numpy**: Handles vector math for embeddings.
