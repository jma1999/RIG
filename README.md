# RiG
Retrieval over ifcJSON Graphs _for AI-Native CMMS_


### IFC-GraphRAG-CMMS

Goal: Load IFC (ifcJSON) into a property graph, power GraphRAG + LLM, and enable safe CRUD for CMMS.

### Repo layout
- `data/raw`: source IFC + ifcJSON
- `data/processed`: exports, graph dumps
- `ingest`: loaders / graphizers
- `graph`: Cypher queries, constraints
- `neo4j`: docker volumes
- `scripts`: utility scripts
- `docs`: notes, diagrams
