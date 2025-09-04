# rag/query.py
import os, json, pathlib, re
from typing import List, Dict, Any, Tuple
import numpy as np
from dotenv import load_dotenv
from neo4j import GraphDatabase
from sentence_transformers import SentenceTransformer
import faiss

load_dotenv()
URI  = os.getenv("NEO4J_URI", "bolt://localhost:7687")
USER = os.getenv("NEO4J_USER", "neo4j")
PASS = os.getenv("NEO4J_PASSWORD", "changeme123")
DB   = os.getenv("NEO4J_DATABASE", "neo4j")

RAG_DIR = pathlib.Path("data/processed/rag")
META = json.loads((RAG_DIR / "meta.json").read_text())
INDEX = faiss.read_index(str(RAG_DIR / "index.faiss"))
MODEL = SentenceTransformer(META["model"])

# Curated reltypes we care about; we’ll intersect with what actually exists.
DESIRED_RELS = [
    "ASSIGNED_TO_SYSTEM","CONTAINS","CONNECTED_TO","FEEDS",
    "HAS_PORT","PORT_CONNECTED_TO","AGGREGATES","SAME_AS",
    "SAME_SYSTEM","IN_SPACE","IN_STOREY"
]
HOPS = int(os.getenv("RAG_HOPS", "3"))
TOPK = int(os.getenv("RAG_TOPK", "20"))

def embed(text: str) -> np.ndarray:
    v = MODEL.encode([text], convert_to_numpy=True, normalize_embeddings=True)
    return v.astype(np.float32)

def vector_seeds(question: str, k: int = TOPK) -> List[Tuple[int, float]]:
    scores, idxs = INDEX.search(embed(question), k)
    return list(zip(idxs[0].tolist(), scores[0].tolist()))

def lexical_seeds(question: str) -> List[str]:
    import re
    toks = [t for t in re.findall(r"[A-Za-z0-9\-]+", question) if len(t) >= 2]
    if not toks:
        return []
    drv = GraphDatabase.driver(URI, auth=(USER, PASS))
    ids: List[str] = []
    with drv.session(database=DB) as s:
        # Name match
        like_parts = [f"toLower(n.name) CONTAINS toLower($t{idx})" for idx, _ in enumerate(toks)]
        params = {f"t{idx}": tok for idx, tok in enumerate(toks)}
        q1 = f"""
        MATCH (n:IfcEntity)
        WHERE {" OR ".join(like_parts)}
        RETURN DISTINCT n.globalId AS id LIMIT 50
        """
        ids += [r["id"] for r in s.run(q1, **params)]

        # Pset Reference (common in 2x3 proxies)
        q2 = """
        UNWIND $toks AS tok
        MATCH (n:IfcEntity)
        WITH n, tok, apoc.convert.fromJsonMap(n.psets_json) AS p
        WITH n, tok, toLower(toString(p['Pset_BuildingElementProxyCommon.Reference'])) AS ref
        WHERE ref CONTAINS toLower(tok)
        RETURN DISTINCT n.globalId AS id LIMIT 50
        """
        ids += [r["id"] for r in s.run(q2, toks=toks)]

        # Bias to Mech when query mentions AHU/VAV/terminal/diffuser
        if re.search(r"\b(ahu|vav|terminal|diffuser|grille|register)\b", question.lower()):
            q3 = """
            MATCH (n:IfcEntity {source:'Mech'})
            WHERE toLower(n.name) CONTAINS toLower($needle)
            RETURN DISTINCT n.globalId AS id LIMIT 50
            """
            for needle in ("vav","ahu","diffuser","terminal"):
                ids += [r["id"] for r in s.run(q3, needle=needle)]
    drv.close()
    # de-dupe while preserving order
    seen, out = set(), []
    for i in ids:
        if i not in seen:
            seen.add(i); out.append(i)
    return out[:50]


def expand_neighborhood(seed_ids: List[str]) -> Dict[str, Any]:
    drv = GraphDatabase.driver(URI, auth=(USER, PASS))
    with drv.session(database=DB) as s:
        # discover existing rel types
        rels_row = s.run("MATCH ()-[r]->() RETURN collect(DISTINCT type(r)) AS t").single()
        existing = rels_row["t"] if rels_row and rels_row["t"] else []
        allowed = [r for r in DESIRED_RELS if r in existing]
        rel_filter = "|".join(allowed) if allowed else ".*"  # allow anything if none detected

        cypher = """
        UNWIND $ids AS seed
        MATCH (n:IfcEntity {globalId: seed})
        CALL apoc.path.expandConfig(n, {
          minLevel: 1, maxLevel: $maxLevel,
          relationshipFilter: $relFilter,
          uniqueness: 'NODE_GLOBAL'
        }) YIELD path
        WITH n, collect(DISTINCT nodes(path)) AS nsets, collect(DISTINCT relationships(path)) AS rsets
        WITH n, apoc.coll.toSet(apoc.coll.flatten(nsets) + [n]) AS nodes,
               apoc.coll.toSet(apoc.coll.flatten(rsets)) AS rels
        RETURN
          [x IN nodes | { id:x.globalId, name:x.name, type:x.type, source:x.source, labels:labels(x) }] AS nodes,
          [r IN rels  | { src:startNode(r).globalId, dst:endNode(r).globalId, type:type(r) }]          AS edges
        """

        nodes: Dict[str, Dict[str, Any]] = {}
        edges: List[Dict[str, Any]] = []
        for rec in s.run(cypher, ids=seed_ids, maxLevel=HOPS, relFilter=rel_filter):
            for n in rec["nodes"]:
                nodes[n["id"]] = n
            edges.extend(rec["edges"])
    drv.close()

    seen, uniq = set(), []
    for e in edges:
        key = (e["src"], e["dst"], e["type"])
        if key not in seen:
            seen.add(key); uniq.append(e)

    return {"nodes": list(nodes.values()), "edges": uniq}

def build_evidence(question: str, k: int = TOPK) -> Dict[str, Any]:
    # 1) vector seeds
    v_seeds = vector_seeds(question, k=k)
    # 2) lexical seeds
    lex_ids = lexical_seeds(question)
    # merge (favor lexical by putting first and giving them a tiny score boost for readability)
    id_map = META["ids"]
    merged_ids = [*lex_ids]
    merged_ids += [id_map[i] for i,_ in v_seeds if id_map[i] not in merged_ids]
    merged_ids = merged_ids[:k]

    sub = expand_neighborhood(merged_ids)
    return {
        "question": question,
        "focus_seeds": [{"id": sid, "score": 1.0 if sid in lex_ids else 0.0} for sid in merged_ids],
        "nodes": sub["nodes"],
        "edges": sub["edges"],
    }

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("question", nargs="+")
    parser.add_argument("--k", type=int, default=TOPK)
    parser.add_argument("--out", default="data/processed/evidence.json")
    args = parser.parse_args()
    q = " ".join(args.question)

    print(f"🔎 Query: {q}")
    ev = build_evidence(q, k=args.k)
    out_path = pathlib.Path(args.out)
    out_path.write_text(json.dumps(ev, indent=2))
    print(f"✅ Wrote evidence to {out_path}")

    # preview useful nodes
    preview = sorted({(n.get("type"), n.get("name")) for n in ev["nodes"] if n.get("name")})[:15]
    print("👀 Evidence sample nodes:", preview)

if __name__ == "__main__":
    main()
