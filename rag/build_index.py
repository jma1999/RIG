# rag/build_index.py
import os, json, pathlib
from typing import List, Dict, Any
import numpy as np
from neo4j import GraphDatabase
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer
import faiss

load_dotenv()
URI  = os.getenv("NEO4J_URI", "bolt://localhost:7687")
USER = os.getenv("NEO4J_USER", "neo4j")
PASS = os.getenv("NEO4J_PASSWORD", "changeme123")
DB   = os.getenv("NEO4J_DATABASE", "neo4j")

OUT_DIR = pathlib.Path("data/processed/rag")
OUT_DIR.mkdir(parents=True, exist_ok=True)

MODEL_NAME = os.getenv("EMB_MODEL", "sentence-transformers/all-MiniLM-L6-v2")

TOP_PSET_KEYS = {
    "Pset_Manufacturer.Manufacturer",
    "Pset_Manufacturer.ModelReference",
    "Pset_Asset.SerialNumber",
    "Pset_Asset.InstallationDate",
    "InstallYear",
    "Pset_MemberCommon.Span",
    "Pset_MemberCommon.IsExternal",
    "Pset_MemberCommon.Reference",
    "Pset_MemberCommon.LoadBearing",
}

def load_nodes_with_context() -> List[Dict[str, Any]]:
    drv = GraphDatabase.driver(URI, auth=(USER, PASS))
    rows = []
    with drv.session(database=DB) as s:
        q = """
        MATCH (n:IfcEntity)
        OPTIONAL MATCH (st:IfcBuildingStorey)-[:CONTAINS*1..4]->(n)
        OPTIONAL MATCH (sp:IfcSpace)-[:CONTAINS*0..4]->(n)
        OPTIONAL MATCH (n)-[:ASSIGNED_TO_SYSTEM]->(sys:IfcSystem)
        WITH n,
             collect(DISTINCT st.name) AS storeys,
             collect(DISTINCT sp.name) AS spaces,
             collect(DISTINCT sys.name) AS systems
        RETURN
            n.globalId AS id,
            labels(n)   AS labels,
            n.name      AS name,
            n.type      AS ifcType,
            n.source    AS source,
            n.psets_json AS psets_json,
            storeys, spaces, systems
        """
        for r in s.run(q):
            rows.append(dict(r))
    drv.close()
    return rows

def text_card(n: Dict[str, Any]) -> str:
    labels = [lab for lab in (n.get("labels") or []) if lab != "IfcEntity"]
    ifc_type = n.get("ifcType") or (labels[-1] if labels else "IfcEntity")
    name = (n.get("name") or "").strip()
    src = n.get("source") or ""
    storeys = [x for x in (n.get("storeys") or []) if x]
    spaces  = [x for x in (n.get("spaces")  or []) if x]
    systems = [x for x in (n.get("systems") or []) if x]

    parts = []
    if name: parts.append(f"name: {name}")
    parts.append(f"type: {ifc_type}")
    if src:  parts.append(f"source: {src}")
    if storeys: parts.append("storey: " + ", ".join(storeys[:2]))
    if spaces:  parts.append("spaces: " + ", ".join(spaces[:5]))
    if systems: parts.append("systems: " + ", ".join(systems[:5]))

    # pick a few useful pset fields
    try:
        psets = json.loads(n.get("psets_json") or "{}")
    except Exception:
        psets = {}
    ptxt = []
    for k in TOP_PSET_KEYS:
        if k in psets and psets[k] not in (None, "", "NULL"):
            ptxt.append(f"{k}={psets[k]}")
    if ptxt:
        parts.append("props: " + "; ".join(ptxt))

    # tiny synonyms to help recall
    al = []
    t = ifc_type
    nm = name.lower()
    if t == "IfcSpace":
        al += ["room", "space", "area"]
    if "ahu" in nm or "air handling unit" in nm or t == "IfcUnitaryEquipment":
        al += ["AHU", "air handling unit", "air handler"]
    if "vav" in nm or t.startswith("IfcFlowController"):
        al += ["VAV", "variable air volume", "air terminal box", "box"]
    if "diffuser" in nm or t.startswith("IfcFlowTerminal"):
        al += ["diffuser", "register", "grille", "outlet", "terminal"]
    if al:
        parts.append("alias: " + "; ".join(sorted(set(al))))

    return " | ".join(parts)

def main():
    print("🔌 Fetching nodes + context from Neo4j …")
    nodes = load_nodes_with_context()
    if not nodes:
        print("No nodes found. Did you ingest yet?")
        return

    print(f"🧱 Building text cards for {len(nodes)} nodes …")
    texts = [text_card(n) for n in nodes]

    print(f"🧠 Loading embedding model: {MODEL_NAME}")
    model = SentenceTransformer(MODEL_NAME)
    vecs = model.encode(texts, batch_size=256, show_progress_bar=True,
                        convert_to_numpy=True, normalize_embeddings=True).astype("float32")
    dim = vecs.shape[1]

    index = faiss.IndexFlatIP(dim)
    index.add(vecs)

    faiss.write_index(index, str(OUT_DIR / "index.faiss"))
    meta = {
        "model": MODEL_NAME,
        "dim": dim,
        "ids": [n["id"] for n in nodes],
        "texts": texts,
    }
    (OUT_DIR / "meta.json").write_text(json.dumps(meta))
    print(f"✅ Saved index to {OUT_DIR/'index.faiss'} and metadata to {OUT_DIR/'meta.json'}")

if __name__ == "__main__":
    main()
