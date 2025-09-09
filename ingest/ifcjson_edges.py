# ingest/ifcjson_edges.py
import os, json, pathlib, argparse
from typing import Any, Dict, List, Optional
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()
URI  = os.getenv("NEO4J_URI", "bolt://localhost:7687")
USER = os.getenv("NEO4J_USER", "neo4j")
PASS = os.getenv("NEO4J_PASSWORD", "changeme123")
DB   = os.getenv("NEO4J_DATABASE", "neo4j")

def ref_id(x: Any) -> Optional[str]:
    if x is None: return None
    if isinstance(x, str): return x
    if isinstance(x, dict):
        return x.get("ref") or x.get("id") or x.get("GlobalId") or x.get("globalId")
    if isinstance(x, list) and x:
        return ref_id(x[0])
    return None

def g(obj: Dict, *keys: str):
    # get key from top-level or from attributes{}
    for k in keys:
        if k in obj: return obj[k]
    attrs = obj.get("attributes") or {}
    for k in keys:
        if k in attrs: return attrs[k]
    return None

def load_instances(data: Dict) -> Dict[str, Dict]:
    for k in ("objects","instances"):
        if isinstance(data.get(k), dict):
            return data[k]
    if isinstance(data, dict) and all(isinstance(v, dict) for v in data.values()):
        return data
    raise ValueError("Unsupported ifcJSON structure")

def merge_rel(tx, a, b, reltype):
    tx.run(f"MATCH (a {{globalId:$a}}),(b {{globalId:$b}}) MERGE (a)-[:{reltype}]->(b)", a=a, b=b)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("path", help="Path to ifcJSON")
    args = ap.parse_args()

    path = pathlib.Path(args.path)
    data = json.loads(path.read_text())
    inst = load_instances(data)

    drv = GraphDatabase.driver(URI, auth=(USER, PASS))
    created = 0
    with drv.session(database=DB) as s:
        for guid, obj in inst.items():
            if not isinstance(obj, dict):
                continue
            t = obj.get("type") or obj.get("class") or obj.get("schema") or ""
            if not t.startswith("IfcRel"):
                continue

            # ----- Spatial containment
            if t in ("IfcRelContainedInSpatialStructure", "IfcRelContainedInSpatialStructure_"):
                rs = g(obj, "RelatingStructure", "relatingStructure", "RelatingSpatialStructure", "relatingSpatialStructure")
                re = g(obj, "RelatedElements", "relatedElements")
                parent = ref_id(rs)
                kids = re if isinstance(re, list) else [re] if re else []
                for k in kids:
                    child = ref_id(k)
                    if parent and child:
                        s.write_transaction(merge_rel, parent, child, "CONTAINS"); created += 1

            # ----- Aggregation (decomposition)
            elif t == "IfcRelAggregates":
                whole = g(obj, "RelatingObject", "relatingObject")
                parts = g(obj, "RelatedObjects", "relatedObjects")
                whole_id = ref_id(whole)
                kids = parts if isinstance(parts, list) else [parts] if parts else []
                for k in kids:
                    part_id = ref_id(k)
                    if whole_id and part_id:
                        s.write_transaction(merge_rel, whole_id, part_id, "AGGREGATES"); created += 1

            # ----- System servicing a building
            elif t == "IfcRelServicesBuildings":
                sysref = g(obj, "RelatingSystem", "relatingSystem")
                blds  = g(obj, "RelatedBuildings", "relatedBuildings")
                sid = ref_id(sysref)
                bs = blds if isinstance(blds, list) else [blds] if blds else []
                for b in bs:
                    bid = ref_id(b)
                    if sid and bid:
                        s.write_transaction(merge_rel, sid, bid, "SERVICES"); created += 1

            # ----- Assign elements to a group (system)
            elif t == "IfcRelAssignsToGroup":
                grp = g(obj, "RelatingGroup", "relatingGroup")
                objs = g(obj, "RelatedObjects", "relatedObjects")
                gid = ref_id(grp)
                os = objs if isinstance(objs, list) else [objs] if objs else []
                for o in os:
                    oid = ref_id(o)
                    if gid and oid:
                        # element -> system
                        s.write_transaction(merge_rel, oid, gid, "ASSIGNED_TO_SYSTEM"); created += 1

            # ----- Connectivity fallbacks (ports / elements)
            elif t in ("IfcRelConnectsPortToElement", "IfcRelConnectsPortToElement_"):
                port = ref_id(g(obj, "RelatingPort", "relatingPort"))
                elem = ref_id(g(obj, "RelatedElement", "relatedElement"))
                if port and elem:
                    s.write_transaction(merge_rel, elem, port, "HAS_PORT"); created += 1

            elif t in ("IfcRelConnectsPorts", "IfcRelConnectsPorts_"):
                p1 = ref_id(g(obj, "RelatingPort", "relatingPort"))
                p2 = ref_id(g(obj, "RelatedPort", "relatedPort"))
                if p1 and p2:
                    s.write_transaction(merge_rel, p1, p2, "PORT_CONNECTED_TO")
                    s.write_transaction(merge_rel, p2, p1, "PORT_CONNECTED_TO")
                    created += 2

            elif t in ("IfcRelConnectsElements", "IfcRelConnectsElements_"):
                a = ref_id(g(obj, "RelatingElement", "relatingElement"))
                b = ref_id(g(obj, "RelatedElement", "relatedElement"))
                if a and b:
                    s.write_transaction(merge_rel, a, b, "CONNECTED_TO")
                    s.write_transaction(merge_rel, b, a, "CONNECTED_TO")
                    created += 2

    drv.close()
    print(f"✅ Edges merged: {created}")

if __name__ == "__main__":
    main()
