# ingest/ifcjson_to_neo4j.py
import os, sys, json, pathlib
from typing import Any, Dict, Iterable, List
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

URI  = os.getenv("NEO4J_URI", "bolt://localhost:7687")
USER = os.getenv("NEO4J_USER", "neo4j")
PASS = os.getenv("NEO4J_PASSWORD", "changeme123")
DB   = os.getenv("NEO4J_DATABASE", "neo4j")

SPATIAL_TYPES = {
    "IfcProject","IfcSite","IfcBuilding","IfcBuildingStorey","IfcSpace"
}
# Super-simple helper to map IFC type → a coarse CMMS-ish class
def cmms_label(ifc_type: str) -> str:
    if ifc_type in SPATIAL_TYPES:
        return ifc_type
    if ifc_type.startswith("IfcSystem"):
        return "IfcSystem"
    if ifc_type.startswith("IfcFlow") or ifc_type.startswith("IfcDistribution"):
        return "IfcDistributionElement"
    if ifc_type.startswith("IfcElement"):
        return "IfcElement"
    return ifc_type  # fallback

def ref_id(x: Any) -> str | None:
    if x is None:
        return None
    if isinstance(x, str):
        return x
    if isinstance(x, dict):
        return x.get("ref") or x.get("id") or x.get("GlobalId") or x.get("globalId")
    if isinstance(x, list) and x:
        # some ifcJSON pack singletons as 1-element arrays
        return ref_id(x[0])
    return None

def get_name(obj: Dict) -> str:
    for k in ("Name","LongName","name","Longname","ObjectName"):
        v = obj.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    # sometimes in "attributes"
    attrs = obj.get("attributes") or {}
    for k in ("Name","LongName","name"):
        v = attrs.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""

def get_type(obj: Dict) -> str:
    for k in ("type","class","ifcType"):
        v = obj.get(k)
        if isinstance(v, str) and v:
            return v
    # some exporters pack under "schema"/"spec"; fall back
    return obj.get("schema") or "Unknown"

def extract_psets(obj: Dict) -> Dict[str, Any]:
    """
    Normalizes property sets when available.
    Looks in common places: 'psets', 'Properties', 'HasPropertySets', 'attributes.PropertySets'
    Produces flat dict like {'Pset_Manufacturer': 'Acme', 'InstallYear': 2016}
    """
    out = {}
    # direct maps
    for key in ("psets","Properties","properties","property_sets"):
        p = obj.get(key)
        if isinstance(p, dict):
            for k, v in p.items():
                if isinstance(v, dict) and "NominalValue" in v:
                    out[k] = v.get("NominalValue")
                else:
                    out[k] = v
    # list-of-sets shape
    candidates = []
    for k in ("HasPropertySets","PropertySets"):
        v = obj.get(k)
        if isinstance(v, list):
            candidates.extend(v)
    attrs = obj.get("attributes") or {}
    for k in ("HasPropertySets","PropertySets"):
        v = attrs.get(k)
        if isinstance(v, list):
            candidates.extend(v)
    for ps in candidates:
        pname = ps.get("Name") or ps.get("name")
        props = ps.get("HasProperties") or ps.get("Properties") or []
        for p in props if isinstance(props, list) else []:
            n = p.get("Name") or p.get("name")
            val = p.get("NominalValue") or p.get("Value") or p.get("nominalValue")
            if pname and n:
                out[f"{pname}.{n}"] = val
            elif n:
                out[n] = val
    # keep only JSON-serializable scalars
    clean = {}
    for k, v in out.items():
        if isinstance(v, (str, int, float, bool)) or v is None:
            clean[k] = v
        else:
            clean[k] = str(v)
    return clean

def load_instances(data: Dict) -> Dict[str, Dict]:
    # ifcJSON variants:
    #  - {"objects": {"GUID": {...}}}
    #  - {"instances": {"GUID": {...}}}
    #  - {"GUID": {...}}  (flat)
    for key in ("objects","instances"):
        if isinstance(data.get(key), dict):
            return data[key]
    # fallback: if file is just a GUID->obj map
    if all(isinstance(v, dict) for v in data.values()):
        return data  # type: ignore
    raise ValueError("Unsupported ifcJSON structure; expected 'objects' or 'instances' mapping")

def main():
    if len(sys.argv) < 2:
        print("Usage: python ingest/ifcjson_to_neo4j.py <path-to-ifc.json>")
        sys.exit(2)
    path = pathlib.Path(sys.argv[1])
    data = json.loads(path.read_text())
    inst = load_instances(data)

    driver = GraphDatabase.driver(URI, auth=(USER, PASS))
    created = 0
    rels: List[tuple[str, str, str]] = []  # (type, a, b) → we will map to specific rels below

    with driver.session(database=DB) as s:
        # Pass 1: create nodes
        for guid, obj in inst.items():
            if not isinstance(obj, dict):
                continue
            ifc_type = get_type(obj)
            name = get_name(obj)
            psets = extract_psets(obj)
            # Every node has :IfcEntity and its IFC type as a label
            label = cmms_label(ifc_type)
            # MERGE node
            s.run(
                f"""
                MERGE (n:IfcEntity:{ifc_type}:{label} {{globalId:$id}})
                ON CREATE SET n.name=$name, n.type=$ifc_type, n.psets=$psets
                ON MATCH  SET n.name=coalesce(n.name,$name), n.type=$ifc_type
                """,
                id=guid, name=name, ifc_type=ifc_type, psets=psets
            )
            created += 1

        # Pass 2: relationships (look for IfcRel*)
        for guid, obj in inst.items():
            if not isinstance(obj, dict):
                continue
            t = get_type(obj)
            if not t.startswith("IfcRel"):
                continue

            # Common field names across exporters:
            rs = obj.get("RelatingStructure") or obj.get("relatingStructure")
            re = obj.get("RelatedElements") or obj.get("relatedElements")
            ro = obj.get("RelatedObjects")  or obj.get("relatedObjects")
            rb = obj.get("RelatedBuildings") or obj.get("relatedBuildings")
            rg = obj.get("RelatingGroup") or obj.get("relatingGroup")
            rso = obj.get("RelatingObject") or obj.get("relatingObject")

            if t == "IfcRelContainedInSpatialStructure" and rs and re:
                parent = ref_id(rs)
                kids = re if isinstance(re, list) else [re]
                for k in kids:
                    child = ref_id(k)
                    if parent and child:
                        # (Spatial) -[:CONTAINS]-> (Element)
                        s.run("""
                            MATCH (a {globalId:$a}), (b {globalId:$b})
                            MERGE (a)-[:CONTAINS]->(b)
                        """, a=parent, b=child)

            elif t == "IfcRelAggregates" and rso and ro:
                parent = ref_id(rso)
                kids = ro if isinstance(ro, list) else [ro]
                for k in kids:
                    child = ref_id(k)
                    if parent and child:
                        s.run("""
                            MATCH (a {globalId:$a}), (b {globalId:$b})
                            MERGE (a)-[:AGGREGATES]->(b)
                        """, a=parent, b=child)

            elif t == "IfcRelServicesBuildings" and rb:
                sys_ref = ref_id(obj.get("RelatingSystem") or obj.get("relatingSystem"))
                blds = rb if isinstance(rb, list) else [rb]
                for b in blds:
                    bld = ref_id(b)
                    if sys_ref and bld:
                        s.run("""
                            MATCH (sys {globalId:$s}), (bld {globalId:$b})
                            MERGE (sys)-[:SERVICES]->(bld)
                        """, s=sys_ref, b=bld)

            elif t == "IfcRelAssignsToGroup" and rg and ro:
                sys_ref = ref_id(rg)
                objs = ro if isinstance(ro, list) else [ro]
                for o in objs:
                    el = ref_id(o)
                    if sys_ref and el:
                        # (Element)-[:ASSIGNED_TO_SYSTEM]->(System)
                        s.run("""
                            MATCH (el {globalId:$e}), (sys {globalId:$s})
                            MERGE (el)-[:ASSIGNED_TO_SYSTEM]->(sys)
                        """, e=el, s=sys_ref)

    driver.close()
    print(f"✅ Loaded nodes from {path.name}. Created/merged: {created}")
    print("✅ Relationships merged: CONTAINS / AGGREGATES / SERVICES / ASSIGNED_TO_SYSTEM (when present)")
    
if __name__ == "__main__":
    main()