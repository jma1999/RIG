# ingest/ifc_to_neo4j.py
import os, json, argparse, pathlib
from typing import Any, Dict, Optional, List
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()
URI  = os.getenv("NEO4J_URI", "bolt://localhost:7687")
USER = os.getenv("NEO4J_USER", "neo4j")
PASS = os.getenv("NEO4J_PASSWORD", "changeme123")
DB   = os.getenv("NEO4J_DATABASE", "neo4j")

# --- IFC ---
import ifcopenshell
from ifcopenshell.util.element import get_psets

SPATIAL_TYPES = {"IfcProject","IfcSite","IfcBuilding","IfcBuildingStorey","IfcSpace"}

def cmms_label(t: str) -> str:
    if t in SPATIAL_TYPES: return t
    if t.startswith("IfcSystem"): return "IfcSystem"
    if t.startswith("IfcFlow") or t.startswith("IfcDistribution"): return "IfcDistributionElement"
    if t.startswith("IfcElement"): return "IfcElement"
    return t

def name_of(o) -> str:
    n = getattr(o, "Name", None) or ""
    try:
        return n.strip() if isinstance(n, str) else str(n)
    except: return ""

def z_from_local_placement(lp) -> Optional[float]:
    """Sum Z translation up the IfcLocalPlacement → PlacementRelTo chain (ignores rotation)."""
    z = 0.0
    steps = 0
    cur = lp
    try:
        while cur and steps < 64:
            rp = getattr(cur, "RelativePlacement", None)
            if rp and getattr(rp, "Location", None):
                coords = rp.Location.Coordinates
                if coords and len(coords) >= 3 and coords[2] is not None:
                    z += float(coords[2])
            cur = getattr(cur, "PlacementRelTo", None)
            steps += 1
        return z
    except Exception:
        return None

def z_of(elem) -> Optional[float]:
    op = getattr(elem, "ObjectPlacement", None)
    if not op: return None
    # Local placement only (ignore grid/axis placements)
    if op.is_a("IfcLocalPlacement"):
        return z_from_local_placement(op)
    return None

def ifc_type(o) -> str:
    try:
        return o.is_a()
    except: return "Unknown"

def flatten_psets(ps: Dict[str, Any]) -> Dict[str, Any]:
    out = {}
    for pset, props in ps.items():
        if isinstance(props, dict):
            for k, v in props.items():
                key = f"{pset}.{k}"
                if isinstance(v, (str, int, float, bool)) or v is None:
                    out[key] = v
                else:
                    # unwrap ifcopenshell value wrappers
                    s = getattr(v, "wrappedValue", None)
                    out[key] = s if isinstance(s, (str, int, float, bool)) else str(v)
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("path", help="Path to .ifc (SPF)")
    ap.add_argument("--source", default=None, help="Logical source tag: Arch/Mech/etc.")
    args = ap.parse_args()

    ifc = ifcopenshell.open(args.path)
    SOURCE = args.source or pathlib.Path(args.path).stem

    drv = GraphDatabase.driver(URI, auth=(USER, PASS))
    with drv.session(database=DB) as s:

        # --- 1) Upsert nodes (only properties we truly need; you already have nodes from JSON)
        # Building storeys (set elevation)
        storeys = ifc.by_type("IfcBuildingStorey")
        for st in storeys:
            gid = st.GlobalId
            elev = getattr(st, "Elevation", None)
            s.run("""
                MERGE (st:IfcEntity:IfcBuildingStorey {globalId:$id})
                ON CREATE SET st.name=$name, st.type='IfcBuildingStorey', st.source=$src
                SET st.elev = $elev
            """, id=gid, name=name_of(st), src=SOURCE, elev=float(elev) if elev is not None else None)

        # Any element with a placement → set z
        elems = ifc.by_type("IfcElement") + ifc.by_type("IfcDistributionElement") + ifc.by_type("IfcSystem")
        for e in elems:
            gid = getattr(e, "GlobalId", None)
            if not gid: continue
            t   = ifc_type(e)
            nm  = name_of(e)
            z   = z_of(e)
            # merge labels by type; do not clobber existing JSON props
            s.run(f"""
                MERGE (n:IfcEntity:{t}:{cmms_label(t)} {{globalId:$id}})
                ON CREATE SET n.name=$name, n.type=$type, n.source=$src
                ON MATCH  SET n.type=$type,  n.source=coalesce(n.source,$src)
                {"SET n.z = $z" if z is not None else ""}
            """, id=gid, name=nm, type=t, src=SOURCE, z=z)

            # Psets (nice to have; you already added many via JSON)
            try:
                ps = get_psets(e) or {}
                if ps:
                    flat = flatten_psets(ps)
                    s.run("""
                        MATCH (n {globalId:$id})
                        SET n.psets_json = coalesce(n.psets_json, $psets_json)
                        SET n += $flat
                    """, id=gid, psets_json=json.dumps(ps, ensure_ascii=False), flat=flat)
            except Exception:
                pass
        
        # --- SPACES (rooms) ---
        spaces = ifc.by_type("IfcSpace")
        for sp in spaces:
            gid  = getattr(sp, "GlobalId", None)
            if not gid:
                continue
            nm   = name_of(sp)                    # often Room Number in Revit
            long = getattr(sp, "LongName", None)  # often Room Name in Revit
            # optional: psets for spaces
            try:
                ps = get_psets(sp) or {}
                flat = flatten_psets(ps)
                pjson = json.dumps(ps, ensure_ascii=False)
            except Exception:
                flat, pjson = {}, None

            s.run("""
                MERGE (x:IfcEntity:IfcSpace {globalId:$id})
                ON CREATE SET x.name=$name, x.type='IfcSpace', x.source=$src
                SET x.longName = $long
            """, id=gid, name=nm, src=SOURCE, long=long)

            if flat or pjson:
                s.run("""
                    MATCH (x:IfcSpace {globalId:$id})
                    SET x.psets_json = coalesce(x.psets_json, $pjson)
                    SET x += $flat
                """, id=gid, pjson=pjson, flat=flat)


        # --- 2) Relationships (spatial, aggregates, systems, connectivity)
        # Spatial containment
        for rel in ifc.by_type("IfcRelContainedInSpatialStructure"):
            parent = getattr(rel.RelatingStructure, "GlobalId", None)
            if not parent: continue
            for child in getattr(rel, "RelatedElements", []) or []:
                gid = getattr(child, "GlobalId", None)
                if not gid: continue
                s.run("MATCH (a {globalId:$a}),(b {globalId:$b}) MERGE (a)-[:CONTAINS]->(b)", a=parent, b=gid)

        # Aggregation
        for rel in ifc.by_type("IfcRelAggregates"):
            whole = getattr(rel.RelatingObject, "GlobalId", None)
            for part in getattr(rel, "RelatedObjects", []) or []:
                pid = getattr(part, "GlobalId", None)
                if whole and pid:
                    s.run("MATCH (a {globalId:$a}),(b {globalId:$b}) MERGE (a)-[:AGGREGATES]->(b)", a=whole, b=pid)

        # Systems (AssignsToGroup)
        for rel in ifc.by_type("IfcRelAssignsToGroup"):
            grp = getattr(rel, "RelatingGroup", None)
            gid = getattr(grp, "GlobalId", None) if grp else None
            if not gid: continue
            # ensure system node exists
            s.run("""
                MERGE (sys:IfcEntity:IfcSystem {globalId:$id})
                ON CREATE SET sys.name=$name, sys.type='IfcSystem', sys.source=$src
            """, id=gid, name=name_of(grp), src=SOURCE)
            for o in getattr(rel, "RelatedObjects", []) or []:
                oid = getattr(o, "GlobalId", None)
                if oid:
                    s.run("MATCH (e {globalId:$e}),(sys {globalId:$s}) MERGE (e)-[:ASSIGNED_TO_SYSTEM]->(sys)", e=oid, s=gid)

        # Ports connectivity (if present)
        for rel in ifc.by_type("IfcRelConnectsPortToElement"):
            port = getattr(rel, "RelatingPort", None)
            elem = getattr(rel, "RelatedElement", None)
            pid  = getattr(port, "GlobalId", None) if port else None
            eid  = getattr(elem, "GlobalId", None) if elem else None
            if pid and eid:
                s.run("MATCH (e {globalId:$e}),(p {globalId:$p}) MERGE (e)-[:HAS_PORT]->(p)", e=eid, p=pid)

        for rel in ifc.by_type("IfcRelConnectsPorts"):
            p1 = getattr(rel, "RelatingPort", None)
            p2 = getattr(rel, "RelatedPort", None)
            a  = getattr(p1, "GlobalId", None) if p1 else None
            b  = getattr(p2, "GlobalId", None) if p2 else None
            if a and b:
                s.run("MATCH (a {globalId:$a}),(b {globalId:$b}) MERGE (a)-[:PORT_CONNECTED_TO]->(b)", a=a, b=b)
                s.run("MATCH (a {globalId:$a}),(b {globalId:$b}) MERGE (b)-[:PORT_CONNECTED_TO]->(a)", a=a, b=b)

        # Element-level connectivity (loose)
        for rel in ifc.by_type("IfcRelConnectsElements"):
            a = getattr(rel, "RelatingElement", None)
            b = getattr(rel, "RelatedElement",  None)
            ga = getattr(a, "GlobalId", None) if a else None
            gb = getattr(b, "GlobalId", None) if b else None
            if ga and gb:
                s.run("MATCH (a {globalId:$a}),(b {globalId:$b}) MERGE (a)-[:CONNECTED_TO]->(b)", a=ga, b=gb)
                s.run("MATCH (a {globalId:$a}),(b {globalId:$b}) MERGE (b)-[:CONNECTED_TO]->(a)", a=ga, b=gb)

    drv.close()
    print("✅ IFC ingest complete for", args.path)

if __name__ == "__main__":
    main()
