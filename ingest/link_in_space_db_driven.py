import os, argparse, math
from typing import Dict, Tuple, Optional
from neo4j import GraphDatabase
from dotenv import load_dotenv
import ifcopenshell
from ifcopenshell.util.placement import get_local_placement

load_dotenv()
URI  = os.getenv("NEO4J_URI", "bolt://localhost:7687")
USER = os.getenv("NEO4J_USER", "neo4j")
PASS = os.getenv("NEO4J_PASSWORD", "changeme123")
DB   = os.getenv("NEO4J_DATABASE", "neo4j")

def world_origin(e):
    op = getattr(e, "ObjectPlacement", None)
    if not op: return None
    try:
        m = get_local_placement(op)
        return (float(m[0][3]), float(m[1][3]), float(m[2][3]))
    except Exception:
        return None

def build_guid_index(f):
    idx = {}
    for e in f:  # iterate whole file once
        gid = getattr(e, "GlobalId", None)
        if gid: idx[gid] = e
    return idx

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("arch_ifc")
    ap.add_argument("mech_ifc")
    ap.add_argument("--z_tol", type=float, default=1e9, help="Z tolerance; default huge to ignore Z")
    ap.add_argument("--xy_max", type=float, default=0, help="0 = no XY cap")
    ap.add_argument("--limit", type=int, default=0, help="limit number of candidates (0 = all)")
    ap.add_argument("--mech_sources", nargs="*", default=["Mech","Mechanical","MEP","MechJSON"])
    args = ap.parse_args()

    # 1) Pull candidate GUIDs from Neo4j (labels, name, source)
    drv = GraphDatabase.driver(URI, auth=(USER,PASS))
    q = """
    MATCH (t:IfcEntity)
    WHERE (t:MEP_DIFFUSER)
       OR toLower(coalesce(t.name,'')) CONTAINS 'diffuser'
       OR t.source IN $sources
    RETURN DISTINCT t.globalId AS id
    """
    with drv.session(database=DB) as s:
        ids = [r["id"] for r in s.run(q, sources=args.mech_sources)]
    if args.limit and len(ids) > args.limit:
        ids = ids[:args.limit]
    print(f"Neo4j candidates: {len(ids)}")

    # 2) Load IFCs and precompute placements
    arch = ifcopenshell.open(args.arch_ifc)
    mech = ifcopenshell.open(args.mech_ifc)
    mech_idx = build_guid_index(mech)

    spaces_pts: Dict[str, Tuple[float,float,float]] = {}
    for sp in arch.by_type("IfcSpace"):
        p = world_origin(sp)
        if p: spaces_pts[sp.GlobalId] = p
    if not spaces_pts:
        print("❌ No IfcSpace placements found in ARCH IFC.")
        return

    # 3) For each candidate GUID, find element in MECH and get placement
    pairs = []
    for gid in ids:
        e = mech_idx.get(gid)
        if not e: 
            continue
        p = world_origin(e)
        if not p:
            continue
        # nearest space by XY with optional Z gating
        px,py,pz = p
        best, best_d2 = None, 1e99
        for sgid, (sx,sy,sz) in spaces_pts.items():
            if abs(pz - sz) > args.z_tol:
                continue
            dx,dy = px-sx, py-sy
            d2 = dx*dx + dy*dy
            if args.xy_max>0 and d2 > args.xy_max*args.xy_max:
                continue
            if d2 < best_d2:
                best_d2, best = d2, sgid
        if best:
            pairs.append((gid, best))
    print(f"Pairs to link: {len(pairs)}")

    # 4) Write IN_SPACE edges
    with drv.session(database=DB) as s:
        for egid, spgid in pairs:
            s.run("""
                MATCH (e {globalId:$e}), (sp:IfcSpace {globalId:$sp})
                MERGE (e)-[:IN_SPACE]->(sp)
            """, e=egid, sp=spgid)
    drv.close()
    print("✅ Done.")

if __name__ == "__main__":
    main()
