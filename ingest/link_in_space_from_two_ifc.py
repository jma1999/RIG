# ingest/link_in_space_from_two_ifc.py
import os, argparse
from typing import Dict, Tuple, Optional, List
from neo4j import GraphDatabase
from dotenv import load_dotenv

import ifcopenshell
import ifcopenshell.geom as geom
from ifcopenshell.util.placement import get_local_placement

load_dotenv()
URI  = os.getenv("NEO4J_URI", "bolt://localhost:7687")
USER = os.getenv("NEO4J_USER", "neo4j")
PASS = os.getenv("NEO4J_PASSWORD", "changeme123")
DB   = os.getenv("NEO4J_DATABASE", "neo4j")

NAME_HINTS = ("diffuser", "air terminal", "grille", "register", "outlet")

def safe_by_type(f, t):
    try: return f.by_type(t)
    except Exception: return []

def aabb_of_shape(sh) -> Tuple[float,float,float,float,float,float]:
    vs = sh.geometry.verts
    xs, ys, zs = vs[0::3], vs[1::3], vs[2::3]
    return (min(xs),min(ys),min(zs), max(xs),max(ys),max(zs))

def center(aabb):
    x0,y0,z0,x1,y1,z1 = aabb
    return ((x0+x1)/2.0, (y0+y1)/2.0, (z0+z1)/2.0)

def contains(aabb, p, tol_xy=0.10, tol_z=1.0):
    x0,y0,z0,x1,y1,z1 = aabb
    x,y,z = p
    return (x0 - tol_xy <= x <= x1 + tol_xy and
            y0 - tol_xy <= y <= y1 + tol_xy and
            z0 - tol_z  <= z <= z1 + tol_z)

def world_origin(e) -> Optional[Tuple[float,float,float]]:
    op = getattr(e, "ObjectPlacement", None)
    if not op: return None
    try:
        m = get_local_placement(op)
        return (m[0][3], m[1][3], m[2][3])
    except Exception:
        return None

def is_diffuser_like(e) -> bool:
    n = getattr(e, "Name", "") or ""
    nl = n.lower() if isinstance(n, str) else str(n).lower()
    return any(h in nl for h in NAME_HINTS)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("arch_ifc", help="ARCH IFC path (has IfcSpace geometry)")
    ap.add_argument("mech_ifc", help="MECH IFC path (has diffusers)")
    args = ap.parse_args()

    # Geometry settings
    s = geom.settings()
    s.set(s.USE_WORLD_COORDS, True)

    # 1) Build AABBs for ARCH spaces
    arch = ifcopenshell.open(args.arch_ifc)
    space_aabb: Dict[str, Tuple[float,float,float,float,float,float]] = {}
    for sp in safe_by_type(arch, "IfcSpace"):
        try:
            sh = geom.create_shape(s, sp)
            space_aabb[sp.GlobalId] = aabb_of_shape(sh)
        except Exception:
            continue

    if not space_aabb:
        print("No space geometry available from ARCH IFC.")
        return

    # 2) Collect diffuser-like elements from MECH
    mech = ifcopenshell.open(args.mech_ifc)
    candidates: List = []
    for t in ("IfcAirTerminal","IfcFlowTerminal","IfcDuctTerminal"):
        candidates += safe_by_type(mech, t)
    # IFC2x3 often uses proxies for terminals
    candidates += [e for e in safe_by_type(mech, "IfcBuildingElementProxy") if is_diffuser_like(e)]

    # De-dupe by GUID
    seen, terms = set(), []
    for e in candidates:
        gid = getattr(e, "GlobalId", None)
        if gid and gid not in seen:
            seen.add(gid); terms.append(e)

    drv = GraphDatabase.driver(URI, auth=(USER, PASS))
    assigned = 0
    with drv.session(database=DB) as session:
        for t in terms:
            p = world_origin(t)
            if not p: 
                continue
            # First: try containment
            hit = None
            for sp_gid, aabb in space_aabb.items():
                if contains(aabb, p):
                    hit = sp_gid
                    break
            # Fallback: nearest space center
            if not hit:
                best_d2, best_sp = 1e99, None
                for sp_gid, aabb in space_aabb.items():
                    cx,cy,cz = center(aabb)
                    d2 = (cx-p[0])**2 + (cy-p[1])**2 + (cz-p[2])**2
                    if d2 < best_d2:
                        best_d2, best_sp = d2, sp_gid
                hit = best_sp

            if hit:
                session.run("""
                    MATCH (e {globalId:$e}), (sp:IfcSpace {globalId:$sp})
                    MERGE (e)-[:IN_SPACE]->(sp)
                """, e=t.GlobalId, sp=hit)
                assigned += 1

    drv.close()
    print(f"✅ Linked {assigned} diffuser-like elements → IN_SPACE")

if __name__ == "__main__":
    main()
