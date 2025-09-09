# ingest/link_in_space_by_placement.py
import os, argparse, math
from typing import List, Tuple, Optional, Dict
from neo4j import GraphDatabase
from dotenv import load_dotenv

import ifcopenshell
from ifcopenshell.util.placement import get_local_placement

load_dotenv()
URI  = os.getenv("NEO4J_URI", "bolt://localhost:7687")
USER = os.getenv("NEO4J_USER", "neo4j")
PASS = os.getenv("NEO4J_PASSWORD", "changeme123")
DB   = os.getenv("NEO4J_DATABASE", "neo4j")

NAME_HINTS = ("diffuser", "air terminal", "grille", "register", "outlet")

def world_origin(elem) -> Optional[Tuple[float,float,float]]:
    op = getattr(elem, "ObjectPlacement", None)
    if not op:
        return None
    try:
        m = get_local_placement(op)  # 4x4 matrix
        return (float(m[0][3]), float(m[1][3]), float(m[2][3]))
    except Exception:
        return None

def safe_by_type(f, t):
    try: return f.by_type(t)
    except Exception: return []

def is_diffuser_like(e) -> bool:
    n = getattr(e, "Name", "") or ""
    nl = n.lower() if isinstance(n, str) else str(n).lower()
    return any(h in nl for h in NAME_HINTS)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("arch_ifc", help="ARCH IFC path (has IfcSpace placements)")
    ap.add_argument("mech_ifc", help="MECH IFC path (has diffusers)")
    ap.add_argument("--z_tol", type=float, default=3.0, help="Z tolerance in meters for space matching")
    ap.add_argument("--xy_max", type=float, default=50.0, help="Optional XY cap (m). If >0, require XY distance < cap")
    args = ap.parse_args()

    # 1) Load spaces with world origins
    arch = ifcopenshell.open(args.arch_ifc)
    spaces_pts: Dict[str, Tuple[float,float,float]] = {}
    spaces_name: Dict[str, str] = {}
    for sp in safe_by_type(arch, "IfcSpace"):
        gid = getattr(sp, "GlobalId", None)
        if not gid: continue
        p = world_origin(sp)
        if not p: continue
        spaces_pts[gid] = p
        nm = getattr(sp, "Name", "") or ""
        ln = getattr(sp, "LongName", "") or ""
        spaces_name[gid] = f"{nm} {ln}".strip()

    if not spaces_pts:
        print("❌ No IfcSpace placements found in ARCH IFC (cannot link).")
        return

    # 2) Collect diffuser-like elements with world origins
    mech = ifcopenshell.open(args.mech_ifc)
    candidates = []
    for t in ("IfcAirTerminal","IfcFlowTerminal","IfcDuctTerminal"):
        candidates += safe_by_type(mech, t)
    candidates += [e for e in safe_by_type(mech, "IfcBuildingElementProxy") if is_diffuser_like(e)]

    terminals: List[Tuple[str, Tuple[float,float,float]]] = []
    for e in candidates:
        gid = getattr(e, "GlobalId", None)
        if not gid: continue
        p = world_origin(e)
        if not p: continue
        terminals.append((gid, p))

    if not terminals:
        print("❌ No diffuser-like terminals with placements found in MECH IFC.")
        return

    # 3) Nearest-space assignment with Z gating
    def nearest_space(pt):
        px,py,pz = pt
        best = None
        best_d2 = 1e99
        for sgid, (sx,sy,sz) in spaces_pts.items():
            if abs(pz - sz) > args.z_tol:
                continue
            dx = px - sx; dy = py - sy
            d2 = dx*dx + dy*dy
            if args.xy_max > 0 and (dx*dx + dy*dy) > args.xy_max*args.xy_max:
                continue
            if d2 < best_d2:
                best_d2 = d2; best = sgid
        return best

    links = []
    for gid, p in terminals:
        sp = nearest_space(p)
        if sp:
            links.append((gid, sp))

    drv = GraphDatabase.driver(URI, auth=(USER, PASS))
    with drv.session(database=DB) as s:
        for e_gid, sp_gid in links:
            s.run("""
                MATCH (e {globalId:$e}), (sp:IfcSpace {globalId:$sp})
                MERGE (e)-[:IN_SPACE]->(sp)
            """, e=e_gid, sp=sp_gid)
    drv.close()
    print(f"✅ Linked {len(links)} diffuser-like elements → IN_SPACE "
          f"(z_tol={args.z_tol}m, xy_max={args.xy_max if args.xy_max>0 else '∞'}m)")

if __name__ == "__main__":
    main()
