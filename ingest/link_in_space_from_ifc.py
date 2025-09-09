import os, argparse, pathlib, math, re
from typing import Dict, Tuple, List, Optional
from neo4j import GraphDatabase
from dotenv import load_dotenv

import ifcopenshell
import ifcopenshell.geom as geom
from ifcopenshell.util.placement import get_local_placement
from ifcopenshell.util.element import get_container

load_dotenv()
URI  = os.getenv("NEO4J_URI", "bolt://localhost:7687")
USER = os.getenv("NEO4J_USER", "neo4j")
PASS = os.getenv("NEO4J_PASSWORD", "changeme123")
DB   = os.getenv("NEO4J_DATABASE", "neo4j")

FAMILY_PREFIXES = {
    "M_Supply Diffuser - Rectangular Face Round Neck - Hosted",
    "M_Supply Diffuser_HEPA - Rectangular Face Round Neck - Hosted",
    "M_Return Diffuser",
}
NAME_HINTS = ("diffuser", "air terminal", "grille", "register", "outlet")

def safe_by_type(ifc, t: str):
    try:
        return ifc.by_type(t)
    except Exception:
        return []

def aabb_of_shape(sh) -> Tuple[float,float,float,float,float,float]:
    vs = sh.geometry.verts  # flat list [x0,y0,z0,x1,y1,z1,...]
    xs = vs[0::3]; ys = vs[1::3]; zs = vs[2::3]
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

def world_origin(elem) -> Optional[Tuple[float,float,float]]:
    op = getattr(elem, "ObjectPlacement", None)
    if not op: 
        return None
    try:
        m = get_local_placement(op)  # 4x4
        return (m[0][3], m[1][3], m[2][3])
    except Exception:
        return None

def name_of(e):
    n = getattr(e, "Name", None)
    return n if isinstance(n, str) else (str(n) if n is not None else "")

def is_diffuser_like(e) -> bool:
    n = name_of(e)
    fam = n.split(":")[0].strip() if ":" in n else n.strip()
    nl = n.lower()
    if fam in FAMILY_PREFIXES:
        return True
    return any(h in nl for h in NAME_HINTS)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("ifc_path", help="Path to .ifc (SPF)")
    ap.add_argument("--source", default=None)
    ap.add_argument("--max_per_storey", type=int, default=2000)
    args = ap.parse_args()

    ifc = ifcopenshell.open(args.ifc_path)
    settings = geom.settings()
    settings.set(settings.USE_WORLD_COORDS, True)

    # 1) Space AABBs (for containment/nearest fallback)
    spaces = safe_by_type(ifc, "IfcSpace")
    space_aabb: Dict[str, Tuple[float,float,float,float,float,float]] = {}
    for s in spaces:
        try:
            sh = geom.create_shape(settings, s)
            space_aabb[s.GlobalId] = aabb_of_shape(sh)
        except Exception:
            # skip spaces without tessellable geometry
            continue

    # 2) Candidate terminals:
    candidates = []
    for tname in ("IfcAirTerminal", "IfcFlowTerminal"):
        candidates.extend(safe_by_type(ifc, tname))
    # plus proxies that look like diffusers
    proxies = [e for e in safe_by_type(ifc, "IfcBuildingElementProxy") if is_diffuser_like(e)]
    candidates.extend(proxies)

    # de-dupe by GUID
    seen = set()
    terms = []
    for e in candidates:
        gid = getattr(e, "GlobalId", None)
        if gid and gid not in seen:
            seen.add(gid); terms.append(e)

    drv = GraphDatabase.driver(URI, auth=(USER, PASS))
    assigned = 0

    with drv.session(database=DB) as s:
        for t in terms:
            # first choice: IfcOpenShell container
            cont = get_container(t)
            if cont and cont.is_a("IfcSpace"):
                s.run("MATCH (e {globalId:$e}),(sp:IfcSpace {globalId:$sp}) "
                      "MERGE (e)-[:IN_SPACE]->(sp)", e=t.GlobalId, sp=cont.GlobalId)
                assigned += 1
                continue

            # else: try point in AABB
            p = world_origin(t)
            if not p or not space_aabb:
                continue
            hit = None
            for sp_gid, aabb in space_aabb.items():
                if contains(aabb, p):
                    hit = sp_gid
                    break
            if not hit:
                # nearest center fallback
                best_d2, best_sp = 1e99, None
                for sp_gid, aabb in space_aabb.items():
                    cx,cy,cz = center(aabb)
                    d2 = (cx-p[0])**2 + (cy-p[1])**2 + (cz-p[2])**2
                    if d2 < best_d2:
                        best_d2, best_sp = d2, sp_gid
                hit = best_sp

            if hit:
                s.run("MATCH (e {globalId:$e}),(sp:IfcSpace {globalId:$sp}) "
                      "MERGE (e)-[:IN_SPACE]->(sp)", e=t.GlobalId, sp=hit)
                assigned += 1

    drv.close()
    print(f"✅ Linked {assigned} diffuser-like elements → IN_SPACE")

if __name__ == "__main__":
    main()
