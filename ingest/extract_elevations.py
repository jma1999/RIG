# ingest/extract_elevations.py
import os, json, pathlib, argparse
from typing import Any, Dict, Optional, List, Set
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()
URI  = os.getenv("NEO4J_URI", "bolt://localhost:7687")
USER = os.getenv("NEO4J_USER", "neo4j")
PASS = os.getenv("NEO4J_PASSWORD", "changeme123")
DB   = os.getenv("NEO4J_DATABASE", "neo4j")

def first(d: Dict, *keys):
    for k in keys:
        if k in d: return d[k]
    a = d.get("attributes") or {}
    for k in keys:
        if k in a: return a[k]
    return None

def _as_float_primitive(x) -> Optional[float]:
    try:
        if isinstance(x, (int, float)): return float(x)
        if isinstance(x, str) and x.strip(): return float(x)
    except Exception:
        return None
    return None

def as_number(x) -> Optional[float]:
    """Robust numeric unwrapping: dicts with 'value', ifc wrappers, 1-elem lists/tuples."""
    if x is None:
        return None
    if isinstance(x, (int, float, str)):
        return _as_float_primitive(x)
    if isinstance(x, (list, tuple)):
        # common patterns: [{"type":"IfcLengthMeasure","value":123.0}, ...] OR [123.0, 0.0, 3.0]
        if not x:
            return None
        # If all elements are primitives/dicts, prefer first non-None
        for it in x:
            v = as_number(it)
            if v is not None:
                return v
        return None
    if isinstance(x, dict):
        # ifcjson wrappers often look like {"type":"IfcLengthMeasure","value":123.0}
        for k in ("value","Value","nominalValue","NominalValue"):
            if k in x:
                v = as_number(x[k])
                if v is not None:
                    return v
        # sometimes coordinates come as {"x":..,"y":..,"z":..}
        if any(k in x for k in ("x","X","y","Y","z","Z")):
            # leave vector dicts to coords handler
            return None
        # or { "IfcLengthMeasure": 123.0 } / { "IfcLengthMeasure": [123.0] }
        for k in list(x.keys()):
            if "IfcLengthMeasure" in k:
                return as_number(x[k])
        # last resort: single-key dict
        if len(x)==1:
            return as_number(list(x.values())[0])
    return None

class IfcResolver:
    def __init__(self, inst: Dict[str, Dict]):
        self.inst = inst

    def deref(self, x: Any) -> Optional[Dict]:
        if x is None:
            return None
        if isinstance(x, dict):
            for k in ("ref", "$ref", "Ref", "REF"):
                if k in x:
                    return self.inst.get(str(x[k]))
            return x
        if isinstance(x, str):
            return self.inst.get(x)
        return None

    def coords_from_point(self, cp: Dict) -> Optional[List[float]]:
        pts = first(cp, "Coordinates", "coordinates")
        # list variant
        if isinstance(pts, list):
            vals = [as_number(v) for v in pts]
            vals = [v for v in vals if v is not None]
            if vals:
                return vals
            return None
        # dict variant with x/y/z (each can be wrapped)
        if isinstance(pts, dict):
            out = []
            for k in ("x","X","y","Y","z","Z"):
                if k in pts:
                    out.append(as_number(pts[k]))
            out = [v for v in out if v is not None]
            return out or None
        return None

    def z_from_local_placement(self, lp: Dict) -> Optional[float]:
        seen: Set[str] = set()
        z_total = 0.0
        steps = 0
        cur = lp
        while isinstance(cur, dict) and steps < 64:
            oid = cur.get("id") or cur.get("GlobalId") or cur.get("globalId")
            if oid:
                soid = str(oid)
                if soid in seen:
                    break
                seen.add(soid)

            rp = self.deref(first(cur, "RelativePlacement", "relativePlacement"))
            if isinstance(rp, dict):
                loc = self.deref(first(rp, "Location", "location"))
                if isinstance(loc, dict):
                    coords = self.coords_from_point(loc)
                    if coords and len(coords) >= 3 and coords[2] is not None:
                        z_total += float(coords[2])

            parent = self.deref(first(cur, "PlacementRelTo", "placementRelTo"))
            if parent is None:
                break
            cur = parent
            steps += 1

        return z_total if steps >= 0 else None

    def z_from_object_placement(self, obj: Dict) -> Optional[float]:
        op = self.deref(first(obj, "ObjectPlacement", "objectPlacement"))
        if not isinstance(op, dict):
            return None
        return self.z_from_local_placement(op)

def load_instances(doc: Dict) -> Dict[str, Dict]:
    for k in ("objects","instances"):
        if isinstance(doc.get(k), dict):
            return doc[k]
    if isinstance(doc, dict) and all(isinstance(v, dict) for v in doc.values()):
        return doc
    raise ValueError("Unsupported ifcJSON structure")

def build_spatial_index(inst: Dict[str, Dict]) -> Dict[str, List[str]]:
    children = {}
    for k, o in inst.items():
        if not isinstance(o, dict):
            continue
        t = o.get("type") or o.get("class") or o.get("schema") or ""
        if t.startswith("IfcRelContainedInSpatialStructure"):
            rel_str = first(o, "RelatingStructure", "relatingStructure", "RelatingSpatialStructure", "relatingSpatialStructure")
            rel_el  = first(o, "RelatedElements", "relatedElements")
            sid = None
            if isinstance(rel_str, dict):
                sid = str(rel_str.get("ref") or rel_str.get("$ref") or rel_str.get("id") or rel_str.get("GlobalId") or rel_str.get("globalId"))
            elif isinstance(rel_str, str):
                sid = rel_str
            if not sid:
                continue
            arr = []
            if isinstance(rel_el, list):
                for e in rel_el:
                    if isinstance(e, dict):
                        eid = e.get("ref") or e.get("$ref") or e.get("id") or e.get("GlobalId") or e.get("globalId")
                        if eid: arr.append(str(eid))
                    elif isinstance(e, str):
                        arr.append(e)
            elif isinstance(rel_el, dict):
                eid = rel_el.get("ref") or rel_el.get("$ref") or rel_el.get("id") or rel_el.get("GlobalId") or rel_el.get("globalId")
                if eid: arr.append(str(eid))
            children.setdefault(sid, []).extend(arr)
    return children

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("paths", nargs="+", help="Paths to ifcJSON files (Arch/Mech)")
    args = ap.parse_args()

    drv = GraphDatabase.driver(URI, auth=(USER, PASS))
    with drv.session(database=DB) as s:
        for path in args.paths:
            data = json.loads(pathlib.Path(path).read_text())
            inst = load_instances(data)
            res = IfcResolver(inst)
            spatial = build_spatial_index(inst)

            set_z_nodes = 0
            set_elev_storeys = 0

            # Pass 1: storey elevation from attribute or placement
            for guid, obj in inst.items():
                if not isinstance(obj, dict): continue
                t = obj.get("type") or obj.get("class") or obj.get("schema") or ""
                if t == "IfcBuildingStorey":
                    elev_raw = first(obj, "Elevation", "elevation", "ElevationOfRefHeight")
                    z = as_number(elev_raw)
                    if z is None:
                        z = res.z_from_object_placement(obj)
                    if z is not None:
                        s.run("MATCH (st:IfcBuildingStorey {globalId:$id}) SET st.elev = $z", id=guid, z=z)
                        set_elev_storeys += 1

            # Pass 2: per-element Z from placement
            for guid, obj in inst.items():
                if not isinstance(obj, dict): continue
                z = res.z_from_object_placement(obj)
                if z is not None:
                    s.run("MATCH (n {globalId:$id}) SET n.z = $z", id=guid, z=z)
                    set_z_nodes += 1

            # Pass 3: fill missing storey elev via mean child Z from JSON relations
            for guid, obj in inst.items():
                if not isinstance(obj, dict): continue
                t = obj.get("type") or obj.get("class") or obj.get("schema") or ""
                if t == "IfcBuildingStorey":
                    rec = s.run("MATCH (st:IfcBuildingStorey {globalId:$id}) RETURN st.elev AS e", id=guid).single()
                    if rec and rec["e"] is not None:
                        continue
                    kid_ids = spatial.get(guid, [])
                    if not kid_ids:
                        continue
                    avg = s.run("""
                        UNWIND $ids AS id
                        MATCH (n {globalId:id})
                        WHERE n.z IS NOT NULL
                        RETURN avg(n.z) AS av
                    """, ids=kid_ids).single()["av"]
                    if avg is not None:
                        s.run("MATCH (st:IfcBuildingStorey {globalId:$id}) SET st.elev = $z", id=guid, z=float(avg))
                        set_elev_storeys += 1

            print(f"✅ {path}: set z on {set_z_nodes} nodes; set elev on {set_elev_storeys} storeys")

    drv.close()

if __name__ == "__main__":
    main()
