# ingest/probe_placements.py
import json, pathlib, itertools

def load_instances(doc):
    for k in ("objects","instances"):
        if isinstance(doc.get(k), dict): return doc[k]
    return doc

def peek(path, max_hits=8):
    data = json.loads(pathlib.Path(path).read_text())
    inst = load_instances(data)

    # 1) How many objects even have a placement?
    has_op = [gid for gid,obj in inst.items()
              if isinstance(obj, dict) and ("ObjectPlacement" in obj or "objectPlacement" in obj)]
    print(f"\n{path}: objects with ObjectPlacement = {len(has_op)}")

    # 2) Print the first few shapes
    hits = 0
    for gid, obj in inst.items():
        if not isinstance(obj, dict): continue
        if "ObjectPlacement" not in obj and "objectPlacement" not in obj:
            continue
        op = obj.get("ObjectPlacement") or obj.get("objectPlacement")
        t  = obj.get("type") or obj.get("class")
        print("\n=== GUID:", gid, "type:", t)
        print("  op type:", type(op).__name__, "keys:", list(op.keys()) if isinstance(op, dict) else None)

        rp = None
        if isinstance(op, dict):
            rp = op.get("RelativePlacement") or op.get("relativePlacement")
            pr = op.get("PlacementRelTo")  or op.get("placementRelTo")
            print("  has PlacementRelTo? ", isinstance(pr, (dict,str)))
            if isinstance(rp, dict):
                loc = rp.get("Location") or rp.get("location")
                print("  rp keys:", list(rp.keys()))
                if isinstance(loc, dict):
                    coords = loc.get("Coordinates") or loc.get("coordinates")
                    print("  loc keys:", list(loc.keys()))
                    print("  coords sample:", str(coords)[:200])
                else:
                    print("  loc:", type(loc).__name__)
            else:
                print("  rp:", type(rp).__name__)

        hits += 1
        if hits >= max_hits:
            break

    # 3) Presence of referenced types (lets us know what to follow)
    n_lp = sum(1 for o in inst.values() if isinstance(o, dict) and (o.get("type")== "IfcLocalPlacement"))
    n_ax = sum(1 for o in inst.values() if isinstance(o, dict) and (o.get("type") in ("IfcAxis2Placement3D","IfcAxis2Placement2D")))
    n_cp = sum(1 for o in inst.values() if isinstance(o, dict) and (o.get("type")== "IfcCartesianPoint"))
    print(f"\nType counts — IfcLocalPlacement: {n_lp}, IfcAxis2Placement3D/2D: {n_ax}, IfcCartesianPoint: {n_cp}")

if __name__ == "__main__":
    import sys
    for p in sys.argv[1:]:
        print("\n### Probing", p)
        peek(p)
