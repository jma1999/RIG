import json, sys, pathlib, math
from typing import Any, Dict, List
try:
    import ifcopenshell
except Exception as e:
    print("ERROR: ifcopenshell not available. `pip install ifcopenshell` first.")
    raise

def sv(v):
    # Safely extract scalar from IFC typed values (e.g., IfcLabel, IfcText, IfcIdentifier)
    try:
        return v.wrappedValue  # older ifcopenshell
    except Exception:
        pass
    try:
        return v.value  # newer ifcopenshell
    except Exception:
        pass
    return v

def get_name(ent) -> str:
    for attr in ("Name", "LongName"):
        if hasattr(ent, attr):
            val = getattr(ent, attr)
            if val:
                val = sv(val)
                if isinstance(val, str) and val.strip():
                    return val.strip()
    return ""

def extract_psets(ent) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    rels = getattr(ent, "IsDefinedBy", None) or []
    for r in rels:
        if not r or not r.is_a("IfcRelDefinesByProperties"):
            continue
        pdef = r.RelatingPropertyDefinition
        if not pdef or not pdef.is_a("IfcPropertySet"):
            continue
        pset_name = sv(pdef.Name) if getattr(pdef, "Name", None) else "Pset"
        for p in pdef.HasProperties or []:
            if p.is_a("IfcPropertySingleValue"):
                key = f"{pset_name}.{sv(p.Name)}" if getattr(p, "Name", None) else pset_name
                val = sv(p.NominalValue) if getattr(p, "NominalValue", None) else None
                # keep scalars only
                if isinstance(val, (str, int, float, bool)) or val is None:
                    out[key] = val
                else:
                    out[key] = str(val)
    return out

def ref(obj) -> Dict[str, str]:
    return {"ref": obj.GlobalId} if obj and getattr(obj, "GlobalId", None) else {}

def convert(ifc_path: pathlib.Path, out_path: pathlib.Path):
    f = ifcopenshell.open(str(ifc_path))
    instances: Dict[str, Dict[str, Any]] = {}

    # Pass 1: dump entities with GUIDs (IfcProject..IfcElement..IfcSystem..IfcSpace..)
    for ent in f:
        gid = getattr(ent, "GlobalId", None)
        if not gid:
            continue
        t = ent.is_a()
        entry: Dict[str, Any] = {
            "type": t,
            "Name": get_name(ent),
            "attributes": {},     # you can add more later
            "psets": extract_psets(ent),
        }

        if t in ("IfcDistributionPort", "IfcPort"):
            fd = getattr(ent, "FlowDirection", None)
            if fd:
                entry.setdefault("attributes", {})["FlowDirection"] = str(fd)
            pdt = getattr(ent, "PredefinedType", None)
            if pdt:
                entry.setdefault("attributes", {})["PredefinedType"] = str(pdt)

        # minimal useful attributes
        for attr in ("ObjectType", "Description"):
            if hasattr(ent, attr) and getattr(ent, attr):
                entry["attributes"][attr] = sv(getattr(ent, attr))
        instances[gid] = entry

    # Pass 2: add core IfcRel* instances we care about
    def put_rel(rel_type: str, make):
        for r in f.by_type(rel_type):
            gid = getattr(r, "GlobalId", None)
            if not gid:
                continue
            instances[gid] = {"type": rel_type, **make(r)}

    # Spatial containment
    put_rel("IfcRelContainedInSpatialStructure", lambda r: {
        "RelatingStructure": ref(r.RelatingStructure),
        "RelatedElements": [ref(e) for e in (r.RelatedElements or [])],
    })

    # Aggregation (e.g., Building -> Storeys, Assemblies)
    put_rel("IfcRelAggregates", lambda r: {
        "RelatingObject": ref(r.RelatingObject),
        "RelatedObjects": [ref(e) for e in (r.RelatedObjects or [])],
    })

    # Systems serving buildings (may be more common in IFC4, but safe to include)
    put_rel("IfcRelServicesBuildings", lambda r: {
        "RelatingSystem": ref(r.RelatingSystem),
        "RelatedBuildings": [ref(b) for b in (r.RelatedBuildings or [])],
    })

    # Assign elements to groups/systems (MEP systems)
    put_rel("IfcRelAssignsToGroup", lambda r: {
        "RelatingGroup": ref(r.RelatingGroup),
        "RelatedObjects": [ref(e) for e in (r.RelatedObjects or [])],
    })

    # Port to Element
    put_rel("IfcRelConnectsPortToElement", lambda r: {
        "RelatingPort": ref(r.RelatingPort),
        "RelatedElement": ref(r.RelatedElement),
    })

    # Port to Port
    put_rel("IfcRelConnectsPorts", lambda r: {
        "RelatingPort": ref(r.RelatingPort),
        "RelatedPort": ref(r.RelatedPort),
    })

    # Element to Element
    put_rel("IfcRelConnectsElements", lambda r: {
        "RelatingElement": ref(r.RelatingElement),
        "RelatedElement": ref(r.RelatedElement),
    })

    # Output
    out = {"instances": instances}
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2))
    print(f"✅ Wrote {out_path}  | instances: {len(instances)}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python scripts/ifc_to_ifcjson_min.py <in.ifc> <out.json>")
        sys.exit(2)
    convert(pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2]))
