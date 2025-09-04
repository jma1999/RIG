import json, pathlib
ev = json.loads(pathlib.Path("data/processed/evidence.json").read_text())
nodes = {n["id"]: n for n in ev["nodes"]}
spaces  = sorted({n["name"] for n in ev["nodes"] if "IfcSpace" in (n.get("labels") or []) and n.get("name")})
storeys = sorted({n["name"] for n in ev["nodes"] if "IfcBuildingStorey" in (n.get("labels") or []) and n.get("name")})
print("Rooms:", ", ".join(spaces) or "(none)")
print("Storeys:", ", ".join(storeys) or "(none)")
