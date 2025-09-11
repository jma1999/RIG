# ingest/print_placements_probe.py
import statistics as stats
import ifcopenshell
from ifcopenshell.util.placement import get_local_placement

ARCH = "data/raw/ifc/sample_hospital/SampleHospital_Ifc2x3_Arch.ifc"
MECH = "data/raw/ifc/sample_hospital/SampleHospital_Ifc2x3_Mech.ifc"

def safe_by_type(f, tname):
    try:
        return f.by_type(tname)
    except Exception:
        return []

def world_origin(e):
    op = getattr(e, "ObjectPlacement", None)
    if not op:
        return None
    m = get_local_placement(op)  # 4x4 transform
    return (float(m[0][3]), float(m[1][3]), float(m[2][3]))

def is_diffuser_like(e):
    n = getattr(e, "Name", "") or ""
    nl = n.lower() if isinstance(n, str) else str(n).lower()
    for h in ("diffuser", "air terminal", "grille", "register", "outlet"):
        if h in nl:
            return True
    return False

def pts_summary(tag, pts):
    if not pts:
        print(f"{tag}: 0")
        return
    xs, ys, zs = zip(*pts)
    print(f"{tag}: n={len(pts)}  "
          f"X≈[{min(xs):.1f},{max(xs):.1f}]  "
          f"Y≈[{min(ys):.1f},{max(ys):.1f}]  "
          f"Z≈[{min(zs):.1f},{max(zs):.1f}]  "
          f"medianZ≈{stats.median(zs):.1f}")

fA = ifcopenshell.open(ARCH)
fM = ifcopenshell.open(MECH)

# Spaces (always IFC2x3-safe)
space_pts = [p for sp in safe_by_type(fA, "IfcSpace")
             if (p := world_origin(sp))]

# Terminals (use only classes that exist; plus proxies + generic distribution elems)
term_pts = []
for tname in ("IfcFlowTerminal", "IfcAirTerminal", "IfcDuctTerminal"):  # some may not exist
    for e in safe_by_type(fM, tname):
        if (p := world_origin(e)):
            term_pts.append(p)

# Proxies that look like diffusers
for e in safe_by_type(fM, "IfcBuildingElementProxy"):
    if is_diffuser_like(e) and (p := world_origin(e)):
        term_pts.append(p)

# Generic distribution elements with diffuser-like names (broad net)
for e in safe_by_type(fM, "IfcDistributionElement"):
    if is_diffuser_like(e) and (p := world_origin(e)):
        term_pts.append(p)

pts_summary("ARCH IfcSpace origins", space_pts)
pts_summary("MECH diffuser-ish origins", term_pts)

if space_pts and term_pts:
    sZmin, sZmax = min(z for _,_,z in space_pts), max(z for _,_,z in space_pts)
    tZmin, tZmax = min(z for _,_,z in term_pts), max(z for _,_,z in term_pts)
    overlap = not (tZmax < sZmin or tZmin > sZmax)
    print(f"Z-overlap? {overlap} (spaces Z≈[{sZmin:.1f},{sZmax:.1f}] vs terms Z≈[{tZmin:.1f},{tZmax:.1f}])")
