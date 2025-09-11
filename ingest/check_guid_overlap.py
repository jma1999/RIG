# ingest/check_guid_overlap.py
import os, json
import ifcopenshell
from neo4j import GraphDatabase
from dotenv import load_dotenv
load_dotenv()

URI  = os.getenv("NEO4J_URI", "bolt://localhost:7687")
USER = os.getenv("NEO4J_USER", "neo4j")
PASS = os.getenv("NEO4J_PASSWORD", "changeme123")
DB   = os.getenv("NEO4J_DATABASE", "neo4j")

MECH_IFC = "data/raw/ifc/sample_hospital/SampleHospital_Ifc2x3_Mech.ifc"

def all_guids(ifc_path):
    f = ifcopenshell.open(ifc_path)
    s = set()
    for e in f:
        gid = getattr(e, "GlobalId", None)
        if gid: s.add(gid)
    return s

ifc_guids = all_guids(MECH_IFC)
print("MECH IFC GUIDs:", len(ifc_guids))

drv = GraphDatabase.driver(URI, auth=(USER, PASS))
with drv.session(database=DB) as s:
    # pull only MECH-sourced nodes to keep it relevant
    db = s.run("MATCH (n:IfcEntity) WHERE n.source IN ['Mech','Mechanical','MechJSON','MEP'] RETURN n.globalId AS id")
    db_guids = {r["id"] for r in db if r["id"]}

overlap = ifc_guids & db_guids
print("DB MECH nodes:", len(db_guids))
print("GUID overlap:", len(overlap))
if overlap:
    print("sample overlap:", list(sorted(overlap))[:10])
drv.close()
