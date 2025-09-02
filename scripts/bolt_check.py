import os, sys
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

URI  = os.getenv("NEO4J_URI", "bolt://localhost:7687")
USER = os.getenv("NEO4J_USER", "neo4j")
PASS = os.getenv("NEO4J_PASSWORD", "changeme123")
DB   = os.getenv("NEO4J_DATABASE", "neo4j")

def run(session, q):
    return list(session.run(q))

def main():
    driver = GraphDatabase.driver(URI, auth=(USER, PASS))
    try:
        driver.verify_connectivity()
    except Exception as e:
        print("verify_connectivity failed:", e)
        sys.exit(2)

    with driver.session(database=DB) as s:
        print(f"✅ Connected to {URI} (db='{DB}') as {USER}")

        ok = run(s, "RETURN 1 AS ok")[0]["ok"]
        print("RETURN 1 →", ok)

        # Neo4j 5-friendly way to get version/edition
        info = run(s, "SHOW SERVER YIELD version, edition RETURN version, edition LIMIT 1")[0]
        print("Neo4j:", info["version"], "|", info["edition"])

        # Check APOC (will raise if not loaded)
        try:
            apoc_ver = run(s, "RETURN apoc.version() AS apoc")[0]["apoc"]
            print("APOC:", apoc_ver)
        except Exception as e:
            print("APOC not available:", f"{e.__class__.__name__}: {e}")

    driver.close()

if __name__ == "__main__":
    main()
