// Base: every node gets :IfcEntity + its IFC type as a label (e.g., :IfcSpace)
CREATE CONSTRAINT IF NOT EXISTS
FOR (n:IfcEntity) REQUIRE n.globalId IS UNIQUE;

// Helpful indexes for early queries
CREATE INDEX IF NOT EXISTS FOR (n:IfcSpace) ON (n.name);
CREATE INDEX IF NOT EXISTS FOR (n:IfcBuildingStorey) ON (n.name);
CREATE INDEX IF NOT EXISTS FOR (n:IfcSystem) ON (n.name);
CREATE INDEX IF NOT EXISTS FOR (n:IfcDistributionElement) ON (n.name, n.type);
