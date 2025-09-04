import ifcopenshell
import os

# Paths to the input IFC files
arch_path = "data/raw/ifc/sample_hospital/SampleHospital_Ifc2x3_Arch.ifc"
mech_path = "data/raw/ifc/sample_hospital/SampleHospital_Ifc2x3_Mech.ifc"

# Output path for the combined IFC file
combined_path = "data/raw/ifc/sample_hospital/SampleHospital_Ifc2x3_Combined.ifc"

# Load both IFC files
arch = ifcopenshell.open(arch_path)
mech = ifcopenshell.open(mech_path)

# Create a new IFC file using the schema from the architectural file
combined = ifcopenshell.file(schema=arch.schema)

# Add all entities from the architectural file
for entity in arch:
    combined.add(entity)

# Add all entities from the mechanical file
for entity in mech:
    combined.add(entity)

# Write the combined IFC file
combined.write(combined_path)

print(f"Combined IFC file created at: {os.path.abspath(combined_path)}")
