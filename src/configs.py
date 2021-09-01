import json

with open("files/ontology_config.json") as ff:
    config = json.load(ff)
for key, val in config["parameters"].items():
    globals()[key] = val
for key, val in config["uris"].items():
    globals()[key] = val

with open("files/i2b2_rdf_mapping.json") as ff:
    config = json.load(ff)
for key, val in config.items():
    globals()[key] = val
