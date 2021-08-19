from i2b2wrappers import *

def generate_ontology_table():
    # Now adding the namespaces so we can refer to them as macros e.g RDF stands for rdflib.Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
    # or whatever is in the graph
    ns = [e for e in ONTOLOGY_GRAPH.namespace_manager.namespaces()]
    for tupp in ns:
        key, val = tupp
        globals()[key.upper()] = rdflib.Namespace(val)

    # Step 1: generate python objects from the entry concept list specified in the config file
    entry_concept_resources = give_entry_concepts()

    # Step 2: For each concept, create an I2B2 converter and extract info from it.
    # This might be suboptimal in terms of memory usage when an entry concept is in fact a directory having a lot of subconcepts
    for concept_res in entry_objs:
        concept = Concept(concept_res)
        concept.explore_children()

        # Initialize the converter using the list of objects
        converter = I2B2Converter(concept)
        # Get the i2b2 db lines related to this concept
        buffer = converter.get_batch()
        while buffer:
            converter.write(METADATA_PATH)
    
    # Step 3: Write the root information in the DB file then merge all concept files into it

    # Step 4 (maybe outside this script?): use SQL to derive I2B2 concept_dimension and modifier_dimension from the ontology table using C_TABLENAME or equivalent
    

def generate_event_tables():
    pass
