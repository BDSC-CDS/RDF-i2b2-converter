from i2b2wrappers import *

def generate_ontology_table():
    # Now adding the namespaces so we can refer to them as macros e.g RDF stands for rdflib.Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
    # or whatever is in the graph
    ns = [e for e in ONTOLOGY_GRAPH.namespace_manager.namespaces()]
    for tupp in ns:
        key, val = tupp
        globals()[key.upper()] = rdflib.Namespace(val)

    entry_objs = give_entry_concepts()

    for concept in entry_objs:
        concept.explore_children()
        # or do this :
        db_lines = concept.toi2b2repr()

    """
    At this point all the concepts can be simply mapped to i2b2 concepts and modifiers
    """

    #calling the i2b2 mapping routines

    """
    Now simply filling the database with every element in memory
    """

def generate_CRC_tables():
    pass
