from rdflib.graph import Graph
from rdfwrappers import *
from i2b2wrappers import *
from starschema import *
from data_loader import *


def check_macros():
    """
    Check the config files are properly formatted and the concepts to use as entrypoints are consistent, without duplicates.
    """
    pass


def generate_ontology_table():
    # First let's setup the graph navigation
    parser = GraphParser([ONTOLOGY_GRAPH_LOCATION, TERMINOLOGIES_LOCATION])
    parser.define_namespaces()
    root_entries = parser.get_entrypoints(ROOT_URIS)

    # Now prepare the directory and writing
    wipe_directory(OUTPUT_TABLES)
    init = True
    # Actual loop over the entrypoints
    for concept_res in root_entries:
        concept = Concept(concept_res)
        # Initialize the converter using the list of objects
        converter = I2B2Converter(concept)
        # Get the i2b2 db lines related to this concept
        buffer = converter.get_batch()
        while buffer:
            converter.write(OUTPUT_TABLES+"METADATA.csv", init_table=init)
            buffer = converter.get_batch()
            init = False

    # Step 3: Write the root information in the DB file then merge all concept files into it
    # Already done if the parser.get_entrypoints was given the root uri
    # Else, we have to make sure the root line is always written (or table will be invalid!)
    # TODO: perhaps always write the root line explicitly instead of in the loop.
    # If so, change the "level=0" option in the set_level since only the root (= the line accessible through table_access) should have level 0

    # Step 4

    gen_concept_modifier_dim(
        folder_path=OUTPUT_TABLES, metadata_filename="METADATA.csv"
    )
    gen_table_access(folder_path=OUTPUT_TABLES, metadata_filenames=["METADATA.csv"])


def load_observations():
    parser = GraphParser(paths=[DATA_GRAPHS_LOCATION])
    parser.define_namespaces()
    entry_classes = parser.get_entrypoints(ENTRY_DATA_CONCEPTS)
    dl = DataLoader(parser, entry_classes, filename=OUTPUT_TABLES+"OBSERVATION_FACT", reset_file=True)
    dl.extract_all()


if __name__ == "__main__":
    generate_ontology_table()
