from rdflib.graph import Graph
from rdfwrappers import *
from i2b2wrappers import *
from starschema import *
from data_loader import *
from scripts.merge_metavaluefields import *
from scripts.fill_metadata_units import *


def check_macros():
    """
    Check the config files are properly formatted and the concepts to use as entrypoints are consistent, without duplicates.
    """
    pass


def generate_ontology_table():
    # First let's setup the graph navigation
    parser = GraphParser(ONTOLOGY_GRAPHS_LOCATIONS)
    parser.define_namespaces()
    root_entries = parser.get_entrypoints(ROOT_URIS)

    # Now prepare the directory and writing
    #wipe_directory(OUTPUT_TABLES_LOCATION)
    init = True
    # Actual loop over the root links
    for concept_res in root_entries:
        concept = Concept(concept_res)
        # Initialize the converter using the list of objects
        converter = I2B2Converter(concept)
        # Get the i2b2 db lines related to this concept
        buffer = converter.get_batch()
        while buffer:
            converter.write(OUTPUT_TABLES_LOCATION + "METADATA.csv", init_table=init)
            buffer = converter.get_batch()
            init = False

    print("Done, all went well and your ontology tables are available in", OUTPUT_TABLES_LOCATION)

    # Step 3: Write the root information in the DB file then merge all concept files into it
    # Already done if the parser.get_entrypoints was given the root uri
    # Else, we have to make sure the root line is always written (or table will be invalid!)
    # TODO: perhaps always write the root line explicitly instead of in the loop.
    # If so, change the "level=0" option in the set_level since only the root (= the line accessible through table_access) should have level 0

    # Step 4

def merge_roots():
    df = pd.read_csv(OUTPUT_TABLES_LOCATION+"METADATA.csv")
    df = df.replace([":Concept"], ["sphn:SPHNConcept"], regex=True)
    lvl = df.loc[df["C_HLEVEL"]==0]
    if len(lvl)>1:
        lvl = lvl.drop(lvl.iloc[[0]].index)
        df = df.drop(lvl.index)
    df.fillna("").to_csv(path_or_buf=OUTPUT_TABLES_LOCATION+"METADATA.csv", mode="w", header=True, index=False)


if __name__ == "__main__":
    
    create_dir(OUTPUT_TABLES_LOCATION)
    generate_ontology_table()
    merge_roots()
    merge_metadatavaluefields(OUTPUT_TABLES_LOCATION, MIGRATIONS)
    insert_units(OUTPUT_TABLES_LOCATION)

    gen_concept_modifier_dim(
        folder_path=OUTPUT_TABLES_LOCATION, metadata_filename="METADATA.csv"
    )
    gen_table_access(folder_path=OUTPUT_TABLES_LOCATION, metadata_filenames=["METADATA.csv"])
