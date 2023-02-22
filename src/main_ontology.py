"""
Main file for ontology conversion.
"""

from rdfwrappers import Concept
from i2b2wrappers import I2B2Converter
from starschema import gen_concept_modifier_dim, gen_table_access
from scripts.merge_metavaluefields import merge_metadatavaluefields
from scripts.fill_metadata_units import insert_units
from utils import merge_roots, GraphParser, create_dir, read_config
from constant import (
    GRAPH_CONFIG_FILE,
    I2B2_MAPPING_FILE,
    METADATA_FILENAME,
    UNITS_FILENAME,
    MIGRATIONS_FILENAME,
)

GRAPH_CONFIG: dict = {}
I2B2_CONFIG: dict = {}


def convert_main(
    parser,
    root_uris,
    metadata_file,
):
    """
    Upper stack of the conversion algorithm: from the ontology root(s),
    """
    # First let's setup the graph navigation
    root_entries = parser.get_entrypoints(entrypoints=root_uris)

    init = True
    # Actual loop over the root links
    for concept_res in root_entries:
        concept = Concept(
            concept_res,
            reserved_uris=GRAPH_URIS,
            pref_language=GRAPH_PARAMETERS["PREF_LANGAGE"],
            mixed_trees=GRAPH_PARAMETERS["ALLOW_MIXED_TREES"],
        )
        # Initialize the converter (auto setup the first level of root descendance)
        converter = I2B2Converter(concept)
        # Sequentially explore the descendance of every instantiable concept,
        # convert it and write the transcript ("batch") to the metadata file.
        buffer = converter.get_batch()
        while buffer:
            converter.write(
                outfile=metadata_file,
                init_status=init,
            )
            buffer = converter.get_batch()
            init = False

    print(
        "Done, all went well and your ontology tables are available in",
        output_tables_location,
    )


if __name__ == "__main__":
    ### Setting up the configs and folders
    GRAPH_CONFIG = read_config(GRAPH_CONFIG_FILE)
    GRAPH_URIS = GRAPH_CONFIG["uris"]
    GRAPH_PARAMETERS = GRAPH_CONFIG["parameters"]
    I2B2_CONFIG = read_config(I2B2_MAPPING_FILE)
    output_tables_location = I2B2_CONFIG["OUTPUT_TABLES_LOCATION"]
    metadata_fpath = output_tables_location + METADATA_FILENAME
    create_dir(output_tables_location)

    ### Trigger the conversion
    convert_main(
        parser=GraphParser(
            paths=GRAPH_PARAMETERS["ONTOLOGY_GRAPHS_LOCATIONS"],
            rdf_format=GRAPH_PARAMETERS["RDF_FORMAT"],
            terminologies_links=GRAPH_PARAMETERS["TERMINOLOGIES_GRAPHS"],
        ),
        root_uris=GRAPH_URIS["ROOT_URIS"],
        metadata_file=metadata_fpath,
    )

    ### Post-processing of the i2b2 CSV tables (pandas manipulations)
    merge_roots(target_file=metadata_fpath)
    merge_metadatavaluefields(
        migrations_config=I2B2_CONFIG["MIGRATIONS"],
        metadata_file=metadata_fpath,
        migrations_logfile=output_tables_location + MIGRATIONS_FILENAME,
    )
    insert_units(
        metadata_file=metadata_fpath,
        units_file=output_tables_location + UNITS_FILENAME,
    )
    gen_concept_modifier_dim(
        output_tables_loc=output_tables_location,
        metadata_file=metadata_fpath,
        columns=I2B2_CONFIG["COLUMNS"],
        debug_status=I2B2_CONFIG["DEBUG"],
    )
    gen_table_access(
        output_tables_loc=output_tables_location,
        metadata_file=metadata_fpath,
        columns=I2B2_CONFIG["COLUMNS"]["TABLE_ACCESS"],
    )
