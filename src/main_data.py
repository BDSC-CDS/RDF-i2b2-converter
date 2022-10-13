from data_loader import *
from scripts.merge_datafields import transfer_obs_numerical_values
from scripts.obs_tools import reindex, fill_nulls, check_basecodes
from starschema import fill_star_schema, query_providers
import psutil

def load_observations():
    parser = GraphParser(paths=[DATA_GRAPHS_LOCATION, CONTEXT_GRAPHS_LOCATION])
    parser.define_namespaces()
    entry_classes = parser.get_entrypoints(ENTRY_DATA_CONCEPTS)
    dl = DataLoader(
        parser,
        entry_classes,
        filename=OUTPUT_TABLES_LOCATION + "OBSERVATION_FACT.csv",
        reset_file=True,
    )
    dl.extract_all()
    return parser


if __name__ == "__main__":

    create_dir(OUTPUT_TABLES_LOCATION)
    graphparser = load_observations()
    providers_generator = query_providers(graphparser)
    print("Before freeing memory, using : ", psutil.virtual_memory()[2], "%")
    graphparser.free_memory()
    print("After freeing memory, using : ", psutil.virtual_memory()[2], "%")
    # Run scripts to modify the table according to project-specific purposes
    transfer_obs_numerical_values(OUTPUT_TABLES_LOCATION)
    if DEBUG!="True":
        # i2b2-formatting routines
        lookup_table = reindex()
        fill_nulls()

        # i2b2 star schema tables creation
        fill_star_schema(mappings=lookup_table, providers=providers_generator)
    # Final sanity check
    check_basecodes()
