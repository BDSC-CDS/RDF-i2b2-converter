from doctest import ELLIPSIS_MARKER
from data_loader import *
from scripts.merge_datafields import *
from scripts.obs_tools import *
from starschema import fill_star_schema


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
    # Run scripts to modify the table according to project-specific purposes
    transfer_obs_numerical_values(OUTPUT_TABLES_LOCATION)
    if DEBUG!="True":
        # i2b2-formatting routines
        lookup_table = reindex()
        fill_nulls()

        # i2b2 star schema tables creation
        fill_star_schema(mappings=lookup_table, graph_parser=graphparser)
    # Final sanity check
    if not check_basecodes():
        check_basecodes(stop=True)
    else:
        print("Success! All items are consistent with the ontology.")
