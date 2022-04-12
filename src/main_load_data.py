from data_loader import *
from scripts.merge_datafields import *
from scripts.obs_tools import *
from starschema import fill_star_schema

def load_observations():
    parser = GraphParser(paths=[DATA_GRAPHS_LOCATION, CONTEXT_GRAPHS_LOCATION])
    parser.define_namespaces()
    entry_classes = parser.get_entrypoints(ENTRY_DATA_CONCEPTS)
    dl = DataLoader(parser, entry_classes, filename=OUTPUT_TABLES+"OBSERVATION_FACT.csv", reset_file=True)
    dl.extract_all()
    pdb.set_trace()

if __name__=="__main__":
    load_observations()
    # Run scripts to modify the table according to project-specific purposes
    transfer_obs_numerical_values()

    # i2b2-formatting routines
    lookup_table = reindex()
    fill_nulls()

    # i2b2 star schema tables creation
    fill_star_schema(mappings = lookup_table)

    # Final sanity check
    assert check_basecodes()
