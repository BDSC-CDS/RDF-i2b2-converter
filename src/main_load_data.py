from data_loader import *
from scripts.merge_datafields import *

def load_observations():
    parser = GraphParser(paths=[DATA_GRAPHS_LOCATION])
    parser.define_namespaces()
    entry_classes = parser.get_entrypoints(ENTRY_DATA_CONCEPTS)
    dl = DataLoader(parser, entry_classes, filename=OUTPUT_TABLES+"OBSERVATION_FACT.csv", reset_file=True)
    pdb.set_trace()
    dl.extract_all()

if __name__=="__main__":
    load_observations()
    transfer_obs_numerical_values()
    check_basecodes()
