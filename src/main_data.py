from data_loader import *
from scripts.merge_datafields import transfer_obs_numerical_values
from scripts.obs_tools import check_basecodes
from starschema import init_star_schema, query_providers
import psutil
import subprocess
import shutil

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
    # i2b2 star schema tables creation
    init_star_schema(providers=providers_generator)
    # Now fill the star schema dimension, doing the reindexing of patients and encounters in the meantime
    # Do it directly if production run, let the user trigger it is verbose run.
    if DEBUG!="True":
        subprocess.run(["bash", "src/scripts/postprod.bash", "--skip-replacing", "-outputF", OUTPUT_TABLES_LOCATION, "-inputF", OUTPUT_TABLES_LOCATION])
    else:
        # copy the bash scripts to the output folder so the user can trigger them from their host
        shutil.copy2("src/scripts/postprod.bash", OUTPUT_TABLES_LOCATION)
        print("Debug tables have been written in your destination folder along with a bash script you can use to generate production-ready tables. \
            To achieve that, go in the said folder and run ($ bash postprod.bash). \
            You can also modify the environment variables defined on top of the file to configure your destination folder.")

    # Final sanity check
    #subprocess.run(["src/scripts/check_basecodes.bash"])
    check_basecodes()
