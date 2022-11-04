# This bash file contains tools to create the production-ready CSV tables.

DEFAULT_DATE="01.01.2022"
PROJECT="BIOREF"
PROD_FOLDER="$(pwd)../output_tables/"
DEBUG_FOLDER="$(pwd)"

help () {
    echo "
    Usage: bash postprod.bash -outputF /home/.../my_production_folder -debugF /home/.../my_debug_folder(can be the current directory)
    (default are now set to $PROD_FOLDER and $DEBUG_FOLDER )

    This script creates production-ready tables. It will reindex the patient and encounter numbers to integers 
    and store the lookup tables in the relevant files.
    \nIt can also replace the codes in tables generated through debug mode, making them production-ready tables.
    \n
    \n
    To skip the debug-to-production code replacement, use the --skip-replacing flag."
}

main () {
    skip=0
    while [ "$1" != "" ];
    do
        case $1 in:
        -outputF ) shift
            PROD_FOLDER=$1
            ;;
        -debugF ) shift
            DEBUG_FOLDER=$1
            ;;
        --skip-replacing
            skip=1
            ;;
        *)
            help
            ;;

    reindex
    fill_default_values
    if ! [ skip ] ;then
        [[ "${PROD_FOLDER}" != */ ]] && PROD_FOLDER="${PROD_FOLDER}/"
        [[ "${DEBUG_FOLDER}" != */ ]] && DEBUG_FOLDER="${DEBUG_FOLDER}/"]
        replace_codes $PROD_FOLDER $DEBUG_FOLDER
    fi
}

fill_default_values() {
    awk -v defdate=$DEFAULT_DATE '($4==""){$4="@"}($5==""){$5=defdate}1' FS=, OFS=, OBSERVATION_FACT.csv > tmp && mv -f tmp OBSERVATION_FACT.csv
}

replace_codes () {
    echo "Replacing debug codes by production codes"
    PATH_HCD=${1}CONCEPT_DIMENSION.csv
    PATH_HMD=${1}MODIFIER_DIMENSION.csv
    PATH_HOF=${1}OBSERVATION_FACT.csv

    LOOKUP_CONCEPTS = ${1}lookup_concepts.csv
    LOOKUP_MODIFIERS = ${1}lookup_modifiers.csv

    PATH_DCD=${2}CONCEPT_DIMENSION_DEBUG.csv
    PATH_DMD=${2}MODIFIER_DIMENSION_DEBUG.csv
    PATH_DOF=${2}OBSERVATION_FACT_DEBUG.csv


    # 1. generate lookup tables between debug and non debug codes:
    if ! [ -f $LOOKUP_CONCEPTS] ; then
        awk 'FNR==NR{a[$1]=$2;next}{ print $2 FS a[$1]}' FS=, OFS=, $PATH_DCD $PATH_HCD > $LOOKUP_CONCEPTS
    fi
    if ! [ -f $LOOKUP_MODIFIERS] ; then
        awk 'FNR==NR{a[$1]=$2;next}{ print $2 FS a[$1]}END{print "@" FS "@"}' FS=, OFS=, $PATH_DMD $PATH_HMD > $LOOKUP_MODIFIERS
    fi

    # 2. replace column of OBSERVATION fact by their equivalent in non-debug mode
    awk 'FNR==NR{a[$2]=$1;next}{ $3=a[$3]}1' FS=, OFS=, $LOOKUP_CONCEPTS $PATH_DOF > $1/tmp
    awk 'FNR==NR{a[$2]=$1;next}{ $6=a[$6]}1' FS=, OFS=, $LOOKUP_MODIFIERS $1/tmp > $PATH_HOF && rm -f $1/tmp
    echo "Debug codes were correctly replaced."
}

reindex () {
    # This function takes care of patient_dimension reindexing and filling of patient_mapping accordingly. 
    # It also does the equivalent job for visit_dimension and encounter_mapping
    # Required: filled patient_dimension and visit_dimension, header-only encounter_mapping and patient_mapping.
    # Source on OBSERVATION_FACT and extract a pruned list of (encounter,patient) pairs with it
    echo "Starting reindexing"
    awk '(NR>1){print $1 FS $2}' FS=, OFS=, OBSERVATION_FACT.csv | awk '!visited[$0]++' FS=, OFS=, > unique_PE_pairs.csv
    reindex_encounters
    reindex_patients
    rm -f unique_PE_pairs.csv 
}

reindex_encounters () {
    echo "Reindexing encounters..."
    awk -v proj=$PROJECT -v default="-1" '(NR==1){print $0; cnter=1; next}
            (NR>FNR && $1!="" && !visited[$1]++){print $1 FS $1 FS proj FS cnter++ FS default FS default FS FS FS FS FS FS FS}' \
        FS=, OFS=, ENCOUNTER_MAPPING.csv unique_PE_pairs.csv > tmp \
        && mv -f tmp ENCOUNTER_MAPPING.csv
    echo "ENCOUNTER_MAPPING written."

    # Fill VISIT_DIMENSION accordingly
    awk '(NR==1){print $0}(NR!=FNR && FNR>1){print $4 FS $4 FS FS FS FS FS FS FS FS FS FS FS FS FS}' \
        FS=, OFS=, VISIT_DIMENSION.csv ENCOUNTER_MAPPING.csv > tmp \
        && mv -f tmp VISIT_DIMENSION.csv
    echo "VISIT_DIMENSION written."

    # Read ENCOUNTER_MAPPING and replace values in OBSERVATION_FACT
    awk '(FNR==NR){a[$1]=$4;next}(FNR!=1){$1=a[$1]}{print $0}' \
        FS=, OFS=, ENCOUNTER_MAPPING.csv OBSERVATION_FACT.csv > tmp \
        && mv -f tmp OBSERVATION_FACT.csv
    echo "Reindexed encounters in OBSERVATION_FACT."

}

reindex_patients () {
    # Fill PATIENT_MAPPING with the info and index from PATIENT_DIMENSION
    awk -v proj=$PROJECT '(NR==1){print $0; cnter=1; next} \
        (NR>FNR && !visited[$2]++){print $2 FS $2 FS cnter++ FS FS proj FS FS FS FS FS FS}' \
        FS=, OFS=, PATIENT_MAPPING.csv unique_PE_pairs.csv > tmp \
        && mv -f tmp PATIENT_MAPPING.csv
    echo "PATIENT_MAPPING written."

    # Fill PATIENT_DIMENSION accordingly
    awk '(NR==1){print $0}(NR!=FNR && FNR>1){print $3 FS $3 FS FS FS FS FS FS FS FS FS FS FS FS FS}' \
        FS=, OFS=, PATIENT_DIMENSION.csv PATIENT_MAPPING.csv > tmp \
        && mv -f tmp PATIENT_DIMENSION.csv
    echo "PATIENT_DIMENSION written."

    # Read PATIENT_MAPPING and replace values in OBSERVATION_FACT
    awk '(FNR==NR){a[$1]=$3;next}(FNR!=1){$2=a[$2]}{print $0}' \
        FS=, OFS=, PATIENT_MAPPING.csv OBSERVATION_FACT.csv > tmp \
        && mv -f tmp OBSERVATION_FACT.csv
    echo "Reindexed patients in OBSERVATION_FACT."
}

main $@