#!/usr/bin/bash
set -Eeuo pipefail

# This bash file contains tools to create the production-ready CSV tables.

# Fill elements without starting date to end of this year
DEFAULT_DATE="$(date +%Y)-12-31 00:00:00"
PROJECT="BIOREF"
PROD_FOLDER="$(pwd)"
INPUT_FOLDER="$(pwd)"

help () {
    echo "
    Usage: bash postprod.bash -outputF /home/.../my_production_folder -inputF /home/.../my_verbose_folder(can be the same)
    (default are now set to $1 and $2 )

    This script creates production-ready tables. It will reindex the patient and encounter numbers to integers 
    and store the lookup tables in the relevant files.

    It will also replace the codes in tables generated through verbose mode, making them production-ready tables.
    To skip this verbose-to-production code replacement, use the --skip-replacing flag."
}

main () {
    skip=0
    while [[ $#>0 ]];
    do
        case $1 in
        -outputF) shift
            PROD_FOLDER=$1
            shift
            ;;
        -inputF) shift
            INPUT_FOLDER=$1
            shift
            ;;
        --skip-replacing) shift
            skip=1
            ;;
        *)
            help $PROD_FOLDER $INPUT_FOLDER
            shift
            ;;
        esac
    done

    [[ "${PROD_FOLDER}" != */ ]] && PROD_FOLDER="${PROD_FOLDER}/"
    [[ "${INPUT_FOLDER}" != */ ]] && INPUT_FOLDER="${INPUT_FOLDER}/"

    [[ "${PROD_FOLDER}" != /* ]] && PROD_FOLDER="$(cd "$(dirname "$PROD_FOLDER")"; pwd)/$(basename "$PROD_FOLDER")/"
    [[ "${INPUT_FOLDER}" != /* ]] && INPUT_FOLDER="$(cd "$(dirname "$INPUT_FOLDER")"; pwd)/$(basename "$INPUT_FOLDER")/"
    
    echo "Reindexing elements from $INPUT_FOLDER into $PROD_FOLDER and filling star schema"

    reindex $PROD_FOLDER $INPUT_FOLDER
    fill_default_values $PROD_FOLDER 
    if [ $skip -eq 0 ] ;then
        replace_codes $PROD_FOLDER $INPUT_FOLDER
    fi
}

fill_default_values() {
    awk -v defdate="$DEFAULT_DATE" -v defenc="-1" -v defprovider="@" '($1==""){$1=defenc}($4==""){$4=defprovider}($5==""){$5=defdate}($NF==""){$NF=FNR-1}1' FS=, OFS=, \
    ${1}OBSERVATION_FACT.csv > ${1}tmp && mv -f ${1}tmp ${1}OBSERVATION_FACT.csv
}

replace_codes () {
    echo "Replacing verbose codes by production codes"
    PATH_HCD=${1}CONCEPT_DIMENSION.csv
    PATH_HMD=${1}MODIFIER_DIMENSION.csv
    PATH_OF=${1}OBSERVATION_FACT.csv

    LOOKUP_CONCEPTS=${2}lookup_concepts.csv
    LOOKUP_MODIFIERS=${2}lookup_modifiers.csv

    PATH_DCD=${2}CONCEPT_DIMENSION_VERBOSE.csv
    PATH_DMD=${2}MODIFIER_DIMENSION_VERBOSE.csv
    init_nl=$(wc -l < $PATH_OF)

    # 1. generate lookup tables between verbose and non verbose codes:
    if ! [ -f $LOOKUP_CONCEPTS ] ; then
        awk 'FNR==NR{a[$1]=$2;next}{ print $2 FS a[$1]}' FS=, OFS=, $PATH_DCD $PATH_HCD > $LOOKUP_CONCEPTS
    fi
    if ! [ -f $LOOKUP_MODIFIERS ] ; then
        awk 'FNR==NR{a[$1]=$2;next}{ print $2 FS a[$1]}END{print "@" FS "@"}' FS=, OFS=, $PATH_DMD $PATH_HMD > $LOOKUP_MODIFIERS
    fi

    # 2. replace column of OBSERVATION fact by their equivalent in non-verbose mode
    awk 'FNR==NR{a[$2]=$1;next}(a[$3]){ $3=a[$3]; print $0}' FS=, OFS=, $LOOKUP_CONCEPTS $PATH_OF > ${1}tmp
    awk 'FNR==NR{a[$2]=$1;next}(a[$6]){ $6=a[$6]; print $0}' FS=, OFS=, $LOOKUP_MODIFIERS ${1}tmp > $PATH_OF && rm -f ${1}tmp
    rm $LOOKUP_CONCEPTS $LOOKUP_MODIFIERS
    skipped=$(( $init_nl - $(wc -l < $PATH_OF) ))
    percent="($(echo "$skipped" "$init_nl" |awk '{printf "%.2f", $1 * 100 / $2}')%)"
    echo "Verbose codes were correctly replaced. $skipped lines $percent skipped due to missing equivalences."
}

reindex () {
    # This function takes care of patient_dimension reindexing and filling of patient_mapping accordingly. 
    # It also does the equivalent job for visit_dimension and encounter_mapping
    # Required: filled patient_dimension and visit_dimension, header-only encounter_mapping and patient_mapping.
    # Source on OBSERVATION_FACT and extract a pruned list of (encounter,patient) pairs with it
    echo "Starting reindexing"
    awk '(NR>1){print $1 FS $2}' FS=, OFS=, ${2}OBSERVATION_FACT.csv | awk '!visited[$0]++' FS=, OFS=, > ${1}unique_PE_pairs.csv
    reindex_encounters $@
    reindex_patients $@
    rm -f ${1}unique_PE_pairs.csv 
}

reindex_encounters () {
    echo "Reindexing encounters..."
    awk -v proj=$PROJECT -v defaultval="-1" '(NR==1){print $0; cnter=1; next}
            (NR>FNR && $1!="" && !visited[$1]++){print $1 FS $1 FS proj FS cnter++ FS defaultval FS defaultval FS FS FS FS FS FS FS}' \
        FS=, OFS=, ${2}ENCOUNTER_MAPPING.csv ${1}unique_PE_pairs.csv > ${1}tmp \
        && mv -f ${1}tmp ${1}ENCOUNTER_MAPPING.csv
    echo "ENCOUNTER_MAPPING written."

    # Fill VISIT_DIMENSION accordingly
    awk '(NR==1){print $0}(NR!=FNR && FNR>1){print $4 FS $4 FS FS FS FS FS FS FS FS FS FS FS FS FS}' \
        FS=, OFS=, ${2}VISIT_DIMENSION.csv ${1}ENCOUNTER_MAPPING.csv > ${1}tmp \
        && mv -f ${1}tmp ${1}VISIT_DIMENSION.csv
    echo "VISIT_DIMENSION written."

    # Read ENCOUNTER_MAPPING and replace values in OBSERVATION_FACT
    awk '(FNR==NR){a[$1]=$4;next}(FNR!=1){$1=a[$1]}{print $0}' \
        FS=, OFS=, ${1}ENCOUNTER_MAPPING.csv ${2}OBSERVATION_FACT.csv > ${1}tmp \
        && mv -f ${1}tmp ${1}OBSERVATION_FACT.csv
    echo "Reindexed encounters in OBSERVATION_FACT."

}

reindex_patients () {
    # Fill PATIENT_MAPPING with the info and index from PATIENT_DIMENSION
    awk -v proj=$PROJECT '(NR==1){print $0; cnter=1; next} \
        (NR>FNR && !visited[$2]++){print $2 FS $2 FS cnter++ FS FS proj FS FS FS FS FS FS}' \
        FS=, OFS=, ${2}PATIENT_MAPPING.csv ${1}unique_PE_pairs.csv > ${1}tmp \
        && mv -f ${1}tmp ${1}PATIENT_MAPPING.csv
    echo "PATIENT_MAPPING written."

    # Fill PATIENT_DIMENSION accordingly
    awk '(NR==1){print $0}(NR!=FNR && FNR>1){print $3 FS FS FS FS FS FS FS FS FS FS FS FS FS FS FS FS FS FS FS}' \
        FS=, OFS=, ${2}PATIENT_DIMENSION.csv ${1}PATIENT_MAPPING.csv > ${1}tmp \
        && mv -f ${1}tmp ${1}PATIENT_DIMENSION.csv
    echo "PATIENT_DIMENSION written."

    # Read PATIENT_MAPPING and replace values in OBSERVATION_FACT
    awk '(FNR==NR){a[$1]=$3;next}(FNR!=1){$2=a[$2]}{print $0}' \
        FS=, OFS=, ${1}PATIENT_MAPPING.csv ${1}OBSERVATION_FACT.csv > ${1}tmp \
        && mv -f ${1}tmp ${1}OBSERVATION_FACT.csv
    echo "Reindexed patients in OBSERVATION_FACT."
}
main $@