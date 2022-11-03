# Pass as first argument the debug folder, as second argument the target folder which should contain the i2b2-ready tables

DEFAULT_DATE="01.01.2022"
PROJECT="BIOREF"

fill_default_values() {
    awk -v defdate=$DEFAULT_DATE '($4==""){$4="@"}($5==""){$5=defdate}1' FS=, OFS=, OBSERVATION_FACT.csv > tmp && mv -f tmp OBSERVATION_FACT.csv
}

replace_codes () {
    PATH_HCD=$1/CONCEPT_DIMENSION.csv
    PATH_HMD=$1/MODIFIER_DIMENSION.csv
    PATH_HOF=$1/OBSERVATION_FACT.csv

    LOOKUP_CONCEPTS = $1/lookup_concepts.csv
    LOOKUP_MODIFIERS = $1/lookup_modifiers.csv

    PATH_DCD=$2/CONCEPT_DIMENSION
    PATH_DMD=$2/MODIFIER_DIMENSION.csv
    PATH_DOF=$2/OBSERVATION_FACT.csv


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
}

reindex () {

    # This function takes care of patient_dimension reindexing and filling of patient_mapping accordingly. 
    # It also does the equivalent job for visit_dimension and encounter_mapping
    # Required: filled patient_dimension and visit_dimension, header-only encounter_mapping and patient_mapping.


    # Source on OBSERVATION_FACT and extract a pruned list of (encounter,patient) pairs with it

    awk '(NR>1){print $2 FS $1}' FS=, OFS=, OBSERVATION_FACT.csv | awk '!visited[$0]++' FS=, OFS=, > unique_PE_pairs.csv
    reindex_encounters
    reindex_patients

    rm -f unique_PE_pairs.csv 
}

reindex_encounters () {
    awk -v proj=$PROJECT -v default="-1" '(NR==1){print $0; cnter=1; next}
            (FNR>1 && $1!="" && !visited[$1]++){print $1 FS $1 FS proj FS cnter++ FS -1 FS -1 FS FS FS FS FS FS FS}' \
        FS=, OFS=, ENCOUNTER_MAPPING.csv unique_PE_pairs.csv > tmp \
        && mv tmp -f ENCOUNTER_MAPPING.csv

    # Fill VISIT_DIMENSION accordingly
    awk '(NR==1){print $0}(NR!=FNR && FNR>1){print $4 FS $4 FS FS FS FS FS FS FS FS FS FS FS FS FS}' \
        FS=, OFS=, VISIT_DIMENSION.csv ENCOUNTER_MAPPING.csv > tmp \
        && mv -f tmp VISIT_DIMENSION.csv

    # Read ENCOUNTER_MAPPING and replace values in OBSERVATION_FACT
    awk '(FNR==NR){a[$1]=$4;next}(FNR==1){print $0;next}{$2=a[$2]; print $0}' FS=, OFS=, ENCOUNTER_MAPPING.csv OBSERVATION_FACT.csv > tmp \\
        && mv -f tmp OBSERVATION_FACT.csv

}

reindex_patients () {
    ################
    # Reindex patients and fill patient_mapping + patient_dimension
    ################

    # Fill PATIENT_MAPPING with the info and index from PATIENT_DIMENSION
    awk -v proj=$PROJECT '(NR==1){print $0; next}(FNR>1 && !visited[$2]++){print $2 FS $2 FS FNR-1 FS FS proj FS FS FS FS FS FS}' \
        FS=, OFS=, PATIENT_MAPPING.csv unique_PE_pairs.csv > tmp \\
        && mv tmp -f PATIENT_MAPPING.csv

    # Fill PATIENT_DIMENSION accordingly
    awk '(NR==1){print $0}(NR!=FNR && FNR>1){print $3 FS $3 FS FS FS FS FS FS FS FS FS FS FS FS FS}' \
        FS=, OFS=, PATIENT_DIMENSION.csv PATIENT_MAPPING.csv > tmp \
        && mv -f tmp PATIENT_DIMENSION.csv

    # Read PATIENT_MAPPING and replace values in OBSERVATION_FACT
    awk '(FNR==NR){a[$1]=$3;next}(FNR!=1){$2=a[$2]}{print $0}' FS=, OFS=, PATIENT_MAPPING.csv OBSERVATION_FACT.csv > tmp && mv tmp -f OBSERVATION_FACT.csv

}