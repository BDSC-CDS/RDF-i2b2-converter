$PROJECT=BIOREF

# This file takes care of patient_dimension reindexing and filling of patient_mapping accordingly. 
# It also does the equivalent job for visit_dimension and encounter_mapping
# Required: filled patient_dimension and visit_dimension, header-only encounter_mapping and patient_mapping.


################
# Reindex patients
################

# Fill PATIENT_MAPPING with the info and index from PATIENT_DIMENSION
awk -v proj=$PROJECT '(NR==1){print $0; next}(FNR>1){print $1  FS $1 FS FNR-1 FS FS proj FS FS FS FS FS FS}' FS=, OFS=, PATIENT_MAPPING.csv PATIENT_DIMENSION.csv > tmp \\
    && mv tmp -f PATIENT_MAPPING.csv

# Replace patient numbers by their index (careful: minus 1!) in PATIENT_DIMENSION
awk '(NR>1){$1=FNR-1}1' FS=, OFS=, PATIENT_DIMENSION.csv > tmp && mv -f tmp PATIENT_DIMENSION.csv

# Read PATIENT_MAPPING and replace values in OBSERVATION_FACT
awk '(FNR==NR){a[$1]=$3;next}(FNR!=1){$2=a[$2]}{print $0}' FS=, OFS=, PATIENT_MAPPING.csv OBSERVATION_FACT.csv > tmp && mv tmp -f OBSERVATION_FACT.csv

################
# Reindex encounters
################

# Fill ENCOUNTER_MAPPING with the info and index from VISIT_DIMENSION
awk -v proj=$PROJECT -v default="-1" '(NR==1){print $0; cnter=1; next}(FNR>1 && $1!=""){print $1 FS $1 FS proj FS cnter++ FS -1 FS -1 FS FS FS FS FS FS FS}' FS=, OFS=, \\
    ENCOUNTER_MAPPING.csv VISIT_DIMENSION.csv > tmp \\
    && mv tmp -f ENCOUNTER_MAPPING.csv

# Replace encounter numbers by their index (minus 1) in VISIT_DIMENSION
awk '(NR==1){print $0}(NR!=FNR && FNR>1){print $4 FS $4 FS FS FS FS FS FS FS FS FS FS FS FS FS}' FS=, OFS=, VISIT_DIMENSION.csv ENCOUNTER_MAPPING.csv > tmp \\
    && mv -f tmp VISIT_DIMENSION.csv

# Read ENCOUNTER_MAPPING and replace values in OBSERVATION_FACT
awk '(FNR==NR){a[$1]=$4;next}(FNR==1){print $0;next}{$2=a[$2]; print $0}' FS=, OFS=, ENCOUNTER_MAPPING.csv OBSERVATION_FACT.csv > tmp \\
    && mv -f tmp OBSERVATION_FACT.csv