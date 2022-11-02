# Pass as first argument the debug folder, as second argument the target folder which should contain the i2b2-ready tables

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
