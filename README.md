# RDF-i2b2 converter tutorial

Dependencies:
* rdflib >= v.5.0.0


The high-level idea is to migrate RDF's
` Subject - Predicate - Object` relation to a `Concept - Attribute - Value `hierarchy, at least for instantiable concepts (referred to as _primary concepts_ here). In the case of i2b2, attributes are named _modifiers_.

Attributes discovery does not depend on the ontology terms nor on the target data format. The only assumption is an equivalent non-cyclic hierarchy of various depths. The main function of this core is (given a set of entrypoints) to walk through a RDF graph and to list all spanned absolute paths.

The converter consists of two parts: an **ontology converter** and a **data converter**. Both use generic modules and project-dependent modules.

## Ontology: how to use

Core components are located in **/src/**. The configuration document is **/files/
The required resources are:
* a RDF ontology file, featuring rdf:type and owl:class predicates ((we used a .ttl file, hence the _format=turtle_ keyword argument in calls to rdflib. Change it accordingly to the format of your RDF files.)
* configuration files, by default named *graph_config.json* and *i2b2-mapping.json* featuring parameters for the ontology extraction and mapping to i2b2 tables.

### A walk through the ontology file:
```python
ONTOLOGY_GRAPH_LOCATION # Relative path to the ontology RDF graph files (can be a whole folder)
TERMINOLOGIES_LOCATION  # Path to the external terminologies as RDF graph files
OUTPUT_TABLES           # Path to destination folder of created i2b2 tables
RDF_FORMAT              # Typically turtle or owl, if supported by rdflib
PREF_LANGUAGE           # Corresponding to the language label tag in RDF resources
ENTRY_CONCEPTS          # A list of URIs that should be treated as entrypoints to the graph, as highest non-abstract level of concept. No instances of more global classes      than the ones listed there should exist in your RDF dataset (if existing)
INDIVIDUAL_CLASS_URI    # If using a Valueset principle for some elements, specify the discriminating class there
BLACKLIST               # Any URI, either class or property URI can be blacklisted and ignored in the graph walk. Use it with caution.
```

Ontology generation pipeline
------

`
$ cd RDF-i2b2-converter/
$ python3 src/main.py
`

## Data loading: how to use

The data loading is made of two parts: values extraction from the instances and junction to the ontology concepts. 

The end-to-end scheme is quite specific to i2b2 but as for the ontology conversion, the core is a graph walk extracting individually every observation and value and loading a list of Python dictionaries with them.

The i2b2 data system is built along a [star schema](https://community.i2b2.org/wiki/display/ServerSideDesign/I2B2+DATA+MART), and concept paths written in the ontology table are not specified in the data table. Instead, a _concept code_ allows to join the data table elements (_observations_) to their corresponding ontology path. Both the path and code are written in the CONCEPT_DIMENSION. The same logic applies to _modifiers_ and the MODIFIER_DIMENSION.


Data loading pipeline
-----------
The input graph can be either a unique files of instances or several files to be joined into a unique data graph. The loading script will discover all files in the `DATA_GRAPH_LOCATION` directory (see config file). 

#######################
UNDER CONSTRUCTION
#######################
