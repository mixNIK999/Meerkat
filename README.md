# Meerkat
[![Build Status](https://travis-ci.org/YaccConstructor/Meerkat.svg?branch=master)](https://travis-ci.org/YaccConstructor/Meerkat)

The Meerat library enables general combinator-style context-free path querying.

## Benchmarking

Branch for benchmarking library on single source query to local neo4j db  

### Getting started 
These instructions will get you a copy of the project up and running on your local machine.

#### Prerequisites
To run evaluation you need: 
* local neo4j 3.4.x. We recommend to use the official docker image https://hub.docker.com/_/neo4j/
* plugin neosemantics 3.4.0.2. It can be download from https://github.com/neo4j-labs/neosemantics/releases/tag/3.4.0.2 

How to install neosemantics: https://neo4j.com/docs/labs/nsmntx/current/install/
##### Importing RDF
After run db with plugin, you can connect to it and load rdf using cypher language:
```
CREATE INDEX ON :Resource(uri)
CALL semantics.importRDF($url, RDF/XML, { typesToLabels: false });
```
* `$url` - URL of the dataset. It can be local ("file:///var/lib/neo4j/import/enzyme.rdf") or remote ("https://protege.stanford.edu/ontologies/pizza/pizza.owl")

For more information see the official guide https://neo4j.com/docs/labs/nsmntx/current/import/

After loading all data shut down local db and give all permission to all neo4j data file:
```
sudo chmod 777 -R /path/to/db/data
``` 

#### Build and Run
Clone this repository, switch to `benchmark` branch and run: 
```
sbt ";project benchmark; run /path/to/db/data/file.db /path/to/db/config/conf $type"
```
* `$type` can be `BT`, `NT` or `type_and_subClass`
    * `BT` will use `broaderTransitive` edges for same generation query
    * `NT` will use `narrowerTransitive`
    * `type_and_subClass` will use two type of edges: `type` and `subClassOf` 