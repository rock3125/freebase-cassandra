# cassandra-bulkload

Sample SSTable generating and bulk loading code for DataStax [Using Cassandra Bulk Loader, Updated](http://www.datastax.com/dev/blog/using-the-cassandra-bulk-loader-updated) blog post.
This uploads transformed FreeBase data into a set of indexes and a set of tuples with IDs.

## Generating SSTables

Run:

    $ gradle clean build
    $ gradle copyRuntimeLibs
    $ mkdir dependencies
    $ cp build/libs/*.jar dependencies/
    $ cp build/dependencies/*.jar dependencies/

    $ ./run.sh /path/to/processed/freebase/data

This will generate two SSTable(s) under `data` directory.

## Bulk loading

First, create schema using `schema.cql` file:

    $ cqlsh -f schema.cql

Then, load SSTables to Cassandra using `sstableloader`:

    $ sstableloader -d <ip address of the node> data/kai/freebase_tuple
    $ sstableloader -d <ip address of the node> data/kai/freebase_index

(assuming you have `cqlsh` and `sstableloader` in your `$PATH`)

## Check loaded data


    $ bin/cqlsh
    Connected to Test Cluster at 127.0.0.1:9042.
    [cqlsh 5.0.1 | Cassandra 2.1.0 | CQL spec 3.2.0 | Native protocol v3]
    Use HELP for help.
    cqlsh> USE kai ;
    cqlsh:quote> SELECT * FROM freebase_tuple LIMIT 3;
