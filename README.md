# freebase cassandra upload

This little Java project uploads the freebase-easy facts.txt (after transforming) to Apache Cassandra (3.x).
This uploads transformed FreeBase data into a set of indexes and a set of tuples with IDs.

## Generating SSTables

Run:

    $ gradle clean build
    $ gradle copyRuntimeLibs
    $ mkdir dependencies
    $ cp build/libs/*.jar dependencies/
    $ cp build/dependencies/*.jar dependencies/

    $ ./1_convert_freebase.sh /path/to/freebase-easy/facts.txt

This generates freebase_vocab.txt, freebase_data.txt, and freebase_predicate_vocab.txt.  These files are used by the next steps in the process.

    $ ./2_generate_inverted_indexes.sh .

This generates a set of inverted indexes for the upload process.  This part requires a lot of RAM as I haven't created a merge-sort for the hashmap and thus the entire freebase int based reference system needs to live in RAM temporarily.  This part of the process creates a file "inverted_indexes.txt".

    $ ./3_upload_tuples.sh .

This process creates an SSTABLE file for a hardwired KEYSPACE "kai_ai" and a CF called "freebase_tuple".  This will create a folder "kai_ai" and a folder inside that called freebase_tuple.  This process reads the "freebase_vocab.txt" and "freebase_data.txt" files to create a vocab/index based set of tuples inside this freebase_tuple CF.

    $ ./4_upload_indexes.sh .

This process creates an SSTABLE file for a hardwired KEYSPACE "kai_ai" and a CF called "freebase_index".  This will create a folder "kai_ai" and a folder inside that called freebase_index.  This process reads the "inverted_indexes.txt" and populates the "freebase_index" SSTABLE files.

    $ ./5_upload_vocab.sh .

This part of the process creates two additional folders in the "kai_ai" KEYSPACE folder called "freebase_vocab" and "freebase_word".  Once this part of the process has finished, all file based SSTABLES can be uploaded to Cassandra.

## Bulk loading

First, create schema using `schema.cql` file:

    $ cqlsh -f schema.cql

Depending on how many Cassandra nodes you have, you must increase (temporarily) the amount of memory available to Cassandra for the upload.  This is especially true if you're only using one node.  Update Cassandra's RAM usage in the conf/cassandra-env.sh file to around 14GB.  Change it back after you finish.  Cassandra will most likely take a couple of hours (on a single node) to process all this information after the bulk upload below has finished.  Be patient.

Then, load SSTables to Cassandra using `sstableloader`:

    $ sstableloader -d <ip address of the node> kai_ai/freebase_tuple
    $ sstableloader -d <ip address of the node> kai_ai/freebase_index
    $ sstableloader -d <ip address of the node> kai_ai/freebase_vocab
    $ sstableloader -d <ip address of the node> kai_ai/freebase_word

(assuming you have `cqlsh` and `sstableloader` in your `$PATH`)

## Check loaded data


    $ bin/cqlsh
    Connected to Test Cluster at 127.0.0.1:9042.
    [cqlsh 5.0.1 | Cassandra 2.1.0 | CQL spec 3.2.0 | Native protocol v3]
    Use HELP for help.
    cqlsh> USE kai_ai;
    cqlsh:quote> SELECT * FROM freebase_tuple LIMIT 3;

