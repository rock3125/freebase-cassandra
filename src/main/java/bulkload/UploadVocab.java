/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package bulkload;

import java.io.*;
import java.util.*;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;

/**
 * Usage: java bulkload.BulkLoad1
 */
public class UploadVocab
{
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////

    private static final String SCHEMA_1 = String.format("CREATE TABLE %s.%s (" +
            "id int, " +
            "word text, " +
            "is_predicate boolean, " +
            "PRIMARY KEY (id) " +
            ") ", Constants.KEYSPACE, Constants.CF_VOCAB);

    private static final String INSERT_STMT_1 =
            String.format("INSERT INTO %s.%s (id, word, is_predicate) VALUES (?, ?, ?)",
                    Constants.KEYSPACE, Constants.CF_VOCAB);

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////

    private static final String SCHEMA_2 = String.format("CREATE TABLE %s.%s (" +
            "word varchar, " +
            "id int, " +
            "is_predicate boolean, " +
            "PRIMARY KEY (word) " +
            ");", Constants.KEYSPACE, Constants.CF_WORD);

    private static final String INSERT_STMT_2 =
            String.format("INSERT INTO %s.%s (word, id, is_predicate) VALUES (?, ?, ?);",
                    Constants.KEYSPACE, Constants.CF_WORD);

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public static void main(String[] args)
    {
        if (args.length != 1) {
            System.out.println("usage: java bulkload.UploadVocab /path/to/free_base/output/");
            return;
        }

        String Freebase_base = args[0];

        // magic!
        Config.setClientMode(true);

        // create main table
        {
            // Create output directory that has Constants.KEYSPACE and table name in the path
            File outputDir1 = new File(Constants.KEYSPACE + File.separator + Constants.CF_VOCAB);
            if (!outputDir1.exists() && !outputDir1.mkdirs()) {
                throw new RuntimeException("Cannot create output directory: " + outputDir1);
            }

            File outputDir2 = new File(Constants.KEYSPACE + File.separator + Constants.CF_WORD);
            if (!outputDir2.exists() && !outputDir2.mkdirs()) {
                throw new RuntimeException("Cannot create output directory: " + outputDir2);
            }

            // read the predicate vocab map
            Map<Integer,String> predicateMap = new HashMap<>();
            try (BufferedReader reader =
                         new BufferedReader(new InputStreamReader(
                                 new FileInputStream(Freebase_base + File.separator + "freebase_predicate_vocab.txt")))) {
                String line;
                // predicate_str|predicate_id
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\\|");
                    if (parts.length == 2) {
                        String word = parts[0];
                        int word_id = Integer.parseInt(parts[1]);
                        predicateMap.put(word_id, word);
                    }
                } // while loop

            } catch (InvalidRequestException | IOException e) {
                e.printStackTrace();
            }

            // Prepare SSTable writer
            CQLSSTableWriter.Builder builder_1 = CQLSSTableWriter.builder();
            // set output directory
            builder_1.inDirectory(outputDir1)
                    // set target schema
                    .forTable(SCHEMA_1)
                    // set CQL statement to put data
                    .using(INSERT_STMT_1)
                    // set partitioner if needed
                    // default is Murmur3Partitioner so set if you use different one.
                    .withPartitioner(new Murmur3Partitioner());

            CQLSSTableWriter writer_1 = builder_1.build();

            // Prepare SSTable writer
            CQLSSTableWriter.Builder builder_2 = CQLSSTableWriter.builder();
            // set output directory
            builder_2.inDirectory(outputDir2)
                    // set target schema
                    .forTable(SCHEMA_2)
                    // set CQL statement to put data
                    .using(INSERT_STMT_2)
                    // set partitioner if needed
                    // default is Murmur3Partitioner so set if you use different one.
                    .withPartitioner(new Murmur3Partitioner());

            CQLSSTableWriter writer_2 = builder_2.build();

            try (BufferedReader reader =
                         new BufferedReader(new InputStreamReader(
                                 new FileInputStream(Freebase_base + File.separator + "freebase_vocab.txt")))) {
                String line;
                // and|2
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\\|");
                    if (parts.length == 2) {
                        String word = parts[0];
                        int word_id = Integer.parseInt(parts[1]);
                        writer_1.addRow(word_id, word, predicateMap.containsKey(word_id));
                        writer_2.addRow(word, word_id, predicateMap.containsKey(word_id));
                    }

                } // while loop

            } catch (InvalidRequestException | IOException e) {
                e.printStackTrace();
            }

            try {
                writer_1.close();
            } catch (IOException ignore) {
            }

            try {
                writer_2.close();
            } catch (IOException ignore) {
            }

        }


        System.out.println("done");
        String path_1 = Constants.KEYSPACE + File.separator + Constants.CF_VOCAB;
        System.out.println("you can upload these files to Cassandra:\nsstableloader -d host " + path_1);

        String path_2 = Constants.KEYSPACE + File.separator + Constants.CF_WORD;
        System.out.println("sstableloader -d host " + path_2);

        System.exit(0);


    } // main

}
