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
public class UploadTuples {

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////

    private static final String SCHEMA_1 = String.format("CREATE TABLE %s.%s (" +
                                                          "id int, " +
                                                          "lhs list<int>, " +
                                                          "predicate int, " +
                                                          "rhs list<int>, " +
                                                          "PRIMARY KEY (id) " +
                                                      ") ", Constants.KEYSPACE, Constants.CF_TUPLE);
    private static final String INSERT_STMT_1 =
            String.format("INSERT INTO %s.%s (id, lhs, predicate, rhs) VALUES (?, ?, ?, ?)",
                    Constants.KEYSPACE, Constants.CF_TUPLE);


    //////////////////////////////////////////////////////////////////////////////////////////////////////////////

    private static List<Integer> toIntList(String line) {
        List<Integer> list = new ArrayList<>();
        String[] parts = line.split(",");
        for (String part : parts) {
            int id = Integer.parseInt(part);
            list.add(id);
        }
        return list;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public static void main(String[] args)
    {
        if (args.length != 1) {
            System.out.println("usage: java bulkload.UploadTuples /path/to/free_base/data/");
            return;
        }

        String Freebase_base = args[0];

        // magic!
        Config.setClientMode(true);

        // create main table
        {
            // Create output directory that has keyspace and table name in the path
            File outputDir1 = new File(Constants.KEYSPACE + File.separator + Constants.CF_TUPLE);
            if (!outputDir1.exists() && !outputDir1.mkdirs()) {
                throw new RuntimeException("Cannot create output directory: " + outputDir1);
            }

            // Prepare SSTable writer
            CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder();
            // set output directory
            builder.inDirectory(outputDir1)
                    // set target schema
                    .forTable(SCHEMA_1)
                    // set CQL statement to put data
                    .using(INSERT_STMT_1)
                    // set partitioner if needed
                    // default is Murmur3Partitioner so set if you use different one.
                    .withPartitioner(new Murmur3Partitioner());

            CQLSSTableWriter writer = builder.build();

            try (BufferedReader reader =
                         new BufferedReader(new InputStreamReader(
                                 new FileInputStream(Freebase_base + File.separator + "freebase_data.txt")))) {

                // Write to SSTable while reading data
                int id_counter = 1;
                int hm_counter = 1;
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\\|");
                    if (parts.length == 3) {
                        List<Integer> lhs = toIntList(parts[0]);
                        List<Integer> rhs = toIntList(parts[2]);
                        int predicate = Integer.parseInt(parts[1]);

                        writer.addRow(id_counter, lhs, predicate, rhs);

                        id_counter += 1;

                        if ((id_counter % 10_000_000) == 0) {
                            System.out.println(id_counter);
                        }

                    }

                } // while loop

            } catch (InvalidRequestException | IOException e) {
                e.printStackTrace();
            }

            try {
                writer.close();
            } catch (IOException ignore) {
            }

        }


        System.out.println("done");
        String path = Constants.KEYSPACE + File.separator + Constants.CF_TUPLE;
        System.out.println("you can upload these files to Cassandra: sstableloader -d host " + path);
        System.exit(0);


    } // main

}
