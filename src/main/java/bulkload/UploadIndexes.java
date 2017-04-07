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

import com.carrotsearch.hppc.IntArrayList;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;

/**
 * Usage: java bulkload.BulkLoad2
 */
public class UploadIndexes
{
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////

    private static final String SCHEMA_2 = String.format("CREATE TABLE %s.%s (" +
            "predicate int, " +
            "word int, " +
            "tuples list<int>, " +
            "PRIMARY KEY ((predicate, word)) " +
            ") ", Constants.KEYSPACE, Constants.CF_INDEX);

    private static final String INSERT_STMT_2 =
            String.format("INSERT INTO %s.%s (predicate, word, tuples) VALUES (?, ?, ?)",
                    Constants.KEYSPACE, Constants.CF_INDEX);

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
            System.out.println("usage: java bulkload.UploadIndexes /path/to/inverted_indexes.txt (see bulkload.CreateInvertedIndices)");
            return;
        }

        String inverted_indexes_file = args[0];

        // magic!
        Config.setClientMode(true);

        // create inverted indexes
        {
            File outputDir2 = new File(Constants.KEYSPACE + File.separator + Constants.CF_INDEX);
            if (!outputDir2.exists() && !outputDir2.mkdirs()) {
                throw new RuntimeException("Cannot create output directory: " + outputDir2);
            }

            // Prepare SSTable writer
            CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder();
            // set output directory
            builder.inDirectory(outputDir2)
                    // set target schema
                    .forTable(SCHEMA_2)
                    // set CQL statement to put data
                    .using(INSERT_STMT_2)
                    // set partitioner if needed
                    // default is Murmur3Partitioner so set if you use different one.
                    .withPartitioner(new Murmur3Partitioner());

            CQLSSTableWriter writer = builder.build();


            try (BufferedReader reader =
                         new BufferedReader(new InputStreamReader(
                                 new FileInputStream(inverted_indexes_file)))) {

                int counter = 0;
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(",");

                    if (parts.length > 1) {

                        long id = Long.parseLong(parts[0]);
                        long l_word_id = id >> 32L;
                        int word_id = (int)l_word_id;
                        int predicate = (int)id;

                        List<Integer> intList = new ArrayList<>();
                        for (int i = 1; i < parts.length; i++) {
                            intList.add(Integer.parseInt(parts[i]));
                        }

                        Collections.sort(intList);

                        if (intList.size() > 0) {
                            try {
                                writer.addRow(predicate, word_id, intList);
                            } catch (InvalidRequestException | IOException e) {
                                e.printStackTrace();
                            }
                        }
                        counter += 1;
                        if ((counter % 1_000_000) == 0) {
                            System.out.println(counter);
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
        String path = Constants.KEYSPACE + File.separator + Constants.CF_INDEX;
        System.out.println("you can upload these files to Cassandra: sstableloader -d host " + path);
        System.exit(0);


    } // main

}
