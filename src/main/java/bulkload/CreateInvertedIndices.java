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
import org.apache.cassandra.exceptions.InvalidRequestException;

/**
 * Usage: java bulkload.CreateInvertedIndices
 */
public class CreateInvertedIndices {

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

    // setup an index
    private static void addIndex(int word, int predicate, int id_counter,
                                 Map<Long, IntArrayList> map) {
        // add an inverted index
        long index = ((long)(word) << 32L) + predicate;
        IntArrayList existing = map.get(index);
        if (existing == null) {
            existing = new IntArrayList();
            map.put(index, existing);
        }
        existing.add(id_counter);
    }

    // setup an index
    private static void save_hashmap(Map<Long, IntArrayList> map) throws IOException {
        String out_filename = "inverted_indexes.txt";
        BufferedWriter bufferedWriter = new BufferedWriter( new FileWriter( out_filename ) );
        List<Long> id_list = new ArrayList<>();
        id_list.addAll(map.keySet());
        Collections.sort(id_list);
        for (long key : id_list) {
            StringBuffer sb = new StringBuffer();
            sb.append(key).append(",");
            IntArrayList list = map.get(key);
            for (int id : list.toArray()) {
                sb.append(id).append(",");
            }
            sb.setLength(sb.length() - 1);
            sb.append("\n");
            bufferedWriter.write(sb.toString());
        }
        bufferedWriter.close();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public static void main(String[] args)
    {
        if (args.length != 1) {
            System.out.println("usage: java bulkload.CreateInvertedIndices /path/to/free_base/data/");
            return;
        }

        String Freebase_base = args[0];

        // magic!
        Config.setClientMode(true);

        // create inverted indexes
        Map<Long, IntArrayList> map = new HashMap<>();

        // create main table
        {
            try (BufferedReader reader =
                         new BufferedReader(new InputStreamReader(
                                 new FileInputStream(Freebase_base + File.separator + "freebase_data.txt")))) {

                // Write to SSTable while reading data
                int id_counter = 1;
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\\|");

                    if (parts.length == 3) {

                        List<Integer> lhs = toIntList(parts[0]);
                        List<Integer> rhs = toIntList(parts[2]);
                        int predicate = Integer.parseInt(parts[1]);

                        Set<Integer> set = new HashSet<>();
                        set.addAll(lhs);
                        set.addAll(rhs);
                        for (int id : set) {
                            addIndex(id, predicate, id_counter, map);
                        }

                        id_counter += 1;

                        if ((id_counter % 10_000_000) == 0) {
                            System.out.println(id_counter + ", size: " + map.size());
                        }

                    }

                } // while loop

                System.out.println("saving map");
                save_hashmap(map);

            } catch (InvalidRequestException | IOException e) {
                e.printStackTrace();
            }

        }


        System.out.println("done");
        System.exit(0);


    } // main

}
