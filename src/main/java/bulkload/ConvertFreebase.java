package bulkload;

import com.carrotsearch.hppc.IntArrayList;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by peter on 29/03/17.
 *
 */
public class ConvertFreebase {

    // run the converter
    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.out.println("usage: java bulkload.ConvertFreebase /path/to/freebase/facts.txt");
            return;
        }
        ConvertFreebase converter = new ConvertFreebase();
        converter.convert(args[0]);
    }

    public ConvertFreebase() {
    }

    // do the conversion
    public void convert(String freebase_fact_txt_path) throws IOException {
        Tokenizer tokenizer = new Tokenizer();
        Map<String,Integer> vocab = new HashMap<>();
        Map<String,Integer> predicates = new HashMap<>();
        int counter = 0;

        BufferedWriter vocab_writer = new BufferedWriter(new FileWriter("freebase_vocab.txt"));
        BufferedWriter data_writer = new BufferedWriter(new FileWriter("freebase_data.txt"));
        BufferedWriter predicate_writer = new BufferedWriter(new FileWriter("freebase_predicate_vocab.txt"));

        try (BufferedReader br = new BufferedReader(new FileReader(freebase_fact_txt_path))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] stripped = line.split("\t");
                if (stripped.length == 4) {
                    String p1 = cleanup(stripped[0]);
                    String p2 = cleanup(stripped[1]);
                    String p3 = cleanup(stripped[2]);

                    if (p1.length() > 1 && p2.length() > 1 && p3.length() > 1) {
                        List<String> list1 = tokenizer.tokenize(p1);
                        List<String> list2 = tokenizer.tokenize(p2);
                        if (list2 != null) {
                            String predicate = join(list2).toLowerCase();
                            List<String> list3 = tokenizer.tokenize(p3);
                            if (list1 != null && list3 != null && list1.size() > 0 &&
                                    list2.size() > 0 && list3.size() > 0) {
                                List<String> combinedList = new ArrayList<>();
                                combinedList.addAll(list1);
                                combinedList.addAll(list3);
                                combinedList.add(predicate);
                                writeVocab(vocab_writer, combinedList, vocab);
                                writeTuple(data_writer, list1, predicate, list3, vocab);
                                if (!predicates.containsKey(predicate)) {
                                    predicates.put(predicate, vocab.get(predicate));
                                }
                                counter += 1;
                                if (counter % 1_000_000 == 0) {
                                    System.out.println(counter);
                                }
                            }
                        }

                    } // if strings are valid

                } // if has 4 tabs

            } // for each line in freebase

            // write predicate vocab
            for (String key : predicates.keySet()) {
                int id = predicates.get(key);
                predicate_writer.write(key + "|" + id + "\n");
            }

        } finally {
            vocab_writer.close();
            data_writer.close();
            predicate_writer.close();
        }

        System.out.println("done");
        System.exit(0);
    }

    // clean a string - remove any references etc.
    private String cleanup(String text) {
        // remove schema type
        int index = text.indexOf("^^");
        if (index >= 0) {
            text = text.substring(0, index);
        }
        // remove language indicators @en, @it, @fr
        index = text.indexOf("@");
        if (index >= 0) {
            text = text.substring(0, index);
        }
        // remove bracket sets
        int i1 = text.indexOf('(');
        while (i1 >= 0) {
            int i2 = text.indexOf(')', i1);
            if (i2 >= 0) {
                String t1 = "";
                if (i1 > 0) {
                    t1 = text.substring(0, i1);
                }
                String t2 = "";
                if (i2 < text.length()) {
                    t2 = text.substring(i2 + 1);
                }
                text = t1 + t2;
            } else {
                break;
            }
            i1 = text.indexOf('(');
        }
        // remove #
        index = text.indexOf("#");
        if (index >= 0) {
            text = text.substring(0, index);
        }
        // still invalid?
        if (text.indexOf('!') >= 0 || text.indexOf('(') >= 0 || text.indexOf(')') >= 0 ) {
            return "";
        }
        // remove enclosing quotes at start and end
        if (text.length() > 1 && text.startsWith("\"") && text.endsWith("\"")) {
            text = text.substring(1, text.length() - 1);
        }
        if (text.length() == 1) {
            return "";
        }
        text = text.replace("\"/\"", " / ");
        text = text.replace("\\'s", "'s");
        return text;
    }

    // write a list of vocab items to file if they're not already known
    private void writeVocab(BufferedWriter writer, List<String> vocabItems,
                            Map<String, Integer> vocab) throws IOException {
        for (String item : vocabItems) {
            String lwrItem = item.toLowerCase();
            if (!vocab.containsKey(lwrItem)) {
                int id = vocab.size() + 1;
                vocab.put(lwrItem, id);
                writer.write(lwrItem + "|" + id + "\n");
            }
        }
    }

    // convert a list of string words into a list of reference integers
    private IntArrayList wordListToIntList(List<String> wordList, Map<String,Integer> vocab) {
        IntArrayList intList = new IntArrayList();
        for (String word : wordList) {
            intList.add(vocab.get(word.toLowerCase()));
        }
        return intList;
    }

    // join items together to form a string using ","
    private String join(IntArrayList list) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < list.size(); i++) {
            int value = list.get(i);
            sb.append(value);
            if (i + 1 < list.size()) {
                sb.append(",");
            }
        }
        return sb.toString();
    }

    // join items together to form a string using "_"
    private String join(List<String> list) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < list.size(); i++) {
            sb.append(list.get(i).toLowerCase());
            if (i + 1 < list.size()) {
                sb.append("_");
            }
        }
        return sb.toString();
    }

    // write a tuple to the data file
    private void writeTuple(BufferedWriter writer, List<String> lhs,
                            String predicate, List<String> rhs,
                            Map<String,Integer> vocab) throws IOException {
        IntArrayList lhs_int = wordListToIntList(lhs, vocab);
        IntArrayList rhs_int = wordListToIntList(rhs, vocab);
        int predicate_id = vocab.get(predicate.toLowerCase());
        writer.write(join(lhs_int) + "|" + predicate_id + "|" + join(rhs_int) + "\n");
    }

}
