package bulkload;

/**
 * Created by peter on 7/04/17.
 *
 * bit primitive - control the names of the keyspace and CF names here
 *
 */
public class Constants {

    // Keyspace name
    public static final String KEYSPACE = "kai_ai";

    // freebase index CF name
    public static final String CF_INDEX = "freebase_index";

    // freebase tuple store CF name
    public static final String CF_TUPLE = "freebase_tuple";

    // freebase vocab CF name (int -> string)
    public static final String CF_VOCAB = "freebase_vocab";

    // freebase word CF name (string -> int)
    public static final String CF_WORD = "freebase_word";

}
