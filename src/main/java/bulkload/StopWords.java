package bulkload;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

/**
 * Created by peter on 7/04/17.
 * list of words not to index
 *
 */
public class StopWords {

    // set for lookup - read only
    private HashSet<String> undesirableSet;

    public StopWords() {
        undesirableSet = new HashSet<>();
        undesirableSet.addAll( Arrays.asList(undesirableList) );
        undesirableSet.addAll( Arrays.asList(specialCharacterList) );
    }

    /**
     * get all undesirables as a single list
     * @return the list of undesirables
     */
    public List<String> getAsList() {
        List<String> undesirableList = new ArrayList<>();
        undesirableList.addAll(undesirableSet);
        return undesirableList;
    }

    // return true if the string passed in is null or an undesirable word
    // these are words that shouldn't really form part of any index because
    // of little value and high frequency
    public boolean isStopWord( String str ) {
        return (str == null ||
                (str.length() == 1 && "i".compareToIgnoreCase(str) != 0)) ||
                 undesirableSet.contains(str.toLowerCase());
    }

    private static final String[] undesirableList = new String[] {
            // articles
            "the", "a", "an",

            // conjunctions
            "after", "although", "and", "as", "as far as", "as how", "as if",
            "as long as", "as soon as", "as though", "as well as", "because",
            "before", "both", "but", "either", "even if", "even", "though",
            "for", "how", "however", "if only", "in case", "in order that",
            "neither", "nor", "now", "once", "only", "or", "provided", "rather",
            "than", "since", "so", "so that", "than", "that", "though", "till",
            "unless", "until", "when", "whenever", "where", "whereas", "wherever",
            "whether", "while", "yet",

            "''", "`"
    };

    // characters that are sort of noise and shouldn't be indexed
    private static final String[] specialCharacterList = {

            // full stops
            "\u002e", "\u06d4", "\u0701", "\u0702",
            "\ufe12", "\ufe52", "\uff0e", "\uff61",

            "!", "?", ",", ":", ";",
            "_", "%", "$", "#", "@", "^", "&", "*", "(", ")", "^",
            "[", "{", "]", "}", "<", ">", "/", "\\", "=", "+", "|", "\"",

            // single quotes
            "\'", "\u02bc", "\u055a", "\u07f4",
            "\u07f5", "\u2019", "\uff07", "\u2018", "\u201a", "\u201b", "\u275b", "\u275c",

            // double quotes
            //"\u0022", "\u00bb", "\u00ab", "\u07f4", "\u07f5", "\u2019", "\uff07",
            "\u201c", "\u201d", "\u201e", "\u201f", "\u2039", "\u203a", "\u275d",
            "\u276e", "\u2760", "\u276f",

            // hyphens
            "\u002d", "\u207b", "\u208b", "\ufe63", "\uff0d",

            // whitespace and noise
            " ",  "\t",  "\r",  "\n", "\u0008",
            "\ufeff", "\u303f", "\u3000", "\u2420", "\u2408", "\u202f", "\u205f",
            "\u2000", "\u2002", "\u2003", "\u2004", "\u2005", "\u2006", "\u2007",
            "\u2008", "\u2009", "\u200a", "\u200b",
    };

}
