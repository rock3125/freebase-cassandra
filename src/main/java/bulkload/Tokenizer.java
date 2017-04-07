package bulkload;

import java.util.ArrayList;
import java.util.List;

/*
 * Created by peter on 17/12/14.
 *
 * Tokenize/clean a string
 *
 */
public class Tokenizer extends TokenizerConstants {

    public Tokenizer() {
    }

    /**
     * take a string apart into tokens
     * @param str the stirng to take apart
     * @return a list of tokens that makes the string
     */
    public List<String> tokenize(String str ) {
        if ( str != null && str.length() > 0 ) {
            List<String> tokenList = new ArrayList<>();

            StringBuilder helper = new StringBuilder();

            char[] chArray = str.toCharArray();
            int length = chArray.length;

            int i = 0;
            while ( i < length ) {
                boolean tokenHandled = false;

                // whitespace scanner
                char ch = chArray[i];
                while ( isWhiteSpace(ch) && i < length ) {
                    tokenHandled = true;
                    i = i + 1;
                    if ( i < length ) ch = chArray[i];
                }

                if ( tokenHandled )
                    tokenList.add(" ");

                // add full-stops?
                while ( isFullStop(ch) && i < length ) {
                    tokenHandled = true;
                    tokenList.add(".");
                    i = i + 1;
                    if ( i < length ) ch = chArray[i];
                }

                // add hyphens?
                while ( isHyphen(ch) && i < length ) {
                    tokenHandled = true;
                    tokenList.add("-");
                    i = i + 1;
                    if ( i < length ) ch = chArray[i];
                }

                // add single quotes?
                while ( isSingleQuote(ch) && i < length ) {
                    tokenHandled = true;
                    tokenList.add("'");
                    i = i + 1;
                    if ( i < length ) ch = chArray[i];
                }

                // add single quotes?
                while ( isDoubleQuote(ch) && i < length ) {
                    tokenHandled = true;
                    tokenList.add("\"");
                    i = i + 1;
                    if ( i < length ) ch = chArray[i];
                }

                // add special characters ( ) etc.
                while ( isSpecialCharacter(ch) && i < length ) {
                    tokenHandled = true;
                    tokenList.add(Character.toString(ch));
                    i = i + 1;
                    if ( i < length ) ch = chArray[i];
                }

                // add punctuation ! ? etc.
                while ( isPunctuation(ch) && i < length ) {
                    tokenHandled = true;
                    tokenList.add(Character.toString(ch));
                    i = i + 1;
                    if ( i < length ) ch = chArray[i];
                }

                // numeric processor
                helper.setLength(0);
                while ( isNumeric(ch) && i < length ) {
                    tokenHandled = true;
                    helper.append(ch);
                    i = i + 1;
                    if ( i < length ) ch = chArray[i];
                }
                if ( helper.length() > 0 )
                    tokenList.add(helper.toString());

                // text processor
                helper.setLength(0);
                while ( isABC(ch) && i < length ) {
                    tokenHandled = true;
                    helper.append(ch);
                    i = i + 1;
                    if ( i < length ) ch = chArray[i];
                }
                if ( helper.length() > 0 )
                    tokenList.add(helper.toString());

                // discard unknown token?
                if ( !tokenHandled ) {
                    i++; // skip
                }

            }

            // return the list if we have something
            if ( tokenList.size() > 0 )
                return filterOutSpaces(tokenList);
        }
        return null;
    }

    /**
     * given a list of tokens, remove all the white list items
     * @param tokenList a list of tokens in
     * @return the modified list of tokens with all white spaces removed
     */
    public List<String> filterOutSpaces( List<String> tokenList ) {
        if ( tokenList != null ) {
            List<String> strippedList = new ArrayList<>();
            for (String str : tokenList ) {
                if (!" ".equals(str)) {
                    strippedList.add(str);
                }
            }
            return strippedList;
        }
        return null;
    }

}

