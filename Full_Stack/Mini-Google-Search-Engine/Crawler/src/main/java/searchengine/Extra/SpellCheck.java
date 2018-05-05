package searchengine.Extra;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Main class to find the correct format of a word, based on the algorithm of SymSpell
 * Edited from WolfGarbe
 * @author: Xiaoyu Deng
 *
 */
public class SpellCheck {
    private static int editDistanceMax = 2;
    // 0: top suggestion
    // 1: all suggestions of smallest edit distance
    // 2: all suggestions <= editDistanceMax (slower, no early termination)
    private static int verbose = 0;
    private static Map<String, Object> dictionary = new HashMap<String, Object>(); // 20000 most-frequent words
    private static List<String> wordlist = new ArrayList<String>(); // frequent-used words

    private static class dictionaryItem {
        public List<Integer> suggestions = new ArrayList<Integer>(); // all the possible corrections
        public int count = 0;
    }

    private static class suggestItem {
        public String term = "";
        public int distance = 0;
        public int count = 0;

        @Override
        public boolean equals(Object obj) {
            return term.equals(((suggestItem)obj).term);
        }

        @Override
        public int hashCode() {
            return term.hashCode();
        }
    }
    /**
     * \w: Alphanumeric characters (including non-Latin characters and digits) plus "_"
     * \d: Digits
     * Provides identical results to  "[a-z]+" for Latin characters, while additionally providing compatibility with non-Latin characters
     *
     * @param text
     * @return
     */
    private static Iterable<String> parseWords(String text) {
        List<String> allMatches = new ArrayList<String>();
        Matcher m = Pattern.compile("[\\w-[\\d_]]+").matcher(text.toLowerCase());
        while (m.find()) {
            allMatches.add(m.group());
        }
        return allMatches;
    }

    public static int maxlength = 0; // maximum dictionary term length

    private static boolean CreateDictionaryEntry(String key) {
        boolean result = false;
        dictionaryItem value = null;
        Object valueo;
        valueo = dictionary.get(key);
        if (valueo != null) {
            // int or dictionaryItem? delete existed before word!
            if (valueo instanceof Integer) {
                int tmp = (int)valueo;
                value = new dictionaryItem();
                value.suggestions.add(tmp);
                dictionary.put(key, value);
            }

            // already exists:
            // 1. word appears several times
            // 2. word1==deletes(word2)
            else {
                value = (dictionaryItem)valueo;
            }

            // prevent overflow
            if (value.count < Integer.MAX_VALUE) value.count++;
        } else if (wordlist.size() < Integer.MAX_VALUE) {
            value = new dictionaryItem();
            value.count++;
            dictionary.put(key, value);

            if (key.length() > maxlength) maxlength = key.length();
        }

        // edits/suggestions are created only once, no matter how often word occurs
        // edits/suggestions are created only as soon as the word occurs in the corpus,
        // even if the same term existed before in the dictionary as an edit from another word
        // a threshold might be specified, when a term occurs so frequently in the corpus that it is considered a valid word for spelling correction
        if (value.count == 1) {
            // word2index
            wordlist.add(key);
            int keyint = (int)(wordlist.size() - 1);

            result = true;

            //create deletes
            for (String delete : Edits(key, 0, new HashSet<String>())) {
                Object value2;
                value2 = dictionary.get(delete);
                if (value2!=null) {
                    // already exists:
                    // 1. word1==deletes(word2)
                    // 2. deletes(word1)==deletes(word2)
                    // int or dictionaryItem? single delete existed before!
                    if (value2 instanceof Integer) {
                        // transform int to dictionaryItem
                        int tmp = (int)value2;
                        dictionaryItem di = new dictionaryItem();
                        di.suggestions.add(tmp);
                        dictionary.put(delete, di);
                        if (!di.suggestions.contains(keyint)) AddLowestDistance(di, key, keyint, delete);
                    }
                    else if (!((dictionaryItem)value2).suggestions.contains(keyint)) AddLowestDistance((dictionaryItem) value2, key, keyint, delete);
                } else {
                    dictionary.put(delete, keyint);
                }

            }
        }
        return result;
    }

    // create a frequency dictionary from a corpus
    public static void CreateDictionary(String corpus) {
        File f = new File(corpus);
        if(!(f.exists() && !f.isDirectory())) {
            System.out.println("File not found: " + corpus);
            return;
        }

        System.out.println("Creating dictionary ...");
        long startTime = System.currentTimeMillis();
        long wordCount = 0;

        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(corpus));
            String line;
            while ((line = br.readLine()) != null) {
                for (String key : parseWords(line)) {
                    if (CreateDictionaryEntry(key)) wordCount++;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        //wordlist.TrimExcess();
        long endTime = System.currentTimeMillis();
        System.out.println("Dictionary: " + wordCount + " words, " + dictionary.size() + " entries, edit distance=" + editDistanceMax + " in " + (endTime-startTime)+"ms ");
    }

    /**
     * Remove all existing suggestions of higher distance, if verbose < 2
     * @param item
     * @param suggestion
     * @param suggestionint
     * @param delete
     */
    private static void AddLowestDistance(dictionaryItem item, String suggestion, int suggestionint, String delete) {
        // index2word
        if ((verbose < 2) && (item.suggestions.size() > 0) && (wordlist.get(item.suggestions.get(0)).length()-delete.length() > suggestion.length() - delete.length())) item.suggestions.clear();
        // do not add suggestion of higher distance than existing, if verbose < 2
        if ((verbose == 2) || (item.suggestions.size() == 0) || (wordlist.get(item.suggestions.get(0)).length()-delete.length() >= suggestion.length() - delete.length())) item.suggestions.add(suggestionint);
    }

    //inexpensive and language independent: only deletes, no transposes + replaces + inserts
    //replaces and inserts are expensive and language dependent (Chinese has 70,000 Unicode Han characters)
    private static HashSet<String> Edits(String word, int editDistance, HashSet<String> deletes) {
        editDistance++;
        if (word.length() > 1) {
            for (int i = 0; i < word.length(); i++) {
                //delete ith character
                String delete =  word.substring(0,i)+word.substring(i+1);
                if (deletes.add(delete)) {
                    //recursion, if maximum edit distance not yet reached
                    if (editDistance < editDistanceMax) Edits(delete, editDistance, deletes);
                }
            }
        }
        return deletes;
    }

    private static List<suggestItem> Lookup(String input, int editDistanceMax) {
        //save some time
        if (input.length() - editDistanceMax > maxlength)
            return new ArrayList<suggestItem>();

        List<String> candidates = new ArrayList<String>();
        HashSet<String> hashset1 = new HashSet<String>();

        List<suggestItem> suggestions = new ArrayList<suggestItem>();
        HashSet<String> hashset2 = new HashSet<String>();

        Object valueo;

        // add original term
        candidates.add(input);

        while (candidates.size()>0) {
            String candidate = candidates.get(0);
            candidates.remove(0);

            // save some time
            // early termination
            // suggestion distance=candidate.distance... candidate.distance+editDistanceMax
            // if candidate distance is already higher than suggestion distance, than there are no better suggestions to be expected

            // label for c# goto replacement
            nosort:{

                if ((verbose < 2) && (suggestions.size() > 0) && (input.length()-candidate.length() > suggestions.get(0).distance))
                    break nosort;

                //read candidate entry from dictionary
                valueo = dictionary.get(candidate);
                if (valueo != null) {
                    dictionaryItem value= new dictionaryItem();
                    if (valueo instanceof Integer)
                        value.suggestions.add((int)valueo);
                    else value = (dictionaryItem)valueo;

                    //if count>0 then candidate entry is correct dictionary term, not only delete item
                    if ((value.count > 0) && hashset2.add(candidate)) {
                        //add correct dictionary term term to suggestion list
                        suggestItem si = new suggestItem();
                        si.term = candidate;
                        si.count = value.count;
                        si.distance = input.length() - candidate.length();
                        suggestions.add(si);
                        //early termination
                        if ((verbose < 2) && (input.length() - candidate.length() == 0))
                            break nosort;
                    }

                    //iterate through suggestions (to other correct dictionary items) of delete item and add them to suggestion list
                    Object value2;
                    for (int suggestionint : value.suggestions) {
                        //save some time
                        //skipping double items early: different deletes of the input term can lead to the same suggestion
                        //index2word
                        String suggestion = wordlist.get(suggestionint);
                        if (hashset2.add(suggestion)) {
                            //True Damerau-Levenshtein Edit Distance: adjust distance, if both distances>0
                            //We allow simultaneous edits (deletes) of editDistanceMax on on both the dictionary and the input term.
                            //For replaces and adjacent transposes the resulting edit distance stays <= editDistanceMax.
                            //For inserts and deletes the resulting edit distance might exceed editDistanceMax.
                            //To prevent suggestions of a higher edit distance, we need to calculate the resulting edit distance, if there are simultaneous edits on both sides.
                            //Example: (bank==bnak and bank==bink, but bank!=kanb and bank!=xban and bank!=baxn for editDistanceMaxe=1)
                            //Two deletes on each side of a pair makes them all equal, but the first two pairs have edit distance=1, the others edit distance=2.
                            int distance = 0;
                            if (suggestion != input) {
                                if (suggestion.length() == candidate.length()) distance = input.length() - candidate.length();
                                else if (input.length() == candidate.length()) distance = suggestion.length() - candidate.length();
                                else {
                                    //common prefixes and suffixes are ignored, because this speeds up the Damerau-levenshtein-Distance calculation without changing it.
                                    int ii = 0;
                                    int jj = 0;
                                    while ((ii < suggestion.length()) && (ii < input.length()) && (suggestion.charAt(ii) == input.charAt(ii))) ii++;
                                    while ((jj < suggestion.length() - ii) && (jj < input.length() - ii) && (suggestion.charAt(suggestion.length() - jj - 1) == input.charAt(input.length() - jj - 1))) jj++;
                                    if ((ii > 0) || (jj > 0)) {
                                        distance = DamerauLevenshteinDistance(suggestion.substring(ii, suggestion.length() - jj), input.substring(ii, input.length() - jj));
                                    }
                                    else distance = DamerauLevenshteinDistance(suggestion, input);
                                }
                            }

                            //save some time.
                            //remove all existing suggestions of higher distance, if verbose<2
                            if ((verbose < 2) && (suggestions.size() > 0) && (suggestions.get(0).distance > distance)) suggestions.clear();
                            //do not process higher distances than those already found, if verbose<2
                            if ((verbose < 2) && (suggestions.size() > 0) && (distance > suggestions.get(0).distance)) continue;

                            if (distance <= editDistanceMax) {
                                value2 = dictionary.get(suggestion);
                                if (value2!=null) {
                                    suggestItem si = new suggestItem();
                                    si.term = suggestion;
                                    si.count = ((dictionaryItem)value2).count;
                                    si.distance = distance;
                                    suggestions.add(si);
                                }
                            }
                        }
                    }
                }

                //add edits
                //derive edits (deletes) from candidate (input) and add them to candidates list
                //this is a recursive process until the maximum edit distance has been reached
                if (input.length() - candidate.length() < editDistanceMax) {
                    //save some time
                    //do not create edits with edit distance smaller than suggestions already found
                    if ((verbose < 2) && (suggestions.size() > 0) && (input.length() - candidate.length() >= suggestions.get(0).distance)) continue;

                    for (int i = 0; i < candidate.length(); i++) {
                        String delete = candidate.substring(0, i)+candidate.substring(i+1);
                        if (hashset1.add(delete)) candidates.add(delete);
                    }
                }
            }
        }

        //sort by ascending edit distance, then by descending word frequency
        if (verbose < 2)
            //suggestions.Sort((x, y) => -x.count.CompareTo(y.count));
            Collections.sort(suggestions, new Comparator<suggestItem>() {
                public int compare(suggestItem f1, suggestItem f2) {
                    return -(f1.count-f2.count);
                }
            });
        else
            //suggestions.Sort((x, y) => 2*x.distance.CompareTo(y.distance) - x.count.CompareTo(y.count));
            Collections.sort(suggestions, new Comparator<suggestItem>()
            {
                public int compare(suggestItem x, suggestItem y)
                {
                    return ((2*x.distance-y.distance)>0?1:0) - ((x.count - y.count)>0?1:0);
                }
            });
        if ((verbose == 0)&&(suggestions.size()>1))
            return suggestions.subList(0, 1);
        else return suggestions;
    }

    public static String Correct(String input) {
        StringBuilder sb = new StringBuilder();
        for (String  word : input.split("\\s+")) {
            List<suggestItem> suggestions = null;
            //check in dictionary for existence and frequency; sort by ascending edit distance, then by descending word frequency
            suggestions = Lookup(word, editDistanceMax);

            // display term
            for (suggestItem suggestion: suggestions) {
                sb.append(suggestion.term + " ");
            }
            if (verbose != 0) System.out.println(suggestions.size() + " suggestions");
        }
        return sb.length() > 0? sb.toString().trim() : input;
    }

    private static void ReadFromStdIn() {
        String text = "helllo";
        System.out.print(Correct(text));
    }

    public static void main(String[] args){
        CreateDictionary("20k.txt");
        ReadFromStdIn();
    }

    /** Damerau–Levenshtein distance algorithm and code
     *  from http://en.wikipedia.org/wiki/Damerau%E2%80%93Levenshtein_distance (as retrieved in June 2012)
     *
     * @param a: one string
     * @param b: the other string
     * @return: distance between two strings
     */
    public static int DamerauLevenshteinDistance(String a, String b) {
        final int inf = a.length() + b.length() + 1;
        int[][] H = new int[a.length() + 2][b.length() + 2];
        for (int i = 0; i <= a.length(); i++) {
            H[i + 1][1] = i;
            H[i + 1][0] = inf;
        }
        for (int j = 0; j <= b.length(); j++) {
            H[1][j + 1] = j;
            H[0][j + 1] = inf;
        }
        HashMap<Character, Integer> DA = new HashMap<Character, Integer>();
        for (int d = 0; d < a.length(); d++) {
            if (!DA.containsKey(a.charAt(d))) DA.put(a.charAt(d), 0);
        }

        for (int d = 0; d < b.length(); d++) {
            if (!DA.containsKey(b.charAt(d))) DA.put(b.charAt(d), 0);
        }

        for (int i = 1; i <= a.length(); i++) {
            int DB = 0;
            for (int j = 1; j <= b.length(); j++) {
                final int i1 = DA.get(b.charAt(j - 1));
                final int j1 = DB;
                int d = 1;
                if (a.charAt(i - 1) == b.charAt(j - 1)) {
                    d = 0;
                    DB = j;
                }
                H[i + 1][j + 1] = min(
                        H[i][j] + d,
                        H[i + 1][j] + 1,
                        H[i][j + 1] + 1,
                        H[i1][j1] + ((i - i1 - 1))
                                + 1 + ((j - j1 - 1)));
            }
            DA.put(a.charAt(i - 1), i);
        }
        return H[a.length() + 1][b.length() + 1];
    }

    private static int min(int a, int b, int c, int d) {
        return Math.min(a, Math.min(b, Math.min(c, d)));
    }
}
