package searchengine.store;

import org.jsoup.nodes.Attribute;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Node;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class CheckDisplayUrlObj {
    private static String[] badWords  = new String[] {"arse","ass","asshole",
            "bastard","bitch","boong","cock","cocksucker","coon",
            "coonnass","crap","cunt","damn","darn","dick","douche",
            "fag","faggot","fuck","gook",
            "motherfucker","piss","pussy","shit","slut","tits","fucking","fucker","fucks","fucked", "shitter", "shits", "shitted",
    "pussies", "crapped", "crapping", "damned", "damning", "bitches", "bitched", "bitching", "pisses", "pissing", "pissed", "pisser",
    "bastards", "fags", "faggs", "fuckers", "cunts", "douches", "dicks", "damnit", "damnitt", "motherfuckers", "motherfucking","titties"};

    public static boolean sameDOMHTML(Document doc1,
                                       Document doc2, boolean childProof){

        if(childProof){
            System.out.println("Is child proof");
            String[] txts = doc1.text().split("\\s+");
            Set<String> txtSet = new HashSet<>();
            for(String txt : txts){
                txtSet.add(txt);
            }
            for(String bad : badWords){
                if(txtSet.contains(bad)){
                    return true;
                }
            }
        }
        ArrayList<Node> parent1 =
                new ArrayList<Node>();
        parent1.add(doc1);
        ArrayList<Node> parent2 =
                new ArrayList<Node>();
        parent2.add(doc2);
        ArrayList<org.jsoup.nodes.Node> child1 =
                new ArrayList<Node>();
        ArrayList<org.jsoup.nodes.Node> child2 =
                new ArrayList<Node>();
        while(parent1.size() > 0 && parent2.size() > 0){
            org.jsoup.nodes.Node cur1 = parent1.get(0);
            parent1.remove(0);
            org.jsoup.nodes.Node cur2 = parent2.get(0);
            parent2.remove(0);
            if(!cur1.nodeName().equals(cur2.nodeName())) return false;
            if(!evaluateTextSimilarity(cur1.toString(), cur2.toString())){

                return false;
            }

            if(cur1.childNodes().size()
                    != cur2.childNodes().size()){
                return false;
            }
            for(int i = 0; i < cur1.childNodes().size(); i++){
                child1.add(cur1.childNodes().get(i));
                child2.add(cur2.childNodes().get(i));
            }
            if(cur1.attributes().size() != cur2.attributes().size()) return false;
            for(Attribute a: cur1.attributes()){
                if(!cur2.hasAttr(a.getKey())) return false;
                if(!a.getValue().equals(cur2.attr(a.getKey()))) return false;
            }
            if(parent1.size() == 0){
                if(parent2.size() != 0) return false;
                parent1 = child1;
                parent2 = child2;
                child1 = new ArrayList<org.jsoup.nodes.Node>();
                child2 = new ArrayList<org.jsoup.nodes.Node>();
            }
        }
        return parent1.size() == 0 && parent2.size() == 0;
    }
    private static boolean evaluateTextSimilarity(String t1, String t2){
        String[] s1 = t1.split(" ");
        String[] s2 = t2.split(" ");
        if(s1.length != s2.length) return false;
        double allowance = s1.length * .2;

        for(int i = 0; i < s1.length; i++){
            if(!s1[i].equals(s2[i])) allowance --;
            if(allowance < 0) return false;
        }
        return allowance >= 0;
    }

    private static ArrayList<String> loadBad() throws Exception{
        ArrayList<String> hold = new ArrayList<String>();
        for(String w : badWords)
            hold.add(w);
        return hold;
    }


}
