package searchengine.Extra;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Arrays;

public class AutoCompleteTrie {
	
	private static class TrieNode{
		TrieNode children[] = new TrieNode[26];
		boolean completedWord = false;
		
		TrieNode(){
		}
	};
	
	private static TrieNode root = new TrieNode();
	
	public AutoCompleteTrie(){
		try {
			//addWords("english3.txt");
			addWords("nounlist.txt");
		}catch(Exception e){
			return;
		}
	}
	
	private static void addWords(String file) throws Exception {
		File f = new File(file);
		BufferedReader br = new BufferedReader(new FileReader(f));
		String line;
		while((line = br.readLine())!= null) {
			if(line.contains("\'") || line.contains("#")) continue;
			line = line.toLowerCase();
			line = line.replace("+", "");
//			line = Normalizer.normalize(line, Normalizer.Form.NFD); 
			line = line.replaceAll("[\\p{InCombiningDiacriticalMarks}]", "");
			add(line.toLowerCase());
		}

	}
	
	public static void add(String word){
		int index;
		TrieNode cur = root;
		for(int i = 0; i < word.length(); i++){
			index = word.charAt(i) - 'a';
			if(cur.children[index] == null){
				cur.children[index] = new TrieNode();
			}
			cur = cur.children[index];
		}
		cur.completedWord = true;
	}
	
	public static void addAll(String[] words){
		for(String word: words) add(word);
	}
	
	public static void addAll(ArrayList<String> words){
		for(String word: words) add(word);
	}
	
	private static ArrayList<String> autocomplete(TrieNode curNode, String input,
			int max){
		ArrayList<String> suggestions = new ArrayList<String>();
		
		if(curNode.completedWord){
			suggestions.add(input);
		}
		if(isFinalNode(curNode)) return suggestions;
		for(int i = 0; i < 26; i++){
			if(curNode.children[i] != null){
				ArrayList<String> output = autocomplete(curNode.children[i],
						input + (char)('a' +(int)i), max);
				suggestions.addAll(output);
				if(suggestions.size() >= max){
					ArrayList<String> ret = new ArrayList<String>();
					ret.addAll(suggestions.subList(0, max));
					return ret;
				}
			}
		}
		return suggestions;
	}
	
	public static ArrayList<String> getAutoSuggestions(String word, int max){
		word = word.toLowerCase();
		int index;
		TrieNode cur = root;
		ArrayList<String> allSuggestions = new ArrayList<String>();
		for(int i = 0; i < word.length(); i++){
			index = word.charAt(i) - 'a';
			if(cur.children[index] == null){
				return null;
			}
			cur = cur.children[index];
		}
//		if(cur.completedWord){
//			System.out.println(word);
//			if(isFinalNode(cur)){
//				allSuggestions.add(word);
//			}
//			return allSuggestions;
//		}
//		if(!isFinalNode(cur)){
//			return autocomplete(cur, word, max);
//		}
		return autocomplete(cur, word, max);
		//return null;
	}
	
	private static boolean isFinalNode(TrieNode cur){
		for(TrieNode node: cur.children){
			if(node != null) return false;
		}
		return true;
	}
	
	public static TrieNode getRoot(){
		return root;
	}

}
