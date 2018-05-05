package edu.upenn.cis.stormlite.spout;

public class WordFileSpout extends FileSpout {
    @Override
	public String getFilename() {
		return "words.txt";
	}
}
