package test.edu.upenn.cis.stormlite.mapreduce;

import edu.upenn.cis.stormlite.spout.FileSpout;

public class WordFileSpout extends FileSpout {
    @Override
	public String getFilename() {
		return "Chocolate_Wikipedia.html";
	}
}
