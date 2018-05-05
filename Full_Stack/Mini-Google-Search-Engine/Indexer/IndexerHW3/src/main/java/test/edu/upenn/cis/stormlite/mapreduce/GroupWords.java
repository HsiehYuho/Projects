package test.edu.upenn.cis.stormlite.mapreduce;

import java.util.Iterator;
import java.util.Random;

import edu.upenn.cis.stormlite.tuple.Values;
import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;

public class GroupWords implements Job {
	@Override
	public void map(String key, String value, Context context) {
        String[] words = value.split("[ \\t\\,.]");

        for (String word: words) {
            context.write("1", "1");
        }

	}

	@Override
	public void reduce(String key, Iterator<String> values, Context context) {
		int i = 0;
		while (values.hasNext()) {
			i++;
			values.next();
		}
		context.write(key, String.valueOf(i));
	}

}
