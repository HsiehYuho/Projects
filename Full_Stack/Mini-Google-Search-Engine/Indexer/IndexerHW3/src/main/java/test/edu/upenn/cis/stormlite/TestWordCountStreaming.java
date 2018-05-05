package test.edu.upenn.cis.stormlite;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.LocalCluster;
import edu.upenn.cis.stormlite.Topology;
import edu.upenn.cis.stormlite.TopologyBuilder;
import edu.upenn.cis.stormlite.tuple.Fields;

/**
 * Simple word counter test case, largely derived from
 * https://github.com/apache/storm/tree/master/examples/storm-mongodb-examples
 * 
 */
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
public class TestWordCountStreaming {
	static Logger log = Logger.getLogger(TestWordCountStreaming.class);

	private static final String WORD_SPOUT = "WORD_SPOUT";
    private static final String COUNT_BOLT = "COUNT_BOLT";
    private static final String PRINT_BOLT = "PRINT_BOLT";
    
    public static void main(String[] args) throws Exception {
        Config config = new Config();

        WordSpout spout = new WordSpout();
        WordCounter bolt = new WordCounter();
        PrintBolt printer = new PrintBolt();

        // wordSpout ==> countBolt ==> MongoInsertBolt
        TopologyBuilder builder = new TopologyBuilder();

        // Only one source ("spout") for the words
        builder.setSpout(WORD_SPOUT, spout, 1);
        
        // Four parallel word counters, each of which gets specific words
        builder.setBolt(COUNT_BOLT, bolt, 4).fieldsGrouping(WORD_SPOUT, new Fields("word"));
        
        // A single printer bolt (and officially we round-robin)
        builder.setBolt(PRINT_BOLT, printer, 4).shuffleGrouping(COUNT_BOLT);

        LocalCluster cluster = new LocalCluster();
        Topology topo = builder.createTopology();

        ObjectMapper mapper = new ObjectMapper();
		try {
			String str = mapper.writeValueAsString(topo);
			
			System.out.println("The StormLite topology is:\n" + str);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        
        cluster.submitTopology("test", config, 
        		builder.createTopology());
        Thread.sleep(30000);
        cluster.killTopology("test");
        cluster.shutdown();
        System.exit(0);
    } 
}
