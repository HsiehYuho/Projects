package edu.upenn.cis.stormlite.bolt;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.io.BufferedWriter;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableCollection;
import com.amazonaws.services.dynamodbv2.model.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.upenn.cis.stormlite.Topology;
import edu.upenn.cis455.mapreduce.DocInfo;
import edu.upenn.cis455.mapreduce.worker.WorkerServer;
import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;

/**
 * A trivial bolt that simply outputs its input stream to the
 * console
 * 
 * @author zives
 *
 */
public class PrintBolt implements IRichBolt {
	static Logger log = Logger.getLogger(PrintBolt.class);
	
	Fields myFields = new Fields();

    /**
     * To make it easier to debug: we have a unique ID for each
     * instance of the PrintBolt, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();

    String outputPath;
    String imagePath;
    BufferedWriter outputWriter = null;
    BufferedWriter imageWriter = null;

    TopologyContext context;

    static DynamoDB dynamoDB;

    static Table table;

    ObjectMapper mapper = new ObjectMapper();

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {

	    // write to local file
//	    this.context = context;
//
//	    // get relative output directory
//	    String outputDir = "";
//	    if (stormConf.containsKey("outputDir")) {
//	        outputDir = stormConf.get("outputDir");
//        }
//
//        // create output directory if none
//        File envHome = new File(WorkerServer.getEnvDirectory() + stormConf.get("outputDir"));
//        if (!envHome.exists())  envHome.mkdir();
//
//        // prepare writer for output.txt (append)
//        outputPath = WorkerServer.getEnvDirectory() + stormConf.get("outputDir") + "/output.json";
//        log.debug("Output file path: " + outputPath);
//
//        imagePath = WorkerServer.getEnvDirectory() + stormConf.get("outputDir") + "/image.json";
//        log.debug("Output image file path: " + imagePath);
//
//        File outputFile = new File (outputPath);
//        if (outputFile.exists()) {
//            outputFile.delete();
//        }
//
//        File imageFile = new File (imagePath);
//        if (imageFile.exists()) {
//            imageFile.delete();
//        }
//
//        try {
//            outputFile.createNewFile();
//            outputWriter = new BufferedWriter(new FileWriter(outputFile, true));
//
//            imageFile.createNewFile();
//            imageWriter = new BufferedWriter(new FileWriter(imageFile, true));
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        // connect to dynamodb
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(new ProfileCredentialsProvider("lanqingy"))
                .withRegion("us-east-1").build();

        dynamoDB = new DynamoDB(client);
        createTable("Index8");
        table = dynamoDB.getTable("Index8");
    }

    /** Create table. */
    private static void createTable(String tableName) {
        try {
            TableCollection<ListTablesResult> tables = dynamoDB.listTables();
            Iterator it = tables.iterator();
            while (it.hasNext()) {
                Table table = (Table) it.next();
                if (table.getTableName().equals(tableName)) {
                    return; // or can delete table here?
                }
            }
            System.out.println("Attempting to create table; please wait...");
            Table table = dynamoDB.createTable(tableName,
                    Arrays.asList(new KeySchemaElement("key", KeyType.HASH), // Partition key
                            new KeySchemaElement("score", KeyType.RANGE)), // Sort key
                    Arrays.asList(new AttributeDefinition("key", ScalarAttributeType.S),
                            new AttributeDefinition("score", ScalarAttributeType.N)),
                    new ProvisionedThroughput(100L, 500L));
            table.waitForActive();
            System.out.println("Success.  Table status: " + table.getDescription().getTableStatus());

        } catch (Exception e) {
            System.err.println("Unable to create table: ");
            System.err.println(e.getMessage());
        }
    }

    @Override
    public void execute(Tuple input) {
	    // write as [key, list of docInfo]
//        if (!input.isEndOfStream()) {
//            String key = input.getStringByField("key");
//            String value = input.getStringByField("value");
////            if (value.length() == 64) {
////                try {
////                    imageWriter.write("{\n" + "\t\"key\": \"" + key + "\",\n" +
////                            "\t\"value\": \"" + value + "\"\n},\n");
////                    imageWriter.flush();
////                } catch (IOException e) {
////                    e.printStackTrace();
////                }
////            } else {
//                try {
//                    outputWriter.write("{\"key\": \"" + key + "\", " +
//                            "\"value\": [" + value + "]},");
//                    outputWriter.newLine();
//                    outputWriter.flush();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//         //   }
//        }

         // write as [key, docInfo]
//        try {
//            outputWriter.write("{\"key\": \"" + key + "\", " + value + "},");
//            outputWriter.newLine();
//            outputWriter.flush();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        // directly write into dynamodb
        if (!input.isEndOfStream()) {
            String key = input.getStringByField("key");
            String value = input.getStringByField("value");

            try {
                // deserialize
                DocInfo jsonStr = mapper.readValue(value, DocInfo.class);

                // Build the item
                Item item = new Item()
                        .withPrimaryKey("key", key, "score", jsonStr.getScore())
                        .withString("docID", jsonStr.getDocID())
                        .withString("ContentType",jsonStr.getContentType())
                        .withNumber("TF", jsonStr.getTF())
                        .withNumber("IDF", jsonStr.getIDF())
                        .withNumber("TITLE", jsonStr.getTitle())
                        .withNumber("URL", jsonStr.getUrlWord())
                        .withNumber("ANCHORTEXT", jsonStr.getAnchorText())
                        .withNumber("META", jsonStr.getMeta())
                        .withNumber("CAP", jsonStr.getCap())
                        .withString("positions", jsonStr.getPositionalIndex());
                table.putItem(item);
                System.out.println("PutItem succeeded: " + key + " " + jsonStr.getScore());
            } catch (Exception e) {
                System.out.println("Error put item into dynamoDB");
            }
        }
    }

    @Override
    public void cleanup() {
        if (outputWriter != null) {
            try {
                outputWriter.close();
            } catch (IOException e) {
                System.out.println("Error closing file writer");
            }
        }
        if (imageWriter != null) {
            try {
                imageWriter.close();
            } catch (IOException e) {
                System.out.println("Error closing image writer");
            }
        }
    }

	@Override
	public String getExecutorId() {
		return executorId;
	}

	@Override
	public void setRouter(StreamRouter router) {
		// Do nothing
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(myFields);
	}

	@Override
	public Fields getSchema() {
		return myFields;
	}

}
