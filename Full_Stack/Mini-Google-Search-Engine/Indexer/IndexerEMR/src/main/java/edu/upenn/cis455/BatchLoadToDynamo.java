package edu.upenn.cis455;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.*;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.*;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class BatchLoadToDynamo {

    /** AWS credentials for DynamoDB. */
    private static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
            .withCredentials(new ProfileCredentialsProvider("lanqingy"))
            .withRegion("us-east-1").build();

    /** DynamoDB for index result. */
    private static DynamoDB dynamoDB;

    /** Dynamo table for index result. */
    private static Table table;

    /** JSON serialize and deserialize. */
    private static ObjectMapper mapper = new ObjectMapper();

    /** Create table. */
    private static Table createTable(String tableName) {
        try {
            TableCollection<ListTablesResult> tables = dynamoDB.listTables();
            Iterator it = tables.iterator();
            while (it.hasNext()) {
                Table table = (Table) it.next();
                if (table.getTableName().equals(tableName)) {
                    return dynamoDB.getTable(tableName);
                }
            }
            System.out.println("Attempting to create table; please wait...");
            Table table = dynamoDB.createTable(tableName,
                    Arrays.asList(new KeySchemaElement("key", KeyType.HASH), // Partition key
                            new KeySchemaElement("score", KeyType.RANGE)), // Sort key
                    Arrays.asList(new AttributeDefinition("key", ScalarAttributeType.S),
                            new AttributeDefinition("score", ScalarAttributeType.N)),
                    new ProvisionedThroughput(100L, 1000L));
            table.waitForActive();
            System.out.println("Success.  Table status: " + table.getDescription().getTableStatus());
            return table;
        } catch (Exception e) {
            System.err.println("Unable to create table: ");
            System.err.println(e.getMessage());
            return null;
        }
    }

    @DynamoDBTable(tableName = "Index1")
    public static class IndexItem {
        private String key;
        private String docID;
        private String contentType;
        private int tf;
        private double idf;
        private int title;
        private int urlWord;
        private int meta;
        private int anchorText;
        private int cap;
        private double score;
        private String positions;

        // Partition key
        @DynamoDBHashKey(attributeName = "key")
        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        // Sort key
        @DynamoDBRangeKey(attributeName = "score")
        public Double getScore() {
            return score;
        }

        public void setScore(Double score) {
            this.score = score;
        }

        @DynamoDBAttribute(attributeName = "docID")
        public String getDocId() {
            return docID;
        }

        public void setDocID(String id) {
            this.docID = id;
        }

        @DynamoDBAttribute(attributeName = "ContentType")
        public String getContentType() {
            return contentType;
        }

        public void setContentType(String contentType) {
            this.contentType = contentType;
        }

        @DynamoDBAttribute(attributeName = "TF")
        public int getTF() {
            return tf;
        }

        public void setTF(int tf) {
            this.tf = tf;
        }

        @DynamoDBAttribute(attributeName = "IDF")
        public double getIDF() {
            return idf;
        }

        public void setIDF(double idf) {
            this.idf = idf;
        }

        @DynamoDBAttribute(attributeName = "TITLE")
        public int getTITLE() {
            return title;
        }

        public void setTITLE(int title) {
            this.title = title;
        }

        @DynamoDBAttribute(attributeName = "URL")
        public int getUrlWord() {
            return urlWord;
        }

        public void setUrlWord(int urlWord) {
            this.urlWord = urlWord;
        }

        @DynamoDBAttribute(attributeName = "META")
        public int getMeta() {
            return meta;
        }

        public void setMeta(int meta) {
            this.meta = meta;
        }

        @DynamoDBAttribute(attributeName = "CAP")
        public int getCAP() {
            return cap;
        }

        public void setCAP(int cap) {
            this.cap = cap;
        }

        @DynamoDBAttribute(attributeName = "ANCHORTEXT")
        public int getANCHORTEXT() {
            return anchorText;
        }

        public void setANCHORTEXT(int anchortext) {
            this.anchorText = anchorText;
        }

        @DynamoDBAttribute(attributeName = "positions")
        public String getPositions() {
            return positions;
        }

        public void setPositions(String positions) {
            this.positions = positions;
        }


        @Override
        public String toString() {
            // generate JSON string
            StringBuilder sb = new StringBuilder();

            // version2: space efficient + [key, list of docInfo]
            sb.append("{\"docID\": \"" + docID + "\", " +
                    "\"Content-Type\": \"" + contentType + "\", " +
                    "\"TF\": " + tf + ", " +
                    "\"IDF\": " + idf + ", " +
                    "\"TITLE\": " + title + ", " +
                    "\"URL\": " + urlWord + ", " +
                    "\"ANCHORTEXT\": " + anchorText + ", " +
                    "\"META\": " + meta + ", " +
                    "\"CAP\": " + cap + ", " +
                    "\"score\": " + score + ", " +
                    "\"positions\": " + positions + "}");
            return sb.toString();
        }
    }

    public static void main(String[] args) throws IOException {

        dynamoDB = new DynamoDB(client);
        table = createTable("Index1");
        if (table == null) return;
        DynamoDBMapper dbmapper = new DynamoDBMapper(client);

        File envHome = new File("./output");
        if (!envHome.exists())  return;

        System.out.println("Starting time: " + new Date());
        int counter = 0;
        for (File file: envHome.listFiles()) {
            if (!file.isDirectory() && !file.isHidden() && !file.getName().equals("_SUCCESS")) {
                try {
                    System.out.println("processing file: " + file);
                    BufferedReader br = new BufferedReader(new FileReader(file));
                    List<Object> objectsToWrite = new ArrayList<>();
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] pair = line.split("\t");
                        if (pair.length != 2) {
                            continue;
                        }

                        String key = pair[0];
                        String value = pair[1];

                        try {
                            // deserialize
                            DocInfo jsonStr = mapper.readValue(value, DocInfo.class);

                            // Build the item
                            IndexItem item = new IndexItem();
                            item.setKey(key);
                            item.setDocID(jsonStr.getDocID());
                            item.setANCHORTEXT(jsonStr.getAnchorText());
                            item.setCAP(jsonStr.getCap());
                            item.setContentType(jsonStr.getContentType());
                            item.setScore(jsonStr.getScore());
                            item.setTF(jsonStr.getTF());
                            item.setIDF(jsonStr.getIDF());
                            item.setTITLE(jsonStr.getTitle());
                            item.setUrlWord(jsonStr.getUrlWord());
                            item.setMeta(jsonStr.getMeta());
                            item.setPositions(jsonStr.getPositionalIndex());

                            objectsToWrite.add(item);
                            //table.putItem(item);
                            //System.out.println("PutItem succeeded: " + key + " " + jsonStr.getScore());
                        } catch (Exception e) {
                            System.out.println("PutItem error: " + key);
                            e.printStackTrace();
                        }

                        if (objectsToWrite.size() == 25) {
                            List<DynamoDBMapper.FailedBatch> outcomes = dbmapper.batchSave(objectsToWrite);
                            for (DynamoDBMapper.FailedBatch outcome: outcomes) {
                                System.out.println(counter++ + " " + outcome.getUnprocessedItems() + "\n\n");
                            }

                            objectsToWrite.clear();
                        }
                    }
                    if (objectsToWrite.size() > 0) {
                        //counter += 25;
                        dbmapper.batchSave(objectsToWrite);
                        objectsToWrite.clear();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("Error reading output.txt");
                }
            }
        }
        System.out.println("Ending time: " + new Date());
        System.out.println("Counter: " + counter);
    }
}
