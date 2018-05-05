package test.edu.upenn.cis.stormlite.mapreduce;

import java.util.Iterator;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableCollection;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;

public class TestDynamoAccess {

    public static void main(String[] args) {

        // connect to dynamodb
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(new ProfileCredentialsProvider("default"))
                .withRegion("us-east-1").build();

        DynamoDB dynamoDB = new DynamoDB(client);

        TableCollection<ListTablesResult> tables = dynamoDB.listTables();

        Iterator it = tables.iterator();

        while (it.hasNext()) {
            Table table = (Table) it.next();
            System.out.println(table.getTableName());
        }

    }
}
