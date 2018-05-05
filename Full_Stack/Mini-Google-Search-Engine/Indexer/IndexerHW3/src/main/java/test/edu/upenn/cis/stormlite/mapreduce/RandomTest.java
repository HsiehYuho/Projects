package test.edu.upenn.cis.stormlite.mapreduce;

import edu.upenn.cis455.mapreduce.DocInfo;
import com.fasterxml.jackson.databind.ObjectMapper;

public class RandomTest {

    public static void main(String[] args) throws Exception {

        // test jackson
        DocInfo docInfo = new DocInfo("222");
        ObjectMapper mapper = new ObjectMapper();

        // serialize
        String jsonStr = mapper.writeValueAsString(docInfo);
        System.out.println(jsonStr);

        // deserialize
        DocInfo result = mapper.readValue(jsonStr, DocInfo.class);
        System.out.println(result.getDocID());

    }
}
