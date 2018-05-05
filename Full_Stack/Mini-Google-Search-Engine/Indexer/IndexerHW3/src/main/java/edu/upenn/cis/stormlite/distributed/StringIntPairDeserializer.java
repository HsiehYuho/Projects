package edu.upenn.cis.stormlite.distributed;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.IntNode;

import edu.upenn.cis.stormlite.StringIntPair;

public class StringIntPairDeserializer extends JsonDeserializer<StringIntPair> {
    public StringIntPairDeserializer() { 
        this(null);
    } 
 
    public StringIntPairDeserializer(Class<?> vc) { 
        super(); 
    }
 
    @Override
    public StringIntPair deserialize(JsonParser jsonparser, DeserializationContext context) 
      throws IOException, JsonProcessingException {
    	JsonNode node = jsonparser.getCodec().readTree(jsonparser);
    	
    	String className = node.get("left").asText();
    	int count = (Integer) ((IntNode) node.get("right")).numberValue();
        
        return new StringIntPair(className, count);
    }
}
