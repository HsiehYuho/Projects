package edu.upenn.cis455.mapreduce;

public interface Context {

  void write(String key, String value);
  
}
