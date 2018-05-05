package edu.upenn.cis.stormlite;

import java.io.Serializable;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import edu.upenn.cis.stormlite.distributed.StringIntPairDeserializer;

@JsonDeserialize(using = StringIntPairDeserializer.class)
public class StringIntPair implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	String left;
	Integer right;
	
	public StringIntPair() {}
	
	public StringIntPair(String left, Integer right) {
		this.left = left;
		this.right = right;
	}

	public String getLeft() {
		return left;
	}

	public void setLeft(String left) {
		this.left = left;
	}

	public Integer getRight() {
		return right;
	}

	public void setRight(Integer right) {
		this.right = right;
	}
	
	@Override
	public int hashCode() {
		return left.hashCode();
	}
}
