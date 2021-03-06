package edu.rosehulman.pughck;

import org.apache.storm.hdfs.bolt.format.RecordFormat;

import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class ClusterRecordFormat implements RecordFormat {

	@Override
	public byte[] format(Tuple tuple) {

		String tweet = tuple.getStringByField("tweet");

		return tweet.getBytes();
	}
}
