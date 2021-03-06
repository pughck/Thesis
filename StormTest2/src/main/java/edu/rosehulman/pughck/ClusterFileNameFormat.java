package edu.rosehulman.pughck;

import java.util.Map;

import org.apache.storm.hdfs.bolt.format.FileNameFormat;

import backtype.storm.task.TopologyContext;

@SuppressWarnings("serial")
public class ClusterFileNameFormat implements FileNameFormat {

	@Override
	public String getName(long rotation, long timestamp) {

		return timestamp + ".txt";
	}

	@Override
	public String getPath() {

		return "/tmp/stormOut/";
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map map, TopologyContext context) {

		// nothing needed here
	}
}
