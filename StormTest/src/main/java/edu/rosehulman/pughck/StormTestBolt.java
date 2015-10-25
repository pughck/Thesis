package edu.rosehulman.pughck;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class StormTestBolt extends BaseRichBolt {

	private OutputCollector outputCollector;

	@Override
	public void execute(Tuple tuple) {

		String tweet = tuple.getStringByField("tweet");

		// PrintWriter writer = new PrintWriter();
		System.out.println(tuple.toString());
//		writer.append();
//		writer.close();
		
		// File file = new File("/tmp/stormTestOutput.txt");
		//
		// if (!file.exists()) {
		// file.createNewFile();
		// }
		//
		// BufferedWriter writer = new BufferedWriter(new
		// FileWriter(file.getAbsolutePath(), true));
		//
		// writer.write(tweet);
		this.outputCollector.ack(tuple);

	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map map, TopologyContext context, OutputCollector collector) {
		this.outputCollector = collector;
		// nothing needed here
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		// nothing needed here
	}
}
