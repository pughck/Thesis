package edu.rosehulman.pughck;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class LocalBolt extends BaseRichBolt {

	private OutputCollector outputCollector;

	@Override
	public void execute(Tuple tuple) {

		String tweet = tuple.getStringByField("tweet");

		try {
			PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter("./docs/test.txt", true)));

			writer.println(tweet);

			writer.close();

			this.outputCollector.ack(tuple);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map map, TopologyContext context, OutputCollector collector) {

		this.outputCollector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		// nothing needed here
	}
}
