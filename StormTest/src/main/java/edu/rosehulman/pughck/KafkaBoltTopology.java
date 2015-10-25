package edu.rosehulman.pughck;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;

import storm.kafka.bolt.KafkaBolt;

public class KafkaBoltTopology {

	public static void main(String[] args) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();
		BaseRichSpout spout = new StormTestTweetSpout("", "", "", "");

		builder.setSpout("tweetSpout", spout, 1);
		KafkaBolt<?, ?> bolt = new KafkaBolt<>();

		builder.setBolt("bolt", bolt, 1).globalGrouping("tweetSpout");

		Config conf = new Config();

		conf.setNumWorkers(1);

		StormSubmitter.submitTopology("test", conf, builder.createTopology());
	}
}
