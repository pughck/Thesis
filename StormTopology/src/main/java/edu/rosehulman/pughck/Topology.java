package edu.rosehulman.pughck;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

public class Topology {

	public static void main(String[] args) throws Exception {

		Config conf = new Config();
		conf.setNumWorkers(1);
		conf.setMaxSpoutPending(5000);

		TopologyBuilder builder = new TopologyBuilder();

		BaseRichSpout spout = new TweetSpout("OgHA59vKcpBKqr92QsVyhGswD",
				"nWComrkhNlHYKVE2SjCb2D1roLzNog1NNDEh5s98c9i6KJJ6XT",
				"4041041357-rkWibnDMhQSwJD1g5iOCsJae2J56Ni4XGbbOVe9", "XaDM2EVfh3om2uWsUD6sWVCeOERFgyDqHQAb6FfwSUUix");

		builder.setSpout("tweetSpout", spout, 1);
		builder.setBolt("groupBolt", new GroupingBolt(), 5).shuffleGrouping("tweetSpout");
		builder.setBolt("hdfsBolt", new HdfsOutBolt(), 5).fieldsGrouping("groupBolt", new Fields("company"));

		StormSubmitter.submitTopology("topology", conf, builder.createTopology());
	}
}
