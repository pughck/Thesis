package edu.rosehulman.pughck;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;

public class LocalTopology {

	public static void main(String[] args) throws Exception {

		LocalCluster cluster = new LocalCluster();

		TopologyBuilder builder = new TopologyBuilder();
		BaseRichSpout spout = new TweetSpout("OgHA59vKcpBKqr92QsVyhGswD",
				"nWComrkhNlHYKVE2SjCb2D1roLzNog1NNDEh5s98c9i6KJJ6XT",
				"4041041357-rkWibnDMhQSwJD1g5iOCsJae2J56Ni4XGbbOVe9", "XaDM2EVfh3om2uWsUD6sWVCeOERFgyDqHQAb6FfwSUUix");
		BaseRichBolt bolt = new LocalBolt();

		builder.setSpout("tweetSpout", spout, 1);
		builder.setBolt("localBolt", bolt, 1).globalGrouping("tweetSpout");

		Config conf = new Config();

		conf.setNumWorkers(1);

		cluster.submitTopology("local", conf, builder.createTopology());
	}
}
