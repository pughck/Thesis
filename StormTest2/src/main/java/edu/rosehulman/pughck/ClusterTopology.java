package edu.rosehulman.pughck;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

public class ClusterTopology {

	public static void main(String[] args) throws Exception {

		Config conf = new Config();
		conf.setNumWorkers(1);
		conf.setMaxSpoutPending(5000);

		TopologyBuilder builder = new TopologyBuilder();

		BaseRichSpout spout = new TweetSpout("OgHA59vKcpBKqr92QsVyhGswD",
				"nWComrkhNlHYKVE2SjCb2D1roLzNog1NNDEh5s98c9i6KJJ6XT",
				"4041041357-rkWibnDMhQSwJD1g5iOCsJae2J56Ni4XGbbOVe9", "XaDM2EVfh3om2uWsUD6sWVCeOERFgyDqHQAb6FfwSUUix");

		// HdfsBolt bolt = new
		// CustomHdfsBolt().withFsUrl("hdfs://hadoop-ckp-1.csse.rose-hulman.edu:8020")
		// .withRecordFormat(new ClusterRecordFormat()).withFileNameFormat(new
		// ClusterFileNameFormat())
		// .withSyncPolicy(new CountSyncPolicy(1000)).withRotationPolicy(new
		// NoRotationPolicy());

		builder.setSpout("tweetSpout", spout, 1);
		builder.setBolt("groupBolt", new GroupingBolt(), 1).shuffleGrouping("tweetSpout");
		builder.setBolt("hdfsBolt", new MyHdfsBolt(), 1).fieldsGrouping("groupBolt", new Fields("company"));

		StormSubmitter.submitTopology("topology", conf, builder.createTopology());
	}
}
