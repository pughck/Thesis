package edu.rosehulman.pughck;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

//import org.apache.storm.hdfs.bolt.HdfsBolt;
//import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
//import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
//import org.apache.storm.hdfs.bolt.format.FileNameFormat;
//import org.apache.storm.hdfs.bolt.format.RecordFormat;
//import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
//import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
//import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
//import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
//import org.apache.storm.hdfs.bolt.sync.SyncPolicy;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import backtype.storm.utils.Utils;
import storm.kafka.bolt.KafkaBolt;
import storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import storm.kafka.bolt.selector.DefaultTopicSelector;
import storm.kafka.trident.TridentKafkaState;
import backtype.storm.task.TopologyContext;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;

public class WordCountTopology {

	private WordCountTopology() {
	}

	@SuppressWarnings("serial")
	static class WordSpout extends BaseRichSpout {

		private Random rand;
		private SpoutOutputCollector collector;
		private String[] wordList;

		@SuppressWarnings("rawtypes")
		@Override
		public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {

			this.rand = new Random(31);
			this.collector = collector;
			this.wordList = new String[] { "Jack", "Mary", "Jill", "McDonald" };
		}

		@Override
		public void nextTuple() {

			Utils.sleep(1000);

			int next = this.rand.nextInt(wordList.length);

			this.collector.emit(new Values(wordList[next]));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {

			declarer.declare(new Fields("word"));
		}
	}

	@SuppressWarnings("serial")
	static class CountBolt extends BaseRichBolt {

		private OutputCollector collector;
		private Map<String, Integer> countMap;

		@SuppressWarnings("rawtypes")
		@Override
		public void prepare(Map map, TopologyContext context, OutputCollector collector) {

			this.collector = collector;
			this.countMap = new HashMap<String, Integer>();
		}

		@Override
		public void execute(Tuple tuple) {

			String word = tuple.getString(0);

			if (countMap.containsKey(word)) {
				countMap.put(word, 1);
			} else {
				Integer val = countMap.get(word);
				countMap.put(word, val + 1);
			}

			collector.emit(new Values(word, this.countMap.get(word)));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {

			declarer.declare(new Fields("word", "count"));
		}
	}

	// @SuppressWarnings("serial")
	// static class ReportBolt extends BaseRichBolt {
	//
	// @SuppressWarnings("rawtypes")
	// @Override
	// public void prepare(Map map, TopologyContext context, OutputCollector
	// collector) {
	// }
	//
	// @Override
	// public void execute(Tuple tuple) {
	//
	// String word = tuple.getStringByField("word");
	// Integer count = tuple.getIntegerByField("count");
	//
	// String out = word + "\t" + count;
	//
	// String path = "/tmp/output/out.txt";
	//
	// }
	//
	// @Override
	// public void declareOutputFields(OutputFieldsDeclarer declarer) {
	// }
	// }

	public static void main(String[] args) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("word-spout", new WordSpout(), 5);
		builder.setBolt("count-bolt", new CountBolt(), 15).fieldsGrouping("word-count", new Fields("word"));
		// builder.setBolt("report-bolt", new ReportBolt(),
		// 1).globalGrouping("count-bolt");

		// RecordFormat format = new
		// DelimitedRecordFormat().withFieldDelimiter("\t");
		// SyncPolicy syncPolicy = new CountSyncPolicy(10);
		// FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f,
		// Units.MB);
		// FileNameFormat fileNameFormat = new DefaultFileNameFormat();
		//
		// HdfsBolt hdfsBolt = new
		// HdfsBolt().withFsUrl("hdfs://hadoop-ckp-1.csse.rose-hulman.edu:8020/tmp/wordOut")
		// .withFileNameFormat(fileNameFormat).withRecordFormat(format).withRotationPolicy(rotationPolicy)
		// .withSyncPolicy(syncPolicy);
		// builder.setBolt("report-bolt",
		// hdfsBolt).globalGrouping("count-bolt");

		@SuppressWarnings({ "unchecked", "rawtypes" })
		KafkaBolt bolt = new KafkaBolt().withTopicSelector(new DefaultTopicSelector("test"))
				.withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());
		builder.setBolt("kafka-bolt", bolt).globalGrouping("count-bolt");

		Config conf = new Config();
		conf.setDebug(true);
		conf.setNumWorkers(3);

		Properties props = new Properties();
		props.put("metadata.broker.list", "hadoop-ckp-1.csse.rose-hulman.edu:2181");
		props.put("request.required.acks", "1");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		conf.put(TridentKafkaState.KAFKA_BROKER_PROPERTIES, props);

		StormSubmitter.submitTopology("words-test", conf, builder.createTopology());
	}
}
