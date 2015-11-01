package edu.rosehulman.pughck;

import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;
import java.util.Map;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.common.rotation.RotationAction;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class ClusterHdfsBolt extends AbstractHdfsBolt {

	private transient FSDataOutputStream out;
	private ClusterRecordFormat format;
	private long offset = 0;

	public ClusterHdfsBolt withFsUrl(String fsUrl) {

		this.fsUrl = fsUrl;
		return this;
	}

	public ClusterHdfsBolt withConfigKey(String configKey) {

		this.configKey = configKey;
		return this;
	}

	public ClusterHdfsBolt withFileNameFormat(ClusterFileNameFormat clusterFileNameFormat) {

		this.fileNameFormat = clusterFileNameFormat;
		return this;
	}

	public ClusterHdfsBolt withRecordFormat(ClusterRecordFormat clusterRecordFormat) {

		this.format = clusterRecordFormat;
		return this;
	}

	public ClusterHdfsBolt withSyncPolicy(SyncPolicy syncPolicy) {

		this.syncPolicy = syncPolicy;
		return this;
	}

	public ClusterHdfsBolt withRotationPolicy(FileRotationPolicy rotationPolicy) {

		this.rotationPolicy = rotationPolicy;
		return this;
	}

	public ClusterHdfsBolt addRotationAction(RotationAction action) {

		this.rotationActions.add(action);
		return this;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void doPrepare(Map conf, TopologyContext topologyContext, OutputCollector collector) throws IOException {

		this.fs = FileSystem.get(URI.create(this.fsUrl), hdfsConfig);
	}

	@Override
	public void execute(Tuple tuple) {
		try {
			byte[] bytes = this.format.format(tuple);
			synchronized (this.writeLock) {
				out.write(bytes);
				this.offset += bytes.length;

				if (this.syncPolicy.mark(tuple, this.offset)) {
					if (this.out instanceof HdfsDataOutputStream) {
						((HdfsDataOutputStream) this.out).hsync(EnumSet.of(SyncFlag.UPDATE_LENGTH));
					} else {
						this.out.hsync();
					}
					this.syncPolicy.reset();
				}
			}

			this.collector.ack(tuple);

			if (this.rotationPolicy.mark(tuple, this.offset)) {
				rotateOutputFile(); // synchronized
				this.offset = 0;
				this.rotationPolicy.reset();
			}
		} catch (IOException e) {
			this.collector.fail(tuple);
		}
	}

	@Override
	void closeOutputFile() throws IOException {

		this.out.close();
	}

	@Override
	Path createOutputFile() throws IOException {

		Path path = new Path(this.fileNameFormat.getPath(),
				this.fileNameFormat.getName(this.rotation, System.currentTimeMillis()));
		this.out = this.fs.create(path);

		return path;
	}
}
