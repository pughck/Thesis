package edu.rosehulman.pughck;

import java.io.IOException;
import java.util.EnumSet;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.storm.hdfs.bolt.format.RecordFormat;

import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class CustomHdfsBolt extends HdfsBolt {

	private transient FSDataOutputStream out;
	private RecordFormat format;
	private long offset = 0;

	@Override
	public void execute(Tuple tuple) {

		try {
			String comp = tuple.getStringByField("company");
			long time = System.currentTimeMillis();

			System.out.println(comp);

			Path path = new Path(this.fileNameFormat.getPath() + comp + "/", time + ".txt");

			System.out.println(path);

			byte[] bytes = this.format.format(tuple);

			// synchronized (this.writeLock) {

			this.out = this.fs.create(path);

			out.write(bytes);
			// this.offset += bytes.length;
			//
			// if (this.syncPolicy.mark(tuple, this.offset)) {
			// if (this.out instanceof HdfsDataOutputStream) {
			// ((HdfsDataOutputStream)
			// this.out).hsync(EnumSet.of(SyncFlag.UPDATE_LENGTH));
			// } else {
			// this.out.hsync();
			// }
			// this.syncPolicy.reset();
			// }
			// }

			this.collector.ack(tuple);

			// if (this.rotationPolicy.mark(tuple, this.offset)) {
			// rotateOutputFile(); // synchronized
			// this.offset = 0;
			// this.rotationPolicy.reset();
			// }
		} catch (IOException e) {
			this.collector.fail(tuple);
		}
	}
}
