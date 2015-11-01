package edu.rosehulman.pughck;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.utils.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.common.rotation.RotationAction;
import org.apache.storm.hdfs.common.security.HdfsSecurityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

@SuppressWarnings("serial")
public abstract class AbstractHdfsBolt extends BaseRichBolt {

	private static final Logger LOG = LoggerFactory.getLogger(AbstractHdfsBolt.class);
	private static final Integer DEFAULT_RETRY_COUNT = 3;

	protected ArrayList<RotationAction> rotationActions = new ArrayList<RotationAction>();
	private Path currentFile;
	protected OutputCollector collector;
	protected transient FileSystem fs;
	protected SyncPolicy syncPolicy;
	protected FileRotationPolicy rotationPolicy;
	protected ClusterFileNameFormat fileNameFormat;
	protected int rotation = 0;
	protected String fsUrl;
	protected String configKey;
	protected transient Object writeLock;
	protected transient Timer rotationTimer; // only used for
												// TimedRotationPolicy
	protected long offset = 0;
	protected Integer fileRetryCount = DEFAULT_RETRY_COUNT;
	protected Integer tickTupleInterval = 0;

	protected transient Configuration hdfsConfig;

	protected void rotateOutputFile() throws IOException {

		LOG.info("Rotating output file...");
		long start = System.currentTimeMillis();
		synchronized (this.writeLock) {
			closeOutputFile();
			this.rotation++;

			Path newFile = createOutputFile();
			LOG.info("Performing {} file rotation actions.", this.rotationActions.size());
			for (RotationAction action : this.rotationActions) {
				action.execute(this.fs, this.currentFile);
			}
			this.currentFile = newFile;
		}
		long time = System.currentTimeMillis() - start;
		LOG.info("File rotation took {} ms.", time);
	}

	/**
	 * Marked as final to prevent override. Subclasses should implement the
	 * doPrepare() method.
	 * 
	 * @param conf
	 * @param topologyContext
	 * @param collector
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public final void prepare(Map conf, TopologyContext topologyContext, OutputCollector collector) {

		this.writeLock = new Object();

		if (this.syncPolicy == null)
			throw new IllegalStateException("SyncPolicy must be specified.");

		if (this.rotationPolicy == null)
			throw new IllegalStateException("RotationPolicy must be specified.");

		if (this.fsUrl == null) {
			throw new IllegalStateException("File system URL must be specified.");
		}

		this.collector = collector;
		this.fileNameFormat.prepare(conf, topologyContext);
		this.hdfsConfig = new Configuration();

		Map<String, Object> map = (Map<String, Object>) conf.get(this.configKey);
		if (map != null) {
			for (String key : map.keySet()) {
				this.hdfsConfig.set(key, String.valueOf(map.get(key)));
			}
		}

		// If interval is non-zero then it has already been explicitly set and
		// we should not default it
		if (conf.containsKey("topology.message.timeout.secs") && tickTupleInterval == 0) {
			Integer topologyTimeout = Utils.getInt(conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS));
			tickTupleInterval = (int) (Math.floor(topologyTimeout / 2));
			LOG.debug("Setting tick tuple interval to [{}] based on topology timeout", tickTupleInterval);
		}

		try {
			HdfsSecurityUtil.login(conf, hdfsConfig);
			doPrepare(conf, topologyContext, collector);
			this.currentFile = createOutputFile();
		} catch (Exception e) {
			throw new RuntimeException("Error preparing HdfsBolt: " + e.getMessage(), e);
		}

		if (this.rotationPolicy instanceof TimedRotationPolicy) {
			long interval = ((TimedRotationPolicy) this.rotationPolicy).getInterval();
			this.rotationTimer = new Timer(true);
			TimerTask task = new TimerTask() {

				@Override
				public void run() {
					try {
						rotateOutputFile();
					} catch (IOException e) {
						LOG.warn("IOException during scheduled file rotation.", e);
					}
				}
			};

			this.rotationTimer.scheduleAtFixedRate(task, interval, interval);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		// nothing needed here
	}

	abstract void closeOutputFile() throws IOException;

	abstract Path createOutputFile() throws IOException;

	@SuppressWarnings("rawtypes")
	abstract void doPrepare(Map conf, TopologyContext context, OutputCollector collector) throws IOException;
}