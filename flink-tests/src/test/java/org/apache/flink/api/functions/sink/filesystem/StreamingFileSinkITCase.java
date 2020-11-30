/*
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

package org.apache.flink.api.functions.sink.filesystem;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.queue.QueueConsumer;
import org.apache.flink.streaming.queue.QueueManager;
import org.apache.flink.api.functions.sink.filesystem.source.Event;
import org.apache.flink.api.functions.sink.filesystem.source.SourceEvents;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/**
 * Test to remove forgotten parts after checkpoint.
 */
public class StreamingFileSinkITCase {

	private static final Logger LOG = LoggerFactory.getLogger(StreamingFileSinkITCase.class);

	private static final String STATE_BACKEND = "filesystem";
	private static final String FS_SCHEMA = "file:///";
	private static final long CHECKPOINT_INTERVAL = Long.MAX_VALUE;
	private static final long THREAD_SLEEP = 1500;
	private static final String PART_PREFIX = "part";
	private static final String PART_SUFFIX = "";

	@Rule
	public final TemporaryFolder tempDataDir = new TemporaryFolder();

	private String savepointsDir;
	private String checkpointsDir;
	private String bucketsDir;

	private ClusterClient<?> clusterClient;

	private QueueManager<Event> eventQueueManager;

	@Before
	public void setUp() throws Exception {
		LOG.info("Temporary data folder: " + this.tempDataDir.getRoot());
		this.savepointsDir = this.tempDataDir.newFolder("savepoints").toString();
		this.checkpointsDir = this.tempDataDir.newFolder("checkpoints").toString();
		this.bucketsDir = this.tempDataDir.newFolder("buckets").toString();

		final Configuration conf = clusterConfig(this.savepointsDir, this.checkpointsDir);
		this.clusterClient = createCluster(conf);

		this.eventQueueManager = new QueueManager<>();
	}

	@After
	public void clean() {
		this.eventQueueManager.close();
		this.clusterClient.shutDownCluster();
	}

	@Test
	public void testRemoveForgottenPartsAfterCheckpoint() throws Exception {
		final JobGraph job = createJob(1);
		final JobID jobID = job.getJobID();
		this.clusterClient.submitJob(job);
		waitJobStatus(jobID, JobStatus.RUNNING);
		LOG.info("Job id: " + jobID);

		final QueueConsumer<Event> eventQueueConsumer = this.eventQueueManager.getQueueConsumer();
		eventQueueConsumer.add(new Event(0, 0));
		eventQueueConsumer.add(new Event(0, 1));
		Thread.sleep(THREAD_SLEEP);
		LOG.info("Bucket directory: " + listBucket("0"));

		final String checkpointDir = this.clusterClient.triggerSavepoint(
			jobID, FS_SCHEMA + this.checkpointsDir
		).get();
		LOG.info("Checkpoint dir: " + checkpointDir);
		Thread.sleep(THREAD_SLEEP);
		LOG.info("Bucket directory: " + listBucket("0"));

		eventQueueConsumer.add(new Event(0, 2));
		eventQueueConsumer.add(new Event(0, 3));
		Thread.sleep(THREAD_SLEEP);
		LOG.info("Bucket directory: " + listBucket("0"));

		this.clusterClient.cancel(jobID).get();
		LOG.info("Bucket directory: " + listBucket("0"));

		final JobGraph newJob = createJob(1);
		final JobID newJobID = newJob.getJobID();
		newJob.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(checkpointDir));
		this.clusterClient.submitJob(newJob);
		waitJobStatus(newJobID, JobStatus.RUNNING);
		LOG.info("Job id: " + newJobID);
		LOG.info("Bucket directory: " + listBucket("0"));

		eventQueueConsumer.add(new Event(0, 4));
		eventQueueConsumer.add(new Event(0, 5));
		Thread.sleep(THREAD_SLEEP);
		LOG.info("Bucket directory: " + listBucket("0"));

		this.clusterClient.cancelWithSavepoint(newJobID, FS_SCHEMA + this.savepointsDir).get();
		waitJobStatus(newJobID, JobStatus.CANCELED);
		LOG.info("Bucket directory: " + listBucket("0"));

		final List<java.nio.file.Path> bucket0 = listBucket("0");
		assertEquals(2, bucket0.size());
		bucketEquals(
			"0",
			PART_PREFIX + "-0-0" + PART_SUFFIX,
			PART_PREFIX + "-0-1" + PART_SUFFIX
		);

		final String part0 = new String(Files.readAllBytes(bucket0.get(0)), StandardCharsets.UTF_8);
		assertEquals("0" + System.lineSeparator() + "1" + System.lineSeparator(), part0);

		final String part1 = new String(Files.readAllBytes(bucket0.get(1)), StandardCharsets.UTF_8);
		assertEquals("4" + System.lineSeparator() + "5" + System.lineSeparator(), part1);
	}

	@Test
	public void testRemoveForgottenPartsAfterCheckpointWithParallelism2Buckets2() throws Exception {
		final JobGraph job = createJob(2);
		final JobID jobID = job.getJobID();
		this.clusterClient.submitJob(job);
		waitJobStatus(jobID, JobStatus.RUNNING);
		LOG.info("Job id: " + jobID);

		final QueueConsumer<Event> eventQueueConsumer = this.eventQueueManager.getQueueConsumer();
		eventQueueConsumer.add(new Event(0, 0));
		eventQueueConsumer.add(new Event(0, 1));
		eventQueueConsumer.add(new Event(1, 2));
		eventQueueConsumer.add(new Event(1, 3));
		Thread.sleep(THREAD_SLEEP);
		LOG.info("Bucket directory: " + listBucket("0"));
		LOG.info("Bucket directory: " + listBucket("1"));

		final String checkpointDir = this.clusterClient.triggerSavepoint(
			jobID, FS_SCHEMA + this.checkpointsDir
		).get();
		LOG.info("Checkpoint dir: " + checkpointDir);
		Thread.sleep(THREAD_SLEEP);
		LOG.info("Bucket directory: " + listBucket("0"));
		LOG.info("Bucket directory: " + listBucket("1"));

		eventQueueConsumer.add(new Event(0, 4));
		eventQueueConsumer.add(new Event(0, 5));
		eventQueueConsumer.add(new Event(1, 6));
		eventQueueConsumer.add(new Event(1, 7));
		Thread.sleep(THREAD_SLEEP);
		LOG.info("Bucket directory: " + listBucket("0"));
		LOG.info("Bucket directory: " + listBucket("1"));

		this.clusterClient.cancel(jobID).get();
		LOG.info("Bucket directory: " + listBucket("0"));
		LOG.info("Bucket directory: " + listBucket("1"));

		final JobGraph newJob = createJob(2);
		final JobID newJobID = newJob.getJobID();
		newJob.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(checkpointDir));
		this.clusterClient.submitJob(newJob);
		waitJobStatus(newJobID, JobStatus.RUNNING);
		LOG.info("Job id: " + newJobID);
		LOG.info("Bucket directory: " + listBucket("0"));
		LOG.info("Bucket directory: " + listBucket("1"));

		eventQueueConsumer.add(new Event(0, 8));
		eventQueueConsumer.add(new Event(0, 9));
		eventQueueConsumer.add(new Event(1, 10));
		eventQueueConsumer.add(new Event(1, 11));
		Thread.sleep(THREAD_SLEEP);
		LOG.info("Bucket directory: " + listBucket("0"));
		LOG.info("Bucket directory: " + listBucket("1"));

		this.clusterClient.cancelWithSavepoint(newJobID, FS_SCHEMA + this.savepointsDir).get();
		waitJobStatus(newJobID, JobStatus.CANCELED);
		LOG.info("Bucket directory: " + listBucket("0"));
		LOG.info("Bucket directory: " + listBucket("1"));

		final List<java.nio.file.Path> bucket0 = listBucket("0");
		assertEquals(4, bucket0.size());
		bucketEquals(
			"0",
			PART_PREFIX + "-0-0" + PART_SUFFIX,
			PART_PREFIX + "-0-2" + PART_SUFFIX,
			PART_PREFIX + "-1-0" + PART_SUFFIX,
			PART_PREFIX + "-1-2" + PART_SUFFIX
		);

		final List<java.nio.file.Path> bucket1 = listBucket("1");
		assertEquals(4, bucket1.size());
		bucketEquals(
			"1",
			PART_PREFIX + "-0-1" + PART_SUFFIX,
			PART_PREFIX + "-0-3" + PART_SUFFIX,
			PART_PREFIX + "-1-1" + PART_SUFFIX,
			PART_PREFIX + "-1-3" + PART_SUFFIX
		);
	}

//	@Test
//	public void t() throws Exception {
//		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//		env.setParallelism(4);
//		env.enableCheckpointing(CHECKPOINT_INTERVAL);
//		env.getCheckpointConfig()
//			.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//
//		env
//			.addSource(
//				new SourceEvents(this.eventQueueManager.getQueueProducer())
//			)
//			.returns(Event.class)
//			.keyBy(event -> event.bucket)
//			.addSink(StreamingFileSink
//				.forRowFormat(
//					new Path(this.bucketsDir),
//					(Encoder<Event>) (element, outputStream) -> new PrintStream(outputStream).println(element.value)
//				)
//				.withBucketAssigner(new KeyBucketAssigner())
//				.withRollingPolicy(OnCheckpointRollingPolicy.build())
//				.withOutputFileConfig(new OutputFileConfig(PART_PREFIX, PART_SUFFIX))
//				.build()
//			);
//
//		final JobGraph job = env.getStreamGraph().getJobGraph();
//		final JobID jobID = job.getJobID();
//		job.setSavepointRestoreSettings(SavepointRestoreSettings.forPath("file:/tmp/junit4508536258946853532/checkpoints/savepoint-227434-6b01f0e70e18"));
//		this.clusterClient.submitJob(job);
//
//		Thread.sleep(1_000_000_000);
//
//	}

//	@Test
//	public void tt() throws Exception {
//		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//		env.setParallelism(2);
//		env.enableCheckpointing(CHECKPOINT_INTERVAL);
//		env.getCheckpointConfig()
//			.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//
//		env
//			.addSource(
//				new SourceEvents(this.eventQueueManager.getQueueProducer())
//			)
//			.returns(Event.class)
//			.keyBy(event -> event.bucket)
//			.addSink(StreamingFileSink
//				.forRowFormat(
//					new Path(this.bucketsDir),
//					(Encoder<Event>) (element, outputStream) -> new PrintStream(outputStream).println(element.value)
//				)
//				.withBucketAssigner(new KeyBucketAssigner())
//				.withRollingPolicy(OnCheckpointRollingPolicy.build())
//				.withOutputFileConfig(new OutputFileConfig(PART_PREFIX, PART_SUFFIX))
//				.build()
//			);
//
//		final JobGraph job = env.getStreamGraph().getJobGraph();
//		final JobID jobID = job.getJobID();
//		this.clusterClient.submitJob(job);
//		waitJobStatus(jobID, JobStatus.RUNNING);
//
//		final QueueConsumer<Event> eventQueueConsumer = this.eventQueueManager.getQueueConsumer();
//
//		eventQueueConsumer.add(new Event(0, 0));
//		eventQueueConsumer.add(new Event(228, 1));
//
//		Thread.sleep(THREAD_SLEEP);
//
//		final String checkpointDir = this.clusterClient.triggerSavepoint(
//			jobID, FS_SCHEMA + this.checkpointsDir
//		).get();
//		LOG.info("Checkpoint dir: " + checkpointDir);
////		Thread.sleep(THREAD_SLEEP);
//		LOG.info("Bucket directory: " + listBucket("0"));
//		LOG.info("Bucket directory: " + listBucket("228"));
//
//		eventQueueConsumer.add(new Event(0, 3));
//		eventQueueConsumer.add(new Event(228, 4));
//
//		Thread.sleep(THREAD_SLEEP);
//
//		eventQueueConsumer.add(new Event(0, 5));
//		eventQueueConsumer.add(new Event(228, 6));
//
//		Thread.sleep(1_000_000_000);
//
//
//		this.clusterClient.cancel(jobID).get();
//		LOG.info("Bucket directory: " + listBucket("0"));
//		LOG.info("Bucket directory: " + listBucket("228"));
//		Thread.sleep(THREAD_SLEEP);
//
//		env
//			.addSource(
//				new SourceEvents(this.eventQueueManager.getQueueProducer())
//			)
//			.returns(Event.class)
//			.keyBy(event -> event.bucket)
//			.addSink(StreamingFileSink
//				.forRowFormat(
//					new Path(this.bucketsDir),
//					(Encoder<Event>) (element, outputStream) -> new PrintStream(outputStream).println(element.value)
//				)
//				.withBucketAssigner(new KeyBucketAssigner())
//				.withRollingPolicy(OnCheckpointRollingPolicy.build())
//				.withOutputFileConfig(new OutputFileConfig(PART_PREFIX, PART_SUFFIX))
//				.build()
//			);
//
//		final JobGraph newJob = env.getStreamGraph().getJobGraph();
//		final JobID newJobID = newJob.getJobID();
//		newJob.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(checkpointDir));
//		this.clusterClient.submitJob(newJob);
//		waitJobStatus(newJobID, JobStatus.RUNNING);
//		LOG.info("Job id: " + newJobID);
//		LOG.info("Bucket directory: " + listBucket("0"));
//		LOG.info("Bucket directory: " + listBucket("228"));
//
//		Thread.sleep(1_000_000_000);
//	}

	private void waitJobStatus(final JobID jobID, final JobStatus jobStatus) throws Exception {
		if (true) {
			Thread.sleep(THREAD_SLEEP * 3);
			return;
		}
		while (!this.clusterClient.getJobStatus(jobID).get().equals(jobStatus)) {
			Thread.sleep(THREAD_SLEEP);
		}
	}

	private void bucketEquals(final String bucketId, final String... expectedParts) throws Exception {
		final List<String> expected = Arrays.asList(expectedParts);
		final List<String> actualBucket = listBucket(bucketId).stream()
			.map(p -> p.getFileName().toString())
			.collect(Collectors.toList());
		assertEquals(expected, actualBucket);
	}

	private List<java.nio.file.Path> listBucket(final String bucketId) throws IOException {
		return Files.list(Paths.get(this.bucketsDir, bucketId)).sorted().collect(Collectors.toList());
	}

	private JobGraph createJob(final int parallelism) {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);
		env.enableCheckpointing(CHECKPOINT_INTERVAL);
		env.getCheckpointConfig()
			.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

		final DataStream<Event> source = env.addSource(
			new SourceEvents(this.eventQueueManager.getQueueProducer())
		).returns(Event.class);

		final StreamingFileSink<Event> sink = StreamingFileSink
			.forRowFormat(
				new Path(this.bucketsDir),
				(Encoder<Event>) (element, outputStream) -> new PrintStream(outputStream).println(element.value)
			)
			.withBucketAssigner(new KeyBucketAssigner())
			.withRollingPolicy(OnCheckpointRollingPolicy
				.<Event, String>builder()
				.removeOutdatedParts(true)
				.build()
			)
			.withOutputFileConfig(new OutputFileConfig(PART_PREFIX, PART_SUFFIX))
			.build();

		source.addSink(sink);
		return env.getStreamGraph().getJobGraph();
	}

	private Configuration clusterConfig(final String savepointsDir, final String checkpointsDir) {
		final Configuration conf = new Configuration();
		conf.setString(CheckpointingOptions.STATE_BACKEND, STATE_BACKEND);
		conf.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, FS_SCHEMA + savepointsDir);
		conf.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, FS_SCHEMA + checkpointsDir);
		return conf;
	}

	private ClusterClient<?> createCluster(final Configuration conf) throws Exception {
		final MiniClusterResourceConfiguration miniClusterConf = new MiniClusterResourceConfiguration.Builder()
			.setConfiguration(conf)
			.setNumberTaskManagers(1)
			.setNumberSlotsPerTaskManager(8)
			.build();
		final MiniClusterWithClientResource miniCluster = new MiniClusterWithClientResource(miniClusterConf);
		miniCluster.before();
		return miniCluster.getClusterClient();
	}

	private static final class KeyBucketAssigner implements BucketAssigner<Event, String> {

		@Override
		public String getBucketId(final Event element, final Context context) {
			return String.valueOf(element.bucket);
		}

		@Override
		public SimpleVersionedSerializer<String> getSerializer() {
			return SimpleVersionedStringSerializer.INSTANCE;
		}
	}
}
