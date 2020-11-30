/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.TestUtils.MockListState;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

/**
 * Tests for {@link Buckets}.
 */
public class BucketsTest {

	@ClassRule
	public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

	@Test
	public void testSnapshotAndRestore() throws Exception {
		final File outDir = TEMP_FOLDER.newFolder();
		final Path path = new Path(outDir.toURI());

		final RollingPolicy<String, String> onCheckpointRollingPolicy = OnCheckpointRollingPolicy.build();

		final Buckets<String, String> buckets = createBuckets(path, onCheckpointRollingPolicy, 0);

		final ListState<byte[]> bucketStateContainer = new MockListState<>();
		final ListState<Long> partCounterContainer = new MockListState<>();
		final ListState<Integer> previousSubtaskIndexState = new MockListState<>();

		buckets.onElement("test1", new TestUtils.MockSinkContext(null, 1L, 2L));
		buckets.snapshotState(0L, bucketStateContainer, partCounterContainer, previousSubtaskIndexState);

		assertThat(buckets.getActiveBuckets().get("test1"), hasSinglePartFileToBeCommittedOnCheckpointAck(path, "test1"));

		buckets.onElement("test2", new TestUtils.MockSinkContext(null, 1L, 2L));
		buckets.snapshotState(1L, bucketStateContainer, partCounterContainer, previousSubtaskIndexState);

		assertThat(buckets.getActiveBuckets().get("test1"), hasSinglePartFileToBeCommittedOnCheckpointAck(path, "test1"));
		assertThat(buckets.getActiveBuckets().get("test2"), hasSinglePartFileToBeCommittedOnCheckpointAck(path, "test2"));

		Buckets<String, String> restoredBuckets =
				restoreBuckets(path, onCheckpointRollingPolicy, 0, bucketStateContainer, partCounterContainer, previousSubtaskIndexState);

		final Map<String, Bucket<String, String>> activeBuckets = restoredBuckets.getActiveBuckets();

		// because we commit pending files for previous checkpoints upon recovery
		Assert.assertTrue(activeBuckets.isEmpty());
	}

	private static TypeSafeMatcher<Bucket<String, String>> hasSinglePartFileToBeCommittedOnCheckpointAck(final Path testTmpPath, final String bucketId) {
		return new TypeSafeMatcher<Bucket<String, String>>() {
			@Override
			protected boolean matchesSafely(Bucket<String, String> bucket) {
				return bucket.getBucketId().equals(bucketId) &&
						bucket.getBucketPath().equals(new Path(testTmpPath, bucketId)) &&
						bucket.getInProgressPart() == null &&
						bucket.getPendingFileRecoverablesForCurrentCheckpoint().isEmpty() &&
						bucket.getPendingFileRecoverablesPerCheckpoint().size() == 1;
			}

			@Override
			public void describeTo(Description description) {
				description.appendText("a Bucket with a single pending part file @ ")
						.appendValue(new Path(testTmpPath, bucketId))
						.appendText("'");
			}
		};
	}

	@Test
	public void testMergeAtScaleInAndMaxCounterAtRecovery() throws Exception {
		final File outDir = TEMP_FOLDER.newFolder();
		final Path path = new Path(outDir.toURI());

		final RollingPolicy<String, String> onCheckpointRP =
				DefaultRollingPolicy
						.builder()
						.withMaxPartSize(7L) // roll with 2 elements
						.build();

		final MockListState<byte[]> bucketStateContainerOne = new MockListState<>();
		final MockListState<byte[]> bucketStateContainerTwo = new MockListState<>();

		final MockListState<Long> partCounterContainerOne = new MockListState<>();
		final MockListState<Long> partCounterContainerTwo = new MockListState<>();

		final MockListState<Integer> previousSubtaskIndexStateOne = new MockListState<>();
		final MockListState<Integer> previousSubtaskIndexStateTwo = new MockListState<>();

		final Buckets<String, String> bucketsOne = createBuckets(path, onCheckpointRP, 0);
		final Buckets<String, String> bucketsTwo = createBuckets(path, onCheckpointRP, 1);

		bucketsOne.onElement("test1", new TestUtils.MockSinkContext(null, 1L, 2L));
		bucketsOne.snapshotState(0L, bucketStateContainerOne, partCounterContainerOne, previousSubtaskIndexStateOne);

		Assert.assertEquals(1L, bucketsOne.getMaxPartCounter());

		// make sure we have one in-progress file here
		Assert.assertNotNull(bucketsOne.getActiveBuckets().get("test1").getInProgressPart());

		// add a couple of in-progress files so that the part counter increases.
		bucketsTwo.onElement("test1", new TestUtils.MockSinkContext(null, 1L, 2L));
		bucketsTwo.onElement("test1", new TestUtils.MockSinkContext(null, 1L, 2L));

		bucketsTwo.onElement("test1", new TestUtils.MockSinkContext(null, 1L, 2L));

		bucketsTwo.snapshotState(0L, bucketStateContainerTwo, partCounterContainerTwo, previousSubtaskIndexStateTwo);

		Assert.assertEquals(2L, bucketsTwo.getMaxPartCounter());

		// make sure we have one in-progress file here and a pending
		Assert.assertEquals(1L, bucketsTwo.getActiveBuckets().get("test1").getPendingFileRecoverablesPerCheckpoint().size());
		Assert.assertNotNull(bucketsTwo.getActiveBuckets().get("test1").getInProgressPart());

		final ListState<byte[]> mergedBucketStateContainer = new MockListState<>();
		final ListState<Long> mergedPartCounterContainer = new MockListState<>();
		final ListState<Integer> mergedPreviousSubtaskIndex = new MockListState<>();

		mergedBucketStateContainer.addAll(bucketStateContainerOne.getBackingList());
		mergedBucketStateContainer.addAll(bucketStateContainerTwo.getBackingList());

		mergedPartCounterContainer.addAll(partCounterContainerOne.getBackingList());
		mergedPartCounterContainer.addAll(partCounterContainerTwo.getBackingList());

		mergedPreviousSubtaskIndex.addAll(previousSubtaskIndexStateOne.getBackingList());
		mergedPreviousSubtaskIndex.addAll(previousSubtaskIndexStateTwo.getBackingList());

		final Buckets<String, String> restoredBuckets =
				restoreBuckets(path, onCheckpointRP, 0, mergedBucketStateContainer, mergedPartCounterContainer, mergedPreviousSubtaskIndex);

		// we get the maximum of the previous tasks
		Assert.assertEquals(2L, restoredBuckets.getMaxPartCounter());

		final Map<String, Bucket<String, String>> activeBuckets = restoredBuckets.getActiveBuckets();
		Assert.assertEquals(1L, activeBuckets.size());
		Assert.assertTrue(activeBuckets.keySet().contains("test1"));

		final Bucket<String, String> bucket = activeBuckets.get("test1");
		Assert.assertEquals("test1", bucket.getBucketId());
		Assert.assertEquals(new Path(path, "test1"), bucket.getBucketPath());

		Assert.assertNotNull(bucket.getInProgressPart()); // the restored part file

		// this is due to the Bucket#merge(). The in progress file of one
		// of the previous tasks is put in the list of pending files.
		Assert.assertEquals(1L, bucket.getPendingFileRecoverablesForCurrentCheckpoint().size());

		// we commit the pending for previous checkpoints
		Assert.assertTrue(bucket.getPendingFileRecoverablesPerCheckpoint().isEmpty());
	}

	@Test
	public void testOnProcessingTime() throws Exception {
		final File outDir = TEMP_FOLDER.newFolder();
		final Path path = new Path(outDir.toURI());

		final OnProcessingTimePolicy<String, String> rollOnProcessingTimeCountingPolicy =
				new OnProcessingTimePolicy<>(2L);

		final Buckets<String, String> buckets =
				createBuckets(path, rollOnProcessingTimeCountingPolicy, 0);

		// it takes the current processing time of the context for the creation time,
		// and for the last modification time.
		buckets.onElement("test", new TestUtils.MockSinkContext(1L, 2L, 3L));

		// now it should roll
		buckets.onProcessingTime(7L);
		Assert.assertEquals(1L, rollOnProcessingTimeCountingPolicy.getOnProcessingTimeRollCounter());

		final Map<String, Bucket<String, String>> activeBuckets = buckets.getActiveBuckets();
		Assert.assertEquals(1L, activeBuckets.size());
		Assert.assertTrue(activeBuckets.keySet().contains("test"));

		final Bucket<String, String> bucket = activeBuckets.get("test");
		Assert.assertEquals("test", bucket.getBucketId());
		Assert.assertEquals(new Path(path, "test"), bucket.getBucketPath());
		Assert.assertEquals("test", bucket.getBucketId());

		Assert.assertNull(bucket.getInProgressPart());
		Assert.assertEquals(1L, bucket.getPendingFileRecoverablesForCurrentCheckpoint().size());
		Assert.assertTrue(bucket.getPendingFileRecoverablesPerCheckpoint().isEmpty());
	}

	@Test
	public void testBucketIsRemovedWhenNotActive() throws Exception {
		final File outDir = TEMP_FOLDER.newFolder();
		final Path path = new Path(outDir.toURI());

		final OnProcessingTimePolicy<String, String> rollOnProcessingTimeCountingPolicy =
			new OnProcessingTimePolicy<>(2L);

		final Buckets<String, String> buckets =
				createBuckets(path, rollOnProcessingTimeCountingPolicy, 0);

		// it takes the current processing time of the context for the creation time, and for the last modification time.
		buckets.onElement("test", new TestUtils.MockSinkContext(1L, 2L, 3L));

		// now it should roll
		buckets.onProcessingTime(7L);
		Assert.assertEquals(1L, rollOnProcessingTimeCountingPolicy.getOnProcessingTimeRollCounter());

		buckets.snapshotState(0L, new MockListState<>(), new MockListState<>(), new MockListState<>());
		buckets.commitUpToCheckpoint(0L);

		Assert.assertTrue(buckets.getActiveBuckets().isEmpty());
	}

	@Test
	public void testPartCounterAfterBucketResurrection() throws Exception {
		final File outDir = TEMP_FOLDER.newFolder();
		final Path path = new Path(outDir.toURI());

		final OnProcessingTimePolicy<String, String> rollOnProcessingTimeCountingPolicy =
				new OnProcessingTimePolicy<>(2L);

		final Buckets<String, String> buckets =
				createBuckets(path, rollOnProcessingTimeCountingPolicy, 0);

		// it takes the current processing time of the context for the creation time, and for the last modification time.
		buckets.onElement("test", new TestUtils.MockSinkContext(1L, 2L, 3L));
		Assert.assertEquals(1L, buckets.getActiveBuckets().get("test").getPartCounter());

		// now it should roll
		buckets.onProcessingTime(7L);
		Assert.assertEquals(1L, rollOnProcessingTimeCountingPolicy.getOnProcessingTimeRollCounter());
		Assert.assertEquals(1L, buckets.getActiveBuckets().get("test").getPartCounter());

		buckets.snapshotState(0L, new MockListState<>(), new MockListState<>(), new MockListState<>());
		buckets.commitUpToCheckpoint(0L);

		Assert.assertTrue(buckets.getActiveBuckets().isEmpty());

		buckets.onElement("test", new TestUtils.MockSinkContext(2L, 3L, 4L));
		Assert.assertEquals(2L, buckets.getActiveBuckets().get("test").getPartCounter());
	}

	private static class OnProcessingTimePolicy<IN, BucketID> implements RollingPolicy<IN, BucketID> {

		private static final long serialVersionUID = 1L;

		private int onProcessingTimeRollCounter = 0;

		private final long rolloverInterval;

		OnProcessingTimePolicy(final long rolloverInterval) {
			this.rolloverInterval = rolloverInterval;
		}

		public int getOnProcessingTimeRollCounter() {
			return onProcessingTimeRollCounter;
		}

		@Override
		public boolean shouldRollOnCheckpoint(PartFileInfo<BucketID> partFileState) {
			return false;
		}

		@Override
		public boolean shouldRollOnEvent(PartFileInfo<BucketID> partFileState, IN element) {
			return false;
		}

		@Override
		public boolean shouldRollOnProcessingTime(PartFileInfo<BucketID> partFileState, long currentTime) {
			boolean result = currentTime - partFileState.getCreationTime() >= rolloverInterval;
			if (result) {
				onProcessingTimeRollCounter++;
			}
			return result;
		}
	}

	@Test
	public void testContextPassingNormalExecution() throws Exception {
		testCorrectTimestampPassingInContext(1L, 2L, 3L);
	}

	@Test
	public void testContextPassingNullTimestamp() throws Exception {
		testCorrectTimestampPassingInContext(null, 2L, 3L);
	}

	private void testCorrectTimestampPassingInContext(Long timestamp, long watermark, long processingTime) throws Exception {
		final File outDir = TEMP_FOLDER.newFolder();
		final Path path = new Path(outDir.toURI());

		final Buckets<String, String> buckets = new Buckets<>(
				path,
				new VerifyingBucketAssigner(timestamp, watermark, processingTime),
				new DefaultBucketFactoryImpl<>(),
				new RowWiseBucketWriter<>(FileSystem.get(path.toUri()).createRecoverableWriter(), new SimpleStringEncoder<>()),
				DefaultRollingPolicy.builder().build(),
				2,
				OutputFileConfig.builder().build()
		);

		buckets.onElement(
				"test",
				new TestUtils.MockSinkContext(
						timestamp,
						watermark,
						processingTime)
		);
	}

	private static class VerifyingBucketAssigner implements BucketAssigner<String, String> {

		private static final long serialVersionUID = 7729086510972377578L;

		private final Long expectedTimestamp;
		private final long expectedWatermark;
		private final long expectedProcessingTime;

		VerifyingBucketAssigner(
				final Long expectedTimestamp,
				final long expectedWatermark,
				final long expectedProcessingTime
		) {
			this.expectedTimestamp = expectedTimestamp;
			this.expectedWatermark = expectedWatermark;
			this.expectedProcessingTime = expectedProcessingTime;
		}

		@Override
		public String getBucketId(String element, BucketAssigner.Context context) {
			final Long elementTimestamp = context.timestamp();
			final long watermark = context.currentWatermark();
			final long processingTime = context.currentProcessingTime();

			Assert.assertEquals(expectedTimestamp, elementTimestamp);
			Assert.assertEquals(expectedProcessingTime, processingTime);
			Assert.assertEquals(expectedWatermark, watermark);

			return element;
		}

		@Override
		public SimpleVersionedSerializer<String> getSerializer() {
			return SimpleVersionedStringSerializer.INSTANCE;
		}
	}

	@Test
	public void testBucketLifeCycleListenerOnCreatingAndInactive() throws Exception {
		File outDir = TEMP_FOLDER.newFolder();
		Path path = new Path(outDir.toURI());
		OnProcessingTimePolicy<String, String> rollOnProcessingTimeCountingPolicy =
			new OnProcessingTimePolicy<>(2L);
		RecordBucketLifeCycleListener bucketLifeCycleListener = new RecordBucketLifeCycleListener();
		Buckets<String, String> buckets = createBuckets(
			path,
			rollOnProcessingTimeCountingPolicy,
			bucketLifeCycleListener,
			null,
			0,
			OutputFileConfig.builder().build());
		ListState<byte[]> bucketStateContainer = new MockListState<>();
		ListState<Long> partCounterContainer = new MockListState<>();
		ListState<Integer> previousSubtaskIndex = new MockListState<>();

		buckets.onElement("test1", new TestUtils.MockSinkContext(null, 1L, 2L));
		buckets.onElement("test2", new TestUtils.MockSinkContext(null, 1L, 3L));

		// Will close the part file writer of the bucket "test1".
		buckets.onProcessingTime(4);
		buckets.snapshotState(0, bucketStateContainer, partCounterContainer, previousSubtaskIndex);
		buckets.commitUpToCheckpoint(0);

		// Will close the part file writer of the bucket "test2".
		buckets.onProcessingTime(6);
		buckets.snapshotState(1, bucketStateContainer, partCounterContainer, previousSubtaskIndex);
		buckets.commitUpToCheckpoint(1);

		List<Tuple2<RecordBucketLifeCycleListener.EventType, String>> expectedEvents = Arrays.asList(
			new Tuple2<>(RecordBucketLifeCycleListener.EventType.CREATED, "test1"),
			new Tuple2<>(RecordBucketLifeCycleListener.EventType.CREATED, "test2"),
			new Tuple2<>(RecordBucketLifeCycleListener.EventType.INACTIVE, "test1"),
			new Tuple2<>(RecordBucketLifeCycleListener.EventType.INACTIVE, "test2"));
		Assert.assertEquals(expectedEvents, bucketLifeCycleListener.getEvents());
	}

	@Test
	public void testBucketLifeCycleListenerOnRestoring() throws Exception {
		File outDir = TEMP_FOLDER.newFolder();
		Path path = new Path(outDir.toURI());
		OnProcessingTimePolicy<String, String> rollOnProcessingTimeCountingPolicy =
			new OnProcessingTimePolicy<>(2L);
		RecordBucketLifeCycleListener bucketLifeCycleListener = new RecordBucketLifeCycleListener();
		Buckets<String, String> buckets = createBuckets(
			path,
			rollOnProcessingTimeCountingPolicy,
			bucketLifeCycleListener,
			null,
			0,
			OutputFileConfig.builder().build());
		ListState<byte[]> bucketStateContainer = new MockListState<>();
		ListState<Long> partCounterContainer = new MockListState<>();
		ListState<Integer> previousSubtaskIndex = new MockListState<>();

		buckets.onElement("test1", new TestUtils.MockSinkContext(null, 1L, 2L));
		buckets.onElement("test2", new TestUtils.MockSinkContext(null, 1L, 3L));

		// Will close the part file writer of the bucket "test1". Now bucket "test1" have only
		// one pending file while bucket "test2" has an on-writing in-progress file.
		buckets.onProcessingTime(4);
		buckets.snapshotState(0, bucketStateContainer, partCounterContainer, previousSubtaskIndex);

		// On restoring the bucket "test1" will commit its pending file and become inactive.
		buckets = restoreBuckets(
			path,
			rollOnProcessingTimeCountingPolicy,
			bucketLifeCycleListener,
			null,
			0,
			bucketStateContainer,
			partCounterContainer,
			previousSubtaskIndex,
			OutputFileConfig.builder().build());

		Assert.assertEquals(new HashSet<>(Collections.singletonList("test2")), buckets.getActiveBuckets().keySet());
		List<Tuple2<RecordBucketLifeCycleListener.EventType, String>> expectedEvents = Arrays.asList(
			new Tuple2<>(RecordBucketLifeCycleListener.EventType.CREATED, "test1"),
			new Tuple2<>(RecordBucketLifeCycleListener.EventType.CREATED, "test2"),
			new Tuple2<>(RecordBucketLifeCycleListener.EventType.INACTIVE, "test1"));
		Assert.assertEquals(expectedEvents, bucketLifeCycleListener.getEvents());
	}

	private static class RecordBucketLifeCycleListener implements BucketLifeCycleListener<String, String> {
		public enum EventType {
			CREATED,
			INACTIVE
		}

		private List<Tuple2<EventType, String>> events = new ArrayList<>();

		@Override
		public void bucketCreated(Bucket<String, String> bucket) {
			events.add(new Tuple2<>(EventType.CREATED, bucket.getBucketId()));
		}

		@Override
		public void bucketInactive(Bucket<String, String> bucket) {
			events.add(new Tuple2<>(EventType.INACTIVE, bucket.getBucketId()));
		}

		public List<Tuple2<EventType, String>> getEvents() {
			return events;
		}
	}

	@Test
	public void testFileLifeCycleListener() throws Exception {
		File outDir = TEMP_FOLDER.newFolder();
		Path path = new Path(outDir.toURI());

		OnProcessingTimePolicy<String, String> rollOnProcessingTimeCountingPolicy =
				new OnProcessingTimePolicy<>(2L);

		TestFileLifeCycleListener fileLifeCycleListener = new TestFileLifeCycleListener();
		Buckets<String, String> buckets = createBuckets(
				path,
				rollOnProcessingTimeCountingPolicy,
				null,
				fileLifeCycleListener,
				0,
				OutputFileConfig.builder().build());

		buckets.onElement("test1", new TestUtils.MockSinkContext(null, 1L, 2L));
		buckets.onElement("test2", new TestUtils.MockSinkContext(null, 1L, 3L));

		// Will close the part file writer of the bucket "test1". Now bucket "test1" have only
		// one pending file while bucket "test2" has an on-writing in-progress file.
		buckets.onProcessingTime(4);

		buckets.onElement("test1", new TestUtils.MockSinkContext(null, 1L, 5L));
		buckets.onElement("test2", new TestUtils.MockSinkContext(null, 1L, 6L));

		Assert.assertEquals(2, fileLifeCycleListener.files.size());
		Assert.assertEquals(
				Arrays.asList("part-0-0", "part-0-1"), fileLifeCycleListener.files.get("test1"));
		Assert.assertEquals(
				Collections.singletonList("part-0-1"), fileLifeCycleListener.files.get("test2"));
	}

	private static class TestFileLifeCycleListener implements FileLifeCycleListener<String> {

		private final Map<String, List<String>> files = new HashMap<>();

		@Override
		public void onPartFileOpened(String bucket, Path newPath) {
			files.computeIfAbsent(bucket, k -> new ArrayList<>()).add(newPath.getName());
		}
	}

	@Test
	public void testRemoveOutdatedPartOnRestoring() throws Exception {
		final File outDir = TEMP_FOLDER.newFolder();
		final Path path = new Path(outDir.toURI());
		final java.nio.file.Path bucketPath = Paths.get(path.getPath(), "test1");
		final RollingPolicy<String, String> policy = OnCheckpointRollingPolicy
			.<String, String>builder()
			.removeOutdatedParts(true)
			.build();
		final Buckets<String, String> buckets = createBuckets(path, policy, 0);
		final ListState<byte[]> bucketStateContainer = new MockListState<>();
		final ListState<Long> partCounterContainer = new MockListState<>();
		final ListState<Integer> previousSubtaskIndexState = new MockListState<>();

		buckets.onElement("test1", new TestUtils.MockSinkContext(null, 1L, 2L));
		buckets.snapshotState(0L, bucketStateContainer, partCounterContainer, previousSubtaskIndexState);
		buckets.onElement("test1", new TestUtils.MockSinkContext(null, 1L, 2L));
		assertThat(listBucket(bucketPath), hasSize(2));

		restoreBuckets(
			path,
			policy,
			0,
			bucketStateContainer,
			partCounterContainer,
			previousSubtaskIndexState
		);
		assertThat(listBucket(bucketPath), hasSize(1));
	}

	// ------------------------------- Utility Methods --------------------------------

	private static Buckets<String, String> createBuckets(
			final Path basePath,
			final RollingPolicy<String, String> rollingPolicy,
			final int subtaskIdx) throws IOException {
		return createBuckets(
				basePath,
				rollingPolicy,
				null,
				null,
				subtaskIdx,
				OutputFileConfig.builder().build());
	}

	private static Buckets<String, String> createBuckets(
			final Path basePath,
			final RollingPolicy<String, String> rollingPolicy,
			final BucketLifeCycleListener<String, String> bucketLifeCycleListener,
			final FileLifeCycleListener<String> fileLifeCycleListener,
			final int subtaskIdx,
			final OutputFileConfig outputFileConfig) throws IOException {
		Buckets<String, String> buckets = new Buckets<>(
				basePath,
				new TestUtils.StringIdentityBucketAssigner(),
				new DefaultBucketFactoryImpl<>(),
				new RowWiseBucketWriter<>(FileSystem.get(basePath.toUri()).createRecoverableWriter(), new SimpleStringEncoder<>()),
				rollingPolicy,
				subtaskIdx,
				outputFileConfig);

		if (bucketLifeCycleListener != null) {
			buckets.setBucketLifeCycleListener(bucketLifeCycleListener);
		}

		if (fileLifeCycleListener != null) {
			buckets.setFileLifeCycleListener(fileLifeCycleListener);
		}

		return buckets;
	}

	private static Buckets<String, String> restoreBuckets(
			final Path basePath,
			final RollingPolicy<String, String> rollingPolicy,
			final int subtaskIdx,
			final ListState<byte[]> bucketState,
			final ListState<Long> partCounterState,
			final ListState<Integer> previousSubtaskIndexState) throws Exception {
		return restoreBuckets(
				basePath,
				rollingPolicy,
				null,
				null,
				subtaskIdx,
				bucketState,
				partCounterState,
				previousSubtaskIndexState,
				OutputFileConfig.builder().build());
	}

	private static Buckets<String, String> restoreBuckets(
			final Path basePath,
			final RollingPolicy<String, String> rollingPolicy,
			final BucketLifeCycleListener<String, String> bucketLifeCycleListener,
			final FileLifeCycleListener<String> fileLifeCycleListener,
			final int subtaskIdx,
			final ListState<byte[]> bucketState,
			final ListState<Long> partCounterState,
			final ListState<Integer> previousSubtaskIndexState,
			final OutputFileConfig outputFileConfig) throws Exception {
		final Buckets<String, String> restoredBuckets = createBuckets(
			basePath,
			rollingPolicy,
			bucketLifeCycleListener,
			fileLifeCycleListener,
			subtaskIdx,
			outputFileConfig);
		restoredBuckets.initializeState(bucketState, partCounterState, previousSubtaskIndexState);
		return restoredBuckets;
	}

	private List<java.nio.file.Path> listBucket(final java.nio.file.Path bucketFolder) throws IOException {
		return Files.list(bucketFolder).collect(Collectors.toList());
	}
}
