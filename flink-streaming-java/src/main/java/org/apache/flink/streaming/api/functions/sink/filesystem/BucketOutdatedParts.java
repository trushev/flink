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

package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An entity responsible for removing outdated parts from bucket.
 * Strongly related to part filename format:
 * .part-SUBTASK_INDEX-PART_COUNTER.inprogress.UUID
 */
@Internal
public class BucketOutdatedParts implements CleanableBucket {

	private static final Logger LOG = LoggerFactory.getLogger(BucketOutdatedParts.class);

	private final long maxPartCounter;
	private final Collection<Integer> previousSubtaskIndexes;
	private final Path bucketPath;
	private final Pattern partFilenameRegexp;

	public BucketOutdatedParts(
		final long maxPartCounter,
		final Collection<Integer> previousSubtaskIndexes,
		final Path bucketPath,
		final OutputFileConfig outputFileConfig
	) {
		this.maxPartCounter = maxPartCounter;
		this.previousSubtaskIndexes = Objects.requireNonNull(previousSubtaskIndexes);
		this.bucketPath = Objects.requireNonNull(bucketPath);
		this.partFilenameRegexp = Pattern.compile(
			"\\." +
			outputFileConfig.getPartPrefix() +
			"-(\\d+)-(\\d+)" +
			outputFileConfig.getPartSuffix() +
			"\\.inprogress\\.[\\w-]+"
		);
	}

	/**
	 * Removes part files from bucket.
	 * Removing condition:
	 * 1) part file is inporgress
	 * 2) part counter from part filename is greater than or equal to maxPartCounter
	 * 3) previousSubtaskIndexes contains subtask index from part filename
	 * @throws IOException Thrown, if any I/O related problem occurred
	 */
	@Override
	public void clean() throws IOException {
		final FileSystem fileSystem = this.bucketPath.getFileSystem();
		Arrays.stream(fileSystem.listStatus(this.bucketPath))
			.filter(file -> !file.isDir())
			.map(file -> new PartFile(file.getPath(), this.partFilenameRegexp))
			.filter(part ->
				part.isInprogress() &&
				part.getPartCounter() >= this.maxPartCounter &&
				this.previousSubtaskIndexes.contains(part.getSubtaskIndex())
			)
			.forEach(part -> {
				try {
					fileSystem.delete(part.getPath(), false);
					LOG.info("Removed part file: " + part.getPath());
				} catch (final IOException e) {
					LOG.warn("Can not remove part file", e);
				}
			});
	}

	private static final class PartFile {
		private final Path path;
		private final Pattern partFilenameRegexp;

		private int subtaskIndex = -1;
		private long partCounter = -1;
		private boolean matched = false;

		private PartFile(final Path path, final Pattern partFilenameRegexp) {
			this.path = Objects.requireNonNull(path);
			this.partFilenameRegexp = Objects.requireNonNull(partFilenameRegexp);
		}

		public boolean isInprogress() {
			final String partName = this.path.getName();
			if (!partName.startsWith(".") || !partName.contains("inprogress")) {
				return false;
			}
			final Matcher matcher = this.partFilenameRegexp.matcher(partName);
			if (!matcher.find()) {
				return false;
			}
			this.subtaskIndex = Integer.parseInt(matcher.group(1));
			this.partCounter = Long.parseLong(matcher.group(2));
			this.matched = true;
			return true;
		}

		public Path getPath() {
			return this.path;
		}

		public int getSubtaskIndex() {
			if (!this.matched) {
				throw new IllegalStateException("isInprogress method should be called first");
			}
			return this.subtaskIndex;
		}

		public long getPartCounter() {
			if (!this.matched) {
				throw new IllegalStateException("isInprogress method should be called first");
			}
			return this.partCounter;
		}
	}
}
