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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link BucketOutdatedParts}.
 */
public class BucketOutdatedPartsTest {

	@Rule
	public final TemporaryFolder bucketsFolder = new TemporaryFolder();

	@Test
	public void clean() throws IOException {
		final Path bucketFolder = this.bucketsFolder.newFolder("bucket").toPath();
		final OutputFileConfig fileConf = new OutputFileConfig("prefix", "123suffix");
		final CleanableBucket cleanableBucket = new BucketOutdatedParts(
			0,
			Stream.of(0).collect(Collectors.toSet()),
			new org.apache.flink.core.fs.Path(bucketFolder.toString()),
			fileConf
		);

		Files.createFile(Paths.get(
			bucketFolder.toString(),
			"." + fileConf.getPartPrefix() + "-0-0" + fileConf.getPartSuffix() + ".inprogress." + UUID.randomUUID()
		));
		assertEquals(
			"There should be one file: " + listBucket(bucketFolder),
			1L,
			listBucket(bucketFolder).size()
		);

		cleanableBucket.clean();
		assertTrue(
			"Bucket should be empty: " + listBucket(bucketFolder),
			listBucket(bucketFolder).isEmpty()
		);
	}

	private List<Path> listBucket(final Path bucketFolder) throws IOException {
		return Files.list(bucketFolder).collect(Collectors.toList());
	}
}
