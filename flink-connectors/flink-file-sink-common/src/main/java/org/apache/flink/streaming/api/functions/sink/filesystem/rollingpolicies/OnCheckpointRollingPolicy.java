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

package org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;

/**
 * A {@link RollingPolicy} which rolls (ONLY) on every checkpoint.
 */
@PublicEvolving
public final class OnCheckpointRollingPolicy<IN, BucketID> extends CheckpointRollingPolicy<IN, BucketID> {

	private static final long serialVersionUID = 1L;

	private static final boolean DEFAULT_REMOVE_OUTDATED_PARTS = false;

	private final boolean removeOutdatedParts;

	private OnCheckpointRollingPolicy(final PolicyBuilder<IN, BucketID> builder) {
		this.removeOutdatedParts = builder.removeOutdatedParts;
	}

	@Override
	public boolean shouldRollOnEvent(PartFileInfo<BucketID> partFileState, IN element) {
		return false;
	}

	@Override
	public boolean shouldRollOnProcessingTime(PartFileInfo<BucketID> partFileState, long currentTime) {
		return false;
	}

	@Override
	public boolean shouldRemoveOutdatedParts() {
		return this.removeOutdatedParts;
	}

	public static <IN, BucketID> PolicyBuilder<IN, BucketID> builder() {
		return new PolicyBuilder<>();
	}

	public static <IN, BucketID> OnCheckpointRollingPolicy<IN, BucketID> build() {
		return new OnCheckpointRollingPolicy<>(new PolicyBuilder<>());
	}

	@PublicEvolving
	public static class PolicyBuilder<IN, BucketID>
		extends CheckpointRollingPolicy.PolicyBuilder<IN, BucketID, PolicyBuilder<IN, BucketID>> {

		private boolean removeOutdatedParts = DEFAULT_REMOVE_OUTDATED_PARTS;

		public PolicyBuilder<IN, BucketID> removeOutdatedParts(final boolean removeOutdatedParts) {
			this.removeOutdatedParts = removeOutdatedParts;
			return this;
		}

		@Override
		protected PolicyBuilder<IN, BucketID> self() {
			return this;
		}

		@Override
		public CheckpointRollingPolicy<IN, BucketID> build() {
			return new OnCheckpointRollingPolicy<>(this);
		}
	}
}
