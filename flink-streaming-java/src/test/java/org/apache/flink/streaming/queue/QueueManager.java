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

package org.apache.flink.streaming.queue;

import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * QueueManager represents pair {@link QueueProducer} and {@link QueueConsumer} for flink pipeline tests.
 * You can pass {@link QueueProducer} to your custom source and put element into related {@link QueueConsumer}.
 * The element will be polled and passed into flink pipeline by your source.
 */
public class QueueManager<T> implements AutoCloseable {
	private static final Map<String, Queue<?>> QUEUES = new ConcurrentHashMap<>();
	private final String queueId;

	public QueueManager() {
		this.queueId = UUID.randomUUID().toString();
		QUEUES.put(this.queueId, new ConcurrentLinkedQueue<>());
	}

	public QueueProducer<T> getQueueProducer() {
		// queueIdCopy variable is used to prevent lambda capturing this reference
		// without the annotation Intellij Idea marks the variable as redundant
		@SuppressWarnings("UnnecessaryLocalVariable") final String queueIdCopy = this.queueId;
		return () -> QueueManager.<T>getQueue(queueIdCopy).poll();
	}

	public QueueConsumer<T> getQueueConsumer() {
		// queueIdCopy variable is used to prevent lambda capturing this reference
		// without the annotation Intellij Idea marks the variable as redundant
		@SuppressWarnings("UnnecessaryLocalVariable") final String queueIdCopy = this.queueId;
		return element -> (QueueManager.<T>getQueue(queueIdCopy)).add(element);
	}

	private static <T> Queue<T> getQueue(final String queueID) {
		// This cast is correct because elements T are added to the queue only via QueueConsumer<T>
		@SuppressWarnings("unchecked") final Queue<T> queue = (Queue<T>) QUEUES.get(queueID);
		if (queue == null) {
			throw new IllegalStateException("Queue " + queueID + " does not exist");
		}
		return queue;
	}

	@Override
	public void close() {
		QUEUES.remove(this.queueId);
	}
}
