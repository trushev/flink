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

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Abstract source for {@link QueueProducer}.
 */
public abstract class AbstractSourceFromQueue<IN, OUT> implements SourceFunction<OUT> {

	private final QueueProducer<IN> queueProducer;

	private volatile boolean running = true;

	protected AbstractSourceFromQueue(final QueueProducer<IN> queueProducer) {
		this.queueProducer = queueProducer;
	}

	@Override
	public void run(final SourceContext<OUT> context) throws Exception {
		while (this.running) {
			Thread.sleep(100);
			final IN event = this.queueProducer.poll();
			if (event == null) {
				continue;
			}
			synchronized (context.getCheckpointLock()) {
				context.collect(createElement(event));
			}
		}
	}

	protected abstract OUT createElement(final IN event);

	@Override
	public void cancel() {
		this.running = false;
	}
}
