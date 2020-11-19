package org.apache.flink.streaming.examples;

import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.PrintStream;

/**
 * asd.
 */
public class FileSinkExample {
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.enableCheckpointing(5000L);
		env.getCheckpointConfig()
			.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
		env.setStateBackend(new FsStateBackend("file:///home/sasha/tmp/checkpoints"));

//		final RollingPolicy<Tuple2<Integer, Integer>, String> rollingPolicy = OnCheckpointRollingPolicy.build();
		final RollingPolicy<Tuple2<Integer, Integer>, String> rollingPolicy = DefaultRollingPolicy.builder().withRolloverInterval(100L).build();


		final DataStream<Tuple2<Integer, Integer>> source = env.addSource(new Sf(250L, 10_000));
		final StreamingFileSink<Tuple2<Integer, Integer>> sink = StreamingFileSink
			.forRowFormat(
				new Path("/home/sasha/tmp/buckets"),
				(Encoder<Tuple2<Integer, Integer>>) (element, outStream) -> new PrintStream(outStream).println(element.f1)
			)
			.withBucketAssigner(new Ba())
			.withRollingPolicy(rollingPolicy)
			.build();
		source.addSink(sink);
		env.execute();
	}

	private static final class Sf implements SourceFunction<Tuple2<Integer, Integer>> {

		private final long interval;
		private final int max;

		private volatile boolean canceled = false;

		private Sf(long interval, int max) {
			this.interval = interval;
			this.max = max;
		}


		@Override
		public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
			 for (int i = 0; i < this.max && !this.canceled; i++) {
				 final Tuple2<Integer, Integer> t = Tuple2.of(0, i);
				 System.out.println("Emitting: " + t);
				 synchronized (ctx.getCheckpointLock()) {
				 	ctx.collect(t);
				 }
				 Thread.sleep(this.interval);
			 }
		}

		@Override
		public void cancel() {
			this.canceled = true;
		}
	}

	private static final class Ba implements BucketAssigner<Tuple2<Integer, Integer>, String> {

		@Override
		public String getBucketId(Tuple2<Integer, Integer> element, Context context) {
			return String.valueOf(element.f0);
		}

		@Override
		public SimpleVersionedSerializer<String> getSerializer() {
			return SimpleVersionedStringSerializer.INSTANCE;
		}
	}
}
