package com.github.trushev.examples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * SQL example.
 */
public class FilterPushDownExample {
	public static void main(final String... args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		final EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
				.useBlinkPlanner()
				.build();
		final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, envSettings);
		final DataStream<Tuple2<String, Integer>> ranksStream = env.fromElements(
				new Tuple2<>("Sasha", 1),
				new Tuple2<>("Masha", 2),
				new Tuple2<>("Pasha", 3),
				new Tuple2<>("Dasha", 4)
		);
		final DataStream<Tuple2<String, Double>> scoresStream = env.fromElements(
				new Tuple2<>("Dasha", 104.0),
				new Tuple2<>("Pasha", 203.0),
				new Tuple2<>("Masha", 302.0),
				new Tuple2<>("Sasha", 401.0)
		);
		tEnv.createTemporaryView("Ranks", ranksStream, $("player"), $("rank"));
		tEnv.createTemporaryView("Scores", scoresStream, $("player"), $("score"));
		final String sqlQuery =
				"SELECT r.*, s.score " +
				"FROM Ranks AS r JOIN Scores AS s " +
				"ON r.player = s.player " +
				"WHERE s.player <> 'Sasha'";
		final Table table = tEnv.sqlQuery(sqlQuery);
		tEnv.toAppendStream(table, Row.class).print();
		System.out.println(table.explain());
//		env.execute();
//		System.out.println(env.getStreamGraph().getStreamingPlanAsJSON());
	}
}
