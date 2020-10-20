## Abstract

```
== Abstract Syntax Tree ==
LogicalProject(player=[$0], rank=[$1], score=[$3])
+- LogicalFilter(condition=[<>($2, _UTF-16LE'Sasha')])
   +- LogicalJoin(condition=[=($0, $2)], joinType=[inner])
      :- LogicalTableScan(table=[[default_catalog, default_database, Ranks]])
      +- LogicalTableScan(table=[[default_catalog, default_database, Scores]])
```

## Default optimized plan

```
== Optimized Logical Plan ==
Calc(select=[player, rank, score])
+- Join(joinType=[InnerJoin], where=[=(player, player0)], select=[player, rank, player0, score], leftInputSpec=[NoUniqueKey], rightInputSpec=[NoUniqueKey])

->   :- Exchange(distribution=[hash[player]])
->   :  +- DataStreamScan(table=[[default_catalog, default_database, Ranks]], fields=[player, rank])

     +- Exchange(distribution=[hash[player]])
        +- Calc(select=[player, score], where=[<>(player, _UTF-16LE'Sasha':VARCHAR(2147483647) CHARACTER SET "UTF-16LE")])
           +- DataStreamScan(table=[[default_catalog, default_database, Scores]], fields=[player, score])
```

## My optimized plan

```
== Optimized Logical Plan ==
Calc(select=[player, rank, score])
+- Join(joinType=[InnerJoin], where=[=(player, player0)], select=[player, rank, player0, score], leftInputSpec=[NoUniqueKey], rightInputSpec=[NoUniqueKey])

->   :- Exchange(distribution=[hash[player]])
->   :  +- Calc(select=[player, rank], where=[<>(player, _UTF-16LE'Sasha':VARCHAR(2147483647) CHARACTER SET "UTF-16LE")])
->   :     +- DataStreamScan(table=[[default_catalog, default_database, Ranks]], fields=[player, rank])

     +- Exchange(distribution=[hash[player]])
        +- Calc(select=[player, score], where=[<>(player, _UTF-16LE'Sasha':VARCHAR(2147483647) CHARACTER SET "UTF-16LE")])
           +- DataStreamScan(table=[[default_catalog, default_database, Scores]], fields=[player, score])

```


## Developing info 

### Plan evolution 1
Debug loop at
```java
org.apache.flink.table.planner.plan.optimize.program.FlinkChainedProgram.optimize
```

Explain tool
```java
FlinkRelOptUtil.getDigest(result)
```

#### init
```
LogicalLegacySink(name=[DataStreamTableSink], fields=[player, rank, score]), rowType=[RAW(RAW('org.apache.flink.types.Row', ?))]
LogicalProject(inputs=[0..1], exprs=[[$3]]), rowType=[RecordType(VARCHAR(2147483647) player, INTEGER rank, DOUBLE score)]
LogicalFilter(condition=[<>($2, _UTF-16LE'Sasha')]), rowType=[RecordType(VARCHAR(2147483647) player, INTEGER rank, VARCHAR(2147483647) player0, DOUBLE score)]
LogicalJoin(condition=[=($0, $2)], joinType=[inner]), rowType=[RecordType(VARCHAR(2147483647) player, INTEGER rank, VARCHAR(2147483647) player0, DOUBLE score)]
LogicalTableScan(table=[[default_catalog, default_database, Ranks]]), rowType=[RecordType(VARCHAR(2147483647) player, INTEGER rank)]
LogicalTableScan(table=[[default_catalog, default_database, Scores]]), rowType=[RecordType(VARCHAR(2147483647) player, DOUBLE score)]
```

1. subquery_rewrite
    ```
    same
    ```

2. temporal_join_rewrite
    ```
    same
    ```

3. decorrelate
    ```
    same
    ```

4. time_indicator
    ```
    same
    ```

5. default_rewrite
    ```
    same
    ```

6. predicate_pushdown
    ```
    LogicalLegacySink(name=[DataStreamTableSink], fields=[player, rank, score]), rowType=[RAW(RAW('org.apache.flink.types.Row', ?))]
    LogicalProject(inputs=[0..1], exprs=[[$3]]), rowType=[RecordType(VARCHAR(2147483647) player, INTEGER rank, DOUBLE score)]
    LogicalJoin(condition=[=($0, $2)], joinType=[inner]), rowType=[RecordType(VARCHAR(2147483647) player, INTEGER rank, VARCHAR(2147483647) player0, DOUBLE score)]
    LogicalTableScan(table=[[default_catalog, default_database, Ranks]]), rowType=[RecordType(VARCHAR(2147483647) player, INTEGER rank)]
    LogicalFilter(condition=[<>($0, _UTF-16LE'Sasha')]), rowType=[RecordType(VARCHAR(2147483647) player, DOUBLE score)]
    LogicalTableScan(table=[[default_catalog, default_database, Scores]]), rowType=[RecordType(VARCHAR(2147483647) player, DOUBLE score)]
    ```

7. logical
    ```
    FlinkLogicalLegacySink(name=[DataStreamTableSink], fields=[player, rank, score]), rowType=[RAW(RAW('org.apache.flink.types.Row', ?))]
    FlinkLogicalCalc(select=[player, rank, score]), rowType=[RecordType(VARCHAR(2147483647) player, INTEGER rank, DOUBLE score)]
    FlinkLogicalJoin(condition=[=($0, $2)], joinType=[inner]), rowType=[RecordType(VARCHAR(2147483647) player, INTEGER rank, VARCHAR(2147483647) player0, DOUBLE score)]
    FlinkLogicalDataStreamTableScan(table=[[default_catalog, default_database, Ranks]]), rowType=[RecordType(VARCHAR(2147483647) player, INTEGER rank)]
    FlinkLogicalCalc(select=[player, score], where=[<>(player, _UTF-16LE'Sasha':VARCHAR(2147483647) CHARACTER SET "UTF-16LE")]), rowType=[RecordType(VARCHAR(2147483647) player, DOUBLE score)]
    FlinkLogicalDataStreamTableScan(table=[[default_catalog, default_database, Scores]]), rowType=[RecordType(VARCHAR(2147483647) player, DOUBLE score)]
    ```

8. logical_rewrite
    ```
    same
    ```

9. physical
    ```
    LegacySink(name=[DataStreamTableSink], fields=[player, rank, score], changelogMode=[NONE]), rowType=[RAW(RAW('org.apache.flink.types.Row', ?))]
    Calc(select=[player, rank, score], changelogMode=[NONE]), rowType=[RecordType(VARCHAR(2147483647) player, INTEGER rank, DOUBLE score)]
    Join(joinType=[InnerJoin], where=[=(player, player0)], select=[player, rank, player0, score], leftInputSpec=[NoUniqueKey], rightInputSpec=[NoUniqueKey], changelogMode=[NONE]), rowType=[RecordType(VARCHAR(2147483647) player, INTEGER rank, VARCHAR(2147483647) player0, DOUBLE score)]
    Exchange(distribution=[hash[player]], changelogMode=[NONE]), rowType=[RecordType(VARCHAR(2147483647) player, INTEGER rank)]
    DataStreamScan(table=[[default_catalog, default_database, Ranks]], fields=[player, rank], changelogMode=[NONE]), rowType=[RecordType(VARCHAR(2147483647) player, INTEGER rank)]
    Exchange(distribution=[hash[player]], changelogMode=[NONE]), rowType=[RecordType(VARCHAR(2147483647) player, DOUBLE score)]
    Calc(select=[player, score], where=[<>(player, _UTF-16LE'Sasha':VARCHAR(2147483647) CHARACTER SET "UTF-16LE")], changelogMode=[NONE]), rowType=[RecordType(VARCHAR(2147483647) player, DOUBLE score)]
    DataStreamScan(table=[[default_catalog, default_database, Scores]], fields=[player, score], changelogMode=[NONE]), rowType=[RecordType(VARCHAR(2147483647) player, DOUBLE score)]
    ```

10. physical_rewrite
    ```
    LegacySink(name=[DataStreamTableSink], fields=[player, rank, score], changelogMode=[NONE]), rowType=[RAW(RAW('org.apache.flink.types.Row', ?))]
    Calc(select=[player, rank, score], changelogMode=[I]), rowType=[RecordType(VARCHAR(2147483647) player, INTEGER rank, DOUBLE score)]
    Join(joinType=[InnerJoin], where=[=(player, player0)], select=[player, rank, player0, score], leftInputSpec=[NoUniqueKey], rightInputSpec=[NoUniqueKey], changelogMode=[I]), rowType=[RecordType(VARCHAR(2147483647) player, INTEGER rank, VARCHAR(2147483647) player0, DOUBLE score)]
    Exchange(distribution=[hash[player]], changelogMode=[I]), rowType=[RecordType(VARCHAR(2147483647) player, INTEGER rank)]
    DataStreamScan(table=[[default_catalog, default_database, Ranks]], fields=[player, rank], changelogMode=[I]), rowType=[RecordType(VARCHAR(2147483647) player, INTEGER rank)]
    Exchange(distribution=[hash[player]], changelogMode=[I]), rowType=[RecordType(VARCHAR(2147483647) player, DOUBLE score)]
    Calc(select=[player, score], where=[<>(player, _UTF-16LE'Sasha':VARCHAR(2147483647) CHARACTER SET "UTF-16LE")], changelogMode=[I]), rowType=[RecordType(VARCHAR(2147483647) player, DOUBLE score)]
    DataStreamScan(table=[[default_catalog, default_database, Scores]], fields=[player, score], changelogMode=[I]), rowType=[RecordType(VARCHAR(2147483647) player, DOUBLE score)]
    ```

## Plan evolution 2

Debug loop at
```java
org.apache.flink.table.planner.plan.optimize.program.FlinkGroupProgram.optimize
```

Breakpoint: FlinkGroupProgram.scala:60

Condition:
```java
programs.get(0)._2.equals("filter rules")
```

### init
```
LogicalLegacySink(name=[DataStreamTableSink], fields=[player, rank, score]), rowType=[RAW(RAW('org.apache.flink.types.Row', ?))]
LogicalProject(inputs=[0..1], exprs=[[$3]]), rowType=[RecordType(VARCHAR(2147483647) player, INTEGER rank, DOUBLE score)]
LogicalFilter(condition=[<>($2, _UTF-16LE'Sasha')]), rowType=[RecordType(VARCHAR(2147483647) player, INTEGER rank, VARCHAR(2147483647) player0, DOUBLE score)]
LogicalJoin(condition=[=($0, $2)], joinType=[inner]), rowType=[RecordType(VARCHAR(2147483647) player, INTEGER rank, VARCHAR(2147483647) player0, DOUBLE score)]
LogicalTableScan(table=[[default_catalog, default_database, Ranks]]), rowType=[RecordType(VARCHAR(2147483647) player, INTEGER rank)]
LogicalTableScan(table=[[default_catalog, default_database, Scores]]), rowType=[RecordType(VARCHAR(2147483647) player, DOUBLE score)]
```

```
0 = {FilterJoinRule$FilterIntoJoinRule@7147} "FilterJoinRule:FilterJoinRule:filter" - target rule
1 = {FilterJoinRule$JoinConditionPushRule@7148} "FilterJoinRule:FilterJoinRule:no-filter"
2 = {FilterAggregateTransposeRule@7149} "FilterAggregateTransposeRule"
3 = {FilterProjectTransposeRule@7150} "FilterProjectTransposeRule"
4 = {FilterSetOpTransposeRule@7151} "FilterSetOpTransposeRule"
5 = {FilterMergeRule@7152} "FilterMergeRule"
6 = {SimplifyFilterConditionRule@7153} "SimplifyFilterConditionRule"
7 = {SimplifyJoinConditionRule@7154} "SimplifyJoinConditionRule"
8 = {JoinConditionTypeCoerceRule@7155} "JoinConditionTypeCoerceRule"
9 = {JoinPushExpressionsRule@7156} "JoinPushExpressionsRule"
10 = {ReduceExpressionsRule$FilterReduceExpressionsRule@7157} "ReduceExpressionsRule(Filter)"
11 = {ReduceExpressionsRule$ProjectReduceExpressionsRule@7158} "ReduceExpressionsRule(Project)"
12 = {ReduceExpressionsRule$CalcReduceExpressionsRule@7159} "ReduceExpressionsRule(Calc)"
13 = {ReduceExpressionsRule$JoinReduceExpressionsRule@7160} "ReduceExpressionsRule(Join)"
```


```
org.apache.flink.table.planner.plan.rules.FlinkStreamRuleSets.FILTER_RULES
```

```
org.apache.calcite.plan.RelOptUtil.classifyFilters
```
