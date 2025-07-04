package io.substrait.isthmus;

import com.google.common.collect.Streams;
import io.substrait.dsl.SubstraitBuilder;
import io.substrait.relation.Aggregate;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Rel;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class AggregationFunctionsTest extends PlanTestBase {

  SubstraitBuilder b = new SubstraitBuilder(extensions);

  static final TypeCreator R = TypeCreator.of(false);
  static final TypeCreator N = TypeCreator.of(true);

  // Create a table with that has a column of every numeric type, both NOT NULL and NULL
  private List<Type> numericTypesR = List.of(R.I8, R.I16, R.I32, R.I64, R.FP32, R.FP64);
  private List<Type> numericTypesN = List.of(N.I8, N.I16, N.I32, N.I64, N.FP32, N.FP64);
  private List<Type> numericTypes =
      Stream.concat(numericTypesR.stream(), numericTypesN.stream()).collect(Collectors.toList());

  private List<Type> tableTypes =
      Stream.concat(
              // Column to Group By
              Stream.of(N.I8),
              // Columns with Numeric Types
              numericTypes.stream())
          .collect(Collectors.toList());
  private List<String> columnNames =
      Streams.mapWithIndex(tableTypes.stream(), (t, index) -> String.valueOf(index))
          .collect(Collectors.toList());
  private NamedScan numericTypesTable = b.namedScan(List.of("example"), columnNames, tableTypes);

  // Create the given function call on the given field of the input
  private Aggregate.Measure functionPicker(Rel input, int field, String fname) {
    switch (fname) {
      case "min":
        return b.min(input, field);
      case "max":
        return b.max(input, field);
      case "sum":
        return b.sum(input, field);
      case "sum0":
        return b.sum0(input, field);
      case "avg":
        return b.avg(input, field);
      default:
        throw new UnsupportedOperationException(
            String.format("no function is associated with %s", fname));
    }
  }

  // Create one function call per numeric type column
  private List<Aggregate.Measure> functions(Rel input, String fname) {
    // first column is for grouping, skip it
    return IntStream.range(1, tableTypes.size())
        .boxed()
        .map(index -> functionPicker(input, index, fname))
        .collect(Collectors.toList());
  }

  @ParameterizedTest
  @ValueSource(strings = {"max", "min", "sum", "sum0", "avg"})
  void emptyGrouping(String aggFunction) {
    Aggregate rel =
        b.aggregate(
            input -> b.grouping(input), input -> functions(input, aggFunction), numericTypesTable);
    assertFullRoundTrip(rel);
  }

  @ParameterizedTest
  @ValueSource(strings = {"max", "min", "sum", "sum0", "avg"})
  void withGrouping(String aggFunction) {
    Aggregate rel =
        b.aggregate(
            input -> b.grouping(input, 0),
            input -> functions(input, aggFunction),
            numericTypesTable);
    assertFullRoundTrip(rel);
  }

  @Test
  void sqlGroupingSingle() throws Exception {
    String create = "CREATE TABLE Sales (Country VARCHAR(255), Amount INT);";
    String sql =
        "SELECT Country, SUM(Amount) AS Total, GROUPING(Country) AS GP_Country"
            + " FROM Sales"
            + " GROUP BY GROUPING SETS ((Country), ())";
    assertFullRoundTrip(sql, create);
    // LogicalAggregate(group=[{0}], groups=[[{0}, {}]], TOTAL=[SUM($1)], GP_COUNTRY=[GROUPING($0)])
    //  LogicalTableScan(table=[[SALES]])
  }

  @Test
  void sqlGroupingRewrite() throws Exception {
    String create = "CREATE TABLE Sales (Country VARCHAR(255), Amount INT, GroupingId INT);";
    String sql =
        "SELECT Country, SUM(Amount) AS Total,"
            + " CASE WHEN SUM(GroupingId) = 0 THEN 0"
            + "      WHEN SUM(GroupingId) = 1 THEN 0"
            + " ELSE 1"
            + " END AS GP_Country"
            + " FROM Sales"
            + " GROUP BY GROUPING SETS ((Country), ())";
    assertFullRoundTrip(sql, create);
    // LogicalProject(COUNTRY=[$0], TOTAL=[$1], GP_COUNTRY=[CASE(SEARCH($2, Sarg[0, 1]), 0, 1)])
    //  LogicalAggregate(group=[{0}], groups=[[{0}, {}]], TOTAL=[SUM($1)], agg#1=[SUM($2)])
    //    LogicalTableScan(table=[[SALES]])

    // Root{
    //   input=Project{
    //     remap=Remap{indices=[3, 4, 5]},
    //       input=Aggregate{
    //         input=NamedScan{initialSchema=NamedStruct{struct=Struct{nullable=false,
    // fields=[VarChar{nullable=true, length=255}, I32{nullable=true}, I32{nullable=true}]},
    // names=[COUNTRY, AMOUNT, GROUPINGID]}, names=[SALES]},
    // groupings=[Grouping{expressions=[FieldReference{segments=[StructField{offset=0}],
    // type=VarChar{nullable=true, length=255}}]}, Grouping{expressions=[]}],
    // measures=[Measure{function=AggregateFunctionInvocation{declaration=sum:i32,
    // arguments=[FieldReference{segments=[StructField{offset=1}], type=I32{nullable=true}}],
    // options=[], aggregationPhase=INITIAL_TO_RESULT, sort=[], outputType=I32{nullable=true},
    // invocation=ALL}}, Measure{function=AggregateFunctionInvocation{declaration=sum:i32,
    // arguments=[FieldReference{segments=[StructField{offset=2}], type=I32{nullable=true}}],
    // options=[], aggregationPhase=INITIAL_TO_RESULT, sort=[], outputType=I32{nullable=true},
    // invocation=ALL}}]}, expressions=[FieldReference{segments=[StructField{offset=0}],
    // type=VarChar{nullable=true, length=255}}, FieldReference{segments=[StructField{offset=1}],
    // type=I32{nullable=true}},
    // IfThen{ifClauses=[IfClause{condition=ScalarFunctionInvocation{declaration=or:bool,
    // arguments=[ScalarFunctionInvocation{declaration=equal:any_any,
    // arguments=[FieldReference{segments=[StructField{offset=2}], type=I32{nullable=true}},
    // I32Literal{nullable=false, value=0}], options=[], outputType=Bool{nullable=true}},
    // ScalarFunctionInvocation{declaration=equal:any_any,
    // arguments=[FieldReference{segments=[StructField{offset=2}], type=I32{nullable=true}},
    // I32Literal{nullable=false, value=1}], options=[], outputType=Bool{nullable=true}}],
    // options=[], outputType=Bool{nullable=true}}, then=I32Literal{nullable=false, value=0}}],
    // elseClause=I32Literal{nullable=false, value=1}}]}, names=[COUNTRY, TOTAL, GP_COUNTRY]}
  }

  @Test
  @Disabled
  void sqlMultipleGrouping() throws SqlParseException {
    String create = "CREATE TABLE Sales (Country VARCHAR(255), Amount INT);";
    String sql =
        "SELECT Country, SUM(Amount) AS Total, GROUPING(Country) AS GP_Country"
            + " FROM Sales"
            + " GROUP BY ROLLUP (Country)";
    assertFullRoundTrip(sql, create);
  }
}
