package io.substrait.dialect;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.expression.FunctionMappings;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlOperator;

/**
 * Generates a Substrait {@link Dialect} describing the capabilities of Calcite/Isthmus. This is the
 * Java equivalent of the Scala DialectGenerator in the spark module.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * DialectGenerator generator = new DialectGenerator();
 * Dialect dialect = generator.generate();
 *
 * // Serialize to YAML (requires jackson-dataformat-yaml dependency)
 * ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
 * String yaml = mapper.writeValueAsString(dialect);
 * }</pre>
 */
public class DialectGenerator {

  private static final Map<String, String> SOURCE_URNS =
      Map.ofEntries(
          Map.entry("extension:io.substrait:functions_aggregate_approx", "aggregate_approx"),
          Map.entry("extension:io.substrait:functions_aggregate_generic", "aggregate_generic"),
          Map.entry("extension:io.substrait:functions_arithmetic", "arithmetic"),
          Map.entry("extension:io.substrait:functions_arithmetic_decimal", "arithmetic_decimal"),
          Map.entry("extension:io.substrait:functions_boolean", "boolean"),
          Map.entry("extension:io.substrait:functions_comparison", "comparison"),
          Map.entry("extension:io.substrait:functions_datetime", "datetime"),
          Map.entry("extension:io.substrait:functions_logarithmic", "logarithmic"),
          Map.entry("extension:io.substrait:functions_rounding", "rounding"),
          Map.entry("extension:io.substrait:functions_rounding_decimal", "rounding_decimal"),
          Map.entry("extension:io.substrait:functions_string", "string"));

  private final SimpleExtension.ExtensionCollection extensions;

  /** Creates a DialectGenerator using the default Substrait extensions. */
  public DialectGenerator() {
    this.extensions = DefaultExtensionCatalog.DEFAULT_COLLECTION;
  }

  /** Creates a DialectGenerator with custom extensions. */
  public DialectGenerator(SimpleExtension.ExtensionCollection extensions) {
    this.extensions = extensions;
  }

  /** Generates a Dialect object representing Calcite's capabilities. */
  public Dialect generate() {
    List<Dialect.SupportedType> types = supportedTypes();
    List<?> expressions = supportedExpressions();
    List<?> relations = supportedRelations();

    List<Dialect.SupportedFunction> scalars =
        supportedFunctions(FunctionMappings.SCALAR_SIGS, extensions.scalarFunctions());
    List<Dialect.SupportedFunction> aggregates =
        supportedFunctions(FunctionMappings.AGGREGATE_SIGS, extensions.aggregateFunctions());
    List<Dialect.SupportedFunction> windows =
        supportedFunctions(FunctionMappings.WINDOW_SIGS, extensions.windowFunctions());

    return new Dialect(
        "Calcite Dialect",
        types,
        expressions,
        relations,
        scalars,
        aggregates,
        windows);
  }

  private List<Dialect.SupportedType> supportedTypes() {
    return List.of(
        new Dialect.SupportedType("I8", new Dialect.TypeMetadata("TINYINT", true)),
        new Dialect.SupportedType("I16", new Dialect.TypeMetadata("SMALLINT", true)),
        new Dialect.SupportedType("I32", new Dialect.TypeMetadata("INTEGER", true)),
        new Dialect.SupportedType("I64", new Dialect.TypeMetadata("BIGINT", true)),
        new Dialect.SupportedType("FP32", new Dialect.TypeMetadata("REAL", true)),
        new Dialect.SupportedType("FP64", new Dialect.TypeMetadata("DOUBLE", true)),
        new Dialect.SupportedType("DECIMAL", new Dialect.TypeMetadata("DECIMAL", true)),
        new Dialect.SupportedType("DATE", new Dialect.TypeMetadata("DATE", true)),
        new Dialect.SupportedType("STRING", new Dialect.TypeMetadata("VARCHAR", true)),
        new Dialect.SupportedType("VARCHAR", new Dialect.TypeMetadata("VARCHAR", true)),
        new Dialect.SupportedType("FIXED_CHAR", new Dialect.TypeMetadata("CHAR", true)),
        new Dialect.SupportedType("BINARY", new Dialect.TypeMetadata("VARBINARY", true)),
        new Dialect.SupportedType("BOOL", new Dialect.TypeMetadata("BOOLEAN", true)),
        new Dialect.SupportedType(
            "PRECISION_TIMESTAMP", new Dialect.TypeMetadata("TIMESTAMP", true)),
        new Dialect.SupportedType(
            "PRECISION_TIMESTAMP_TZ", new Dialect.TypeMetadata("TIMESTAMP WITH TIME ZONE", true)),
        new Dialect.SupportedType(
            "INTERVAL_DAY", new Dialect.TypeMetadata("INTERVAL DAY TO SECOND", true)),
        new Dialect.SupportedType(
            "INTERVAL_YEAR", new Dialect.TypeMetadata("INTERVAL YEAR TO MONTH", true)),
        new Dialect.SupportedType("LIST", new Dialect.TypeMetadata("ARRAY", true)),
        new Dialect.SupportedType("MAP", new Dialect.TypeMetadata("MAP", true)),
        new Dialect.SupportedType("STRUCT", new Dialect.TypeMetadata("ROW", true)));
  }

  private List<?> supportedExpressions() {
    return List.of(
        "LITERAL",
        "SELECTION",
        "SCALAR_FUNCTION",
        "IF_THEN",
        "SINGULAR_OR_LIST",
        "CAST",
        Map.ofEntries(
          Map.entry("expression", "SUBQUERY"), Map.entry("subquery_types", List.of("SCALAR", "IN_PREDICATE"))));
  }

  private List<?> supportedRelations() {
    return List.of(
        "FILTER",
        "FETCH",
        "AGGREGATE",
        "SORT",
        "PROJECT",
        "CROSS",
        Map.ofEntries(
          Map.entry("relation", "READ"),
          Map.entry("read_types", List.of("VIRTUAL_TABLE", "LOCAL_FILES", "NAMED_TABLE"))),
        Map.ofEntries(
            Map.entry("relation", "JOIN"),
            Map.entry("join_types", List.of("INNER", "OUTER", "LEFT", "RIGHT", "SEMI", "ANTI"))),
        Map.ofEntries(
            Map.entry("relation", "SET"),
            Map.entry("operations", List.of(
                "UNION_ALL",
                "UNION_DISTINCT",
                "INTERSECTION_ALL",
                "INTERSECTION_DISTINCT",
                "MINUS_ALL",
                "MINUS_DISTINCT"))));
  }

  /**
   * Generates supported function entries by matching FunctionMappings signatures against loaded
   * extension functions.
   */
  private List<Dialect.SupportedFunction> supportedFunctions(
      List<FunctionMappings.Sig> sigs,
      List<? extends SimpleExtension.Function> extensionFunctions) {

    List<Dialect.SupportedFunction> result = new ArrayList<>();

    for (FunctionMappings.Sig sig : sigs) {
      SqlOperator operator = sig.operator();
      String functionName = sig.name();

      // Determine notation based on operator type
      String notation = (operator instanceof SqlBinaryOperator) ? "INFIX" : "FUNCTION";
      String sqlName = operator.getName();

      Dialect.FunctionMetadata metadata = new Dialect.FunctionMetadata(sqlName, notation);

      // Group function variants by URN
      Map<String, List<String>> variantsByUrn = new HashMap<>();

      for (SimpleExtension.Function func : extensionFunctions) {
        if (func.name().equals(functionName)) {
          String urn = func.urn();
          String key = func.key();

          // Extract signature part after the colon (e.g., "add:i32_i32" -> "i32_i32")
          int colonIndex = key.indexOf(':');
          String signature = colonIndex >= 0 ? key.substring(colonIndex + 1) : "";

          if (!signature.isEmpty()) {
            variantsByUrn.computeIfAbsent(urn, k -> new ArrayList<>()).add(signature);
          }
        }
      }

      // Create a SupportedFunction for each URN that has variants
      for (Map.Entry<String, List<String>> entry : variantsByUrn.entrySet()) {
        String urn = entry.getKey();
        List<String> variants = entry.getValue();
        String source = SOURCE_URNS.getOrDefault(urn, "");

        if (!source.isEmpty()) {
          result.add(new Dialect.SupportedFunction(source, functionName, metadata, variants));
        }
      }
    }

    return result;
  }
}
