package io.substrait.dialect;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.util.List;
import java.util.Map;

/**
 * Represents a Substrait dialect definition that describes the capabilities of a specific SQL
 * engine. This class is the Java equivalent of the Scala case class in DialectGenerator.scala.
 *
 * <p>Designed to be serialized to YAML using Jackson with snake_case property names.
 */
@JsonPropertyOrder({
  "name",
  "supported_types",
  "supported_expressions",
  "supported_relations",
  "dependencies",
  "supported_scalar_functions",
  "supported_aggregate_functions",
  "supported_window_functions"
})
public class Dialect {

  private final String name;

  @JsonProperty("supported_types")
  private final List<SupportedType> supportedTypes;

  @JsonProperty("supported_expressions")
  private final List<?> supportedExpressions;

  @JsonProperty("supported_relations")
  private final List<?> supportedRelations;

  @JsonProperty("supported_scalar_functions")
  private final List<SupportedFunction> supportedScalarFunctions;

  @JsonProperty("supported_aggregate_functions")
  private final List<SupportedFunction> supportedAggregateFunctions;

  @JsonProperty("supported_window_functions")
  private final List<SupportedFunction> supportedWindowFunctions;

  public Dialect(
      String name,
      List<SupportedType> supportedTypes,
      List<?> supportedExpressions,
      List<?> supportedRelations,
      List<SupportedFunction> supportedScalarFunctions,
      List<SupportedFunction> supportedAggregateFunctions,
      List<SupportedFunction> supportedWindowFunctions) {
    this.name = name;
    this.supportedTypes = supportedTypes;
    this.supportedExpressions = supportedExpressions;
    this.supportedRelations = supportedRelations;
    this.supportedScalarFunctions = supportedScalarFunctions;
    this.supportedAggregateFunctions = supportedAggregateFunctions;
    this.supportedWindowFunctions = supportedWindowFunctions;
  }

  public String getName() {
    return name;
  }

  public List<SupportedType> getSupportedTypes() {
    return supportedTypes;
  }

  public List<?> getSupportedExpressions() {
    return supportedExpressions;
  }

  public List<?> getSupportedRelations() {
    return supportedRelations;
  }

  public List<SupportedFunction> getSupportedScalarFunctions() {
    return supportedScalarFunctions;
  }

  public List<SupportedFunction> getSupportedAggregateFunctions() {
    return supportedAggregateFunctions;
  }

  public List<SupportedFunction> getSupportedWindowFunctions() {
    return supportedWindowFunctions;
  }

  /** Metadata describing how a Substrait type maps to the target system's type. */
  public static class TypeMetadata {
    private final String name;

    @JsonProperty("supported_as_column")
    private final boolean supportedAsColumn;

    public TypeMetadata(String name, boolean supportedAsColumn) {
      this.name = name;
      this.supportedAsColumn = supportedAsColumn;
    }

    public String getName() {
      return name;
    }

    public boolean isSupportedAsColumn() {
      return supportedAsColumn;
    }
  }

  /** Represents a supported Substrait type with its system-specific metadata. */
  public static class SupportedType {
    private final String type;

    @JsonProperty("system_metadata")
    private final TypeMetadata systemMetadata;

    public SupportedType(String type, TypeMetadata systemMetadata) {
      this.type = type;
      this.systemMetadata = systemMetadata;
    }

    public String getType() {
      return type;
    }

    public TypeMetadata getSystemMetadata() {
      return systemMetadata;
    }
  }

  /** Metadata describing a function's SQL representation. */
  public static class FunctionMetadata {
    private final String name;
    private final String notation;

    public FunctionMetadata(String name, String notation) {
      this.name = name;
      this.notation = notation;
    }

    public String getName() {
      return name;
    }

    public String getNotation() {
      return notation;
    }
  }

  /** Represents a supported Substrait function with its implementations. */
  public static class SupportedFunction {
    private final String source;
    private final String name;

    @JsonProperty("system_metadata")
    private final FunctionMetadata systemMetadata;

    @JsonProperty("supported_impls")
    private final List<String> supportedImpls;

    public SupportedFunction(
        String source, String name, FunctionMetadata systemMetadata, List<String> supportedImpls) {
      this.source = source;
      this.name = name;
      this.systemMetadata = systemMetadata;
      this.supportedImpls = supportedImpls;
    }

    public String getSource() {
      return source;
    }

    public String getName() {
      return name;
    }

    public FunctionMetadata getSystemMetadata() {
      return systemMetadata;
    }

    public List<String> getSupportedImpls() {
      return supportedImpls;
    }
  }
}
