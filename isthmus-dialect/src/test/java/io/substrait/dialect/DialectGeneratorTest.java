package io.substrait.dialect;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class DialectGeneratorTest {

  @Test
  void testGenerateDialect() {
    DialectGenerator generator = new DialectGenerator();
    Dialect dialect = generator.generate();

    assertNotNull(dialect);
    assertEquals("Calcite Dialect", dialect.getName());

    // Verify types
    assertFalse(dialect.getSupportedTypes().isEmpty());
    assertTrue(
        dialect.getSupportedTypes().stream().anyMatch(t -> t.getType().equals("I32")),
        "Should support I32 type");

    // Verify expressions
    assertFalse(dialect.getSupportedExpressions().isEmpty());
    assertTrue(dialect.getSupportedExpressions().contains("LITERAL"));
    assertTrue(dialect.getSupportedExpressions().contains("SCALAR_FUNCTION"));

    // Verify relations
    assertFalse(dialect.getSupportedRelations().isEmpty());
    assertTrue(dialect.getSupportedRelations().contains("FILTER"));
    assertTrue(dialect.getSupportedRelations().contains("PROJECT"));

    // Verify functions are generated
    assertFalse(dialect.getSupportedScalarFunctions().isEmpty());
    assertTrue(
        dialect.getSupportedScalarFunctions().stream().anyMatch(f -> f.getName().equals("add")),
        "Should have add function");
  }

  @Test
  void testSupportedTypeStructure() {
    DialectGenerator generator = new DialectGenerator();
    Dialect dialect = generator.generate();

    Dialect.SupportedType intType =
        dialect.getSupportedTypes().stream()
            .filter(t -> t.getType().equals("I32"))
            .findFirst()
            .orElseThrow();

    assertEquals("INTEGER", intType.getSystemMetadata().getName());
    assertTrue(intType.getSystemMetadata().isSupportedAsColumn());
  }

  @Test
  void testSupportedFunctionStructure() {
    DialectGenerator generator = new DialectGenerator();
    Dialect dialect = generator.generate();

    Dialect.SupportedFunction addFunc =
        dialect.getSupportedScalarFunctions().stream()
            .filter(f -> f.getName().equals("add"))
            .findFirst()
            .orElseThrow();

    assertNotNull(addFunc.getSource());
    assertNotNull(addFunc.getSystemMetadata());
    assertEquals("+", addFunc.getSystemMetadata().getName());
    assertEquals("INFIX", addFunc.getSystemMetadata().getNotation());
    assertFalse(addFunc.getSupportedImpls().isEmpty());
  }
}
