package io.substrait.relation;

import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import io.substrait.util.VisitationContext;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.immutables.value.Value;

@Value.Immutable
public abstract class Aggregate extends SingleInputRel implements HasExtension {

  public abstract List<Grouping> getGroupings();

  public abstract List<Measure> getMeasures();

  @Override
  protected Type.Struct deriveRecordType() {
    List<Grouping> groupings = getGroupings();

    Stream<Type> uniqueGroupingExpressions =
        groupings.stream()
            .flatMap(g -> g.getExpressions().stream())
            .distinct()
            .map(Expression::getType);
    Stream<Type> measures =
        getMeasures().stream().map(Measure::getFunction).map(AggregateFunctionInvocation::getType);

    Stream<Type> resultTypes = Stream.concat(uniqueGroupingExpressions, measures);

    if (groupings.size() > 1) {
      resultTypes = Stream.concat(resultTypes, Stream.of(TypeCreator.REQUIRED.I32));
    }

    return TypeCreator.REQUIRED.struct(resultTypes);
  }

  @Override
  public <O, C extends VisitationContext, E extends Exception> O accept(
      RelVisitor<O, C, E> visitor, C context) throws E {
    return visitor.visit(this, context);
  }

  @Value.Immutable
  public abstract static class Grouping {
    public abstract List<Expression> getExpressions();

    public static ImmutableGrouping.Builder builder() {
      return ImmutableGrouping.builder();
    }
  }

  @Value.Immutable
  public abstract static class Measure {
    public abstract AggregateFunctionInvocation getFunction();

    public abstract Optional<Expression> getPreMeasureFilter();

    public static ImmutableMeasure.Builder builder() {
      return ImmutableMeasure.builder();
    }
  }

  public static ImmutableAggregate.Builder builder() {
    return ImmutableAggregate.builder();
  }
}
