/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.StdVarianceDoubleGroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.StdVarianceIntGroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.StdVarianceLongGroupingAggregatorFunction;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

public class StdvarOverTime extends TimeSeriesAggregateFunction implements OptionalArgument, ToAggregator {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "StdvarOverTime",
        StdvarOverTime::new
    );

    private final Expression timestamp;

    @FunctionInfo(
        type = FunctionType.TIME_SERIES_AGGREGATE,
        returnType = { "double" },
        description = "Calculates the statistical variance over time of a gauge metric field.",
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.3.0") },
        preview = true,
        examples = { @Example(file = "k8s-timeseries-stats-over-time", tag = "stdvar") }
    )
    public StdvarOverTime(Source source, @Param(name = "field", type = { "long", "integer", "double" }) Expression field) {
        this(source, field, new UnresolvedAttribute(source, "@timestamp"));
    }

    public StdvarOverTime(
        Source source,
        @Param(name = "field", type = { "long", "integer", "double" }) Expression field,
        Expression timestamp
    ) {
        this(source, field, Literal.TRUE, timestamp);
    }

    // compatibility constructor used when reading from the stream
    private StdvarOverTime(Source source, Expression field, Expression filter, List<Expression> children) {
        this(source, field, filter, children.getFirst());
    }

    private StdvarOverTime(Source source, Expression field, Expression filter, Expression timestamp) {
        super(source, field, filter, List.of(timestamp));
        this.timestamp = timestamp;
    }

    public StdvarOverTime(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteableCollectionAsList(Expression.class)
        );
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<StdvarOverTime> info() {
        return NodeInfo.create(this, StdvarOverTime::new, field(), timestamp);
    }

    @Override
    public StdvarOverTime replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() != 3) {
            assert false : "expected 3 children for field, filter, @timestamp; got " + newChildren;
            throw new IllegalArgumentException("expected 3 children for field, filter, @timestamp; got " + newChildren);
        }
        return new StdvarOverTime(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    public StdvarOverTime withFilter(Expression filter) {
        return new StdvarOverTime(source(), field(), filter, timestamp);
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

    @Override
    protected TypeResolution resolveType() {
        return isType(field(), dt -> dt.isNumeric() && DataType.isCounter(dt) == false, sourceText(), FIRST, "long", "integer", "double");
    }

    @Override
    public AggregatorFunctionSupplier supplier() {
        final DataType type = field().dataType();
        return switch (type) {
            case LONG -> new StdVarianceLongGroupingAggregatorFunction.FunctionSupplier();
            case INTEGER -> new StdVarianceIntGroupingAggregatorFunction.FunctionSupplier();
            case DOUBLE -> new StdVarianceDoubleGroupingAggregatorFunction.FunctionSupplier();
            default -> throw EsqlIllegalArgumentException.illegalDataType(type);
        };
    }

    @Override
    public StdvarOverTime perTimeSeriesAggregation() {
        return this;
    }

    @Override
    public String toString() {
        return "StdvarOverTime(" + field() + ")";
    }
}
