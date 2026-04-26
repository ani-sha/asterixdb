package org.apache.asterix.runtime.operators.plaque;

import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.aggregates.std.PlaqueLocalSqlMinMaxAggregateFunction;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class PlaqueLocalAggregateEvaluatorFactory implements IAggregateEvaluatorFactory {

    private static final long serialVersionUID = 1L;

    private final IScalarEvaluatorFactory[] args;
    private final boolean isMin;
    private final SourceLocation sourceLoc;
    private final IAType aggFieldType;
    private final String handle;
    private final boolean isMax;

    public PlaqueLocalAggregateEvaluatorFactory(IScalarEvaluatorFactory[] args, boolean isMin,
                                                SourceLocation sourceLoc, IAType aggFieldType, String handle, boolean isMax) {
        this.args = args;
        this.isMin = isMin;
        this.sourceLoc = sourceLoc;
        this.aggFieldType = aggFieldType;
        this.handle = handle;
        this.isMax = isMax;
    }

    @Override
    public IAggregateEvaluator createAggregateEvaluator(IEvaluatorContext ctx)
            throws HyracksDataException {
        FilterState state = FilterStateRegistry.getOrCreate(ctx.getTaskContext(), handle, isMax);

        return new PlaqueLocalSqlMinMaxAggregateFunction(
                args,
                ctx,
                isMin,
                sourceLoc,
                aggFieldType,
                state);
    }

}