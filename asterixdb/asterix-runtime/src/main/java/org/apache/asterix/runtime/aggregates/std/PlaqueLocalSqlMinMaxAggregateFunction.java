package org.apache.asterix.runtime.aggregates.std;

import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.operators.plaque.FilterState;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;

public class PlaqueLocalSqlMinMaxAggregateFunction extends SqlMinMaxAggregateFunction {

    private final FilterState filterState;

    public PlaqueLocalSqlMinMaxAggregateFunction(IScalarEvaluatorFactory[] args,
                                                 IEvaluatorContext context, boolean isMin, SourceLocation sourceLoc,
                                                 IAType aggFieldType, FilterState filterState) throws HyracksDataException {
        super(args, context, isMin, Type.LOCAL, sourceLoc, aggFieldType);
        this.filterState = filterState;
    }

    @Override
    protected void onMinMaxChanged() throws HyracksDataException {
        filterState.update(
                outputVal.getByteArray(),
                outputVal.getStartOffset(),
                outputVal.getLength());
    }
}