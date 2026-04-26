package org.apache.asterix.runtime.operators.plaque;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.job.JobId;


public final class FilterStateRegistry {

    private FilterStateRegistry() {
    }

    public static FilterState getOrCreate(IHyracksTaskContext ctx, String handle, boolean isMax) {
        FilterState state = (FilterState) ctx.getStateObject(handle);
        if (state == null) {
            state = new FilterState(isMax);

            JobId jobId = ctx.getJobletContext().getJobId();
            state.setJobId(jobId);
            state.setId(handle);

            ctx.setStateObject(state);
        }
        return state;
    }
}