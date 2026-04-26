package org.apache.asterix.runtime.operators.plaque;

import java.nio.ByteBuffer;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntime;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class PlaqueFilterRuntimeFactory implements IPushRuntimeFactory {

    private static final long serialVersionUID = 1L;

    private final int columnIdx;
    private final boolean isMax;
    private final String handle;

    public PlaqueFilterRuntimeFactory(int columnIdx, boolean isMax, String handle) {
        this.columnIdx = columnIdx;
        this.isMax = isMax;
        this.handle = handle;
    }

    @Override
    public IPushRuntime[] createPushRuntime(IHyracksTaskContext ctx) throws HyracksDataException {
        return new IPushRuntime[] { new PlaqueFilterRuntime(ctx, columnIdx, isMax, handle) };
    }

    @Override
    public String toString() {
        return "plaque-filter [col=" + columnIdx + ", "
                + (isMax ? "MAX" : "MIN") + ", h=" + handle + "]";
    }

    private static final class PlaqueFilterRuntime
            extends AbstractOneInputOneOutputOneFramePushRuntime {

        private final IHyracksTaskContext ctx;
        private final int columnIdx;
        private final boolean isMax;
        private final String handle;

        private FilterState filterState;

        private PlaqueFilterRuntime(IHyracksTaskContext ctx, int columnIdx, boolean isMax,
                                    String handle) {
            super();
            this.ctx = ctx;
            this.columnIdx = columnIdx;
            this.isMax = isMax;
            this.handle = handle;
        }

        @Override
        public void open() throws HyracksDataException {
            initAccessAppendRef(ctx);

            // Read shared state from task registry; create if absent.
            filterState = FilterStateRegistry.getOrCreate(ctx, handle, isMax);

            super.open();
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            tAccess.reset(buffer);
            int nTuple = tAccess.getTupleCount();

            for (int t = 0; t < nTuple; t++) {
                tRef.reset(tAccess, t);

                byte[] data = tRef.getFieldData(columnIdx);
                int start = tRef.getFieldStart(columnIdx);
                int len = tRef.getFieldLength(columnIdx);

                if (len <= 0) {
                    appendTupleToFrame(t);
                    continue;
                }

                byte typeTag = data[start];

                // Pass NULL / MISSING / SYSTEM_NULL through unchanged.
                if (typeTag == ATypeTag.SERIALIZED_NULL_TYPE_TAG
                        || typeTag == ATypeTag.SERIALIZED_MISSING_TYPE_TAG
                        || typeTag == ATypeTag.SERIALIZED_SYSTEM_NULL_TYPE_TAG) {
                    appendTupleToFrame(t);
                    continue;
                }

                // Read-only filter: never updates shared state.
                if (filterState.passes(data, start, len)) {
                    appendTupleToFrame(t);
                }
            }
        }

        @Override
        public void flush() throws HyracksDataException {
            appender.flush(writer);
        }
    }
}