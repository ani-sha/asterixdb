package org.apache.asterix.runtime.operators.plaque;

import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.base.AbstractStateObject;

public class FilterState extends AbstractStateObject {

    private static final long serialVersionUID = 1L;

    private final boolean isMax;
    private final IBinaryComparator comparator;

    private byte[] thresholdBytes;
    private int thresholdLength;
    private boolean initialized;

    public FilterState(boolean isMax) {
        super();
        this.isMax = isMax;
        this.comparator = new IBinaryComparator() {
            @Override
            public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
                    throws HyracksDataException {
                int minLen = Math.min(l1, l2);
                for (int i = 0; i < minLen; i++) {
                    int diff = (b1[s1 + i] & 0xff) - (b2[s2 + i] & 0xff);
                    if (diff != 0) {
                        return diff;
                    }
                }
                return l1 - l2;
            }
        };
        this.thresholdBytes = null;
        this.thresholdLength = 0;
        this.initialized = false;
    }

    public void update(byte[] data, int start, int len) throws HyracksDataException {
        if (!initialized) {
            thresholdBytes = new byte[len];
            System.arraycopy(data, start, thresholdBytes, 0, len);
            thresholdLength = len;
            initialized = true;
            return;
        }

        int cmp = comparator.compare(data, start, len, thresholdBytes, 0, thresholdLength);
        if ((isMax && cmp > 0) || (!isMax && cmp < 0)) {
            if (thresholdBytes.length < len) {
                thresholdBytes = new byte[len];
            }
            System.arraycopy(data, start, thresholdBytes, 0, len);
            thresholdLength = len;
        }
    }

    public boolean passes(byte[] data, int start, int len) throws HyracksDataException {
        if (!initialized) {
            return true;
        }

        int cmp = comparator.compare(data, start, len, thresholdBytes, 0, thresholdLength);
        return isMax ? cmp >= 0 : cmp <= 0;
    }

    public boolean isInitialized() {
        return initialized;
    }

    public boolean isMax() {
        return isMax;
    }

    public int getThresholdLength() {
        return thresholdLength;
    }

    public byte[] getThresholdBytes() {
        return thresholdBytes;
    }
}