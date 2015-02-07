package org.apache.storm.hdfs.bolt.sync;

import org.apache.storm.hdfs.bolt.rotation.TimeUnit;

import backtype.storm.tuple.Tuple;

public class TimedSyncPolicy implements SyncPolicy {

    private long interval;

    public TimedSyncPolicy(float count, TimeUnit units){
        this.interval = (long)(count * units.getMilliSeconds());
    }

    /**
     * Called for every tuple the HdfsBolt executes.
     *
     * @param tuple  The tuple executed.
     * @param offset current offset of file being written
     * @return true if a file rotation should be performed
     */
    @Override
    public boolean mark(Tuple tuple, long offset) {
        return false;
    }

    /**
     * Called after the HdfsBolt rotates a file.
     */
    @Override
    public void reset() {

    }

    public long getInterval(){
        return this.interval;
    }
}
