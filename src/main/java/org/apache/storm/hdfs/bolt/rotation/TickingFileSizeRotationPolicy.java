package org.apache.storm.hdfs.bolt.rotation;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Tuple;

public class TickingFileSizeRotationPolicy implements TickingFileRotationPolicy {
    private static final Logger LOG = LoggerFactory.getLogger(TickingFileSizeRotationPolicy.class);
    
    // File size 
    private long maxBytes;
    private long lastOffset = 0;
    private long currentBytesWritten = 0;
    
    // Time limit
    private long lastTimeWritten = System.currentTimeMillis();
    private long timeLimit = 0;
    private boolean ticking = false;
    
    public TickingFileSizeRotationPolicy(float count, Units units){
        this.maxBytes = (long)(count * units.getByteCount());
    }

    public TickingFileSizeRotationPolicy withTimeLimit(float count, TimeUnit units){
        this.ticking = true;
    	this.timeLimit = (long)(count * units.getMilliSeconds());
        return this;
    }
    
    @Override
    public boolean mark(Tuple tuple, long offset) {
        long diff = offset - this.lastOffset;
        this.currentBytesWritten += diff;
        this.lastOffset = offset;
        if(ticking){
        	this.lastTimeWritten = System.currentTimeMillis();
        }
        
        return this.currentBytesWritten >= this.maxBytes;
    }

    @Override
    public void reset() {
        this.currentBytesWritten = 0;
        this.lastOffset = 0;
        if(ticking){
        	this.lastTimeWritten = System.currentTimeMillis();	
        }
    }

	@Override
	public boolean shouldFinalize() {
		if(ticking){
			if((System.currentTimeMillis() - this.lastTimeWritten) >= this.timeLimit){
				if(this.currentBytesWritten != 0){
					return true;	
				}
			}
		}
		
		return false;
	}

	@Override
	public long getInterval() {
		return this.timeLimit/2;
	}
}
