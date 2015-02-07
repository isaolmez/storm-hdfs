package org.apache.storm.hdfs.bolt.rotation;
public enum TimeUnit {

    SECONDS((long)1000),
    MINUTES((long)1000*60),
    HOURS((long)1000*60*60),
    DAYS((long)1000*60*60*24);

    private long milliSeconds;

    private TimeUnit(long milliSeconds){
        this.milliSeconds = milliSeconds;
    }

    public long getMilliSeconds(){
        return milliSeconds;
    }
}