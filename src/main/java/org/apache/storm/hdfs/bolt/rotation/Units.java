package org.apache.storm.hdfs.bolt.rotation;
public enum Units {

    KB((long)Math.pow(2, 10)),
    MB((long)Math.pow(2, 20)),
    GB((long)Math.pow(2, 30)),
    TB((long)Math.pow(2, 40));

    private long byteCount;

    private Units(long byteCount){
        this.byteCount = byteCount;
    }

    public long getByteCount(){
        return byteCount;
    }
}