package org.apache.storm.hdfs.bolt.format;

import java.util.Map;

import org.apache.hadoop.fs.Path;

import backtype.storm.task.TopologyContext;

@SuppressWarnings("serial")
public class DefaultPartitionedFileNameFormat implements PartitionedFileNameFormat {
    private String path = "/data/default";
    private String prefix = "";
    private String extension = ".txt";
    private String partitionKey = "";

    /**
     * Overrides the default prefix.
     *
     * @param prefix
     * @return
     */
    public DefaultPartitionedFileNameFormat withPrefix(String prefix){
        this.prefix = prefix;
        return this;
    }

    /**
     * Overrides the default file extension.
     *
     * @param extension
     * @return
     */
    public DefaultPartitionedFileNameFormat withExtension(String extension){
        this.extension = extension;
        return this;
    }

    public DefaultPartitionedFileNameFormat withPath(String path){
        this.path = path;
        return this;
    }
    
    public DefaultPartitionedFileNameFormat withPartitionKey(String partitionKey){
        this.partitionKey = partitionKey;
        return this;
    }

    @Override
    public void prepare(Map conf, TopologyContext topologyContext) {
    }

    @Override
    public String getName(long rotation, long timeStamp) {
        return this.prefix + "_" + timeStamp + "_" + rotation + "_" + partitionKey.replaceAll("-", "_") + this.extension;
    }

    @Override
    public String getPath(){
        return this.path;
    }
    
    @Override
    public String getFullPath(){
        return new Path(this.path, this.partitionKey.replaceAll("-", "_")).toString();
    }
    
    @Override
	public String getPartition() {
		return this.partitionKey;
	}
}
