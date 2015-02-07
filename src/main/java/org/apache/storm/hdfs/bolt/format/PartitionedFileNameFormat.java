package org.apache.storm.hdfs.bolt.format;

public interface PartitionedFileNameFormat extends FileNameFormat {

	/**
	 * Returns the partition key
	 * @return
	 */
	String getPartition();

	/**
	 * Gets the full path with partition key
	 * @return
	 */
	String getFullPath();
}
