package org.apache.storm.hdfs.common.enums;

public enum CompressionTypeEnum {
	BZIP2("bzip2", "bz2"), GZIP("gzip","gz");
	
	private String extension;
	private String name;
	
	private CompressionTypeEnum(String name, String extension) {
		this.name = name;
		this.extension = extension;
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getExtension() {
		return extension;
	}

	public void setExtension(String extension) {
		this.extension = extension;
	}

	@Override
	public String toString() {
		return this.getName() + "\t" + this.getExtension();
	}

}
