package org.apache.storm.hdfs.common.rotation;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.storm.hdfs.common.enums.CompressionTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompressFileAction implements RotationActionWithLocalFS {
	private static final long serialVersionUID = 8164963616544267185L;
	private static final Logger logger = LoggerFactory.getLogger(CompressFileAction.class);

    private CompressionTypeEnum compressionCodec = CompressionTypeEnum.BZIP2;

    public CompressFileAction withCompression(CompressionTypeEnum compressionCodec){
    	this.compressionCodec = compressionCodec;
        return this;
    }
    
    /**
	 * Compress the file
	 */
    @Override
    public void execute(FileSystem localFileSystem, FileSystem distributedFileSystem, Path filePath) throws IOException {
    	
    	String compressedFileName = filePath.toUri().toString() + "." + compressionCodec.getExtension();
    	final OutputStream out = new FileOutputStream(compressedFileName);
		CompressorOutputStream cos;
		try {
			cos = new CompressorStreamFactory().createCompressorOutputStream(compressionCodec.getName(), out);
			IOUtils.copy(new FileInputStream(filePath.toUri().toString()), cos);
			cos.close();
		} catch (CompressorException e) {
			logger.error("Error when compressing file {}: {}", filePath, e.getMessage());
		}
    }
}
