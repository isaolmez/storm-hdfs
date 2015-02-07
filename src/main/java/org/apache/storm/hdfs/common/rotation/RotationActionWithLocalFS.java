package org.apache.storm.hdfs.common.rotation;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.Serializable;

public interface RotationActionWithLocalFS extends Serializable {
    void execute(FileSystem localFileSystem, FileSystem distributedFileSystem, Path filePath) throws IOException;
}
