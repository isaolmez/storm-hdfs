/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.hdfs.bolt;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.storm.hdfs.bolt.format.PartitionedFileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TickingFileRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.bolt.sync.TimedSyncPolicy;
import org.apache.storm.hdfs.common.rotation.RotationActionWithLocalFS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;

public abstract class AbstractExportManager implements Serializable {
	private static final Logger LOG = LoggerFactory.getLogger(AbstractExportManager.class);

	protected ArrayList<RotationActionWithLocalFS> rotationActions = new ArrayList<RotationActionWithLocalFS>();
	private Path currentFile;
	protected OutputCollector collector;
	protected transient FileSystem localFs;
	protected transient FileSystem distributedFs;
	protected transient FileSystem writerFs;
	protected SyncPolicy syncPolicy;
	protected RecordFormat recordFormat;
	protected FileRotationPolicy rotationPolicy;
	protected PartitionedFileNameFormat fileNameFormat;
	protected int rotation = 0;
	protected long offset = 0;
	protected String fsUrl;
	protected String configKey;
	protected transient Object writeLock;
	protected transient Timer rotationTimer; // only used for TimedRotationPolicy
	protected transient Timer syncTimer; // only used for TimedSyncPolicy

	protected transient Configuration hdfsConfig;

	protected void rotateOutputFile() throws IOException {
		LOG.info("Rotating output file...");
		long start = System.currentTimeMillis();
		synchronized (this.writeLock) {
			closeOutputFile();
			this.rotation++;
			
			Path newFile = createOutputFile();
			LOG.info("Performing {} file rotation actions.", this.rotationActions.size());
			for (RotationActionWithLocalFS action : this.rotationActions) {
				action.execute(this.localFs, this.distributedFs, this.currentFile);
			}
			this.currentFile = newFile;
		}
		
		long time = System.currentTimeMillis() - start;
		LOG.info("File rotation took {} ms.", time);
	}

	public final AbstractExportManager init() {
		this.writeLock = new Object();
        if (this.syncPolicy == null) {
        	throw new IllegalStateException("SyncPolicy must be specified.");
        }
        
        if (this.rotationPolicy == null) { 
        	throw new IllegalStateException("RotationPolicy must be specified.");
        }
        
        if (this.recordFormat == null) { 
        	throw new IllegalStateException("RecordFormat must be specified.");
        }
        
        if (this.fileNameFormat == null) { 
        	throw new IllegalStateException("FileNameFormat must be specified.");
        }
		
        /**
         * TickingFileRotationPolicy enables to rotate files getting no writes for a specified time period
         * 
         */
		if (this.rotationPolicy instanceof TickingFileRotationPolicy) {
			long interval = ((TickingFileRotationPolicy) this.rotationPolicy).getInterval();
			this.rotationTimer = new Timer(true);
			TimerTask task = new TimerTask() {
				@Override
				public void run() {
					try {
						if(((TickingFileRotationPolicy) AbstractExportManager.this.rotationPolicy).shouldFinalize()){
							rotateOutputFile();	
							AbstractExportManager.this.offset = 0;
							AbstractExportManager.this.rotationPolicy.reset();
						}
					} catch (IOException e) {
						LOG.warn("IOException during scheduled file rotation.", e);
					}
				}
			};
			this.rotationTimer.scheduleAtFixedRate(task, interval, interval);
		}
		
		if (this.syncPolicy instanceof TimedSyncPolicy) {
			long interval = ((TimedSyncPolicy) this.syncPolicy).getInterval();
			this.syncTimer = new Timer(true);
			TimerTask task = new TimerTask() {
				@Override
				public void run() {
					try {
						syncOutputFile();
					} catch (IOException e) {
						LOG.warn("IOException during scheduled file rotation.", e);
					}
				}
			};
			this.syncTimer.scheduleAtFixedRate(task, interval, interval);
		}
		
		try {
			this.currentFile = this.createOutputFile();
		} catch (IOException e) {
			LOG.error("Error occured: {}", e.getMessage());
			throw new RuntimeException("Error preparing HdfsBolt: " + e.getMessage(), e);
		}
		
		return this;
	}

	abstract Path createOutputFile() throws IOException;

	abstract void writeToOutputFile(Tuple tuple, OutputCollector collector);
	
	abstract void syncOutputFile() throws IOException;
	
	abstract void closeOutputFile() throws IOException;
	
}
