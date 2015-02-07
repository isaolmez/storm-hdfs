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
import java.net.URI;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.storm.hdfs.bolt.format.DefaultPartitionedFileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.bolt.utils.DeepCopier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

public class PartitionedHdfsBolt extends AbstractPartitionedHdfsBolt{
	private static final long serialVersionUID = 3323996312364546696L;
	private static final Logger LOG = LoggerFactory.getLogger(PartitionedHdfsBolt.class);

	public PartitionedHdfsBolt withFsUrl(String fsUrl){
        this.fsUrl = fsUrl;
        return this;
    }

    public PartitionedHdfsBolt withConfigKey(String configKey){
        this.configKey = configKey;
        return this;
    }
	
    public PartitionedHdfsBolt withExportManager(AbstractExportManager exportManager){
        this.exportManagerPrototype = exportManager;
        return this;
    }
    
    @Override
    public void doPrepare(Map conf, TopologyContext topologyContext, OutputCollector collector) throws IOException {
        LOG.info("Preparing HDFS Bolt...");
        this.distributedFs = FileSystem.get(URI.create(this.fsUrl), hdfsConfig);
        this.localFs = FileSystem.getLocal(new Configuration());
    }

    @Override
    public void execute(Tuple tuple) {
		String partitionKey = tuple.getStringByField("partition");
		
		if(!this.currentFiles.containsKey(partitionKey)){
			AbstractExportManager exportManager = new DefaultExportManager()
			.withFileNameFormat(((DefaultPartitionedFileNameFormat) DeepCopier.copy(this.exportManagerPrototype.fileNameFormat)).withPartitionKey(partitionKey))
			.withRecordFormat((RecordFormat) DeepCopier.copy(this.exportManagerPrototype.recordFormat))
			.withRotationPolicy((FileRotationPolicy) DeepCopier.copy(this.exportManagerPrototype.rotationPolicy))
			.withSyncPolicy((SyncPolicy) DeepCopier.copy(this.exportManagerPrototype.syncPolicy))
			.addRotationActions(this.exportManagerPrototype.rotationActions)
			.withDistributedFS(this.distributedFs)
			.withLocalFS(this.localFs)
			.useDistributedForWrite()
			.init();
			
			this.currentFiles.put(partitionKey, exportManager);
		}
		
		AbstractExportManager currentExportManager = this.currentFiles.get(partitionKey);
		currentExportManager.writeToOutputFile(tuple, collector);
    }

}
