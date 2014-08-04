package com.poc.standard.topology.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

import com.poc.standard.AnalysisLogicBolt;
import com.poc.standard.BatchSizeFilterBolt;
import com.poc.standard.LogGenerationSpout;
import com.poc.standard.PersistanceBolt;
import com.poc.standard.PersistancePreprationBolt;

public class ChimperLogAnalysisTopology {

	private static final Logger logger = LoggerFactory
			.getLogger(ChimperLogAnalysisTopology.class);

	public static void main(String[] args) throws Exception {

		try {

			TopologyBuilder builder = new TopologyBuilder();
			// Topology configuration
			Config conf = new Config();
			conf.setNumAckers(10);
			conf.setMessageTimeoutSecs(300);
			conf.setMaxSpoutPending(100);
			conf.setDebug(false);
			
			conf.setNumWorkers(4);
			// conf.setMaxTaskParallelism(3);
			int batchSizeFilterBoltExecutor = 1;
			int batchSizeFilterBoltTask = 1;

			int analysisLogicBoltExecutor = 1;
			int analysisLogicBoltTask = 1;

			int persistancePrepreationBoltExecutor = 1;
			int persistancePrepreationBoltTask = 1;

			int persistanceBoltExector = 1;
			int persistanceBoltTask = 1;
			
			int logGenerationSpoutExecutor = 1;
			
			int batchSize = 50;

			if(args.length!=10){
				throw new Exception("Not enough numbers of input argument.");
			}
			if (args != null && args.length > 0) {
				for (int i = 0; i < args.length; i++) {
					
					
					batchSizeFilterBoltExecutor = Integer.parseInt(args[0]);
					batchSizeFilterBoltTask = Integer.parseInt(args[1]);

					analysisLogicBoltExecutor = Integer.parseInt(args[2]);
					analysisLogicBoltTask = Integer.parseInt(args[3]);

					persistancePrepreationBoltExecutor = Integer
							.parseInt(args[4]);
					persistancePrepreationBoltTask = Integer.parseInt(args[5]);

					persistanceBoltExector = Integer.parseInt(args[6]);
					
					persistanceBoltTask = Integer.parseInt(args[7]);
					
					batchSize = Integer.parseInt(args[8]);
					
					logGenerationSpoutExecutor = Integer.parseInt(args[9]);
				}

			}
			
			

			builder.setSpout("LogGenerationSpout", new LogGenerationSpout(
					batchSize), logGenerationSpoutExecutor);
			builder.setBolt("BatchSizeFilterBolt",
					new BatchSizeFilterBolt(batchSize),
					batchSizeFilterBoltExecutor)
					.setNumTasks(batchSizeFilterBoltTask)
					.shuffleGrouping("LogGenerationSpout");

			builder.setBolt("AnalysisLogicBolt", new AnalysisLogicBolt(),
					analysisLogicBoltExecutor)
					.setNumTasks(analysisLogicBoltTask)
					.shuffleGrouping("BatchSizeFilterBolt");

			builder.setBolt("PersistancePrepreationBolt",
					new PersistancePreprationBolt(),
					persistancePrepreationBoltExecutor)
					.setNumTasks(persistancePrepreationBoltTask)
					.shuffleGrouping("AnalysisLogicBolt");

			builder.setBolt("PersistanceBolt", new PersistanceBolt(),
					persistanceBoltExector).setNumTasks(persistanceBoltTask)
					.shuffleGrouping("PersistancePrepreationBolt");

			if (args != null && args.length > 0) {
				// Submit topology
				StormSubmitter.submitTopology("logProcessing", conf,
						builder.createTopology());

			} else {
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology("logProcessing", conf,
						builder.createTopology());
			}
		} catch (AlreadyAliveException | InvalidTopologyException e) {
			logger.error("RequestException", e);
		}
	}

}
