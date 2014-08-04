package com.poc.standard;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class BatchSizeFilterBolt extends BaseBasicBolt {

	private static final Logger LOG = LoggerFactory
			.getLogger(BatchSizeFilterBolt.class);
	
    // TODO not thread safe
	ConcurrentHashMap<String, List<String>> concurrentMap = new  ConcurrentHashMap<String, List<String>>();


	int batchSize;
	public BatchSizeFilterBolt(int batchSize) {
		this.batchSize = batchSize;
		// TODO Auto-generated constructor stub
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {

		List<String> row = new ArrayList<String>();
		row.add(input.getString(1));
		row.add(input.getString(2));
		row.add(input.getString(4));
		row.add(input.getString(5));
		row.add(input.getString(7));
	
		concurrentMap.put(input.getString(0), row);
		if(concurrentMap.size() == this.batchSize){
			collector.emit(new Values(concurrentMap));
			 concurrentMap = new ConcurrentHashMap<String, List<String>>();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("batch"));
    }

}
