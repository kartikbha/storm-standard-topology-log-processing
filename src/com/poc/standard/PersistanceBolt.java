package com.poc.standard;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;

public class PersistanceBolt extends BaseRichBolt {

	public void execute(Tuple input, BasicOutputCollector collector) {

		Map<String,List<String>> recordToSave =  (Map<String, List<String>>) input.getValueByField("recordToPersist");
		
		for (Entry<String, List<String>> entry : recordToSave.entrySet()) {
			//System.out.println(" row  "+entry.getValue());
		 }
		System.out.println("recordToSave  "+recordToSave);
		DBCollection collection = db.getCollection("testcollection");
		collection.insert(new BasicDBObject(recordToSave));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	DB db;
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		Mongo mongo = null;
		try {
			mongo = new Mongo("localhost", 27017);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		db = mongo.getDB("test");
		
		
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		
	}

	
}
