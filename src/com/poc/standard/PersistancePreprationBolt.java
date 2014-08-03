package com.poc.standard;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class PersistancePreprationBolt extends BaseBasicBolt {

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {

		Map<String, List<Map<String, List<Integer>>>> aggregatedMap = (Map<String, List<Map<String, List<Integer>>>>) input
				.getValueByField("aggregatedBatch");
		
		ConcurrentHashMap<String, List<String>> mapForAnalysis = (ConcurrentHashMap<String, List<String>>) input
				.getValueByField("mapForAnalysis");
		
		
		Map<String,List<String>> totalRecords = new HashMap<String,List<String>>();
				
		int keyIndex = 0;
		
       for (Entry<String, List<Map<String, List<Integer>>>> entry : aggregatedMap.entrySet()) {
             	
			String key  = entry.getKey();
			List<Map<String, List<Integer>>> aggregation =  entry.getValue();
			List<Integer> indexes = aggregation.get(0).get(key+"index");
			List<Integer> freq = aggregation.get(1).get(key+"freq");
			List<String> rowTOSaveNoSQL = new ArrayList<String>();
				
			float avgBid = 0;
			String date="";
			String pub="";
			String geo="";
			for(Integer index : indexes){
				List<String> logRow = mapForAnalysis.get(index.toString());
				String bid = logRow.get(3);
				avgBid = avgBid + Float.parseFloat(bid);
				// date 1	
				date = logRow.get(4);
				pub = logRow.get(1);
				geo = logRow.get(0);
			}
			
			// date 1
			rowTOSaveNoSQL.add(getDateUptoMinute(date));
			// pub  2
			rowTOSaveNoSQL.add(pub);
			// geo  3
			rowTOSaveNoSQL.add(geo);
			
			//System.out.println(" before doing avg avgBid  "+avgBid+" freq "+(float)freq.get(0).intValue());
			avgBid = avgBid / (float)freq.get(0).intValue();
		   // System.out.println(" avgBid  "+avgBid);
			// avg bid 4
			rowTOSaveNoSQL.add(Float.toString(avgBid));
			// uniques
			rowTOSaveNoSQL.add(Float.toString( (float)freq.get(0).intValue()));
			
			keyIndex++;
			totalRecords.put(new Integer(keyIndex).toString(),rowTOSaveNoSQL);
			
		}
		
		
	     System.out.println(" aggregatedMap  "+aggregatedMap);
	  
		 collector.emit(new Values(totalRecords));
	}

	private String getDateUptoMinute(String field) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm");
		String dateUptoMinute = null;
		try {
			dateUptoMinute = df.format(df.parse(field));
		} catch (ParseException e) {
		}
		return dateUptoMinute;
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("recordToPersist"));
	}

}
