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

public class AnalysisLogicBolt extends BaseBasicBolt {

	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {

		ConcurrentHashMap<String, List<String>> mapForAnalysis = (ConcurrentHashMap<String, List<String>>) input
				.getValueByField("batch");

		Map<String, List<Map<String, List<Integer>>>> aggregateMap = new HashMap<String, List<Map<String, List<Integer>>>>();
		Map<String, Map<String, List<Integer>>> tmpFreqMap = new HashMap<String, Map<String, List<Integer>>>();
		Map<String, Map<String, List<Integer>>> tmpIndexMap = new HashMap<String, Map<String, List<Integer>>>();
		Map<String, List<Integer>> tmpIndexListMap = new HashMap<String, List<Integer>>();

		// key1(geo_pub_time) ------> value(< geo_pub_time_index =
		// "lists of index", geo_pub_time_freq = frequency>)

		// iterate entry set above map again once done

		// get key ....parse value....get list of indexes
		// {mapForAnalysis}....calculate bid avg/frequency

		// lets impression as frequency now.
		System.out.println(" mapForAnalysis " + mapForAnalysis);

		List<String> keyGeoPubTimeList = new ArrayList<String>();
		List<String> keyGeoPubIndexList = new ArrayList<String>();

		Map<String, List<Integer>> geoPubTimeFreqMap = null;
		Map<String, List<Integer>> geoPubTimeIndexMap = null;
		List<Integer> indexList = null;
		for (Entry<String, List<String>> entry : mapForAnalysis.entrySet()) {
			List<String> logRow = entry.getValue();
			// key are geo,pub,time upto minute.
			String keyGeoPub = logRow.get(0) + logRow.get(1);
			String keyGeoPubTimePub = keyGeoPub
					+ getDateUptoMinute(logRow.get(4));

			if (!keyGeoPubTimeList.contains(keyGeoPubTimePub + "freq")) {
				keyGeoPubTimeList.add(keyGeoPubTimePub + "freq");
				geoPubTimeFreqMap = new HashMap<String, List<Integer>>();
				tmpFreqMap.put(keyGeoPubTimePub + "freq", geoPubTimeFreqMap);
			} else {
				geoPubTimeFreqMap = tmpFreqMap.get(keyGeoPubTimePub + "freq");
			}

			if (!keyGeoPubIndexList.contains(keyGeoPubTimePub + "index")) {
				keyGeoPubIndexList.add(keyGeoPubTimePub + "index");
				geoPubTimeIndexMap = new HashMap<String, List<Integer>>();
				indexList = new ArrayList<Integer>();
				tmpIndexMap.put(keyGeoPubTimePub + "index", geoPubTimeIndexMap);
				tmpIndexListMap.put(keyGeoPubTimePub + "index", indexList);

			} else {
				geoPubTimeIndexMap = tmpIndexMap
						.get(keyGeoPubTimePub + "index");
				indexList = tmpIndexListMap.get(keyGeoPubTimePub + "index");
			}

			collectFrequencyBasedOnKey(keyGeoPub, keyGeoPubTimePub,
					entry.getKey(), geoPubTimeFreqMap);

			collectIndexBasedOnKey(keyGeoPub, keyGeoPubTimePub, entry.getKey(),
					geoPubTimeIndexMap, indexList);

			// System.out.println(" geoPubTimeFreqMap " + geoPubTimeFreqMap);
			// System.out.println(" geoPubTimeIndexMap " + geoPubTimeIndexMap);

			List<Map<String, List<Integer>>> values = new ArrayList<Map<String, List<Integer>>>();
			values.add(geoPubTimeIndexMap);
			values.add(geoPubTimeFreqMap);

			// ConcurrentHashMap<String, List<String>> mapForAnalysis

			aggregateMap.put(keyGeoPubTimePub, values);

		}

		Values val = new Values();
		val.add(mapForAnalysis);
		val.add(aggregateMap);
		collector.emit(val);

		// System.out.println(" aggregateMap " + aggregateMap);

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declare(new Fields("mapForAnalysis","aggregatedBatch"));

	}

	private void collectIndexBasedOnKey(String keyGeoPub,
			String keyGeoPubTimePub, String idIndex,
			Map<String, List<Integer>> geoPubTimeIndexMap,
			List<Integer> indexList) {

		indexList.add(new Integer(Integer.parseInt(idIndex)));
		geoPubTimeIndexMap.put(keyGeoPubTimePub + "index", indexList);
	}

	// NY_pub1_time1 --> <NY_pub1_time1_index --> [2,3,4,5,17],
	// NY_pub1_time1_freq--> [5]> 9 5
	// NY_pub1_time2 --> <NY_pub1_time1_index --> [12,13],
	// NY_pub1_time1_freq-->[2]> 9 2
	// NY_pub1_time3 --> <NY_pub1_time1_index --> [8,9], NY_pub1_time1_freq-->
	// [2]> 9 2
	// impression -- NY_pub , count - 9

	private void collectFrequencyBasedOnKey(String keyGeoPub,
			String keyGeoPubTime, String idIndex,
			Map<String, List<Integer>> geoPubTimeFreqMap) {

		if (geoPubTimeFreqMap.containsKey(keyGeoPubTime + "freq")) {

			Integer freq = geoPubTimeFreqMap.get(keyGeoPubTime + "freq").get(0);
			List<Integer> newfreq = new ArrayList<Integer>();
			newfreq.add(freq + 1);
			geoPubTimeFreqMap.put(keyGeoPubTime + "freq", newfreq);

		} else {
			List<Integer> freq = new ArrayList<Integer>();
			freq.add(1);
			geoPubTimeFreqMap.put(keyGeoPubTime + "freq", freq);
		}

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
}
