package com.poc.standard.topology.test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersistancePreprationBolt {

	private static final Logger LOG = LoggerFactory
			.getLogger(PersistancePreprationBolt.class);

	public void execute(
			Map<String, Map<String, List<List<Integer>>>> aggregatedMap,
			ConcurrentMap<String, List<String>> totalMap) {

		Map<String, List<String>> totalRecords = new HashMap<String, List<String>>();
		int keyIndex = 0;
		
		for (Entry<String, Map<String, List<List<Integer>>>> entry : aggregatedMap
				.entrySet()) {		
			
			Map<String, List<List<Integer>>> aggregation = entry.getValue();
			List<String> rowTOSaveNoSQL = new ArrayList<String>();
			for (Entry<String, List<List<Integer>>> row : aggregation
					.entrySet()) {

				List<List<Integer>> holderList = row.getValue();
				List<Integer> indexes = holderList.get(0);
				List<Integer> totalImpressions = holderList.get(1);

				float avgBid = 0;
				String date = "";
				String pub = "";
				String geo = "";
				for (Integer index : indexes) {
					List<String> logRow = totalMap.get(index.toString());
					String bid = logRow.get(3);
					avgBid = avgBid + Float.parseFloat(bid);
					// date 1
					date = logRow.get(4);
					pub = logRow.get(1);
					geo = logRow.get(0);
				}
				// date 1
				rowTOSaveNoSQL.add(getDateUptoMinute(date));
				// pub 2
				rowTOSaveNoSQL.add(pub);
				// geo 3
				rowTOSaveNoSQL.add(geo);
				// System.out.println(" before doing avg avgBid  "+avgBid+" freq "+(float)freq.get(0).intValue());
				avgBid = avgBid / (float) indexes.size();
				// System.out.println(" avgBid  "+avgBid);
				// avg bid 4
				rowTOSaveNoSQL.add(Float.toString(avgBid));
				// total impression 5
				rowTOSaveNoSQL.add(Integer.toString(totalImpressions.size()));
				// uniques  6
				rowTOSaveNoSQL.add(Integer.toString(indexes.size()));
				keyIndex++;
				totalRecords.put(new Integer(keyIndex).toString(),rowTOSaveNoSQL);
			}
		}
		System.out.println(" totalRecords  " + totalRecords);
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
