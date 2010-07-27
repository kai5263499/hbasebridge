package com.werxltd.hbasebridge;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HServerLoad.RegionLoad;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.json.JSONException;
import org.json.JSONObject;

public class HBaseInfo {
	private HBaseConfiguration hbconf;
	private HBaseAdmin hbadmin;

	public HBaseInfo() {
		
	}

	private void connect() throws Exception {
		hbconf = new HBaseConfiguration();
		
		hbadmin = new HBaseAdmin(hbconf);
		HBaseAdmin.checkHBaseAvailable(hbconf);
		if (!hbadmin.isMasterRunning())
			throw new Exception("Cannot find master server!");
	}
	
	private JSONObject getServerStatus(HServerInfo hserverinfo) throws Exception {
		connect();
		
		JSONObject result = new JSONObject();
		result.put("hostname", hserverinfo.getName());
		result.put("server", hserverinfo.getServerName());
		result.put("startcode", hserverinfo.getStartCode());

		HServerLoad hserverload = hserverinfo.getLoad();

		result.put("load", hserverload.getLoad());
		result.put("maxheapmb", hserverload.getMaxHeapMB());
		result.put("memstoresizeinmb", hserverload.getMemStoreSizeInMB());
		result.put("numberofregions", hserverload.getNumberOfRegions());
		result.put("numberofrequests", hserverload.getNumberOfRequests());

		Collection<RegionLoad> regionsload = hserverload.getRegionsLoad();
		Iterator<RegionLoad> itr = regionsload.iterator();
		while (itr.hasNext()) {
			RegionLoad regionload = (RegionLoad) itr.next();
			JSONObject regionloadjson = new JSONObject();
			regionloadjson.put("name", regionload.getNameAsString());
			regionloadjson.put("storefilesizeinmb", regionload
					.getStorefileIndexSizeMB());
			regionloadjson.put("storefiles", regionload.getStorefiles());
			regionloadjson.put("stores", regionload.getStores());
			regionloadjson
					.put("memstoresizemb", regionload.getMemStoreSizeMB());
			regionloadjson.put("storefileindexsizemb", regionload
					.getStorefileIndexSizeMB());
			result.append("regionsload", regionloadjson);
		}

		result.put("storefileindexsizeinmb", hserverload
				.getStorefileIndexSizeInMB());
		result.put("storefiles", hserverload.getStorefiles());
		result.put("storefilesizeinmb", hserverload.getStorefileSizeInMB());
		result.put("usedheapmb", hserverload.getUsedHeapMB());
		result.put("hashcode", hserverload.hashCode());
		return result;
	}

	public JSONObject clusterstatus() throws Exception {
		connect();
		
		JSONObject result = new JSONObject();

		ClusterStatus clusterstatus = hbadmin.getClusterStatus();

		result.put("numregionservers", clusterstatus.getServers());
		result.put("numdeadservers", clusterstatus.getDeadServers());
		result.put("regionscount", clusterstatus.getRegionsCount());
		result.put("averageload", clusterstatus.getAverageLoad());
		result.put("hbaseversion", clusterstatus.getHBaseVersion());

		for (String servername : clusterstatus.getServerNames())
			result.append("servernames", servername);
		for (String deadservername : clusterstatus.getDeadServerNames())
			result.append("deadservernames", deadservername);
		for (HServerInfo hserverinfo : clusterstatus.getServerInfo())
			result.append("serverinfo", getServerStatus(hserverinfo));

		return result;
	}

	public JSONObject listtables() throws Exception {
		JSONObject result = new JSONObject();
		
		connect();
		HTableDescriptor[] tables = hbadmin.listTables();

		for (int i = 0; i < tables.length; i++) {
			result.append("tables", tables[i].getNameAsString());
		}

		return result;
	}
}
