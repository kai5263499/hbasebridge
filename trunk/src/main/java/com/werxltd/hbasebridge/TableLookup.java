package com.werxltd.hbasebridge;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.RegionOfflineException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.werxltd.jsonrpc.events.JSONRPCMessage;
import com.werxltd.jsonrpc.events.JSONRPCMessageEvent;

public class TableLookup {
	protected final static Logger LOG = Logger.getLogger(TableLookup.class);

	private HBaseConfiguration hbconf;
	private HBaseAdmin hbadmin;
	private HTable hbtable;
	private String tablename;

	private int numversions = 1;

	private int MAXSCANRESULTS = 100;
	private HashMap<String, ResultScanner> scanMap;

	public TableLookup() throws MasterNotRunningException {
		LOG.info("TableLookup loaded.");
		hbconf = new HBaseConfiguration();
		hbconf.set("hbase.client.retries.number", "2");

		hbadmin = new HBaseAdmin(hbconf);
		HBaseAdmin.checkHBaseAvailable(hbconf);
	}

	private void connect() throws Exception {

		if (!hbadmin.isMasterRunning())
			throw new Exception("Cannot find master server!");

		LOG.info("connect() successful");
	}

	private void disconnect() throws Exception {
		hbtable = null;
	}

	public JSONObject lookup(JSONObject params) throws Exception {
		JSONObject result = new JSONObject();
		connect();

		LOG.info("lookup params: " + params.toString());

		if (params.has("versions"))
			numversions = params.getInt("versions");
		else {
			numversions = 1;
		}

		if (!params.has("table"))
			throw new Exception("Unspecified table.");
		tablename = params.getString("table");
		if (params.has("keys")) {
			JSONArray rowkeys = params.getJSONArray("keys");
			for (int k = 0; k < rowkeys.length(); k++) {
				result.append("rows", getrow(rowkeys.getString(k)));
			}
		} else if (params.has("filter")) {
			result.put("rows", scanrow(params.getString("filter")));
		} else {
			throw new Exception("Unspecified row keys or filter.");
		}

		for (int k = 0; k < rowkeys.length(); k++) {
			result.append("rows", getrow(rowkeys.getString(k)));
		}

		disconnect();
		return result;
	}

	private Get getByKey(String rowkey) throws IOException {
		Get get = new Get(rowkey.getBytes());
		if (numversions > 1) {
			get = get.setMaxVersions(numversions);
		} else {
			// get = get.setMaxVersions();
		}
		return get;
	}

	/*
	 * This function takes an HBase Result object and iterates over it,
	 * collecting all available family:column values. URLencoding is needed to
	 * prevent ints stored as bytecode (WHY?!) from mucking with json
	 * encode/decode methods. Hopefully that will change one day -Wes
	 */
	private JSONObject scrapeResult(Result r)
			throws UnsupportedEncodingException, JSONException {
		JSONObject result = new JSONObject();

		// LOG.info("Result"+r.toString());

		if (numversions > 1) {
			TreeMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map1 = (TreeMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>) r
					.getMap();
			Iterator<byte[]> map1Iterator = map1.keySet().iterator();
			while (map1Iterator.hasNext()) {
				byte[] cur_key = (byte[]) map1Iterator.next();
				String familyname = new String(cur_key);
				TreeMap<byte[], NavigableMap<Long, byte[]>> map2 = (TreeMap<byte[], NavigableMap<Long, byte[]>>) map1
						.get(cur_key);
				Iterator<byte[]> map2Iterator = map2.keySet().iterator();
				while (map2Iterator.hasNext()) {
					byte[] col_key = (byte[]) map2Iterator.next();
					String columnname = new String(col_key);

					TreeMap<Long, byte[]> map3 = (TreeMap<Long, byte[]>) map2
							.get(col_key);
					Iterator<Long> map3Iterator = map3.keySet().iterator();
					while (map3Iterator.hasNext()) {
						JSONObject versions = new JSONObject();
						Long version = map3Iterator.next();
						String value = new String((byte[]) map3.get(version));
						versions.put("version", version);
						versions
								.put("value", URLEncoder.encode(value, "UTF-8"));
						result.append(familyname + ":" + columnname, versions);
					}
				}
			}
		} else {
			TreeMap<byte[], NavigableMap<byte[], byte[]>> map1 = (TreeMap<byte[], NavigableMap<byte[], byte[]>>) r
					.getNoVersionMap();
			Iterator<byte[]> map1Iterator = map1.keySet().iterator();
			while (map1Iterator.hasNext()) {
				byte[] cur_key = (byte[]) map1Iterator.next();
				String familyname = new String(cur_key);
				TreeMap<byte[], byte[]> map2 = (TreeMap<byte[], byte[]>) map1
						.get(cur_key);
				Iterator<byte[]> map2Iterator = map2.keySet().iterator();
				while (map2Iterator.hasNext()) {
					byte[] col_key = (byte[]) map2Iterator.next();
					String columnname = new String(col_key);
					String value = new String((byte[]) map2.get(col_key));
					LOG.info("Got value [" + value + "]");
					result.put(familyname + ":" + columnname, URLEncoder
							.encode(value, "UTF-8"));
				}
			}
		}

		return result;
	}

	private JSONObject handleGenericTableLookup(String rowkey) throws Exception {
		JSONObject result = new JSONObject();

		Get get = null;
		Result r = null;

		LOG.info("handleGenericTableLookup for table [" + tablename
				+ "] of key [" + rowkey + "]");

		if (hbtable == null) {
			if (!hbadmin.tableExists(tablename))
				throw new Exception("Specified table [" + tablename
						+ "] does not exist.");
			hbtable = new HTable(hbconf, tablename);
		}

		get = getByKey(rowkey);
		if (hbtable.exists(get)) {
			r = hbtable.get(get);
		} else {
			String rowkeylower = rowkey.toLowerCase();
			get = getByKey(rowkeylower);
			if (hbtable.exists(get)) {
				rowkey = rowkeylower;
				r = hbtable.get(get);
			} else {
				String rowkeyupper = rowkey.toUpperCase();
				get = getByKey(rowkeyupper);
				if (hbtable.exists(get)) {
					rowkey = rowkeyupper;
					r = hbtable.get(get);
				} else {
					result.put(rowkey, (Map<byte[], byte[]>) null);
					return result;
				}
			}
		}

		result.put(rowkey, scrapeResult(r));

		return result;
	}

	private JSONObject getrow(String rowkey) throws Exception {
		try {
			return handleGenericTableLookup(rowkey);
		} catch (NotServingRegionException e) {
			LOG.error("NotServingRegionException caught, skipping record");
			return new JSONObject();
		} catch (RetriesExhaustedException e) {
			LOG.error("RetriesExhaustedException caught, skipping record");
			return new JSONObject();
		}
	}

	private JSONArray scanrow(String filterValue) throws ServletException,
			Exception {

		String scanId = tablename + ":" + filterValue;

		LOG.info("Looking for scanId: " + scanId);

		if (scanMap == null) {
			LOG.info("Creating new scanMap");
			scanMap = new HashMap<String, ResultScanner>();
		}

		if (hbtable == null) {
			LOG.info("Connecting to HTable");
			if (!hbadmin.tableExists(tablename))
				throw new ServletException("Specified table [" + tablename
						+ "] does not exist.");
			hbtable = new HTable(hbconf, tablename);
		}

		ResultScanner resultscanner = scanMap.get(scanId);
		if (resultscanner == null) {
			LOG.info("Creating new scanner");

			byte[][] columnsArray = getAllColumns(hbtable);

			Filter filter = new ValueFilter(CompareOp.EQUAL,
					new RegexStringComparator(filterValue));

			Scan scan = new Scan();
			scan.addColumns(columnsArray);
			scan.setFilter(filter);

			if (numversions < 1) {
				scan.setMaxVersions(scan.getMaxVersions());
				LOG.info("scan getMaxVersions: " + scan.getMaxVersions());
			} else if (numversions > 1) {
				LOG.info("scan numversions: " + numversions);
				scan.setMaxVersions(numversions);
			} else {
				scan.setMaxVersions();
			}

			resultscanner = hbtable.getScanner(scan);

			scanMap.put(scanId, resultscanner);
		}

		JSONArray result = new JSONArray();

		LOG.info("resultscanner next(" + MAXSCANRESULTS + ")");
		Result r[] = resultscanner.next(MAXSCANRESULTS);

		LOG.info("processing " + r.length + " scan results");

		for (int c = 0; c < r.length; c++) {
			if (r[c] == null) {
				resultscanner.close();
				scanMap.remove(scanId);
				break;
			}

			JSONObject iresult = new JSONObject();

			String key = Bytes.toString(r[c].raw()[0].getRow());

			iresult.put(key, scrapeResult(r[c]));

			result.put(iresult);
		}

		return result;
	}

	private byte[][] getAllColumns(HTable table) throws IOException {
		HColumnDescriptor[] cds = table.getTableDescriptor()
				.getColumnFamilies();
		byte[][] columns = new byte[cds.length][];
		for (int i = 0; i < cds.length; i++) {
			columns[i] = cds[i].getNameWithColon();
		}
		return columns;
	}

	public JSONObject resetScanner(JSONObject params) throws Exception {
		JSONObject result = new JSONObject();

		if (!params.has("key")) {
			throw new Exception("No scanner key specified");
		}

		result.put("status", scanMap.remove(params.getString("key")));

		return result;
	}

	public JSONObject resetScanners() throws JSONException {
		JSONObject result = new JSONObject();

		result.put("items", scanMap.size());

		scanMap.clear();

		return result;
	}

	public void messageReceived(JSONRPCMessageEvent me) {
		switch (me.message().getCode()) {
		case JSONRPCMessage.INIT:
			ServletConfig config = me.message().getServletConfig();
			if (config.getInitParameter("maxresults") != null)
				MAXSCANRESULTS = Integer.parseInt(config
						.getInitParameter("maxresults"));
			break;
		}
	}
}
