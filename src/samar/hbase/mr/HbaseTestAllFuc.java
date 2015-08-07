package samar.hbase.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseTestAllFuc {
	private static Configuration conf;
	HTable table;

	HbaseTestAllFuc(String tableName, String[] colFams) throws IOException {
		conf = HBaseConfiguration.create();
		//conf.set("hbase.master","192.168.1.4:6000");
		conf.set("hbase.zookeeper.quorum" ,"192.168.1.3");
		conf.set("hbase.zookeeper.property.clientPort","2181");
		// conf.addResource(new
		// Path("/path_to_your_hbase/hbase-0.20.6/conf/hbase-site.xml"));
		//createTable( tableName, colFams);
		table = new HTable(conf, tableName);
	}

	void createTable( String tableName, String[] colFams) throws IOException {
		HBaseAdmin hbase = new HBaseAdmin(conf);
		HTableDescriptor desc = new HTableDescriptor(tableName);
		for (String eachColFam : colFams) {
			HColumnDescriptor meta = new HColumnDescriptor(eachColFam.getBytes());
			desc.addFamily(meta);
		}

		hbase.createTable(desc);
		
		hbase.close();
	}
	
	
	void dropTable(String tableName) throws IOException {
		HBaseAdmin hbaseAdmin = new HBaseAdmin(conf);
		hbaseAdmin.disableTable(Bytes.toBytes(tableName));
		hbaseAdmin.deleteTable(Bytes.toBytes(tableName));
		hbaseAdmin.close();
	}
	
	
	
	void incremementCounter() throws IOException{
		 table.incrementColumnValue(Bytes.toBytes("row1"), Bytes.toBytes("cf"), Bytes.toBytes("col"), 10L);
	}

	// assigns a value to a particular column of a record

	public void addAColumnEntry(String tableName, String row, String colFamilyName, String colName, String data) throws IOException {

		byte[] rowKey = Bytes.toBytes(row);
		Put putdata = new Put(rowKey);
		putdata.add(Bytes.toBytes(colFamilyName), Bytes.toBytes(colName), Bytes.toBytes(data));
		table.put(putdata);
		
		

	}

	public void addABulk(String tableName, String row, String colFamilyName, String colName) throws IOException {
		for (int j = 0; j < 1000; j++) {
			List<Put> batchPut =  new ArrayList<Put>();
			for (int i = 0; i < 2000; i++) {
				byte[] rowKey = Bytes.toBytes(j + row + i);
				Put putdata = new Put(rowKey);
				putdata.add(Bytes.toBytes(colFamilyName), Bytes.toBytes(colName), Bytes.toBytes("default"));
				
				batchPut.add(putdata);
			}
			
			table.put(batchPut);
		}
	}

	// returns entry of a particular column of a record

	public String getColEntry(String tableName, String rowName, String colFamilyName, String colName) {
		System.out.println("HBase Get for TableName: " + tableName +" rowName: " +rowName +" column " + colFamilyName +":" +colName);
		String result = null;
		try {
			byte[] rowKey = Bytes.toBytes(rowName);
			Get getRowData = new Get(rowKey);
			//getRowData.setMaxVersions(2);
			Result res = table.get(getRowData);
			byte[] obtainedRow = res.getValue(Bytes.toBytes(colFamilyName), Bytes.toBytes(colName));
			//res.getValue(Bytes.toBytes(colFamilyName), Bytes.toBytes("));
			result = Bytes.toString(obtainedRow);
		} catch (IOException e) {
			System.out.println("Exception occured in retrieving data");
		}
		return result;
	}

	// returns an arraylist of all entries of a column.

	public ArrayList<String> getCol(String tableName, String colFamilyName, String colName) {
		ArrayList<String> al = new ArrayList<String>();
		ResultScanner rs = null;
		Result res = null;

		try {

			Scan scan = new Scan();
			scan.addColumn(Bytes.toBytes(colFamilyName), Bytes.toBytes(colName));
			
			rs = table.getScanner(scan);
			while ((res = rs.next()) != null) {
				String colEntry = null;
				byte[] obtCol = res.getValue(Bytes.toBytes(colFamilyName), Bytes.toBytes(colName));
				colEntry = Bytes.toString(obtCol);
				al.add(colEntry);
			}

		} catch (IOException e) {
			System.out.println("Exception occured in retrieving data");
		} finally {
			rs.close();
		}
		return al;

	}

	// returns a list of hashmaps, each hashmap containing entries of a single
	// record.

	public ArrayList<HashMap<String, String>> getTable(String tableName, String[] colFamilyName, String[][] colName) {
		ResultScanner rs = null;
		ArrayList<HashMap<String, String>> al = new ArrayList<HashMap<String, String>>();
		Result res = null;
		try {
			Scan scan = new Scan();
			rs = table.getScanner(scan);
			while ((res = rs.next()) != null) {
				HashMap<String, String> map = new HashMap<String, String>();
				String s = null;
				for (int i = 0; i < colFamilyName.length; i++) {
					for (int j = 0; j < colName[i].length; j++) {
						byte[] obtainedRow = res.getValue(Bytes.toBytes(colFamilyName[i]), Bytes.toBytes(colName[i][j]));
						s = Bytes.toString(obtainedRow);
						map.put(colName[i][j], s);
					}
				}
				al.add(map);
			}
		} catch (IOException e) {
			System.out.println("Exception occured in retrieving data");
		} finally {
			rs.close();
		}
		return al;
	}

	// function to delete a row from the table.

	public String deleteTableRow(String tableName, String rowName) {
		String result = null;
		try {
			byte[] rowKey = Bytes.toBytes(rowName);
			Delete delRowData = new Delete(rowKey);
			table.delete(delRowData);
		} catch (IOException e) {
			System.out.println("Exception occured in retrieving data");
		}
		return result;

	}

	// function implementing having clause.

	public ArrayList<HashMap<String, String>> filterWhere(String tableName, String colFamilyName, String havingColName, String value, String[] resultColName) {
		ResultScanner rs = null;
		ArrayList<HashMap<String, String>> al = new ArrayList<HashMap<String, String>>();
		Result res = null;
		try {
			HTable table = new HTable(conf, tableName);
			Scan scan = new Scan();
			SingleColumnValueFilter singleColumnValueFilterA = new SingleColumnValueFilter(Bytes.toBytes(colFamilyName), 
					Bytes.toBytes(havingColName),CompareOp.EQUAL, Bytes.toBytes(value));

			singleColumnValueFilterA.setFilterIfMissing(true);
			FilterList filter = new FilterList(Operator.MUST_PASS_ALL, Arrays.asList((Filter) singleColumnValueFilterA));
			scan.setFilter(filter);
			rs = table.getScanner(scan);
			while ((res = rs.next()) != null) {
				HashMap<String, String> map = new HashMap<String, String>();
				String s = null;
				for (int j = 0; j < resultColName.length; j++) {
					byte[] obtainedRow = res.getValue(Bytes.toBytes(colFamilyName), Bytes.toBytes(resultColName[j]));
					System.out.println(resultColName[j]);
					s = Bytes.toString(obtainedRow);
					map.put(resultColName[j], s);
				}
				al.add(map);
			}
		} catch (IOException e) {
			System.out.println("Exception occured in retrieving data");
		} finally {
			rs.close();
		}
		return al;
	}

	public static void main(String args[]) throws IOException {

		String tableName = "testing_table_5";
		String[] colFamilyNames = { "name", "add" };
		String[][] colNames = { { "first", "second" }, { "house", "street" } };

		HbaseTestAllFuc test = new HbaseTestAllFuc(tableName, colFamilyNames);

		String rowKey = "row1";

		test.addAColumnEntry(tableName, rowKey, colFamilyNames[0], colNames[0][0], "Ram");
		test.addAColumnEntry(tableName, rowKey, colFamilyNames[0], colNames[0][1], "Kumar");
		test.addAColumnEntry(tableName, rowKey, colFamilyNames[1], colNames[1][0], "10");
		test.addAColumnEntry(tableName, rowKey, colFamilyNames[1], colNames[1][1], "8th main");

		String rowKey2 = "row2";

		test.addAColumnEntry(tableName, rowKey2, colFamilyNames[0], colNames[0][0], "Krishna");
		test.addAColumnEntry(tableName, rowKey2, colFamilyNames[0], colNames[0][1], "Das");
		test.addAColumnEntry(tableName, rowKey2, colFamilyNames[1], colNames[1][0], "9");
		test.addAColumnEntry(tableName, rowKey2, colFamilyNames[1], colNames[1][1], "6th main");

		//do bulk add
	   	test.addABulk(tableName , rowKey , colFamilyNames[0] ,  colNames[0][0]);
		//System.out.println("finished bulk upload");
		
		// specify the rowKey as per your table

		String value = test.getColEntry(tableName, rowKey, colFamilyNames[0], colNames[0][0]);
		System.out.println("result got from single get is " + value);

		ArrayList<HashMap<String, String>> listofmaps = new ArrayList<HashMap<String, String>>();
		listofmaps = test.getTable(tableName, colFamilyNames, colNames);
		System.out.println("Scan result :" + listofmaps);

		ArrayList<HashMap<String, String>> wherefilterresult = test.filterWhere(tableName, colFamilyNames[0], 
				colNames[0][0], "Ram", colNames[0]);
		System.out.println("where filter result :" + wherefilterresult);
		// specify the rowKey as per your table

		//test.deleteTableRow(tableName, rowKey);
		
		//test.dropTable( tableName);
	}

}