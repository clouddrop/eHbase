package samar.hbase.test;

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

public class HbaseTestAllFuc2 {
	private static Configuration conf;
	HTable table;

	HbaseTestAllFuc2(String tableName, String[] colFams ,String zooKeeper) throws IOException {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum" ,"gs1131.blue.ua2.inmobi.com");
		conf.set("zookeeper.znode.parent" ,"/"+ zooKeeper);
		createTable( tableName, colFams);
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
	
	
	

	public void addAColumnEntry(String tableName, String row, String colFamilyName, String colName, String data) throws IOException {

		byte[] rowKey = Bytes.toBytes(row);
		Put putdata = new Put(rowKey);
		putdata.add(Bytes.toBytes(colFamilyName), Bytes.toBytes(colName), Bytes.toBytes(data));
		table.put(putdata);
		
		

	}


	public static void main(String args[]) throws IOException {
		
		if(args.length !=2)
		{
			System.out.println("enter both tablename and zookeper base bath . ");
		}
		String tableName =args[0] ;
		String zooKeeper = args[1];
		//String tableName = "testing_table_blue_2";
		String[] colFamilyNames = { "name", "add" };
		String[][] colNames = { { "first", "second" }, { "house", "street" } };

		HbaseTestAllFuc2 test = new HbaseTestAllFuc2(tableName, colFamilyNames , zooKeeper);

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

		
	}

}