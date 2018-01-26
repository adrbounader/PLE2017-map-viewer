package mapviewer.dem3.batchlayer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.function.VoidFunction;

import mapviewer.Constants;

/**
 * Lambda function to give to reducer to get min and max of records.
 */
public class ForEacher implements VoidFunction<Double[]> {

	@Override
	public void call(Double[] record) throws Exception {
		// open database connection
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "10.0.8.3");
		config.set("hbase.cluster.distributed", "true");
		HBaseAdmin.checkHBaseAvailable(config);
		Connection connection = ConnectionFactory.createConnection(config);
		Table table = connection.getTable(TableName.valueOf(Constants.HBASE_TABLE_NAME));
		
		Put put = new Put(Bytes.toBytes("key"));
		put.addColumn(Constants.HBASE_FAMILY_COORDINATES, Bytes.toBytes("lat"), Bytes.toBytes(record[0].toString()));
		put.addColumn(Constants.HBASE_FAMILY_COORDINATES, Bytes.toBytes("lng"), Bytes.toBytes(record[1].toString()));
		put.addColumn(Constants.HBASE_FAMILY_ELEV, Bytes.toBytes("elev"), Bytes.toBytes(record[2].toString()));
		table.put(put);
		
		connection.close();
	}
}
