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
import scala.Tuple2;

public class ForEacher implements VoidFunction<Tuple2<String,Iterable<Tuple2<String,Double>>>> {
	@Override
	public void call(Tuple2<String,Iterable<Tuple2<String,Double>>> record) throws Exception {
		Configuration config_lambda = HBaseConfiguration.create();
		config_lambda.set("hbase.zookeeper.quorum", "10.0.8.3");
		HBaseAdmin.checkHBaseAvailable(config_lambda);
		Connection connection_lambda = ConnectionFactory.createConnection(config_lambda);
		Table table = connection_lambda.getTable(TableName.valueOf(Constants.HBASE_TABLE_NAME));
		
		String[] blockCoordinates = record._1.split(Constants.SEPARATOR);
		Tuple2<Integer, Integer> blockCoordinatesInt = new Tuple2<Integer, Integer>(
				Integer.parseInt(blockCoordinates[0]),
				Integer.parseInt(blockCoordinates[1])
		);
		
		Tuple2<Double, Double> blockCoordinatesDouble = new Tuple2<Double, Double>(
				Double.parseDouble(blockCoordinates[0]) / 10000.0,
				Double.parseDouble(blockCoordinates[1]) / 10000.0
		);

		
		Put put = new Put(Bytes.toBytes(blockCoordinatesDouble._1.toString() + Constants.SEPARATOR + blockCoordinatesDouble._2.toString()));
		
		for(Tuple2<String,Double> elevationRecord: record._2) {
			String[] pixelCoordinates = elevationRecord._1.split(Constants.SEPARATOR);
			Tuple2<Integer, Integer> relativePixelCoordinates = new Tuple2<Integer, Integer>(
					Math.abs(Integer.parseInt(pixelCoordinates[0]) - blockCoordinatesInt._1),
					Math.abs(Integer.parseInt(pixelCoordinates[1]) - blockCoordinatesInt._2)
			);
			
			Integer elevation = elevationRecord._2.intValue();
			put.addColumn(
					Constants.HBASE_FAMILY_PIXEL,
					Bytes.toBytes(relativePixelCoordinates._1 + Constants.SEPARATOR + relativePixelCoordinates._2),
					Bytes.toBytes(elevation.toString())
			);
		}
		
		table.put(put);
		table.close();
		connection_lambda.close();
	}

}
