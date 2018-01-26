package mapviewer;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkApp {

		private static final byte[] FAMILY_COORDINATES = Bytes.toBytes("coor");
		private static final byte[] FAMILY_ELEV = Bytes.toBytes("value");
		private static final byte[] TABLE_NAME = Bytes.toBytes("testbm");
	
	public static void main(String[] args) throws Exception {
		
		if (args.length > 0) {
			String inputPath;
			SparkConf conf = new SparkConf().setAppName("Map Viewer");
			JavaSparkContext context = new JavaSparkContext(conf);
			
			switch(args[0]) {
				case "minMaxAnalysis": {
					inputPath = "/raw_data/dem3_lat_lng.txt";
					JavaRDD<String> rdd = context.textFile(inputPath);
					JavaRDD<Double[]> rddAnalysis = rdd
							.map(new mapviewer.minmaxanalysis.Mapper())
							.filter(new mapviewer.minmaxanalysis.Filter());

					Double[] latAnalysis = rddAnalysis.reduce(new mapviewer.minmaxanalysis.Reducer(Constants.INPUT_INDEX_LAT));
					Double[] lngAnalysis = rddAnalysis.reduce(new mapviewer.minmaxanalysis.Reducer(Constants.INPUT_INDEX_LNG));
					Double[] elevationAnalysis = rddAnalysis.reduce(new mapviewer.minmaxanalysis.Reducer(Constants.INPUT_INDEX_ELEVATION));

					System.out.println("Latitude: { min:" + latAnalysis[0] + ", max: " + latAnalysis[1] + " }");
					System.out.println("Longitude: { min:" + lngAnalysis[0] + ", max: " + lngAnalysis[1] + " }");
					System.out.println("Elevation: { min:" + elevationAnalysis[0] + ", max: " + elevationAnalysis[1] + " }");
					
					
					break;
				}
				
				case "testHBASE": {
					inputPath = "/raw_data/dem3_lat_lng.txt";
					JavaRDD<String> rdd = context.textFile(inputPath);
					List<Double[]> rddAnalysis = rdd
							.map(new mapviewer.minmaxanalysis.Mapper())
							.filter(new mapviewer.minmaxanalysis.Filter())
							.take(10);

					Configuration config = null;
					try {
						config = HBaseConfiguration.create();
						config.set("hbase.zookeeper.quorum", "10.0.8.3");
						HBaseAdmin.checkHBaseAvailable(config);
						System.out.println("HBase is running!");
			 		} 
					catch (Exception ce){ 
					        ce.printStackTrace();
					}
					 
					Connection connection = ConnectionFactory.createConnection(config);
					
					HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
					descriptor.addFamily(new HColumnDescriptor(FAMILY_COORDINATES));
					descriptor.addFamily(new HColumnDescriptor(FAMILY_ELEV));
					
					Admin admin = connection.getAdmin(); 
					
					if (admin.tableExists(descriptor.getTableName())) {
		                admin.disableTable(descriptor.getTableName());
		                admin.deleteTable(descriptor.getTableName());
		            }
		            admin.createTable(descriptor);
					
					Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
										
					int i = 1;
					for (Double[] d: rddAnalysis) {
						Put put = new Put(Bytes.toBytes("key " + i));
						put.addColumn(FAMILY_COORDINATES, Bytes.toBytes("lat"), Bytes.toBytes(d[0].toString()));
						put.addColumn(FAMILY_COORDINATES, Bytes.toBytes("lng"), Bytes.toBytes(d[1].toString()));
						put.addColumn(FAMILY_ELEV, Bytes.toBytes("elev"), Bytes.toBytes(d[2].toString()));
						table.put(put);
						i++;
					}
					break;
				}
				default: {
					throw new Exception("Unknown program name.");
				}
			}
		}
		else {
			throw new Exception("You should give a program name.");
		}
	}
}
