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
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkApp {

	private static final byte[] FAMILY_COORDINATES = "coor".getBytes();
	private static final byte[] FAMILY_ELEV = "value".getBytes();
	private static final byte[] TABLE_NAME = "testbm".getBytes();

	
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
					       config.set("testBM", "10.0.8.3:16010");
					       HBaseAdmin.checkHBaseAvailable(config);
					       System.out.println("HBase is running!");
					     } 
					catch (Exception ce){ 
					        ce.printStackTrace();
					}
					 
					Connection connection = ConnectionFactory.createConnection(config);
					HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
					Admin admin = connection.getAdmin(); 
		            admin.createTable(descriptor);
					descriptor.addFamily(new HColumnDescriptor(FAMILY_COORDINATES));
					Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
					
					int i = 1;
					for (Double[] d: rddAnalysis) {
						Put put = new Put(("key " + i).getBytes());
						put.addColumn(FAMILY_COORDINATES, "lat".getBytes(), d[0].toString().getBytes());
						put.addColumn(FAMILY_COORDINATES, "lng".getBytes(), d[1].toString().getBytes());
						put.addColumn(FAMILY_ELEV, "elev".getBytes(), d[2].toString().getBytes());
						table.put(put);
						i++;
					}
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
