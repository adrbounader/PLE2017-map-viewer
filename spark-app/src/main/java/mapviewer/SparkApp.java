/*Latitude: { min:0.0, max: 59.99916736 }
Longitude: { min:-180.0, max: 179.0009992 }
Elevation: { min:-160.0, max: 7430.0 }

Records number: 8124743863*/

package mapviewer;

import java.io.IOException;
import java.util.Arrays;
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
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkApp {
	
	public static void main(String[] args) throws Exception {
		
		if (args.length > 0) {
			String inputPath;
			SparkConf conf = new SparkConf().setAppName("Map Viewer");
			JavaSparkContext context = new JavaSparkContext(conf);
			
			switch(args[0]) {
				case "analysis": {
					inputPath = "/raw_data/dem3_lat_lng.txt";
					JavaRDD<Integer[]> analysis = context.textFile(inputPath)
							.map(new mapviewer.dem3.ToDoubleArrayMapper())
							.filter(new mapviewer.dem3.DoubleArrayFilter())
							.mapToPair(new mapviewer.dem3.ToTupleMapper())
							.distinct()
							.map(new mapviewer.dem3.analysis.ToIntegerArrayMapper());
					
					Integer[] latAnalysis = analysis.reduce(new mapviewer.dem3.analysis.MinMaxReducer(Constants.INPUT_INDEX_LAT));
					Integer[] lngAnalysis = analysis.reduce(new mapviewer.dem3.analysis.MinMaxReducer(Constants.INPUT_INDEX_LNG));
					Integer[] elevationAnalysis = analysis.reduce(new mapviewer.dem3.analysis.MinMaxReducer(Constants.INPUT_INDEX_ELEVATION));

					System.out.println("Latitude: { min:" + (latAnalysis[0] / 10000) + ", max: " + (latAnalysis[1] / 10000) + " }");
					System.out.println("Longitude: { min:" + (lngAnalysis[0] / 10000) + ", max: " + (lngAnalysis[1] / 10000) + " }");
					System.out.println("Elevation: { min:" + elevationAnalysis[0] + ", max: " + elevationAnalysis[1] + " }");
					System.out.println("\nRecords number: " + analysis.count());
					
					context.close();
					break;
				}
				
				case "batch-layer": {
					inputPath = "/raw_data/dem3_lat_lng.txt";
					JavaPairRDD<String,Iterable<Tuple2<String,Double>>> rdd = context.textFile(inputPath)
							.map(new mapviewer.dem3.ToDoubleArrayMapper())
							.filter(new mapviewer.dem3.DoubleArrayFilter())
							.mapToPair(new mapviewer.dem3.ToTupleMapper())
							.distinct()
							.groupBy(new mapviewer.dem3.batchlayer.GroupByer());

					// open database connection
					Configuration config = HBaseConfiguration.create();
					config.set("hbase.zookeeper.quorum", Constants.HBASE_IP_ADDRESS);
					HBaseAdmin.checkHBaseAvailable(config);
					Connection connection = ConnectionFactory.createConnection(config);
					
					// table configuration
					HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(Constants.HBASE_TABLE_NAME));
					descriptor.addFamily(new HColumnDescriptor(Constants.HBASE_FAMILY_PIXEL));
					
					Admin admin = connection.getAdmin(); 
					
					if (admin.tableExists(descriptor.getTableName())) {
						if (admin.isTableEnabled(descriptor.getTableName())) {
							admin.disableTable(descriptor.getTableName());
						}
		                admin.deleteTable(descriptor.getTableName());
		            }
		            admin.createTable(descriptor);
		            if (!admin.isTableEnabled(descriptor.getTableName())) {
		            	admin.enableTable(descriptor.getTableName());
		            }
		            
		            admin.close();
		            connection.close();
		            
		            rdd.foreach(new mapviewer.dem3.batchlayer.ForEacher());
		            
					break;
				}
				default: {
					context.close();
					throw new Exception("Unknown program name.");
				}
			}
		}
		else {
			throw new Exception("You should give a program name.");
		}
	}
}
