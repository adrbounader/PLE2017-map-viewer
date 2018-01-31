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
							.groupBy((Tuple2<String, Double> record) -> {
								String[] coordinates = record._1.split(Constants.SEPARATOR);
								int posX = Integer.parseInt(coordinates[0]);
								int posY = Integer.parseInt(coordinates[1]);
								Integer posXBlock = posX - (posX % Constants.SIZE_BLOCK);
								Integer posYBlock = posY - (posY % Constants.SIZE_BLOCK);
								
								return posXBlock.toString() + Constants.SEPARATOR + posYBlock.toString();
							});

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
		            
		            rdd.foreach((Tuple2<String,Iterable<Tuple2<String,Double>>> record) -> {
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
							
							int elevation = elevationRecord._2.intValue();
							int r = 251 * (elevation - (-160)) / (7430 - (-160)) + 4;
							int g = 116 * (elevation - (-160)) / (7430 - (-160)) + 139;
							int b = 101 * (elevation - (-160)) / (7430 - (-160)) + 154;
							String colorHex = String.format("#%02x%02x%02x", r, g, b);
							
							put.addColumn(
									Constants.HBASE_FAMILY_PIXEL,
									Bytes.toBytes(relativePixelCoordinates._1 + Constants.SEPARATOR + relativePixelCoordinates._2),
									Bytes.toBytes(colorHex)
							);
						}
						
						table.put(put);
						table.close();
						connection_lambda.close();
		            });
		            
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
