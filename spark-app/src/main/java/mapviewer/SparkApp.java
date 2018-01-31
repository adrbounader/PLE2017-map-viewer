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
					descriptor.addFamily(new HColumnDescriptor(Constants.HBASE_FAMILY_COORDINATES));
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
						
						Put put = new Put(Bytes.toBytes(System.currentTimeMillis()));
						
						String[] blockCoordinates = record._1.split(Constants.SEPARATOR);
						Tuple2<Integer, Integer> blockCoordinatesInt = new Tuple2(
								Integer.parseInt(blockCoordinates[0]),
								Integer.parseInt(blockCoordinates[1])
						);
						
						Tuple2<Double, Double> blockCoordinatesDouble = new Tuple2(
								Double.parseDouble(blockCoordinates[0]) / 10000.0,
								Double.parseDouble(blockCoordinates[1]) / 10000.0
						);
						
						put.addColumn(Constants.HBASE_FAMILY_COORDINATES, Bytes.toBytes("lat"), Bytes.toBytes(blockCoordinates[0]));
						put.addColumn(Constants.HBASE_FAMILY_COORDINATES, Bytes.toBytes("lng"), Bytes.toBytes(blockCoordinates[1]));
						
						for(Tuple2<String,Double> elevationRecord: record._2) {
							//
							String[] pixelCoordinates = elevationRecord._1.split(Constants.SEPARATOR);
							Tuple2<Integer, Integer> relativePixelCoordinates = new Tuple2(
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
		            
		            /*rdd.foreachPartition((partition) -> {
		            	Configuration config_lambda = HBaseConfiguration.create();
						config_lambda.set("hbase.zookeeper.quorum", "10.0.8.3");
						HBaseAdmin.checkHBaseAvailable(config_lambda);
						Connection connection_lambda = ConnectionFactory.createConnection(config_lambda);
						Table table = connection_lambda.getTable(TableName.valueOf(Constants.HBASE_TABLE_NAME));
						
						while(partition.hasNext()) {
							Double[] record = partition.next();
							int r = 251 * (record[2].intValue() - (-160)) / (7430 - (-160)) + 4;
							int g = 116 * (record[2].intValue() - (-160)) / (7430 - (-160)) + 139;
							int b = 101 * (record[2].intValue() - (-160)) / (7430 - (-160)) + 154;
							System.out.println("color : " + r + ' ' + g + ' ' + b);
							String colorHex = String.format("#%02x%02x%02x", r, g, b);
							
							Put put = new Put(Bytes.toBytes(System.currentTimeMillis()));
							put.addColumn(Constants.HBASE_FAMILY_COORDINATES, Bytes.toBytes("lat"), Bytes.toBytes(record[0].toString()));
							put.addColumn(Constants.HBASE_FAMILY_COORDINATES, Bytes.toBytes("lng"), Bytes.toBytes(record[1].toString()));
							//put.addColumn(Constants.HBASE_FAMILY_ELEV, Bytes.toBytes("elev"), Bytes.toBytes(record[2].toString()));
							put.addColumn(Constants.HBASE_FAMILY_COLOR, Bytes.toBytes("color"), Bytes.toBytes(colorHex));
							try {
								table.put(put);
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
						
						connection_lambda.close();
		            });*/
		            
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
