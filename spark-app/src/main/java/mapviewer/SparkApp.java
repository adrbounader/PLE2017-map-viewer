package mapviewer;

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
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkApp {
	
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
							.map(new mapviewer.dem3.Mapper())
							.filter(new mapviewer.dem3.Filter());

					Double[] latAnalysis = rddAnalysis.reduce(new mapviewer.dem3.minmaxanalysis.Reducer(Constants.INPUT_INDEX_LAT));
					Double[] lngAnalysis = rddAnalysis.reduce(new mapviewer.dem3.minmaxanalysis.Reducer(Constants.INPUT_INDEX_LNG));
					Double[] elevationAnalysis = rddAnalysis.reduce(new mapviewer.dem3.minmaxanalysis.Reducer(Constants.INPUT_INDEX_ELEVATION));

					System.out.println("Latitude: { min:" + latAnalysis[0] + ", max: " + latAnalysis[1] + " }");
					System.out.println("Longitude: { min:" + lngAnalysis[0] + ", max: " + lngAnalysis[1] + " }");
					System.out.println("Elevation: { min:" + elevationAnalysis[0] + ", max: " + elevationAnalysis[1] + " }");
					
					context.close();
					break;
				}
				
				case "batch-layer": {
					inputPath = "/raw_data/dem3_lat_lng.txt";
					JavaRDD<String> rdd = context.textFile(inputPath)
							.map(new mapviewer.dem3.Mapper())
							.filter(new mapviewer.dem3.Filter())
							.map((Double[] record) -> record[0] + "," + record[1] + "," + record[2]);

					// open database connection
					Configuration config = HBaseConfiguration.create();
					config.set("hbase.zookeeper.quorum", Constants.HBASE_IP_ADDRESS);
					HBaseAdmin.checkHBaseAvailable(config);
					Connection connection = ConnectionFactory.createConnection(config);
					
					// table configuration
					HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(Constants.HBASE_TABLE_NAME));
					descriptor.addFamily(new HColumnDescriptor(Constants.HBASE_FAMILY_COORDINATES));
					descriptor.addFamily(new HColumnDescriptor(Constants.HBASE_FAMILY_ELEV));
					
					Admin admin = connection.getAdmin(); 
					
					if (admin.tableExists(descriptor.getTableName())) {
						if (admin.isTableEnabled(descriptor.getTableName())) {
							admin.disableTable(descriptor.getTableName());
						}
		                admin.deleteTable(descriptor.getTableName());
		            }
		            admin.createTable(descriptor);		            
		            
		            JavaHBaseContext hbaseContext = new JavaHBaseContext(context, config);
		            /*hbaseContext.bulkPut(rdd, , (Double[] record) -> {
		            	
		            }, true);
		            */
		            /*hbaseContext.bulkPut(rdd, Constants.HBASE_TABLE_NAME, (record) -> {
		            	Put put = new Put(Bytes.toBytes(System.currentTimeMillis()));
						put.addColumn(Constants.HBASE_FAMILY_COORDINATES, Bytes.toBytes("lat"), Bytes.toBytes(record[0].toString()));
						put.addColumn(Constants.HBASE_FAMILY_COORDINATES, Bytes.toBytes("lng"), Bytes.toBytes(record[1].toString()));
						put.addColumn(Constants.HBASE_FAMILY_ELEV, Bytes.toBytes("elev"), Bytes.toBytes(record[2].toString()));
						return put;
		            });*/
		            hbaseContext.bulkPut(rdd, TableName.valueOf(Constants.HBASE_TABLE_NAME), (String line) -> {
		            	Double[] parsedLine = null;
		        		
		        		String regexpOnlyNumber = "([^0-9\\.\\-]+)";
	        			String latStr = line.split(Constants.SEPARATOR)[Constants.INPUT_INDEX_LAT].replaceAll(regexpOnlyNumber, "");
	        			String lngStr = line.split(Constants.SEPARATOR)[Constants.INPUT_INDEX_LNG].replaceAll(regexpOnlyNumber, "");
	        			String elevationStr = line.split(Constants.SEPARATOR)[Constants.INPUT_INDEX_ELEVATION].replaceAll(regexpOnlyNumber, "");
	        			parsedLine[Constants.INPUT_INDEX_LAT] = Double.parseDouble(latStr);
	        			parsedLine[Constants.INPUT_INDEX_LNG] = Double.parseDouble(lngStr);
	        			parsedLine[Constants.INPUT_INDEX_ELEVATION] = Double.parseDouble(elevationStr);
		        		
		        		Put put = new Put(Bytes.toBytes(System.currentTimeMillis()));
						put.addColumn(Constants.HBASE_FAMILY_COORDINATES, Bytes.toBytes("lat"), Bytes.toBytes(parsedLine[0].toString()));
						put.addColumn(Constants.HBASE_FAMILY_COORDINATES, Bytes.toBytes("lng"), Bytes.toBytes(parsedLine[1].toString()));
						put.addColumn(Constants.HBASE_FAMILY_ELEV, Bytes.toBytes("elev"), Bytes.toBytes(parsedLine[2].toString()));
						return put;
		            });
		            
		            admin.close();
		            connection.close();

									/*	
					rdd.foreach((Double[] record) -> {
						// open database connection
						Configuration config_lambda = HBaseConfiguration.create();
						config_lambda.set("hbase.zookeeper.quorum", "10.0.8.3");
						HBaseAdmin.checkHBaseAvailable(config_lambda);
						Connection connection_lambda = ConnectionFactory.createConnection(config_lambda);
						Table table = connection_lambda.getTable(TableName.valueOf(Constants.HBASE_TABLE_NAME));
						
						
						table.put(put);
						
						connection_lambda.close();
					});*/
					context.close();
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
