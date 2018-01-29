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
					System.out.println("\nRecords number: " + rddAnalysis.count());
					
					context.close();
					break;
				}
				
				case "batch-layer": {
					inputPath = "/raw_data/dem3_lat_lng.txt";
					JavaRDD<Double[]> rdd = context.textFile(inputPath)
							.repartition(Integer.parseInt(conf.get("spark.executor.instances")))
							.map(new mapviewer.dem3.Mapper())
							.filter(new mapviewer.dem3.Filter());

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
		            if (!admin.isTableEnabled(descriptor.getTableName())) {
		            	admin.enableTable(descriptor.getTableName());
		            }
		            
		            admin.close();
		            connection.close();
		            
		            rdd.foreachPartition((partition) -> {
		            	Configuration config_lambda = HBaseConfiguration.create();
						config_lambda.set("hbase.zookeeper.quorum", "10.0.8.3");
						HBaseAdmin.checkHBaseAvailable(config_lambda);
						Connection connection_lambda = ConnectionFactory.createConnection(config_lambda);
						Table table = connection_lambda.getTable(TableName.valueOf(Constants.HBASE_TABLE_NAME));
						
						while(partition.hasNext()) {
							Double[] record = partition.next();
							Put put = new Put(Bytes.toBytes(System.currentTimeMillis()));
							put.addColumn(Constants.HBASE_FAMILY_COORDINATES, Bytes.toBytes("lat"), Bytes.toBytes(record[0].toString()));
							put.addColumn(Constants.HBASE_FAMILY_COORDINATES, Bytes.toBytes("lng"), Bytes.toBytes(record[1].toString()));
							put.addColumn(Constants.HBASE_FAMILY_ELEV, Bytes.toBytes("elev"), Bytes.toBytes(record[2].toString()));
							try {
								table.put(put);
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
						
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
