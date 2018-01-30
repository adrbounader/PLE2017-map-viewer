package mapviewer;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
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
			
			HBaseConnectionFactory hbaseConnectionFactory = new HBaseConnectionFactory();
			
			switch(args[0]) {
				case "analysis": {
					inputPath = "/raw_data/dem3_lat_lng.txt";
					
					JavaPairRDD<Tuple2<Double, Double>, Double> analysis = context.textFile(inputPath)
							.mapToPair(new mapviewer.dem3.Mapper())
							.filter(new mapviewer.dem3.Filter())
							.distinct();

					/*Tuple2<Double, Double> latAnalysis = analysis.reduce(new mapviewer.dem3.minmaxanalysis.Reducer(Constants.INPUT_INDEX_LAT));
					Tuple2<Double, Double> lngAnalysis = analysis.reduce(new mapviewer.dem3.minmaxanalysis.Reducer(Constants.INPUT_INDEX_LNG));
					Tuple2<Double, Double> elevationAnalysis = analysis.reduce(new mapviewer.dem3.minmaxanalysis.Reducer(Constants.INPUT_INDEX_ELEVATION));

					System.out.println("Latitude: { min:" + latAnalysis[0] + ", max: " + latAnalysis[1] + " }");
					System.out.println("Longitude: { min:" + lngAnalysis[0] + ", max: " + lngAnalysis[1] + " }");
					System.out.println("Elevation: { min:" + elevationAnalysis[0] + ", max: " + elevationAnalysis[1] + " }");*/
					System.out.println("\nRecords number: " + analysis.count());
					
					context.close();
					break;
				}
				
				case "batch-layer": {
					inputPath = "/raw_data/dem3_lat_lng.txt";
					JavaPairRDD<Tuple2<Double, Double>, Double> rdd = context.textFile(inputPath)
							.repartition(Integer.parseInt(conf.get("spark.executor.instances")))
							.mapToPair(new mapviewer.dem3.Mapper())
							.filter(new mapviewer.dem3.Filter())
							.distinct();

					// open database connection
					Connection connection = hbaseConnectionFactory.createConnection();
					
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
		            hbaseConnectionFactory.closeConnection(connection);
		            
		            
		            rdd.foreachPartition((partition) -> {
		            	HBaseConnectionFactory hbaseConnectionFactoryLambda = new HBaseConnectionFactory();
		            	Connection connectionLambda = hbaseConnectionFactoryLambda.createConnection();
			            
						Table table = connectionLambda.getTable(TableName.valueOf(Constants.HBASE_TABLE_NAME));
						
						while(partition.hasNext()) {
							Tuple2<Tuple2<Double, Double>, Double> record = partition.next();
							Double lat = record._1._1;
							Double lng = record._1._2;
							Double elevation = record._2;
							
							Put put = new Put(Bytes.toBytes(System.currentTimeMillis()));
							put.addColumn(Constants.HBASE_FAMILY_COORDINATES, Bytes.toBytes("lat"), Bytes.toBytes(lat.toString()));
							put.addColumn(Constants.HBASE_FAMILY_COORDINATES, Bytes.toBytes("lng"), Bytes.toBytes(lng.toString()));
							put.addColumn(Constants.HBASE_FAMILY_ELEV, Bytes.toBytes("elev"), Bytes.toBytes(elevation.toString()));
							try {
								table.put(put);
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
						
						hbaseConnectionFactoryLambda.closeConnection(connectionLambda);
		            });
		            
		            
					break;
				}
				case "serving-layer":
					Configuration hbaseConf = hbaseConnectionFactory.createConf();
				    hbaseConf.set(TableInputFormat.INPUT_TABLE, "BounaderMarzinTable");
				    JavaPairRDD<ImmutableBytesWritable,Result> javaPairRdd = context.newAPIHadoopRDD(hbaseConf, TableInputFormat.class,ImmutableBytesWritable.class, Result.class);
				    List<Tuple2<ImmutableBytesWritable,Result>> list =  javaPairRdd.take(10);
				    System.out.println("--- SHOW RESULT ---");
				    for (Tuple2<ImmutableBytesWritable,Result> t: list) {
				    	System.out.println(t);
				    }
				    
					break;
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
