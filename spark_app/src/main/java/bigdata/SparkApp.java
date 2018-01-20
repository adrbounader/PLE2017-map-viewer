package bigdata;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkApp {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Map Viewer");
		JavaSparkContext context = new JavaSparkContext(conf);
		
		String inputPath = "/raw_data/dem3_lat_lng.txt";
		JavaRDD<String> rdd = context.textFile(inputPath);
		rdd = rdd.repartition(Integer.parseInt(conf.get("spark.executor.instances")));
		
		JavaRDD<String> rddHeight = rdd
				.map((String x) -> {
					String[] values = x.split(",");
					return values[2];
				});
		
		System.out.println("Nb lignes : " + rddHeight.count());
		
		/*String heights = rdd
			.reduce((lastHeights, currentLine) -> {
				String[] line = currentLine.split(",");
				/*if (!line[2].equals("")) {
					try {
						int lastMinHeight = Integer.parseInt(lastHeights.split("|")[0]);
						int lastMaxHeight = Integer.parseInt(lastHeights.split("|")[1]);
						int currentHeight = Integer.parseInt(line[2]);
						String res = "";
						if (currentHeight < lastMinHeight) {
							res = line[2] + '|' + lastHeights.split("|")[1];
						}
						else if (currentHeight > lastMaxHeight) { 
							res = lastHeights.split("|")[0] + '|' + line[2];
						}
						else {
							res = lastHeights;
						}
						return res;
				    } catch(Exception e) { }
				}
				return line[2];
			});*/
		
	}
	
}
