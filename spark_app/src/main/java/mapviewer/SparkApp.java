package mapviewer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import mapviewer.sparkjob.MinMaxHeight;
import mapviewer.sparkjob.SparkJob;
import scala.Tuple2;

public class SparkApp {
	private static final int RES_INDEX_MIN_HEIGHT = 0;
	private static final int RES_INDEX_MAX_HEIGHT = 1;

	private static final int INPUT_INDEX_HEIGHT = 2;
	
	private static final String SEPARATOR = ",";


	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Map Viewer");
		JavaSparkContext context = new JavaSparkContext(conf);
		
		String inputPath = "/raw_data/dem3_lat_lng.txt";
		JavaRDD<String> rdd = context.textFile(inputPath);
	
		
		String s = rdd.fold("0,0", (lastCompute,current) -> {

			try {
				String[] lineSplitted = current.split(SEPARATOR);
				if (lineSplitted.length >= 3) {
					String computeMinStr = lastCompute.split(SEPARATOR)[0];
					String computeMaxStr = lastCompute.split(SEPARATOR)[1];
					double computeMin = Double.parseDouble(computeMinStr);
					double computeMax = Double.parseDouble(computeMaxStr);
	
					
					String currentHeightStr = lineSplitted[INPUT_INDEX_HEIGHT];
					double currentHeight = Double.parseDouble(currentHeightStr);
					if (currentHeight > computeMax) {
						lastCompute = computeMinStr + SEPARATOR + "50";
					}
					else if (currentHeight < computeMin) {
						lastCompute = "-50" + SEPARATOR + computeMaxStr;
					}
				}
		    } catch(Exception e) { }
			return lastCompute;
		});

		System.out.println("res : " + s);
		
	}
	
}