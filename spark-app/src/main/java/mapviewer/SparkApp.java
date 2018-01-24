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
	
		
		int elevationMax = rdd.map((line) -> {
			int elevation = -1;
			try {
				String[] lineSplitted = line.split(SEPARATOR);
				if (lineSplitted.length >= 3) {
					String elevationStr = lineSplitted[INPUT_INDEX_HEIGHT];
					elevation = Integer.parseInt(elevationStr);
				}
		    } catch(Exception e) { }
			return elevation;
		}).reduce((elevation1, elevation2) -> {
			return elevation1 > elevation2 ? elevation1 : elevation2; 
		});

		System.out.println("res : " + elevationMax);
		
	}
	
}
