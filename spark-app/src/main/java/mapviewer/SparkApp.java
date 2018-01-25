package mapviewer;

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
