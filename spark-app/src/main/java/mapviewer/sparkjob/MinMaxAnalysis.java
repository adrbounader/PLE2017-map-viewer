package mapviewer.sparkjob;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

/**
 * Implement run SparkJob.
 * Display result of analysis of dem3_lat_lng.txt file for min / max of each column.
 */
public class MinMaxAnalysis implements SparkJob {
	
	/**
	 * Index in splitted line of the lattitude. 
	 */
	private static final int INPUT_INDEX_LAT = 0;
	
	/**
	 * Index in splitted line of the longitude. 
	 */
	private static final int INPUT_INDEX_LNG = 1;
	
	/**
	 * Index in splitted line of the elevation. 
	 */
	private static final int INPUT_INDEX_ELEVATION = 2;
	
	/**
	 * Separator in row of input.
	 */
	private static final String SEPARATOR = ",";
	
	/**
	 * Lambda function to give to reducer to get min and max of records.
	 */
	private class MinMaxReducer implements Function2<Double[], Double[], Double[]> {
		
		private int analysisIndex;
		
		public MinMaxReducer(int analysisIndex) {
			this.analysisIndex = analysisIndex;
		}

		@Override
		public Double[] call(Double[] analysis1, Double[] analysis2) throws Exception {
			Double[] value1 = (analysis1.length == 3 ? // come from mapper ?
					(new Double[] { analysis1[this.analysisIndex], analysis1[this.analysisIndex] }) :
					analysis1);
			Double[] value2 = (analysis2.length == 3 ? // come from mapper ?
					(new Double[] { analysis2[this.analysisIndex], analysis2[this.analysisIndex] }) :
					analysis2);
			
			return new Double[] {
				(value1[0] < value2[0] ? value1[0] : value2[0]), 
				(value1[1] > value2[1] ? value1[1] : value2[1])
			}; 
		}
	}
	
	/**
	 * Execute a map reduce on dem3_lat_lng.txt file.
	 */
	public void run(SparkConf conf, JavaSparkContext context, String[] args) {
		String inputPath = "/raw_data/dem3_lat_lng.txt";
		JavaRDD<String> rdd = context.textFile(inputPath);
		
		JavaRDD<Double[]> rddAnalysis = rdd.map((line) -> {
			Double[] parsedLine = { null, null, null };
			
			try {
				String latStr = line.split(SEPARATOR)[INPUT_INDEX_LAT];
				String lngStr = line.split(SEPARATOR)[INPUT_INDEX_LNG];
				String elevationStr = line.split(SEPARATOR)[INPUT_INDEX_ELEVATION];
				parsedLine[INPUT_INDEX_LAT] = Double.parseDouble(latStr);
				parsedLine[INPUT_INDEX_LNG] = Double.parseDouble(lngStr);
				parsedLine[INPUT_INDEX_ELEVATION] = Double.parseDouble(elevationStr);
		    }
			catch(Exception e) {}
			
			return parsedLine;
		}).filter((line) -> line[INPUT_INDEX_LAT] != null && line[INPUT_INDEX_LNG] != null && line[INPUT_INDEX_ELEVATION] != null);

		Double[] latAnalysis = rddAnalysis.reduce(new MinMaxReducer(INPUT_INDEX_LAT));
		Double[] lngAnalysis = rddAnalysis.reduce(new MinMaxReducer(INPUT_INDEX_LNG));
		Double[] elevationAnalysis = rddAnalysis.reduce(new MinMaxReducer(INPUT_INDEX_ELEVATION));

		System.out.println("Latitude: { min:" + latAnalysis[0] + ", max: " + latAnalysis[1] + " }");
		System.out.println("Longitude: { min:" + lngAnalysis[0] + ", max: " + lngAnalysis[1] + " }");
		System.out.println("Elevation: { min:" + elevationAnalysis[0] + ", max: " + elevationAnalysis[1] + " }");
	}
}
