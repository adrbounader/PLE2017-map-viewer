package mapviewer.sparkjob;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class MinMaxHeight implements SparkJob {
	
	private static final int RES_INDEX_MIN_HEIGHT = 0;
	private static final int RES_INDEX_MAX_HEIGHT = 1;

	private static final int INPUT_INDEX_HEIGHT = 2;
	
	private static final String HEIGHT_SEPARATOR = "|";
	
	/**
	 * 
	 */
	public void run(SparkConf conf, JavaSparkContext context) {
		String inputPath = "/raw_data/dem3_lat_lng.txt";
		JavaRDD<String> rdd = context.textFile(inputPath);
	
		
		String heights = rdd
			.reduce((lastHeights, currentLine) -> {
				String currentHeightStr = currentLine.split(",")[INPUT_INDEX_HEIGHT];
				
				if (lastHeights == null || lastHeights == "") {
					lastHeights = "0" + HEIGHT_SEPARATOR + "0";
				}
				
				if (!currentHeightStr.equals("")) {
					try {
						String lastMinHeightStr = lastHeights.split(HEIGHT_SEPARATOR)[RES_INDEX_MIN_HEIGHT];
						String lastMaxHeightStr = lastHeights.split(HEIGHT_SEPARATOR)[RES_INDEX_MAX_HEIGHT];
						
						
						int lastMinHeight = Integer.parseInt(lastMinHeightStr);
						int lastMaxHeight = Integer.parseInt(lastMaxHeightStr);
						int currentHeight = Integer.parseInt(currentHeightStr);
						
						return (currentHeight < lastMinHeight) ? (currentHeightStr + HEIGHT_SEPARATOR + lastMaxHeightStr) :
							   (currentHeight > lastMaxHeight) ? (lastMinHeightStr + HEIGHT_SEPARATOR + currentHeightStr) :
							   lastHeights;
				    } catch(Exception e) { }
				}
				return lastHeights;
			});
		

		System.out.println(heights);
	}
}
