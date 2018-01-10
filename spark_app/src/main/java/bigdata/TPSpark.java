package bigdata;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class TPSpark {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Map Viewer");
		JavaSparkContext context = new JavaSparkContext(conf);
		
	}
	
}
