package mapviewer.sparkjob;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public interface SparkJob {
	public void run(SparkConf conf, JavaSparkContext context, String[] args);
}
