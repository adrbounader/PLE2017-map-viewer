package mapviewer;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import mapviewer.sparkjob.MinMaxAnalysis;
import mapviewer.sparkjob.SparkJob;
import scala.Tuple2;

public class SparkApp {

	public static void main(String[] args) throws Exception {
		SparkConf conf = new SparkConf().setAppName("Map Viewer");
		JavaSparkContext context = new JavaSparkContext(conf);
	
		if (args.length > 0) {
			SparkJob sparkJob;
			switch(args[0]) {
				case "minMaxAnalysis": {
					sparkJob = new MinMaxAnalysis();
					break;
				}
				default: {
					throw new Exception("Unknown program name.");
				}
			}
			
			String[] argsTail = Arrays.copyOfRange(args, 1, args.length);
			sparkJob.run(conf, context, argsTail);
		}
		else {
			throw new Exception("You should give a programm name.");
		}
	}
}
