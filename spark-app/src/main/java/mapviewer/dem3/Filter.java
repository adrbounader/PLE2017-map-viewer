package mapviewer.dem3;

import org.apache.spark.api.java.function.Function;

import mapviewer.Constants;
import scala.Tuple2;

/**
 * Lambda function to give to reducer to get min and max of records.
 */
public class Filter implements Function<Tuple2<Tuple2<Double, Double>, Double>, Boolean> {
	@Override
	public Boolean call(Tuple2<Tuple2<Double, Double>, Double> record) throws Exception {
		
		if (record != null) {
			try {
				Double lat = record._1._1;
				Double lng = record._1._2;
				Double elevation = record._2;
				
				return  lat >= -90 && lat <= 90 &&
						lng >= -180 && lng <= 180 &&
						elevation >= -500 && elevation <= 9000;
			}
			catch(Exception e) {}
		}
		
		return false;
	}
}
