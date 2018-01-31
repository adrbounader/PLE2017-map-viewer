package mapviewer.dem3.analysis;

import org.apache.spark.api.java.function.Function;

import mapviewer.Constants;
import scala.Tuple2;

/**
 * Lambda function to give to reducer to get min and max of records.
 */
public class ToIntegerArrayMapper implements Function<Tuple2<String, Double>, Integer[]> {
	@Override
	public Integer[] call(Tuple2<String, Double> record) throws Exception {
		String[] coordinates = record._1.split(Constants.SEPARATOR);
		return new Integer[] {
			Integer.parseInt(coordinates[0]),
			Integer.parseInt(coordinates[1]),
			record._2.intValue()
		};
	}
}
