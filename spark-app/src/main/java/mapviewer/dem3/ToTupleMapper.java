package mapviewer.dem3;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import mapviewer.Constants;
import scala.Tuple2;

/**
 * Lambda function to give to reducer to get min and max of records.
 */
public class ToTupleMapper implements PairFunction<Double[], String, Double> {
	@Override
	public Tuple2<String, Double> call(Double[] record) throws Exception {
		Long latInPixel = Math.round(record[Constants.INPUT_INDEX_LAT] * 10000.0);
		Long lngInPixel = Math.round(record[Constants.INPUT_INDEX_LNG] * 10000.0);
		
		return new Tuple2<String, Double>(
			(latInPixel.toString() + Constants.SEPARATOR + lngInPixel.toString()),
			record[Constants.INPUT_INDEX_ELEVATION]
		);
	}
}
