package mapviewer.dem3;

import org.apache.spark.api.java.function.Function;

import mapviewer.Constants;

/**
 * Lambda function to give to reducer to get min and max of records.
 */
public class DoubleArrayFilter implements Function<Double[], Boolean> {
	@Override
	public Boolean call(Double[] record) throws Exception {
		try {
			return  record != null && record.length == 3 &&
					record[Constants.INPUT_INDEX_LAT] >= -90 && record[Constants.INPUT_INDEX_LAT] <= 90 &&
					record[Constants.INPUT_INDEX_LNG] >= -180 && record[Constants.INPUT_INDEX_LNG] <= 180 &&
					record[Constants.INPUT_INDEX_ELEVATION] >= -500 && record[Constants.INPUT_INDEX_ELEVATION] <= 9000;
		}
		catch(Exception e)  {
			return false;
		}
	}
}
