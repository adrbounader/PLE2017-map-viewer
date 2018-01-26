package mapviewer.dem3;

import org.apache.spark.api.java.function.Function;

import mapviewer.Constants;

/**
 * Lambda function to give to reducer to get min and max of records.
 */
public class Filter implements Function<Double[], Boolean> {
	@Override
	public Boolean call(Double[] line) throws Exception {
		return  line != null && 
				line[Constants.INPUT_INDEX_LAT] != null && line[Constants.INPUT_INDEX_LAT] >= -90 && line[Constants.INPUT_INDEX_LAT] <= 90 &&
				line[Constants.INPUT_INDEX_LNG] != null && line[Constants.INPUT_INDEX_LNG] >= -180 && line[Constants.INPUT_INDEX_LNG] <= 180 &&
				line[Constants.INPUT_INDEX_ELEVATION] != null && line[Constants.INPUT_INDEX_ELEVATION] >= -500 && line[Constants.INPUT_INDEX_ELEVATION] <= 9000				;
	}
}
