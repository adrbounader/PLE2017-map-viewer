package mapviewer.minmaxanalysis;

import org.apache.spark.api.java.function.Function;

import mapviewer.Constants;

/**
 * Lambda function to give to reducer to get min and max of records.
 */
public class Mapper implements Function<String, Double[]> {
	@Override
	public Double[] call(String line) throws Exception {
		Double[] parsedLine = { null, null, null };
		
		try {
			String latStr = line.split(Constants.SEPARATOR)[Constants.INPUT_INDEX_LAT];
			String lngStr = line.split(Constants.SEPARATOR)[Constants.INPUT_INDEX_LNG];
			String elevationStr = line.split(Constants.SEPARATOR)[Constants.INPUT_INDEX_ELEVATION];
			parsedLine[Constants.INPUT_INDEX_LAT] = Double.parseDouble(latStr);
			parsedLine[Constants.INPUT_INDEX_LNG] = Double.parseDouble(lngStr);
			parsedLine[Constants.INPUT_INDEX_ELEVATION] = Double.parseDouble(elevationStr);
	    }
		catch(Exception e) {}
		
		return parsedLine;
	}
}
