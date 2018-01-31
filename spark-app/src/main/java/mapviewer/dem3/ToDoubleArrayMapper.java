package mapviewer.dem3;

import org.apache.spark.api.java.function.Function;

import mapviewer.Constants;

/**
 * Lambda function to give to reducer to get min and max of records.
 */
public class ToDoubleArrayMapper implements Function<String, Double[]> {
	@Override
	public Double[] call(String line) throws Exception {
		Double[] parsedLine = new Double[3];
		
		String regexpOnlyNumber = "([^0-9\\.\\-]+)";
		
		try {
			String latStr = line.split(Constants.SEPARATOR)[Constants.INPUT_INDEX_LAT].replaceAll(regexpOnlyNumber, "");
			String lngStr = line.split(Constants.SEPARATOR)[Constants.INPUT_INDEX_LNG].replaceAll(regexpOnlyNumber, "");
			String elevationStr = line.split(Constants.SEPARATOR)[Constants.INPUT_INDEX_ELEVATION].replaceAll(regexpOnlyNumber, "");
			parsedLine[Constants.INPUT_INDEX_LAT] = Double.parseDouble(latStr);
			parsedLine[Constants.INPUT_INDEX_LNG] = Double.parseDouble(lngStr);
			parsedLine[Constants.INPUT_INDEX_ELEVATION] = Double.parseDouble(elevationStr);
	    }
		catch(Exception e) {
			return null;
		}
		
		return parsedLine;
	}
}
