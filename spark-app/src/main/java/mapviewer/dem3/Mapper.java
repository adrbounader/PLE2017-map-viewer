package mapviewer.dem3;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import mapviewer.Constants;
import scala.Tuple2;

/**
 * Lambda function to give to reducer to get min and max of records.
 */
public class Mapper implements PairFunction<String, Tuple2<Double, Double>, Double> {
	@Override
	public Tuple2<Tuple2<Double, Double>, Double> call(String line) throws Exception {
		String regexpOnlyNumber = "([^0-9\\.\\-]+)";
		
		try {
			String[] splittedLine = line.split(Constants.SEPARATOR);
			Tuple2<Double, Double> coordinate = new Tuple2<Double, Double>(
					Double.parseDouble(splittedLine[Constants.INPUT_INDEX_LAT].replaceAll(regexpOnlyNumber, "")),
					Double.parseDouble(splittedLine[Constants.INPUT_INDEX_LNG].replaceAll(regexpOnlyNumber, ""))
			);
			
			// take only 4 decimals
			Double elevation = Double.parseDouble(splittedLine[Constants.INPUT_INDEX_ELEVATION].replaceAll(regexpOnlyNumber, ""));
			elevation = Math.round(elevation * 10000.0) / 10000.0;
			
			Tuple2<Tuple2<Double, Double>, Double> record = new Tuple2<Tuple2<Double, Double>, Double>(
					coordinate,
					elevation
			);
			
			return record;
	    }
		catch(Exception e) {}
		return null;
	}
}
