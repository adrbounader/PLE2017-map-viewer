package mapviewer.dem3.minmaxanalysis;

import org.apache.spark.api.java.function.Function2;

import mapviewer.Constants;
import scala.Tuple2;

/**
 * Lambda function to give to reducer to get min and max of records.
 */
public class Reducer implements Function2<Tuple2<Object, Object>, Tuple2<Object, Object>, Tuple2<Object, Object>>{
	
	private int analysisIndex;
	
	public Reducer(int analysisIndex) {
		this.analysisIndex = analysisIndex;
	}

	@Override
	public Tuple2<Object, Object> call(Tuple2<Object, Object> analysis1, Tuple2<Object, Object> analysis2) throws Exception {
		Tuple2<Double, Double> value1 = (analysis1._1 instanceof Tuple2 ? // come from mapper ?
				(new Tuple2<Double, Double>((Double) this.getValue(analysis1), (Double) this.getValue(analysis1))) :
				(new Tuple2<Double, Double>((Double) analysis1._1, (Double) analysis1._2)));
		Tuple2<Double, Double> value2 = (analysis2._1 instanceof Tuple2 ? // come from mapper ?
				(new Tuple2<Double, Double>((Double) this.getValue(analysis2), (Double) this.getValue(analysis2))) :
				(new Tuple2<Double, Double>((Double) analysis2._1, (Double) analysis2._2)));
		
		return new Tuple2<Object, Object>(
			(value1._1 < value2._1 ? value1._1 : value2._1), 
			(value1._2 > value2._2 ? value1._2 : value2._2)
		); 
	}

	private Double getValue(Tuple2<Object, Object> record) {
		Tuple2<Double, Double> coordinate = (Tuple2<Double, Double>) record._1;
		return analysisIndex == Constants.INPUT_INDEX_LAT ? coordinate._1 :
			   analysisIndex == Constants.INPUT_INDEX_LNG ? coordinate._2 :
			   ((Double) record._2);
	}
}
