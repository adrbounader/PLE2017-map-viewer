package mapviewer.minmaxanalysis;

import org.apache.spark.api.java.function.Function2;

/**
 * Lambda function to give to reducer to get min and max of records.
 */
public class Reducer implements Function2<Double[], Double[], Double[]> {
	
	private int analysisIndex;
	
	public Reducer(int analysisIndex) {
		this.analysisIndex = analysisIndex;
	}

	@Override
	public Double[] call(Double[] analysis1, Double[] analysis2) throws Exception {
		Double[] value1 = (analysis1.length == 3 ? // come from mapper ?
				(new Double[] { analysis1[this.analysisIndex], analysis1[this.analysisIndex] }) :
				analysis1);
		Double[] value2 = (analysis2.length == 3 ? // come from mapper ?
				(new Double[] { analysis2[this.analysisIndex], analysis2[this.analysisIndex] }) :
				analysis2);
		
		return new Double[] {
			(value1[0] < value2[0] ? value1[0] : value2[0]), 
			(value1[1] > value2[1] ? value1[1] : value2[1])
		}; 
	}
}
