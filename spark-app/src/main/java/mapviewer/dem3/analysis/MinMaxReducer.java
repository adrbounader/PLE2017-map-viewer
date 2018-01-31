package mapviewer.dem3.analysis;

import org.apache.spark.api.java.function.Function2;

/**
 * Lambda function to give to reducer to get min and max of records.
 */
public class MinMaxReducer implements Function2<Integer[], Integer[], Integer[]> {
	
	private int analysisIndex;
	
	public MinMaxReducer(int analysisIndex) {
		this.analysisIndex = analysisIndex;
	}

	@Override
	public Integer[] call(Integer[] analysis1, Integer[] analysis2) throws Exception {
		Integer[] value1 = (analysis1.length == 3 ? // come from mapper ?
				(new Integer[] { analysis1[this.analysisIndex], analysis1[this.analysisIndex] }) :
				analysis1);
		Integer[] value2 = (analysis2.length == 3 ? // come from mapper ?
				(new Integer[] { analysis2[this.analysisIndex], analysis2[this.analysisIndex] }) :
				analysis2);
		
		return new Integer[] {
			(value1[0] < value2[0] ? value1[0] : value2[0]), 
			(value1[1] > value2[1] ? value1[1] : value2[1])
		}; 
	}
}
