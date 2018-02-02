package mapviewer.dem3.batchlayer;

import org.apache.spark.api.java.function.Function;

import mapviewer.Constants;
import scala.Tuple2;

public class GroupByer implements Function<Tuple2<String, Double>, String> {
	@Override
	public String call(Tuple2<String, Double> record) throws Exception {
		String[] coordinates = record._1.split(Constants.SEPARATOR);
		int posX = Integer.parseInt(coordinates[0]);
		int posY = Integer.parseInt(coordinates[1]);
		Integer posXBlock = posX - (posX % Constants.SIZE_BLOCK);
		Integer posYBlock = posY - (posY % Constants.SIZE_BLOCK);
		
		return posXBlock.toString() + Constants.SEPARATOR + posYBlock.toString();
	}

}
