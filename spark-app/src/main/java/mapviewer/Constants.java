package mapviewer;

import org.apache.hadoop.hbase.util.Bytes;

public class Constants {
	/**
	 * Index in splitted line of the lattitude. 
	 */
	public static final int INPUT_INDEX_LAT = 0;
	
	/**
	 * Index in splitted line of the longitude. 
	 */
	public static final int INPUT_INDEX_LNG = 1;
	
	/**
	 * Index in splitted line of the elevation. 
	 */
	public static final int INPUT_INDEX_ELEVATION = 2;
	
	/**
	 * Separator in row of input.
	 */
	public static final String SEPARATOR = ",";
	
	/**
	 * IP adresse for launch hbase connection.
	 */
	public static final String HBASE_IP_ADDRESS = "10.0.8.3";
	
	public static final byte[] HBASE_FAMILY_COORDINATES = Bytes.toBytes("coor");
	public static final byte[] HBASE_FAMILY_ELEV = Bytes.toBytes("elevValue");
	public static final byte[] HBASE_FAMILY_COLOR = Bytes.toBytes("colorValue");
	public static final byte[] HBASE_TABLE_NAME = Bytes.toBytes("BounaderMarzinTable");
}
