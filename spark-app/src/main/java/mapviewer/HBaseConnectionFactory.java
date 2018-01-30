package mapviewer;


import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import com.google.protobuf.ServiceException;

public class HBaseConnectionFactory {
	
	/**
	 * IP adresse for launch hbase connection.
	 */
	private static final String IP_ADDRESS = "10.0.8.3";
	
	public Connection createConnection() throws MasterNotRunningException, ZooKeeperConnectionException, ServiceException, IOException {
		Configuration config = createConf();
		Connection connection = ConnectionFactory.createConnection(config);
		return connection;
	}
	
	public Configuration createConf() throws MasterNotRunningException, ZooKeeperConnectionException, ServiceException, IOException {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", IP_ADDRESS);
		HBaseAdmin.checkHBaseAvailable(config);
		return config;
	}
	
	public void closeConnection(Connection c) throws IOException {
		if (!c.isClosed()) {
			c.close();
		}
	}
	
	
}
