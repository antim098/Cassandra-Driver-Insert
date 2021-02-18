package com.spark;

import com.datastax.driver.core.*;
import com.datastax.driver.core.Cluster.Builder;
import org.apache.log4j.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * The Class CassandraConnector.
 */
public class CassandraConnector {

    /**
     * The Constant LOGGER.
     */
    private static final Logger LOGGER = Logger.getLogger(CassandraConnector.class);

    /**
     * The cluster.
     */
    private static Cluster cluster;

    /**
     * The node.
     */
    private static String node = null;

    /**
     * The port.
     */
    private static Integer port = null;

    /**
     * The user name.
     */
    private static String userName = null;

    /**
     * The password.
     */
    private static String password = null;

    private static int heartBeatInterval = 30;

    /**
     * The connector.
     */
    //private static CassandraConnector connector = null;

    private static Session session = null;

    // private static Map<String, KeyspaceModel> keyspaceMap = new ConcurrentHashMap();

    /**
     * Instantiates a new cassandra connector.
     *
     * @param node  the node
     * @param port  the port
     * @param uName the u name
     * @param pwd   the pwd
     */
    private CassandraConnector(String node, Integer port, String uName, String pwd, int heartBeatInterval) {
        CassandraConnector.node = node;
        CassandraConnector.port = port;
        CassandraConnector.userName = uName;
        CassandraConnector.password = pwd;
        CassandraConnector.heartBeatInterval = heartBeatInterval;
    }


    /**
     * Gets the single instance of CassandraConnector.
     *
     * @param node  the node
     * @param port  the port
     * @param uName the u name
     * @param pwd   the pwd
     * @return single instance of CassandraConnector
     */
    public static void initialize(String node, Integer port, String uName, String pwd, int heartBeatInterval) {
    	/*if(connector == null){
    		connector = new CassandraConnector(node, port, uName, pwd, heartBeatInterval);
    		initialize();
    	}
    	return connector;
    	*/

        CassandraConnector.node = node;
        CassandraConnector.port = port;
        CassandraConnector.userName = uName;
        CassandraConnector.password = pwd;
        CassandraConnector.heartBeatInterval = heartBeatInterval;
        initialize();
    }

    /**
     * Connect.
     *
     * @return the session
     */
//    public static Session connect() {
//        if (session == null || session.isClosed()) {
//            if (cluster == null || cluster.isClosed()) {
//                initialize();
//            }
//            session = cluster.connect();
//        }
//        return session;
//
//    	/*initialize();
//        return cluster.connect();*/
//    }
    public static Session connect() {
        if (session == null || session.isClosed()) {
            if (cluster == null || cluster.isClosed()) {
                initialize();
            }
            session = cluster.connect();
        }
        return session;

    	/*initialize();
        return cluster.connect();*/
    }


    /**
     * Initialize.
     */
    private synchronized static void initialize() {
        if (cluster == null || cluster.isClosed()) {
            LOGGER.info("Initializing cassandra connector node=" + node);
            StringTokenizer nodeTokens = new StringTokenizer(node, ",");
            List<InetAddress> nodes = new ArrayList<>();
            while (nodeTokens.hasMoreTokens()) {
                String v = nodeTokens.nextToken();
                if (v != null && !v.trim().equals("")) {
                    InetAddress address;
                    try {
                        address = InetAddress.getByName(v.trim());
                        nodes.add(address);
                    } catch (UnknownHostException e) {
                        System.out.println("Invalid cassandra host " + v);
                    }
                }
            }

            //PoolingOptions poolingOptions = new PoolingOptions().setHeartbeatIntervalSeconds(heartBeatInterval).setIdleTimeoutSeconds(120);
            PoolingOptions poolingOptions = new PoolingOptions();
            poolingOptions
                    .setConnectionsPerHost(HostDistance.LOCAL, 5, 10) //Default core=max=1 in Protocol v3
                    .setConnectionsPerHost(HostDistance.REMOTE, 1, 4)
                    .setMaxRequestsPerConnection(HostDistance.LOCAL, 5120) //Default 1024 in Protocol v3
                    .setMaxRequestsPerConnection(HostDistance.REMOTE, 1280); //Default 256 in Protocol v3
            //Builder b = Cluster.builder().addContactPoints(nodes).withCredentials(userName, password).withPoolingOptions(poolingOptions);
            Builder b = Cluster.builder().addContactPoints(nodes).withCredentials(userName, password).withPoolingOptions(poolingOptions);
            if (port != null) {
                b.withPort(port);
            }
            cluster = b.build();

            cluster.init();
            for (Host host : cluster.getMetadata().getAllHosts()) {
                LOGGER.info(String.format("Address: %s, Rack: %s, Datacenter: %s, Tokens: %s\n", host.getAddress(),
                        host.getDatacenter(), host.getRack(), host.getTokens()));
            }
        }
    }

    /**
     * Gets the keyspaces.
     *
     * @return the keyspaces
     */
    public static List<KeyspaceMetadata> getKeyspaces() {
        return cluster.getMetadata().getKeyspaces();
    }

    /**
     * Gets the keyspace.
     *
     * @param ksName the ks name
     * @return the keyspace
     */
//    public static KeyspaceModel getKeyspace(String ksName) {
//        return keyspaceMap.get(ksName.toLowerCase());
//    }
//
//
//    public static void loadKeyspaceModels() {
//        Metadata metadata = cluster.getMetadata();
//        List<KeyspaceMetadata> keyspaceMetadatas = metadata.getKeyspaces();
//        for (com.datastax.driver.core.KeyspaceMetadata keyspaceMetadata : keyspaceMetadatas) {
//            KeyspaceModel model = getKeyspaceModel(keyspaceMetadata.getName().toLowerCase(), keyspaceMetadata);
//            keyspaceMap.put(keyspaceMetadata.getName().toLowerCase(), model);
//        }
//    }


//    private static KeyspaceModel getKeyspaceModel(String ksName,
//                                                  com.datastax.driver.core.KeyspaceMetadata keyspaceMetadata) {
//        KeyspaceModel keyspaceModel = new KeyspaceModel();
//        keyspaceModel.setKeyspaceName(ksName);
//        Collection<TableMetadata> tableMetadatas = keyspaceMetadata.getTables();
//        for (TableMetadata table : tableMetadatas) {
//            Table tableObj = new Table();
//            tableObj.setTableName(table.getName());
//            tableObj.setCqlQuery(table.asCQLQuery());
//            tableObj.setKeyspaceName(ksName);
//            keyspaceModel.addTable(table.getName(), tableObj);
//            List<ColumnMetadata> columns = table.getColumns();
//            for (ColumnMetadata col : columns) {
//                Column column = new Column();
//                column.setName(col.getName().toUpperCase());
//                column.setType(col.getType());
//                tableObj.addColumn(column.getName(), column);
//            }
//
//            columns = table.getPartitionKey();
//            for (ColumnMetadata col : columns) {
//                tableObj.addPartitionKey(col.getName());
//            }
//
//            columns = table.getClusteringColumns();
//            for (ColumnMetadata col : columns) {
//                tableObj.addClusteringColumn(col.getName());
//            }
//        }
//
//        Collection<UserType> udts = keyspaceMetadata.getUserTypes();
//        for (UserType udt : udts) {
//            keyspaceModel.addUserType(udt);
//            keyspaceModel.addUserTypeCqlQuery(udt.getTypeName(), udt.asCQLQuery());
//        }
//        return keyspaceModel;
//    }

    /**
     * Gets the table.
     *
     * @param keyspaceName the keyspace name
     * @param tableName    the table name
     * @return the table
     */
//    public static Table getTable(String keyspaceName, String tableName) {
//        KeyspaceModel model = null;
//        if (keyspaceMap.containsKey(keyspaceName.toLowerCase())) {
//            model = keyspaceMap.get(keyspaceName.toLowerCase());
//        } else {
//            model = getKeyspace(keyspaceName);
//            keyspaceMap.put(keyspaceName.toLowerCase(), model);
//        }
//
//        if (model.getTables().containsKey(tableName)) {
//            return model.getTables().get(tableName);
//        }
//        return null;
//    }

    /**
     * Gets the table schema.
     *
     * @param keyspaceName the keyspace name
     * @param tableName    the table name
     * @return the table schema
     */
//    public static String getTableSchema(String keyspaceName, String tableName) {
//        KeyspaceModel model = getKeyspace(keyspaceName);
//        if (model.getTables().containsKey(tableName) && model.getTables().get(tableName) != null) {
//            return model.getTables().get(tableName).getCqlQuery();
//        }
//        return null;
//    }

    /**
     * Gets the column family store.
     *
     * @param keyspaceName the keyspace name
     * @param cfName       the cf name
     * @return the column family store
     */
//    private static ColumnFamilyStore getColumnFamilyStore(String keyspaceName,
//                                                          String cfName) {
//        // Start by validating keyspace name
//        if (isValidKeySpace(keyspaceName)) {
//            Keyspace keyspace = Keyspace.open(keyspaceName);
//            // Make it works for indexes too - find parent cf if necessary
//            String baseName = cfName;
//            if (cfName.contains(".")) {
//                String[] parts = cfName.split("\\.", 2);
//                baseName = parts[0];
//            }
//
//            // IllegalArgumentException will be thrown here if ks/cf pair does not
//            // exist
//            try {
//                return keyspace.getColumnFamilyStore(baseName);
//            } catch (Throwable t) {
//                LOGGER.error(String
//                        .format("The provided column family is not part of this cassandra keyspace: keyspace = %s, column family = %s",
//                                keyspaceName, cfName));
//            }
//        }
//        return null;
//    }

    /**
     * Gets the keyspace metadata.
     *
     * @param keyspaceName the keyspace name
     * @return the keyspace metadata
     */
//    private static KeyspaceMetadata getKeyspaceMetadata(String keyspaceName) {
//        if (isValidKeySpace(keyspaceName)) {
//            Keyspace keyspace = Keyspace.open(keyspaceName);
//            return keyspace.getMetadata();
//        }
//
//        return null;
//    }

    /**
     * Checks if is valid key space.
     *
     * @param keyspaceName the keyspace name
     * @return true, if is valid key space
     */
//    private static boolean isValidKeySpace(String keyspaceName) {
//        if (Schema.instance.getKSMetaData(keyspaceName) == null) {
//            LOGGER.info(String.format(
//                    "Reference to nonexistent keyspace: %s!", keyspaceName));
//            return false;
//        }
//        return true;
//    }

    /**
     * Close.
     */
    public static void closeCluster() {
        if (session != null) {
            session.close();
            LOGGER.info("Closed session status=" + session.isClosed());
        }

        if (cluster != null) {
            cluster.close();
            LOGGER.info("Closed cluster status=" + cluster.isClosed());
        }
    }

    public static void closeSession(Session session) {
        if (session != null && !session.isClosed()) {
            session.closeAsync();
            LOGGER.info("Closed session status=" + session.isClosed());
        }
    }
}
