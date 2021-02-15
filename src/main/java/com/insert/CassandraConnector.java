package com.insert;

import com.datastax.driver.core.*;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import static java.lang.System.out;

/**
 * Class used for connecting to Cassandra database.
 */
public class CassandraConnector {
    /**
     * Cassandra Cluster.
     */
    private static Cluster cluster;
    /**
     * Cassandra Session.
     */
    private static Session session;

    /**
     * Connect to Cassandra Cluster specified by provided node IP
     * address and port number.
     *
     * @param port Port of cluster host.
     */
    public static void connectCluster(final int port) {

        //configure socket options
        SocketOptions options = new SocketOptions();
        options.setConnectTimeoutMillis(900000000);
        options.setReadTimeoutMillis(900000000);
        options.setTcpNoDelay(true);

        cluster = Cluster.builder()//.addContactPoints("10.105.22.171","10.105.22.172","10.105.22.173")
                .addContactPoints("localhost")
                .withPort(port)//.withCredentials("cassandra", "cassandra")
                .withProtocolVersion(ProtocolVersion.V4)
                //.withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.QUORUM))
                .withSocketOptions(options).build();
        final Metadata metadata = cluster.getMetadata();
        out.printf("Connected to cluster: %s\n", metadata.getClusterName());
        for (final Host host : metadata.getAllHosts()) {
            out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
                    host.getDatacenter(), host.getAddress(), host.getRack());
        }
        //session = cluster.connect();
    }


    public static Session connect() {
//        if (session == null || session.isClosed()) {
//            if (cluster == null || cluster.isClosed()) {
//                connectCluster(9042);
//            }
//            session = cluster.connect();
//        }
//        if (cluster == null || cluster.isClosed()) {
////            connectCluster(9042);
////        }
        connectCluster(9042);
        Session session = cluster.connect();
        return session;

    	/*initialize();
        return cluster.connect();*/
    }

    public static void closeSession(Session session) {
        if (session != null) {
            session.close();
            //LOGGER.info("Closed session status=" + session.isClosed());
        }
    }


    /**
     * Provide my Session.
     *
     * @return My session.
     */
    public static Session getSession() {
        return session;
    }

    /**
     * Close cluster.
     */
    public void close() {
        cluster.close();
    }
}