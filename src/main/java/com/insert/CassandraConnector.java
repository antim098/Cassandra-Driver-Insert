package com.insert;

import com.datastax.driver.core.*;

import static java.lang.System.out;

/**
 * Class used for connecting to Cassandra database.
 */
public class CassandraConnector {
    /**
     * Cassandra Cluster.
     */
    static Cluster cluster;
    /**
     * Cassandra Session.
     */
    static Session session;

    /**
     * Connect to Cassandra Cluster specified by provided node IP
     * address and port number.
     *
     * @param port Port of cluster host.
     */
    public static void connect(final int port) {

        //configure socket options
        SocketOptions options = new SocketOptions();
        options.setConnectTimeoutMillis(900000000);
        options.setReadTimeoutMillis(900000000);
        options.setTcpNoDelay(true);

        cluster = Cluster.builder().addContactPoints("localhost")
                .withPort(port).withCredentials("cassandra", "cassandra")
                .withProtocolVersion(ProtocolVersion.V4)
                //.withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.QUORUM))
                .withSocketOptions(options).build();
        final Metadata metadata = cluster.getMetadata();
        out.printf("Connected to cluster: %s\n", metadata.getClusterName());
        for (final Host host : metadata.getAllHosts()) {
            out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
                    host.getDatacenter(), host.getAddress(), host.getRack());
        }
        session = cluster.connect();
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