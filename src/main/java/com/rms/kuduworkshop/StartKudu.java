package com.rms.kuduworkshop;

import org.apache.kudu.client.MiniKuduCluster;
import org.apache.kudu.client.shaded.com.google.common.net.HostAndPort;

import java.util.List;
/**
 Things to have in workshop..
 Spark population
 Spark reads
 pushdown filter and projection example

 For Java:
 Create a table... primary keys, hashing/ranges, replication
 populate table with fake data (bulk write example)
 Read back fake data single threaded
 Read back fake data multi threaded
 Kudu async client example?

 Maybe create multiple table with different partitioning for quick performance comparison

 For presentation:
 Overview of kudu master
 Overview of kudu tservers (slaves)
 http://kudu.apache.org/docs/images/kudu-architecture-2.png
 HA configurations with multi master
 HA configuration with replication
 Storage on disk
 Client side partition pruning

 What not to do... and what too do
 Make sure you have all masters listed
 Singleton pattern for kudu client
 If using spark use the spark datasource!
 Kudu Async client isn't what you think it is ( doesn't run a thread pool)
 If you want to add parallelism in java use token client with thread pool
 Data larger than 64k per cell


 */




/**
 * Created by cgeorge on 1/20/17.
 */
public class StartKudu {
    public static void main(String[] args) {
        try {
            MiniKuduCluster miniCluster = new MiniKuduCluster.MiniKuduClusterBuilder()
                    .numMasters(1)
                    .numTservers(3)
                    .build();

            miniCluster.waitForTabletServers(1);
            System.out.println("Server running press enter to exit");

            List<HostAndPort> masterHostPorts = miniCluster.getMasterHostPorts();
            for (HostAndPort masterHostPort : masterHostPorts) {
                System.out.println("masterHostPort = " + masterHostPort);
            }

            System.in.read();
            System.out.println("Shutdown initiating");
            if (miniCluster != null) miniCluster.shutdown();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
