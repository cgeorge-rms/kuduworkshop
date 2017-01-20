package com.rms.kuduworkshop;

import org.apache.kudu.client.MiniKuduCluster;
import org.apache.kudu.client.shaded.com.google.common.net.HostAndPort;

import java.util.List;

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
