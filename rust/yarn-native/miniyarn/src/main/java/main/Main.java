package main;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;

public final class Main {
    public static void main(String[] args) throws Exception {
        String workDir = System.getenv("YARN_NATIVE_TEST_DIR");
        if (workDir == null || workDir.isEmpty()) {
            throw new IllegalStateException("YARN_NATIVE_TEST_DIR must be set");
        }
        System.setProperty("test.build.data", workDir);

        YarnConfiguration conf = new YarnConfiguration();
        conf.setBoolean(YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS, false);

        MiniYARNCluster cluster = new MiniYARNCluster("yarn-native", 1, 1, 1, 1);
        AtomicBoolean stopped = new AtomicBoolean(false);
        Runnable stopCluster = () -> {
            if (stopped.compareAndSet(false, true)) {
                cluster.stop();
            }
        };
        Runtime.getRuntime().addShutdownHook(new Thread(stopCluster, "miniyarn-shutdown"));

        try {
            cluster.init(conf);
            cluster.start();

            String address = cluster.getResourceManager(0).getConfig()
                    .get(YarnConfiguration.RM_ADDRESS);
            if (address == null || address.isEmpty()) {
                throw new IllegalStateException("MiniYARNCluster did not publish the RM address");
            }

            System.out.println("READY\t" + address);
            System.out.flush();
            new BufferedReader(new InputStreamReader(System.in)).readLine();
        } finally {
            stopCluster.run();
        }
    }
}
