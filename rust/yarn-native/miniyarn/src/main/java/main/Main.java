package main;

import java.io.File;
import java.io.FileOutputStream;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.ha.HAServiceProtocol.RequestSource;
import org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.*;
import org.apache.hadoop.yarn.server.MiniYARNCluster;

public class Main {

    // Tests ask for a ResourceManager restart by creating this file, and wait for the done file.
    private static final File RESTART_TRIGGER = new File("target/test/restart-rm");
    private static final File RESTART_DONE = new File("target/test/rm-restarted");
    // With the ha flag, tests ask for a failover from rm0 to rm1 the same way.
    private static final File FAILOVER_TRIGGER = new File("target/test/failover-rm");
    private static final File FAILOVER_DONE = new File("target/test/rm-failed-over");

    public static void main(String args[]) throws Exception {
        Set<String> flags = new HashSet<>();
        for (String arg : args) {
            flags.add(arg);
        }

        new File("target/test").mkdirs();
        RESTART_TRIGGER.delete();
        RESTART_DONE.delete();
        FAILOVER_TRIGGER.delete();
        FAILOVER_DONE.delete();
        boolean ha = flags.contains("ha");
        boolean recovery = ha || flags.contains("recovery");

        YarnConfiguration conf = new YarnConfiguration();

        MiniKdc kdc = null;
        MiniDFSCluster dfs = null;
        if (flags.contains("security")) {
            kdc = new MiniKdc(MiniKdc.createConf(), new File("target/test/kdc"));
            kdc.setTransport("UDP");
            kdc.start();
            // The services and the tests all authenticate as yarn/localhost. createPrincipal
            // overwrites the keytab file, so all principals must be created in one call.
            String keytab = new File("target/test/yarn.keytab").getAbsolutePath();
            kdc.createPrincipal(new File(keytab), "yarn/localhost", "HTTP/localhost");
            String principal = "yarn/localhost@" + kdc.getRealm();

            conf.set(HADOOP_SECURITY_AUTHENTICATION, "kerberos");
            conf.set(HADOOP_SECURITY_AUTHORIZATION, "true");
            conf.set(HADOOP_RPC_PROTECTION, "authentication");
            // The ResourceManager only hands an unmanaged ApplicationMaster's AMRMToken to a caller
            // whose user name equals the application's user. A Kerberos principal's full name
            // never does, so let yarn/localhost impersonate users, as gateway services do.
            conf.set("hadoop.proxyuser.yarn.users", "*");
            conf.set("hadoop.proxyuser.yarn.hosts", "*");
            conf.set(YarnConfiguration.RM_PRINCIPAL, principal);
            conf.set(YarnConfiguration.RM_KEYTAB, keytab);
            conf.set(YarnConfiguration.NM_PRINCIPAL, principal);
            conf.set(YarnConfiguration.NM_KEYTAB, keytab);
            UserGroupInformation.setConfiguration(conf);

            // A Kerberized HDFS, so containers can be tested reading it with a delegation token
            conf.set(DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY, keytab);
            conf.set(DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, principal);
            conf.set(DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY, keytab);
            conf.set(DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY, principal);
            conf.set(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, "true");
            // Without data transfer protection, which the NodeManager's localizer doesn't get
            // from this config, so it can download application files from the DataNode
            conf.set(DFSConfigKeys.IGNORE_SECURE_PORTS_FOR_TESTING_KEY, "true");
            HdfsConfiguration hdfsConf = new HdfsConfiguration(conf);
            dfs = new MiniDFSCluster.Builder(hdfsConf)
                .nameNodePort(19000)
                .numDataNodes(1)
                .build();
            dfs.waitActive();
            conf.set(FS_DEFAULT_NAME_KEY, "hdfs://127.0.0.1:19000");
        }
        // Small containers so tests fit on any machine
        conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 64);
        conf.setInt(YarnConfiguration.NM_PMEM_MB, 4096);
        conf.setBoolean(YarnConfiguration.NM_PMEM_CHECK_ENABLED, false);
        conf.setBoolean(YarnConfiguration.NM_VMEM_CHECK_ENABLED, false);
        // Don't mark the node unhealthy when the test machine's disk is nearly full
        conf.setFloat(
            YarnConfiguration.NM_MAX_PER_DISK_UTILIZATION_PERCENTAGE, 100.0f);
        // Fail fast so tests don't wait for retries of an attempt
        conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 1);

        if (recovery) {
            // Keep application state on disk so a restarted or newly active ResourceManager
            // recovers it, and keep containers running across the change
            File stateStore = new File("target/test/rm-state-store").getAbsoluteFile();
            deleteRecursively(stateStore);
            conf.setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
            conf.setBoolean(YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_ENABLED, true);
            conf.set(YarnConfiguration.RM_STORE,
                "org.apache.hadoop.yarn.server.resourcemanager.recovery.FileSystemRMStateStore");
            conf.set(YarnConfiguration.FS_RM_STATE_STORE_URI, stateStore.toURI().toString());
            // Reconnect quickly once the restarted ResourceManager is up, instead of after the
            // default 30 second retry interval
            conf.setLong(YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS, 500);
            conf.setLong(YarnConfiguration.NM_RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS, 500);
            // By default the NodeManager calls the ResourceManager in-process, so it would keep
            // talking to the stopped instance. Over RPC it reconnects, which needs fixed ports.
            conf.setBoolean(YarnConfiguration.YARN_MINICLUSTER_USE_RPC, true);
            conf.setBoolean(YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS, true);
        }
        if (ha) {
            // Two ResourceManagers sharing the state store. Without ZooKeeper there is no
            // automatic failover, so tests switch the active one through the admin service.
            conf.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, false);
            conf.set(YarnConfiguration.RM_HA_IDS, "rm0,rm1");
            String[][] ports = {
                {YarnConfiguration.RM_ADDRESS, "032"},
                {YarnConfiguration.RM_SCHEDULER_ADDRESS, "030"},
                {YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS, "031"},
                {YarnConfiguration.RM_ADMIN_ADDRESS, "033"},
                {YarnConfiguration.RM_WEBAPP_ADDRESS, "088"},
                {YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS, "090"},
            };
            for (int i = 0; i < 2; i++) {
                for (String[] port : ports) {
                    conf.set(port[0] + ".rm" + i, "localhost:" + (18 + i * 10) + port[1]);
                }
            }
        } else if (recovery) {
            conf.set(YarnConfiguration.RM_ADDRESS, "localhost:18032");
            conf.set(YarnConfiguration.RM_SCHEDULER_ADDRESS, "localhost:18030");
            conf.set(YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS, "localhost:18031");
            conf.set(YarnConfiguration.RM_ADMIN_ADDRESS, "localhost:18033");
            conf.set(YarnConfiguration.RM_WEBAPP_ADDRESS, "localhost:18088");
        }

        MiniYARNCluster yarn = new MiniYARNCluster("yarn-native", ha ? 2 : 1, 1, 1, 1);
        yarn.init(conf);
        yarn.start();
        if (!yarn.waitForNodeManagersToConnect(60_000)) {
            throw new IllegalStateException("NodeManager did not register with the ResourceManager");
        }

        // The cluster's config holds the ports it bound
        yarn.getConfig().writeXml(new FileOutputStream("target/test/yarn-site.xml"));
        File coreSite = new File("target/test/core-site.xml");
        if (dfs != null) {
            // The security and HDFS settings, for clients and for Hadoop commands in containers
            conf.writeXml(new FileOutputStream(coreSite));
        } else {
            coreSite.delete();
        }

        final MiniYARNCluster shutdownYarn = yarn;
        final MiniDFSCluster shutdownDfs = dfs;
        final MiniKdc shutdownKdc = kdc;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                shutdownYarn.stop();
            } catch (Exception e) {
                e.printStackTrace(System.err);
            }
            if (shutdownDfs != null) {
                try {
                    shutdownDfs.close();
                } catch (Exception e) {
                    e.printStackTrace(System.err);
                }
            }
            if (shutdownKdc != null) {
                try {
                    shutdownKdc.stop();
                } catch (Exception e) {
                    e.printStackTrace(System.err);
                }
            }
        }, "miniyarn-shutdown"));

        if (recovery) {
            Thread restarter = new Thread(() -> watchForRestarts(shutdownYarn), "miniyarn-restarter");
            restarter.setDaemon(true);
            restarter.start();
        }

        System.out.println("Ready!");
        if (kdc != null) {
            System.out.println(kdc.getKrb5conf().toPath().toString());
        }

        // SIGTERM triggers the JVM shutdown hook above, which stops the cluster.
        new CountDownLatch(1).await();
    }

    private static void watchForRestarts(MiniYARNCluster yarn) {
        while (true) {
            try {
                if (FAILOVER_TRIGGER.delete()) {
                    StateChangeRequestInfo request =
                        new StateChangeRequestInfo(RequestSource.REQUEST_BY_USER_FORCED);
                    yarn.getResourceManager(0).getRMContext().getRMAdminService()
                        .transitionToStandby(request);
                    yarn.getResourceManager(1).getRMContext().getRMAdminService()
                        .transitionToActive(request);
                    if (!yarn.waitForNodeManagersToConnect(60_000)) {
                        throw new IllegalStateException("NodeManager did not register with rm1");
                    }
                    FAILOVER_DONE.createNewFile();
                }
                if (RESTART_TRIGGER.delete()) {
                    yarn.restartResourceManager(0);
                    if (!yarn.waitForNodeManagersToConnect(60_000)) {
                        throw new IllegalStateException("NodeManager did not re-register");
                    }
                    RESTART_DONE.createNewFile();
                }
                Thread.sleep(100);
            } catch (Exception e) {
                e.printStackTrace(System.err);
                return;
            }
        }
    }

    private static void deleteRecursively(File file) {
        File[] children = file.listFiles();
        if (children != null) {
            for (File child : children) {
                deleteRecursively(child);
            }
        }
        file.delete();
    }
}
