package main;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.kms.server.MiniKMS;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreService;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.Credentials.SerializedFormat;
import org.apache.hadoop.security.token.Token;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.*;
import static org.apache.hadoop.fs.viewfs.Constants.*;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.*;

public class Main {

    public static void main(String args[]) throws Exception {
        Set<String> flags = new HashSet<>();
        for (String arg : args) {
            flags.add(arg);
        }
        MiniKdc kdc = null;

        // If an existing token exists, make sure to delete it
        new File("target/test/delegation_token").delete();

        Configuration conf = new Configuration();
        conf.set("dfs.blocksize", "16777216"); // 16 MiB instead of 128 MiB
        if (flags.contains("trash")) {
            conf.set(FS_TRASH_INTERVAL_KEY, "60");
        }

        MiniKMS kms = null;
        String kmsProviderUri = null;
        if (flags.contains("kms")) {
            kms = startMiniKMS();
            URL kmsUrl = kms.getKMSUrl();
            // Convert "http://host:port/kms" to "kms://http@host:port/kms",
            // matching `hadoop.security.key.provider.path` syntax.
            kmsProviderUri = "kms://" + kmsUrl.getProtocol() + "@" + kmsUrl.getAuthority() + kmsUrl.getPath();
            conf.set("hadoop.security.key.provider.path", kmsProviderUri);
        }
        if (flags.contains("security")) {
            kdc = new MiniKdc(MiniKdc.createConf(), new File("target/test/kdc"));
            kdc.setTransport("UDP");
            kdc.start();
            kdc.createPrincipal(new File("target/test/hdfs.keytab"), "hdfs/localhost");

            conf.set(HADOOP_SECURITY_AUTHENTICATION, "kerberos");
            conf.set(HADOOP_SECURITY_AUTHORIZATION, "true");
            if (flags.contains("privacy")) {
                conf.set(HADOOP_RPC_PROTECTION, "privacy");
                conf.set(DFS_DATA_TRANSFER_PROTECTION_KEY, "privacy");
                if (flags.contains("aes")) {
                    conf.set(DFS_ENCRYPT_DATA_TRANSFER_CIPHER_SUITES_KEY, "AES/CTR/NoPadding");
                }
            } else if (flags.contains("integrity")) {
                conf.set(HADOOP_RPC_PROTECTION, "integrity");
                conf.set(DFS_DATA_TRANSFER_PROTECTION_KEY, "integrity");
            } else {
                conf.set(HADOOP_RPC_PROTECTION, "authentication");
                conf.set(DFS_DATA_TRANSFER_PROTECTION_KEY, "authentication");
            }
            if (flags.contains("data_transfer_encryption")) {
                // Force encryption for all connections, legacy method before SASL connections were a thing
                conf.set(DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY, "true");
            }
            conf.set(DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY, "target/test/hdfs.keytab");
            conf.set(DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, "hdfs/localhost@" + kdc.getRealm());
            conf.set(DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY, "target/test/hdfs.keytab");
            conf.set(DFS_DATANODE_KERBEROS_PRINCIPAL_KEY, "hdfs/localhost@" + kdc.getRealm());
            conf.set(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, "true");
            conf.set(DFSConfigKeys.IGNORE_SECURE_PORTS_FOR_TESTING_KEY, "true");
        }

        HdfsConfiguration hdfsConf = new HdfsConfiguration(conf);

        MiniDFSCluster dfs = null;
        StateStoreDFSCluster routerDfs = null;
        if (flags.contains("rbf")) {
            routerDfs = new StateStoreDFSCluster(false, 2);

            Configuration routerOverrides = new RouterConfigBuilder()
                .stateStore()
                .rpc()
                .build();

            routerDfs.addRouterOverrides(routerOverrides);
            routerDfs.startCluster(hdfsConf);
            routerDfs.startRouters();

            RouterContext routerContext = routerDfs.getRandomRouter();
            StateStoreService stateStore = routerContext.getRouter().getStateStore();
            routerDfs.createTestMountTable(stateStore);

            routerDfs.waitClusterUp();

            hdfsConf.addResource(routerDfs.generateClientConfiguration());
            hdfsConf.addResource(routerDfs.getRouterClientConf());
            hdfsConf.set(FS_DEFAULT_NAME_KEY, "hdfs://fed");
        } else {
            MiniDFSNNTopology nnTopology = generateTopology(flags, hdfsConf);

            int numDataNodes = 4;
            if (flags.contains("ec")) {
                // Enough for the largest EC policy
                numDataNodes = 14;
            }

            dfs = new MiniDFSCluster.Builder(hdfsConf)
                .nameNodePort(9000)
                .nameNodeHttpPort(9870)
                .nnTopology(nnTopology)
                .numDataNodes(numDataNodes)
                .build();

            if (flags.contains("viewfs")) {
                hdfsConf.set(FS_DEFAULT_NAME_KEY, "viewfs://minidfs-viewfs");
            } else if (flags.contains("ha")) {
                hdfsConf.set(FS_DEFAULT_NAME_KEY, "hdfs://minidfs-ns");
            } else {
                hdfsConf.set(FS_DEFAULT_NAME_KEY, "hdfs://127.0.0.1:9000");
            }

            dfs.waitActive();

            int activeNamenode = 0;
            if (flags.contains("viewfs")) {
                // Each name services has two namenodes
                dfs.transitionToActive(0);
                dfs.transitionToActive(2);
            } else if (flags.contains("ha")) {
                activeNamenode = 2;
                dfs.transitionToObserver(1);
                dfs.transitionToActive(activeNamenode);
            }

            if (flags.contains("ec")) {
                DistributedFileSystem fs = dfs.getFileSystem(activeNamenode);
                fs.enableErasureCodingPolicy("RS-3-2-1024k");
                fs.enableErasureCodingPolicy("RS-10-4-1024k");
                fs.mkdirs(new Path("/ec-3-2"), new FsPermission("755"));
                fs.mkdirs(new Path("/ec-6-3"), new FsPermission("755"));
                fs.mkdirs(new Path("/ec-10-4"), new FsPermission("755"));
                fs.setErasureCodingPolicy(new Path("/ec-3-2"), "RS-3-2-1024k");
                fs.setErasureCodingPolicy(new Path("/ec-6-3"), "RS-6-3-1024k");
                fs.setErasureCodingPolicy(new Path("/ec-10-4"), "RS-10-4-1024k");
            }

            if (flags.contains("kms")) {
                DistributedFileSystem fs = dfs.getFileSystem(activeNamenode);
                // Create the encryption-zone master key via the KMS, then mark
                // /ezone as a zone backed by it. The Rust integration test reads
                // a file inside this zone and expects to recover the plaintext
                // written here through the standard Java HDFS client.
                KeyProvider keyProvider = fs.getClient().getKeyProvider();
                KeyProvider.Options keyOpts = new KeyProvider.Options(hdfsConf)
                    .setBitLength(128)
                    .setCipher("AES/CTR/NoPadding")
                    .setDescription("hdfs-native test key");
                keyProvider.createKey("test-key", keyOpts);
                keyProvider.flush();

                fs.mkdirs(new Path("/ezone"), new FsPermission("755"));
                HdfsAdmin admin = new HdfsAdmin(fs.getUri(), hdfsConf);
                admin.createEncryptionZone(new Path("/ezone"), "test-key");

                byte[] payload = "hdfs-native TDE round-trip test payload".getBytes();
                try (FSDataOutputStream out = fs.create(new Path("/ezone/file"))) {
                    out.write(payload);
                }
            }

            if (flags.contains("token")) {
                Credentials creds = new Credentials();
                if (flags.contains("ha")) {
                    Token<DelegationTokenIdentifier> token = dfs.getNameNodeRpc(2).getDelegationToken(null);
                    token.setService(new Text("ha-hdfs:minidfs-ns"));
                    creds.addToken(new Text("ha-hdfs:minidfs-ns"), token);
                } else {
                    Token<DelegationTokenIdentifier> token = dfs.getNameNodeRpc().getDelegationToken(null);
                    token.setService(new Text(dfs.getNameNode().getTokenServiceName()));
                    creds.addToken(new Text(dfs.getNameNode().getTokenServiceName()), token);
                }

                try (DataOutputStream os = new DataOutputStream(new FileOutputStream("target/test/delegation_token"))) {
                    creds.writeTokenStorageToStream(os, SerializedFormat.WRITABLE);
                }
            }
        }

        hdfsConf.writeXml(new FileOutputStream("target/test/core-site.xml"));

        System.out.println("Ready!");
        if (flags.contains("security")) {
            System.out.println(kdc.getKrb5conf().toPath().toString());
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        reader.readLine();

        if (dfs != null) {
            dfs.close();
        }
        if (routerDfs != null) {
            routerDfs.shutdown();
        }

        if (kms != null) {
            kms.stop();
        }

        if (flags.contains("security")) {
            kdc.stop();
        }
    }

    /**
     * Start a MiniKMS for HDFS Transparent Data Encryption tests. Uses simple
     * (pseudo) auth and a JCEKS keystore in target/test/kms.
     */
    private static MiniKMS startMiniKMS() throws Exception {
        File kmsDir = new File("target/test/kms").getAbsoluteFile();
        kmsDir.mkdirs();

        File keystoreFile = new File(kmsDir, "kms.keystore");
        // Wipe any leftover keystore from a previous run so `createKey` does
        // not collide with a stale entry.
        if (keystoreFile.exists()) {
            keystoreFile.delete();
        }

        // The Hadoop 3.5 JKS provider reads the keystore password from the
        // HADOOP_KEYSTORE_PASSWORD env var (set by the Rust harness) — its
        // password-file lookup only resolves classpath resources, not
        // filesystem paths, so an env var is the simplest path here.
        Configuration kmsConf = new Configuration(false);
        kmsConf.set(
            "hadoop.kms.key.provider.uri",
            "jceks://file@" + keystoreFile.toURI().getPath()
        );
        kmsConf.set("hadoop.kms.authentication.type", "simple");
        // Allow the proxy user used by the cluster to talk to the KMS.
        kmsConf.set("hadoop.kms.proxyuser.HTTP.users", "*");
        kmsConf.set("hadoop.kms.proxyuser.HTTP.hosts", "*");

        try (FileOutputStream fos = new FileOutputStream(new File(kmsDir, "kms-site.xml"))) {
            kmsConf.writeXml(fos);
        }
        try (FileOutputStream fos = new FileOutputStream(new File(kmsDir, "core-site.xml"))) {
            new Configuration(false).writeXml(fos);
        }
        // Per-key ACLs gate operations on individual keys. The defaults deny
        // everything; for tests we open them up.
        Configuration aclsConf = new Configuration(false);
        for (String op : new String[] {
                "MANAGEMENT", "GENERATE_EEK", "DECRYPT_EEK", "READ", "ALL"}) {
            aclsConf.set("default.key.acl." + op, "*");
            aclsConf.set("whitelist.key.acl." + op, "*");
        }
        try (FileOutputStream fos = new FileOutputStream(new File(kmsDir, "kms-acls.xml"))) {
            aclsConf.writeXml(fos);
        }

        MiniKMS kms = new MiniKMS.Builder()
            .setKmsConfDir(kmsDir)
            .build();
        kms.start();
        return kms;
    }

    public static MiniDFSNNTopology generateTopology(Set<String> flags, Configuration conf) {
        MiniDFSNNTopology nnTopology = null;
        if (flags.contains("viewfs")) {
            nnTopology = MiniDFSNNTopology.simpleHAFederatedTopology(2);
            conf.set(HdfsClientConfigKeys.Failover.PROXY_PROVIDER_KEY_PREFIX + ".ns0", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
            conf.set(HdfsClientConfigKeys.Failover.PROXY_PROVIDER_KEY_PREFIX + ".ns1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
            conf.set(DFSConfigKeys.DFS_NAMENODE_STATE_CONTEXT_ENABLED_KEY, "true");
            conf.set(DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_KEY, "true");
            conf.set(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, "0ms");
            conf.set(CONFIG_VIEWFS_PREFIX + ".minidfs-viewfs.link./mount1", "hdfs://ns0/nested");
            conf.set(CONFIG_VIEWFS_PREFIX + ".minidfs-viewfs.linkFallback", "hdfs://ns1/nested");
        } else if (flags.contains("ha")) {
            nnTopology = MiniDFSNNTopology.simpleHATopology(3);
            conf.set(HdfsClientConfigKeys.Failover.PROXY_PROVIDER_KEY_PREFIX + ".minidfs-ns", "org.apache.hadoop.hdfs.server.namenode.ha.ObserverReadProxyProvider");
            conf.set(DFSConfigKeys.DFS_NAMENODE_STATE_CONTEXT_ENABLED_KEY, "true");
            conf.set(DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_KEY, "true");
            conf.set(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, "0ms");
            conf.set(DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY, "0");
        }
        return nnTopology;
    }
}
