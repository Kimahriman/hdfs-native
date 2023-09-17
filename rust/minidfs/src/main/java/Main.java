import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.Credentials.SerializedFormat;
import org.apache.hadoop.security.token.Token;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.*;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.*;

public class Main {

    public static void main(String args[]) throws Exception {
        Set<String> flags = new HashSet<>();
        for (String arg : args) {
            flags.add(arg);
        }
        MiniKdc kdc = null;

        Configuration conf = new Configuration();
        if (flags.contains("security")) {
            kdc = new MiniKdc(MiniKdc.createConf(), new File("target/test/kdc"));
            kdc.setTransport("UDP");
            kdc.start();
            kdc.createPrincipal(new File("target/test/hdfs.keytab"), "hdfs/localhost");

            conf.set(HADOOP_SECURITY_AUTHENTICATION, "kerberos");
            conf.set(HADOOP_SECURITY_AUTHORIZATION, "true");
            if (flags.contains("privacy")) {
                conf.set(HADOOP_RPC_PROTECTION, "privacy");
            } else {
                conf.set(HADOOP_RPC_PROTECTION, "authentication");
            }
            conf.set(DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY, "target/test/hdfs.keytab");
            conf.set(DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, "hdfs/localhost@" + kdc.getRealm());
            conf.set(DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY, "target/test/hdfs.keytab");
            conf.set(DFS_DATANODE_KERBEROS_PRINCIPAL_KEY, "hdfs/localhost@" + kdc.getRealm());
            conf.set(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, "true");
            // conf.set(DFS_DATA_TRANSFER_PROTECTION_KEY, "authentication");
            conf.set(DFSConfigKeys.IGNORE_SECURE_PORTS_FOR_TESTING_KEY, "true");
        }

        MiniDFSNNTopology nnTopology = null;
        if (flags.contains("ha")) {
            nnTopology = MiniDFSNNTopology.simpleHATopology(3);
            conf.set(HdfsClientConfigKeys.Failover.PROXY_PROVIDER_KEY_PREFIX + ".minidfs-ns", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
            conf.set(DFSConfigKeys.DFS_NAMENODE_STATE_CONTEXT_ENABLED_KEY, "true");
            conf.set(DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_KEY, "true");
        }

        int numDataNodes = 1;
        if (flags.contains("ec")) {
            // Enough for the largest EC policy
            numDataNodes = 14;
        }

        HdfsConfiguration hdfsConf = new HdfsConfiguration(conf);
        MiniDFSCluster dfs = new MiniDFSCluster.Builder(hdfsConf)
            .nameNodePort(9000)
            .nameNodeHttpPort(9870)
            .nnTopology(nnTopology)
            .numDataNodes(numDataNodes)
            .build();

        hdfsConf.writeXml(new FileOutputStream("target/test/core-site.xml"));
        dfs.waitActive();

        int activeNamenode = 0;
        if (flags.contains("ha")) {
            activeNamenode = 2;
            // dfs.transitionToObserver(1);
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

        if (flags.contains("token")) {
            Credentials creds = new Credentials();
            if (flags.contains("ha")) {
                System.err.println("Getting token from namenode! " + dfs.getNameNode(2).getTokenServiceName());
                Token<DelegationTokenIdentifier> token = dfs.getNameNodeRpc(2).getDelegationToken(null);
                token.setService(new Text("ha-hdfs:minidfs-ns"));
                creds.addToken(new Text("ha-hdfs:minidfs-ns"), token);
            } else {
                System.err.println("Getting token from namenode! " + dfs.getNameNode().getTokenServiceName());
                Token<DelegationTokenIdentifier> token = dfs.getNameNodeRpc().getDelegationToken(null);
                token.setService(new Text(dfs.getNameNode().getTokenServiceName()));
                creds.addToken(new Text(dfs.getNameNode().getTokenServiceName()), token);
            }
            
            DataOutputStream os = new DataOutputStream(new FileOutputStream("target/test/delegation_token"));
            creds.writeTokenStorageToStream(os, SerializedFormat.WRITABLE);
            os.close();
        }

        System.out.println("Ready!");
        if (flags.contains("security")) {
            System.out.println(kdc.getKrb5conf().toPath().toString());
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        reader.readLine();
        dfs.close();

        if (flags.contains("security")) {
            kdc.stop();
        }
    }
}