import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
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
        }

        HdfsConfiguration hdfsConf = new HdfsConfiguration(conf);
        MiniDFSCluster dfs = new MiniDFSCluster.Builder(hdfsConf)
            .nameNodePort(9000)
            .nnTopology(nnTopology)
            .build();

        hdfsConf.writeXml(new FileOutputStream("target/test/core-site.xml"));
        dfs.waitActive();

        if (flags.contains("ha")) {
            dfs.transitionToObserver(1);
            dfs.transitionToActive(2);
        }

        System.out.println("Ready!");
        if (flags.contains("security")) {
            System.out.println(kdc.getKrb5conf().toPath().toString());
        }

        if (flags.contains("token")) {
            Token<DelegationTokenIdentifier> token = dfs.getNameNodeRpc().getDelegationToken(null);
            token.setService(new Text(dfs.getNameNode().getTokenServiceName()));
            Credentials creds = new Credentials();
            creds.addToken(new Text("localhost:9000"), token);
            DataOutputStream os = new DataOutputStream(new FileOutputStream("target/test/delegation_token"));
            creds.writeTokenStorageToStream(os, SerializedFormat.PROTOBUF);
            os.close();
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        reader.readLine();
        dfs.close();

        if (flags.contains("security")) {
            kdc.stop();
        }
    }
}