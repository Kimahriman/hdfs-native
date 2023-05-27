import java.io.IOException;

import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;

public class Main {

    public static void main(String args[]) throws IOException {
        System.out.println("Hello world!");
        HdfsConfiguration conf = new HdfsConfiguration();
        MiniDFSCluster dfs = new MiniDFSCluster.Builder(conf)
            .nameNodePort(9000)
            .build();
        dfs.waitActive();
    }
}