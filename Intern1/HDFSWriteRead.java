// hw2:
// 按照助教提供的 HDFS 文件的基本创建、读写操作的 java 代码，编程实现在 HDFS 中创建大批量小文件(文件内容随意)，分析小文件的 Block 是多大，体会使用hdfs存储小文件的缺点。
// 报告内容：请在报告中详细写明你的实验步骤、技术方法、实习体会等，附上相应的代码段和截图。

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

// 通过创建1000个小文件，测试性能。分析Block指令是"hdfs fsck hw2"
// 结果显式，每个小文件的块大小是11B，这就意味着产生了1000个块，应该不是什么好事情
public class HDFSWriteRead {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://Master:9000");
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            FileSystem fs = FileSystem.get(conf);
            int fileNum = 1000;
            for (int i = 0; i < fileNum; ++i) {
                String fileName = "hw2/" + Integer.toString(i);
                Path file = new Path(fileName);
                createFile(fs, file);
            }
            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void createFile(FileSystem fs, Path file) throws IOException {
        byte[] buff = "Hello World".getBytes();
        FSDataOutputStream os = fs.create(file);
        os.write(buff, 0, buff.length);
        System.out.println("Create:" + file.getName());
        os.close();
    }
    public static void readFile(FileSystem fs, Path file) throws IOException {
        FSDataInputStream in = fs.open(file);
        BufferedReader d = new BufferedReader(new InputStreamReader( in ));
        String content = d.readLine();
        System.out.println(content);
        d.close(); in .close();
    }
}