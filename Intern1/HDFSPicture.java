// hw3:
// 把图片都写入到一个文件中，为此需要在本地构建一个文件索引表，（图片名、图片在文件中的偏移地址，图片大小），把图片作为大文件的记录插入，插入时需要同时维护这个文件索引表，读取图片时也是先查找这个索引表。
// 这个实习很有实战意义，很多深度学习项目的训练集都会包含大量的图片，训练集很大，图片很多，那么这个实习也算是给出一个解决方案。
// 报告内容：请在报告中写明你的实现逻辑，和将每张图片单独作为小文件来存储的性能比较，并附上相应的代码段和截图。
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import java.io.*;


// 第一次运行的时候取消掉18行的注释，从而建立索引文件和HDFS文件。
// 以后运行记得重新注释掉18行.（懒得写文件存在条件判断了）。每个索引项内容包括：fileName, index, offset, length
public class HDFSPicture {
    public static int fileNum = 140;
    public static void main(String[] args) {
        //		all2one();
        readHDFS(1, "/home/alice/target1.tiff");
        readHDFS(2, "/home/alice/target2.tiff");
    }

    public static void all2one() {
        // Conduct the Index file and make all-in-one HDFS file
        try {
            // ************** Index File configuration **********
            String folderName = "/home/alice/work/non_cancer_subset00";
            String indexName = "/home/alice/work/index.log";
            File folderFile = new File(folderName);
            File[] files = folderFile.listFiles();
            File indexFile = new File(indexName);
            // if (indexFile.exists())
            // {
            // 	return; // We have already construct the index file
            // }
            FileWriter w = new FileWriter(indexFile);
            // ************** Index File configuration **********

            // **************  HDFS configuration ****************
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://Master:9000");
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            FileSystem fs = FileSystem.get(conf);
            String HDFSFileName = "all2one";
            FSDataOutputStream HDFSw = fs.create(new Path(HDFSFileName));
            // **************  HDFS configuration ****************

            //*************** Common configuration **************
            int index = 0;
            int bufferSize = 1024;
            byte[] buffer = new byte[bufferSize];
            int offset = 0;
            int fileLen = 0;
            int tmpLen = 0;
            //*************** Common configuration **************

            for (File file: files) {
                String fullFileName = folderName + "/" + file.getName();
                BufferedInputStream bin = new BufferedInputStream(new FileInputStream(fullFileName));
                fileLen = 0;
                tmpLen = 0;
                // Note that for each file, we have four lines: fileName, index, offset, length
                w.write(fullFileName + "\n");
                w.write(Integer.toString(index) + "\n");
                w.write(Integer.toString(offset) + "\n");
                while ((tmpLen = bin.read(buffer, 0, bufferSize)) > 0) {
                    offset += tmpLen;
                    fileLen += tmpLen;
                    HDFSw.write(buffer, 0, tmpLen);
                }
                w.write(Integer.toString(fileLen) + "\n");
                index += 1;
                bin.close();
                System.out.println("Finish img: " + Integer.toString(index));
            }
            w.close();
            HDFSw.close();
            fs.close();
            fileNum = index;
        } catch (Exception e) {
            e.printStackTrace();
            // do nothing
        }
    }
    public static void readHDFS(int index, String targetName) {
        try {
            String indexName = "/home/alice/work/index.log";
            BufferedReader indexReader = new BufferedReader(new InputStreamReader(new FileInputStream(indexName)));
            for (int i = 0; i < fileNum; ++i) {
                String name = indexReader.readLine();
                String _index = indexReader.readLine();
                String offset = indexReader.readLine();
                String length = indexReader.readLine();
                if (index == Integer.parseInt(_index)) {
                    indexReader.close();
                    xReadHDFS(Integer.parseInt(offset), targetName);
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void readHDFS(String rawName, String targetName) {
        try {
            String indexName = "/home/alice/work/index.log";
            BufferedReader indexReader = new BufferedReader(new InputStreamReader(new FileInputStream(indexName)));
            for (int i = 0; i < fileNum; ++i) {
                String name = indexReader.readLine();
                String _index = indexReader.readLine();
                String offset = indexReader.readLine();
                String length = indexReader.readLine();
                if (rawName == name) {
                    indexReader.close();
                    xReadHDFS(Integer.parseInt(offset), targetName);
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void xReadHDFS(int _offset, String targetName) {
        try {
            // **************  HDFS configuration ****************
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://Master:9000");
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            FileSystem fs = FileSystem.get(conf);
            String HDFSFileName = "all2one";
            FSDataInputStream HDFSin = fs.open(new Path(HDFSFileName));
            // **************  HDFS configuration ****************

            // ************** Index File configuration **********
            String indexName = "/home/alice/work/index.log";
            BufferedReader indexReader = new BufferedReader(new InputStreamReader(new FileInputStream(indexName)));
            BufferedOutputStream targetWriter = new BufferedOutputStream(new FileOutputStream(targetName));

            for (int i = 0; i < fileNum; ++i) {
                String name = indexReader.readLine();
                String index = indexReader.readLine();
                String offset = indexReader.readLine();
                String length = indexReader.readLine();
                if (_offset == Integer.parseInt(offset)) {
                    HDFSin.seek(Integer.parseInt(offset));
                    int targetLength = Integer.parseInt(length);
                    int bufferSize = 1024;
                    byte[] buffer = new byte[bufferSize];
                    int tmpLength = 0;
                    int cnt = 0;
                    while ((tmpLength = HDFSin.read(buffer, 0, bufferSize)) > 0) {
                        cnt += tmpLength;
                        if (cnt <= targetLength) {
                            targetWriter.write(buffer, 0, tmpLength);
                            if (cnt == targetLength) {
                                break;
                            }
                        } else {
                            targetWriter.write(buffer, 0, tmpLength - (cnt - targetLength));
                            break;
                        }
                    }
                    break;
                }
            }
            HDFSin.close();
            fs.close();
            indexReader.close();
            targetWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}