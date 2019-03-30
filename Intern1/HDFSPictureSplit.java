import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;


public class HDFSWriteRead {
	 public static void main(String[] args){
        try{
            
			// **************  HDFS configuration ****************
			Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://Master:9000");
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            FileSystem fs = FileSystem.get(conf);
            String HDFSFileName = "split/";
			// **************  HDFS configuration ****************
            
            
	        // ************** Index File configuration **********
			String folderName = "/home/alice/work/non_cancer_subset00";
			File folderFile = new File(folderName);
			File[] files = folderFile.listFiles();
	        // ************** Index File configuration **********
			
			
			//*************** Common configuration **************
			int index = 0;
			int bufferSize = 1024;
			byte[] buffer = new byte[bufferSize];
			int offset = 0;
			int fileLen = 0;
			int tmpLen = 0;
			//*************** Common configuration **************
			
			
			for (File file:files)
			{
				
				String fullFileName = folderName + "/" + file.getName();
				BufferedInputStream bin = new BufferedInputStream(new FileInputStream(fullFileName));
				fileLen = 0;
				tmpLen = 0;
	            FSDataOutputStream HDFSw = fs.create(new Path(HDFSFileName + Integer.toString(index)));
				while ((tmpLen = bin.read(buffer, 0, bufferSize)) > 0)
				{
					offset += tmpLen;
					fileLen += tmpLen;
					HDFSw.write(buffer, 0, tmpLen);
				}
				index += 1;
				bin.close();
				System.out.println("Finish img: " + Integer.toString(index));
			}
            fs.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}