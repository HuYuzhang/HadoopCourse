# 大数据管理
这里是大三下《大数据管理》课程实习作业，主要是学习Hadoop的使用方式。附上：
[助教gg提供的Hadoop配置流程](http://course.pku.edu.cn/bbcswebdav/pid-499733-dt-content-rid-3933358_2/courses/048-04833520-0006168313-1/hadoop_installation%20%28singlefile%29.html)

*其中有个小错误：*
```SHELL
ssh Slave1 "cd /usr/local/hadoop/hadoop-2.7.7;rm -r ./tmp ./logs/*"
```
*这句出现了两遍。注意第二次出现的地方应该是*
```SHELL
ssh Slave2 "cd /usr/local/hadoop/hadoop-2.7.7;rm -r ./tmp ./logs/*"
```

---

## 实习一： HDFS操作(4.1)
1. 每位同学完成基本的 HDFS Shell 命令操作。
 至少完成这些命令：cat chmod chown cp du get ls mkdir mv put rm setrep stat tail test touchz
 报告内容：这个实习比较简单，请在报告中附上相应的命令和截图即可，表明你真真切切操练了一把。

2.  两位同学组队，按照助教提供的 HDFS 文件的基本创建、读写操作的 java 代码，编程实现在 HDFS 中创建大批量小文件(文件内容随意)，分析小文件的 Block 是多大，体会使用hdfs存储小文件的缺点。
 报告内容：请在报告中详细写明你的实验步骤、技术方法、实习体会等，附上相应的代码段和截图。

3. 两位同学组队，接着第二项实习内容，我已经把一个图片数据集放上去了，名称为non_cancer_subset00.zip，包含大约140张图片，接近一个G的大小。请同学们做如下实验：
 把图片都写入到一个文件中，为此需要在本地构建一个文件索引表，（图片名、图片在文件中的偏移地址，图片大小），把图片作为大文件的记录插入，插入时需要同时维护这个文件索引表，读取图片时也是先查找这个索引表。
 这个实习很有实战意义，很多深度学习项目的训练集都会包含大量的图片，训练集很大，图片很多，那么这个实习也算是给出一个解决方案。
 报告内容：请在报告中写明你的实现逻辑，和将每张图片单独作为小文件来存储的性能比较，并附上相应的代码段和截图。[数据链接](http://course.pku.edu.cn/bbcswebdav/pid-500253-dt-content-rid-3938326_2/xid-3938326_2)

