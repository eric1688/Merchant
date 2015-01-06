package org.bai.hadoopTest;


import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.JobConf;

public class HdfsDAO {
	public static final String HDFS="hdfs://192.168.75.133:9000";
	public HdfsDAO(Configuration conf ){
		this(HDFS,conf);
	}
	public HdfsDAO(String hdfs,Configuration conf){
		this.hdfsPath=hdfs;
		this.conf=conf;
	}
	//hdfs路径
	private String hdfsPath;
	//hadoop系统配置
	private Configuration conf;
	public static void main(String[] args)throws IOException{
		JobConf conf=config();
		HdfsDAO hdfs=new HdfsDAO(conf);
	//hdfs.mkdirs("/tmp/new/one");
//		hdfs.ls("/tmp/new");
//	hdfs.rmr("/tmp/new/one");
//		hdfs.rmr("/tmp/hadoop-hadoop");
//		hdfs.ls("/tmp");
//		hdfs.ls("/tmp/new");
		//item.csv 对应eclipse上的文件地址，也可以设置windows上文件地址
		//hdfs.rmr("/CFRecommender/tmp");
		//hdfs.rmr("/user/orisun/input/tmp");
//		
		hdfs.copyFile("D:/11.doc", "/hddtmn/doc");
//		hdfs.copyFile("D:/score.txt", "/CFRecommender/score/score");
//		hdfs.copyFile("D:/tenantID.txt", "/CFRecommender/item/tenantID");
		//hdfs.copyFile("D:/tenantID.txt", "/recommender/item/tenantID.txt");
		//hdfs.ls("/recommender");
	//	hdfs.rename("/user/hdfs/matrix/output", "/user/hdfs/matrix/output1");
	}
	public static JobConf config(){
		JobConf conf=new JobConf(HdfsDAO.class);
		conf.setJobName("HdfsDAO");
		//classpath:/classpath:/  classpath:/hadoop/core-site.xml
		conf.addResource("hadoop/core-site.xml");
		conf.addResource("hadoop/hdfs-site.xml");
		conf.addResource("hadoop/mapred-site.xml");
		return conf;
		
	}
	public void createFile(String file,String content)throws IOException{
		FileSystem fs=FileSystem.get(URI.create(hdfsPath),conf);
		byte[] buff=content.getBytes();
		FSDataOutputStream os=null;
		try{
		os=fs.create(new Path(file));
		os.write(buff,0,buff.length);
		System.out.println("Create: "+file);
		}finally{
			if(os!=null)
				os.close();
		}
		fs.close();
	}
	public void mkdirs(String folder)throws IOException{
		Path path=new Path(folder);
		FileSystem fs=FileSystem.get(URI.create(hdfsPath), conf);
		if(!fs.exists(path)){
			fs.mkdirs(path);
			System.out.println("Create: "+folder);		
		}
		fs.close();
	}
	public void cat(String remoteFile)throws IOException{
		Path path=new Path(remoteFile);
		FileSystem fs=FileSystem.get(URI.create(hdfsPath),conf);
		FSDataInputStream fsdis=null;
		System.out.println("cat:"+remoteFile);
		try{
			fsdis=fs.open(path);
			/*
			 * 	in - InputStrem to read from
				out - OutputStream to write to
				buffSize - the size of the buffer
				close - whether or not close the InputStream and OutputStream at the end. The streams are closed in the finally clause.
			 */
			IOUtils.copyBytes(fsdis, System.out, 4096,false);
			}finally{
				IOUtils.closeStream(fsdis);
				fs.close();
			}
	}
	public void rmr(String folder) throws IOException{
		Path path=new Path(folder);
		FileSystem fs=FileSystem.get(URI.create(hdfsPath), conf);
		fs.deleteOnExit(path);
		System.out.println("Delete: "+folder);
		fs.close();
	}
	public void ls(String folder)throws IOException{
		Path path=new Path(folder);
		FileSystem fs=FileSystem.get(URI.create(hdfsPath),conf);
		FileStatus[] list=fs.listStatus(path);
		System.out.println("ls: "+folder);
		System.out.println("==========================================================");
		for(FileStatus f:list){
			System.out.printf("name:%s,folder:%s,size:%d\n",f.getPath(),f.isDir(),f.getLen());
		}
		System.out.println("==========================================================");
		fs.close();
	}
	public void copyFile(String local ,String remote)throws IOException{
		FileSystem fs=FileSystem.get(URI.create(hdfsPath), conf);
		fs.copyFromLocalFile(new Path(local), new Path(remote));
		System.out.println("copy form:" +local+"to"+remote);
		fs.close();
	}
	public void rename(String src, String dst) throws IOException {
        Path name1 = new Path(src);
        Path name2 = new Path(dst);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        fs.rename(name1, name2);
        System.out.println("Rename: from " + src + " to " + dst);
        fs.close();
    }
}
