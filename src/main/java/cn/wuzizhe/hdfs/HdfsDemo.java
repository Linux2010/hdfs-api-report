package cn.wuzizhe.hdfs;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

public class HdfsDemo {
public  static void main(String[] args) throws Exception {
		
		Configuration conf =new Configuration();
		
		conf.set("dfs.replication","2");
	
		conf.set("dfs.blocksize","64m");
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.216.11:9000/"),conf,"root");
	 
		fs.copyFromLocalFile(new Path("E:/Wuzizhe2.txt"), new Path("/"));
		fs.close();
		
	}


	FileSystem fs = null;

	@Before
	public void init() throws Exception {
		Configuration conf = new Configuration();
		conf.set("dfs.replication", "2");
		conf.set("dfs.blocksize", "64m");

		fs = FileSystem.get(new URI("hdfs://192.168.216.11:9000/"), conf, "root");

	}
	

	/**
	 * 从HDFS中下载文件到客户端本地磁盘
	 * 
	 * @throws IOException
	 * @throws IllegalArgumentException
	 */
	@Test
	public void testGet() throws IllegalArgumentException, IOException {

		fs.copyToLocalFile(new Path("/Wuzizhe2.txt"), new Path("D:/"));
		fs.close();

	}

	/**
	 * 在hdfs内部移动文件\修改名称
	 */
	@Test
	public void testRename() throws Exception {

		fs.rename(new Path("/xx"), new Path("/Wuzizhe"));

		fs.close();

	}

	/**
	 * 在hdfs中创建文件夹
	 */
	@Test
	public void testMkdir() throws Exception {

		fs.mkdirs(new Path("/Wuzizhe/Wuzizhe2.txt"));

		fs.close();
	}

	/**
	 * 在hdfs中删除文件或文件夹
	 */
	@Test
	public void testRm() throws Exception {

		fs.delete(new Path("/Wuzizhe/Wuzizhe2.tx"), true);

		fs.close();
	}

	/**
	 * 查询hdfs指定目录下的文件信息
	 */
	@Test
	public void testLs() throws Exception {
		// 只查询文件的信息,不返回文件夹的信息
		RemoteIterator<LocatedFileStatus> iter = fs.listFiles(new Path("/Wuzizhe2.txt"), true);

		while (iter.hasNext()) {
			LocatedFileStatus status = iter.next();
			System.out.println("文件全路径：" + status.getPath());
			System.out.println("块大小：" + status.getBlockSize());
			System.out.println("文件长度：" + status.getLen());
			System.out.println("副本数量：" + status.getReplication());
			System.out.println("块信息：" + Arrays.toString(status.getBlockLocations()));

			System.out.println("--------------------------------");
		}
		fs.close();
	}


	/**
	 * 读取hdfs中的文件的内容
	 * 
	 * @throws IOException
	 * @throws IllegalArgumentException
	 */
	@Test
	public void testReadData() throws IllegalArgumentException, IOException {

		FSDataInputStream in = fs.open(new Path("/Wuzizhe2.txt"));

		BufferedReader br = new BufferedReader(new InputStreamReader(in, "utf-8"));

		String line = null;
		while ((line = br.readLine()) != null) {
			System.out.println(line);
		}

		br.close();
		in.close();
		fs.close();

	}

	



}
