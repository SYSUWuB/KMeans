package kmeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeans {
	public static Integer valueNumber = Integer.valueOf(0);
	
	public static class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Cluster>{
		// 存储上一次迭代得到的聚类集合
		private ArrayList<Cluster> kClusters = new ArrayList<Cluster>();
		
		// 执行Map任务之前，进行变量的集中初始化
		@Override
		protected void setup(Context context) throws IOException, InterruptedException{
			super.setup(context);
			valueNumber = Integer.valueOf(context.getConfiguration().get("valueNumber"));
			if(valueNumber == 0) {
				System.out.println("number is error!");
				return;
			}
			
			FileSystem fs = FileSystem.get(context.getConfiguration());
			// 从聚类中心文件中读取聚类中心数据
			FileStatus[] fileList = fs.listStatus(new Path(context.getConfiguration().get("clusterPath")));
			BufferedReader in = null;
			FSDataInputStream fsi = null;
			
			String line = null;
			for(int i = 0; i < fileList.length; i++) {
				if(!fileList[i].isDirectory()) {
					fsi = fs.open(fileList[i].getPath());
					in = new BufferedReader(new InputStreamReader(fsi, "UTF-8"));
					while((line = in.readLine()) != null) {
						// 读取上一次迭代的选出的聚类中心，初始化聚类
						// 聚类ID， 聚类点个数， 聚类中心对象坐标
						Cluster cluster = new Cluster(line);
						kClusters.add(cluster);
					}
				}
			}
			in.close();
			fsi.close();
		}
		
		// 遍历Points.txt，找到每个点最近的聚类ID，然后以该点为中心构造新聚类
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			// 从Point.txt中读取行
			Point point = new Point(value.toString());
			if(point.getValues().size() != valueNumber) {
				System.out.println("Point.txt is error!");
				return;
			}
			
			int clusterID;
			try {
				// 找到最近的聚类中心的ID
				clusterID = getNearest(point);
				if(clusterID == -1) {
					throw new InterruptedException("find the nearest cluster is failed");
				} else {
					// 以当前对象为中心， 构造聚类对象， 点数为1
					Cluster cluster = new Cluster(clusterID, (long) 1, point);
					
					// 各点分到的新聚类的结果(id， 聚类)， 点的个数为1，聚类中心为当前遍历到的点
					// 聚类ID， 点的个数1， 坐标
					context.write(new IntWritable(clusterID), cluster);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		// 返回距离最近的聚类的ID
		public int getNearest(Point point) throws Exception{
			int clusterID = -1;
			double minDistance = Double.MAX_VALUE;
			double newDistance = 0.0;
			
			// 计算每个点的欧式距离
			for(Cluster cluster : kClusters) {
				for(int i=0; i < point.getValues().size(); i++) {
					newDistance += Math.pow((cluster.getCenter().getValues().get(i)-point.getValues().get(i)), 2);
				}
				newDistance = Math.sqrt(newDistance);
				if(newDistance < minDistance) {
					clusterID = cluster.getClusterID();
					minDistance = newDistance;
				}
			}
			
			return clusterID;
		}
		
	}
	
	// 合并，减少传送到reducer的数据个数
	// 输入输出类型相同，即于map/reduce相同
	public static class KMeansCombiner extends Reducer <IntWritable, Cluster, IntWritable, Cluster> {
		public void reduce(IntWritable key, Iterable<Cluster> value, Context context) throws IOException, InterruptedException{
			// 存放累计各个属性值
			ArrayList<Double> values = new ArrayList<Double>();
			for (int i = 0; i < valueNumber; i++) {
				values.add(0.0);
			}
			
			int numPoint = 0;
			// 遍历相同ID的聚类， 计数， 累加各点属性
			for (Cluster cluster : value) {
				// 计数，同一个ID聚类中对象个数，每个聚类的点都是1个
				numPoint ++;
				
				// 坐标累加
				for (int i = 0; i < values.size(); i++) {
					values.set(i, values.get(i)+cluster.getCenter().getValues().get(i));
				}
			}
			
			// 如果聚类中的点数不为0
			if(numPoint > 0) {
				// 构建新聚类中心， 累计值的平均值
				for (int i = 0; i < values.size(); i++) {
					values.set(i, values.get(i)/numPoint);
				}
			}
			
			// 新聚类
			Cluster cluster = new Cluster(key.get(), (long) numPoint, new Point(values));
			
			// 聚类ID， 聚类中点的个数， 聚类坐标(相同ID的，数量为1的聚类的中心坐标累计平均值)
			context.write(key, cluster);
		}
	}

	// 输出新cluster， 聚类ID， 合并不同节点的combiner结果
	// 这些combiner最后输出的key可能有几个时，相同的要做最后的合并
	public static class KMeansReducer extends Reducer<IntWritable, Cluster, NullWritable, Cluster> {
		public void reduce(IntWritable key, Iterable<Cluster> value, Context context) throws IOException, InterruptedException{
			// 存放累计各个属性值
			ArrayList<Double> values = new ArrayList<Double>();
			for (int i = 0; i < valueNumber; i++) {
				values.add(0.0);
			}
			
			int numPoint = 0;
			for(Cluster cluster : value) {
				numPoint += cluster.getNumCluster();
				
				// 坐标累加
				for (int i = 0; i < values.size(); i++) {
					values.set(i, values.get(i)+cluster.getCenter().getValues().get(i)*cluster.getNumCluster());
				}
			}
			
			// 聚类中点的个数不为0
			if (numPoint > 0) {
				// 构造新聚类中心， 坐标为累计值的平均值
				for (int i = 0; i < values.size(); i++) {
					values.set(i, values.get(i)/numPoint);
				}
			}
			Cluster cluster = new Cluster(key.get(), (long) numPoint, new Point(values));
			
			context.write(NullWritable.get(), cluster);
		}
	}
}
