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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeansClusterView {
	public static class KMeansClusterMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
		private ArrayList<Cluster> kClusters = new ArrayList<Cluster>();
		
		// 读取最后一次得到的聚类中心结果
		protected void setup(Context context) throws IOException, InterruptedException{
			super.setup(context);
			FileSystem fs = FileSystem.get(context.getConfiguration());
			FileStatus[] fileList = fs.listStatus(new Path(context.getConfiguration().get("clusterPath")));
			BufferedReader in = null;
			FSDataInputStream fsi = null;
			
			String line = null;
			for (int i = 0; i < fileList.length; i++) {
				if(!fileList[i].isDirectory()) {
					fsi = fs.open(fileList[i].getPath());
					in = new BufferedReader(new InputStreamReader(fsi, "UTF-8"));
					while ((line = in.readLine())!= null) {
						System.out.println("read a line:" + line);
						Cluster cluster = new Cluster(line);
						kClusters.add(cluster);
					}
				}
			}
			in.close();
			fsi.close();
		}
		
		// 为源数据中的每个点找到最近的聚类
		// 聚类ID， 数据对象字符串
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			Point instance = new Point(value.toString());
			int id;
			try {
				id = getNearest(instance);
				if(id == -1)
					throw new InterruptedException("id == -1");
				else {
					context.write(new IntWritable(id), value);
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
	
	public static class KMeansClusterReducer extends Reducer<IntWritable, Text, Text, Text>{
		public void reduce(IntWritable key, Iterable<Text> value, Context context)throws 
		IOException, InterruptedException{
			String str = "\n";
			int count = 0;
			for(Text textIntance: value){
				str= str + textIntance.toString()+"\n";
				count++;
			}
			context.write(new Text("cluster"+key+" has "+count+" objects:"),new Text(str));
		}
		
	}
	
	
}
