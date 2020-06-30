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
		// �洢��һ�ε����õ��ľ��༯��
		private ArrayList<Cluster> kClusters = new ArrayList<Cluster>();
		
		// ִ��Map����֮ǰ�����б����ļ��г�ʼ��
		@Override
		protected void setup(Context context) throws IOException, InterruptedException{
			super.setup(context);
			valueNumber = Integer.valueOf(context.getConfiguration().get("valueNumber"));
			if(valueNumber == 0) {
				System.out.println("number is error!");
				return;
			}
			
			FileSystem fs = FileSystem.get(context.getConfiguration());
			// �Ӿ��������ļ��ж�ȡ������������
			FileStatus[] fileList = fs.listStatus(new Path(context.getConfiguration().get("clusterPath")));
			BufferedReader in = null;
			FSDataInputStream fsi = null;
			
			String line = null;
			for(int i = 0; i < fileList.length; i++) {
				if(!fileList[i].isDirectory()) {
					fsi = fs.open(fileList[i].getPath());
					in = new BufferedReader(new InputStreamReader(fsi, "UTF-8"));
					while((line = in.readLine()) != null) {
						// ��ȡ��һ�ε�����ѡ���ľ������ģ���ʼ������
						// ����ID�� ���������� �������Ķ�������
						Cluster cluster = new Cluster(line);
						kClusters.add(cluster);
					}
				}
			}
			in.close();
			fsi.close();
		}
		
		// ����Points.txt���ҵ�ÿ��������ľ���ID��Ȼ���Ըõ�Ϊ���Ĺ����¾���
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			// ��Point.txt�ж�ȡ��
			Point point = new Point(value.toString());
			if(point.getValues().size() != valueNumber) {
				System.out.println("Point.txt is error!");
				return;
			}
			
			int clusterID;
			try {
				// �ҵ�����ľ������ĵ�ID
				clusterID = getNearest(point);
				if(clusterID == -1) {
					throw new InterruptedException("find the nearest cluster is failed");
				} else {
					// �Ե�ǰ����Ϊ���ģ� ���������� ����Ϊ1
					Cluster cluster = new Cluster(clusterID, (long) 1, point);
					
					// ����ֵ����¾���Ľ��(id�� ����)�� ��ĸ���Ϊ1����������Ϊ��ǰ�������ĵ�
					// ����ID�� ��ĸ���1�� ����
					context.write(new IntWritable(clusterID), cluster);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		// ���ؾ�������ľ����ID
		public int getNearest(Point point) throws Exception{
			int clusterID = -1;
			double minDistance = Double.MAX_VALUE;
			double newDistance = 0.0;
			
			// ����ÿ�����ŷʽ����
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
	
	// �ϲ������ٴ��͵�reducer�����ݸ���
	// �������������ͬ������map/reduce��ͬ
	public static class KMeansCombiner extends Reducer <IntWritable, Cluster, IntWritable, Cluster> {
		public void reduce(IntWritable key, Iterable<Cluster> value, Context context) throws IOException, InterruptedException{
			// ����ۼƸ�������ֵ
			ArrayList<Double> values = new ArrayList<Double>();
			for (int i = 0; i < valueNumber; i++) {
				values.add(0.0);
			}
			
			int numPoint = 0;
			// ������ͬID�ľ��࣬ ������ �ۼӸ�������
			for (Cluster cluster : value) {
				// ������ͬһ��ID�����ж��������ÿ������ĵ㶼��1��
				numPoint ++;
				
				// �����ۼ�
				for (int i = 0; i < values.size(); i++) {
					values.set(i, values.get(i)+cluster.getCenter().getValues().get(i));
				}
			}
			
			// ��������еĵ�����Ϊ0
			if(numPoint > 0) {
				// �����¾������ģ� �ۼ�ֵ��ƽ��ֵ
				for (int i = 0; i < values.size(); i++) {
					values.set(i, values.get(i)/numPoint);
				}
			}
			
			// �¾���
			Cluster cluster = new Cluster(key.get(), (long) numPoint, new Point(values));
			
			// ����ID�� �����е�ĸ����� ��������(��ͬID�ģ�����Ϊ1�ľ�������������ۼ�ƽ��ֵ)
			context.write(key, cluster);
		}
	}

	// �����cluster�� ����ID�� �ϲ���ͬ�ڵ��combiner���
	// ��Щcombiner��������key�����м���ʱ����ͬ��Ҫ�����ĺϲ�
	public static class KMeansReducer extends Reducer<IntWritable, Cluster, NullWritable, Cluster> {
		public void reduce(IntWritable key, Iterable<Cluster> value, Context context) throws IOException, InterruptedException{
			// ����ۼƸ�������ֵ
			ArrayList<Double> values = new ArrayList<Double>();
			for (int i = 0; i < valueNumber; i++) {
				values.add(0.0);
			}
			
			int numPoint = 0;
			for(Cluster cluster : value) {
				numPoint += cluster.getNumCluster();
				
				// �����ۼ�
				for (int i = 0; i < values.size(); i++) {
					values.set(i, values.get(i)+cluster.getCenter().getValues().get(i)*cluster.getNumCluster());
				}
			}
			
			// �����е�ĸ�����Ϊ0
			if (numPoint > 0) {
				// �����¾������ģ� ����Ϊ�ۼ�ֵ��ƽ��ֵ
				for (int i = 0; i < values.size(); i++) {
					values.set(i, values.get(i)/numPoint);
				}
			}
			Cluster cluster = new Cluster(key.get(), (long) numPoint, new Point(values));
			
			context.write(NullWritable.get(), cluster);
		}
	}
}
