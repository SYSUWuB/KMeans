package kmeans;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KMeansController {
	private int k;
	private int iterationNum;
	private String inputPath;
	private String outputPath;
	private Configuration conf;
	
	public KMeansController(int k, int iterationNum, String inputPath, String outputPath, Configuration conf) {
		this.k = k;
		this.iterationNum = iterationNum;
		this.inputPath = inputPath;
		this.outputPath = outputPath;
		this.conf = conf;
	}
	
	// �������Ľ��е������
	public void clusterCenterJob() throws IOException, InterruptedException, ClassNotFoundException{
		// ����iterationNum�ε���
		// ÿһ��ȡǰһ�εĽ�������µļ���
		for (int i = 0; i < iterationNum; i++) {
			Job clusterCenterJob = Job.getInstance(conf,"clusterCenterJob" + i);
			clusterCenterJob .setJarByClass(KMeans.class);			
			clusterCenterJob.getConfiguration().set("clusterPath", outputPath + "/cluster-" + i +"/");
			clusterCenterJob.setMapperClass(KMeans.KMeansMapper.class);
			clusterCenterJob.setMapOutputKeyClass(IntWritable.class);
			clusterCenterJob.setMapOutputValueClass(Cluster.class);
			clusterCenterJob.setCombinerClass(KMeans.KMeansCombiner.class);
			clusterCenterJob.setReducerClass(KMeans.KMeansReducer.class);
			clusterCenterJob.setOutputKeyClass(NullWritable.class);
			clusterCenterJob.setOutputValueClass(Cluster.class);
			FileInputFormat.addInputPath(clusterCenterJob, new Path(inputPath));
			FileOutputFormat.setOutputPath(clusterCenterJob, new Path(outputPath + "/cluster-" + (i + 1) +"/"));		
			clusterCenterJob.waitForCompletion(true);
			System.out.println("We have repeated " + String.valueOf(i) + " times.");
		}
	}

	// �������һ�ε��������ԭʼ�����ݽ������·���
	public void KMeansClusterJob() throws IOException, InterruptedException, ClassNotFoundException{
		Job kMeansClusterJob = Job.getInstance(conf,"KMeansClusterJob");
		kMeansClusterJob.setJarByClass(KMeansClusterView.class);		
		kMeansClusterJob.getConfiguration().set("clusterPath", outputPath + "/cluster-" + (iterationNum - 1) +"/");		
		kMeansClusterJob.setMapperClass(KMeansClusterView.KMeansClusterMapper.class);
		kMeansClusterJob.setMapOutputKeyClass(IntWritable.class);
		kMeansClusterJob.setMapOutputValueClass(Text.class);
		kMeansClusterJob.setReducerClass(KMeansClusterView.KMeansClusterReducer.class);
		kMeansClusterJob.setOutputKeyClass(Text.class);
		kMeansClusterJob.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(kMeansClusterJob, new Path(inputPath));
		FileOutputFormat.setOutputPath(kMeansClusterJob, new Path(outputPath + "/clusteredInstances" + "/"));		
		kMeansClusterJob.waitForCompletion(true);
		System.out.println("finished!");
	}
	
	//���ɳ�ʼ����K�������ģ�������д��cluster-0�ļ���
	public void generateInitialCluster(){
		//�����ʹ�����������������ļ���ԭ����·������ʼ�������ĸ���
		RandomCluster generator = new RandomCluster(conf, inputPath, k);
		//��ʼ����
		generator.InitialCluster(outputPath + "/");
	}	
		
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		System.out.println("start");
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://localhost:9000");
		conf.set("mapred.job.tracker", "localhost:9001");
		conf.set("valueNumber","4");
		int k = 3;				//���ĸ���
		int iterationNum = 3;	//��������
		String[] myArgs= { "KMeans_in", "KMeans_out"};
		String inputPath = myArgs[0];
		String outputPath = myArgs[1];
		KMeansController controller = new KMeansController(k, iterationNum, inputPath, outputPath, conf);
		//��ȡ�������ݣ���������ݵĳ�ʼ������
		controller.generateInitialCluster();
			
		System.out.println("initial cluster finished");
			
		controller.clusterCenterJob();
		controller.KMeansClusterJob();
	}
}
