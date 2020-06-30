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
	
	// 聚类中心进行迭代求解
	public void clusterCenterJob() throws IOException, InterruptedException, ClassNotFoundException{
		// 进行iterationNum次迭代
		// 每一次取前一次的结果进行新的计算
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

	// 根据最后一次迭代结果对原始的数据进行重新分类
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
	
	//生成初始化的K个簇中心，将他们写到cluster-0文件中
	public void generateInitialCluster(){
		//构造初使化簇生产器，配置文件，原数据路径，初始化簇中心个数
		RandomCluster generator = new RandomCluster(conf, inputPath, k);
		//初始化簇
		generator.InitialCluster(outputPath + "/");
	}	
		
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		System.out.println("start");
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://localhost:9000");
		conf.set("mapred.job.tracker", "localhost:9001");
		conf.set("valueNumber","4");
		int k = 3;				//中心个数
		int iterationNum = 3;	//迭代次数
		String[] myArgs= { "KMeans_in", "KMeans_out"};
		String inputPath = myArgs[0];
		String outputPath = myArgs[1];
		KMeansController controller = new KMeansController(k, iterationNum, inputPath, outputPath, conf);
		//读取输入数据，并完成数据的初始化处理
		controller.generateInitialCluster();
			
		System.out.println("initial cluster finished");
			
		controller.clusterCenterJob();
		controller.KMeansClusterJob();
	}
}
