package kmeans;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;


// �������k������
public final class RandomCluster {
	private int k;
	private FileStatus[] fileList;
	private FileSystem fs;
	private ArrayList<Cluster> kClusters;
	private Configuration conf;
	
	public RandomCluster(Configuration conf, String filePath, int k) {
		this.k = k;
		try {
			fs = FileSystem.get(URI.create(filePath),conf);
			fileList = fs.listStatus((new Path(filePath)));
			
			//�����ʼ����ΪK�Ŀ�arraylist��kcluster.size =0
			kClusters = new ArrayList<Cluster>(k);
			this.conf = conf;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	// ��ȡ���ݣ������ʼ����
	public void InitialCluster(String destinationPath) {
		Text line = new Text();
		FSDataInputStream fsi = null;
		
		try {
			for(int i = 0;i < fileList.length;i++){
				int count = 0;
				fsi = fs.open(fileList[i].getPath());
				LineReader lineReader = new LineReader(fsi,conf);
				while(lineReader.readLine(line) > 0){//��ȡ���ݣ������ݲ�Ϊ��ʱ
					System.out.println("read a line:" + line);
					if(line.toString().length()==0) {
						continue;
					}
					
					//��ȡ�������
					Point point = new Point(line.toString());
					//������150������count 0-149
					makeDecision(point,count);
					count++;
				}
				//lineReader.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				fsi.close();
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
		
		//������д�ص��ļ�
		writeBackToFile(destinationPath);
	}
	
	// �����жϣ����������滻��������
	public void makeDecision(Point point, int count) {
		// 3��mapper
		int split = count/50;
		
		if (kClusters.size() == split) {  // �����ǰ�������ĸ������ڷֶ�����Ҫ����
			Cluster cluster = new Cluster(kClusters.size() + 1, point);
			kClusters.add(cluster);
		}else {  // ������ȷ���Ƿ����Ӹþ���
			int choise = randomChoose(50);
			if (!(choise==-1)) {
				int id = split + 1;
				kClusters.remove(split);
				Cluster cluster = new Cluster(id, point);
				kClusters.add(cluster);
			}
		}
	}
	
	// ��1/(1+k)�ĸ��ʷ���һ��[0,k-1]�е����������򷵻�-1
	public int randomChoose(int k) {
		Random random = new Random();
		if (random.nextInt(k+1)==0) {
			return new Random().nextInt(k);
		}
		else {
			return -1;
		}
	}
	
	
	// ��������ɵľ�������д�ص��ļ���
	public void writeBackToFile(String destinationPath) {
		Path path = new Path(destinationPath + "cluster-0/clusters");
		FSDataOutputStream fsi = null;
		try {
			fsi = fs.create(path);
			for(Cluster cluster : kClusters) {
				fsi.write((cluster.toString() + "\n").getBytes());
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				fsi.close();
			} catch (IOException e2) {
				e2.printStackTrace();
			}
		}
	}
	
}
