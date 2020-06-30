package kmeans;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

// 聚类的定义
public class Cluster implements Writable{
	// 聚类ID
	private int clusterID;
	
	// 聚类中的总点数
	private long numCluster;
	
	//聚类中心点
	private Point center;
	
	public Cluster() {
		this.clusterID = -1;
		this.numCluster = 0;
		this.center = new Point();
	}
	
	public Cluster(int clusterID, Point center) {
		this.clusterID = clusterID;
		this.numCluster = 0;
		this.center = center;
	}
	
	public Cluster(int clusterID, Long numCluster, Point center) {
		this.clusterID = clusterID;
		this.numCluster = numCluster;
		this.center = center;
	}
	
	public Cluster(String line) {
		String[] values = line.split(".",3);
		clusterID = Integer.parseInt(values[0]);
		numCluster = Integer.parseInt(values[1]);
		center = new Point(values[2]);
	}
	
	public String toString() {
		String result = String.valueOf(clusterID) + "," + String.valueOf(numCluster) + "," + center.toString();
		return result;
	}
	
	public int getClusterID() {
		return clusterID;
	}
	
	public long getNumCluster() {
		return numCluster;
	}
	
	public Point getCenter() {
		return center;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		clusterID = in.readInt();
		numCluster = in.readLong();
		center.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(clusterID);
		out.writeLong(numCluster);
		center.write(out);
	}
	
}
