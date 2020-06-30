package kmeans;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

// ��Ķ���
public class Point implements Writable{
	// ��ά����
	ArrayList<Double> values;
	String type;
	
	public Point() {
		values = new ArrayList<Double>();
	}
	public Point(ArrayList<Double> tempValues) {
		values = tempValues;
	}
	
	// ��ȡ���ݣ�ǰ4������һ����������꣬���һ����������
	public Point(String line) {
		String[] valueStrings = line.split(",");
		values = new ArrayList<Double>();
		for (int i = 0; i < 4; i++) {
			values.add(Double.parseDouble(valueStrings[i]));
		}
		type = valueStrings[4];
	}
	
	public void setValues(ArrayList<Double> tempValue) {
		values = tempValue;
	}
	
	public ArrayList<Double> getValues() {
		return values;
	}
	
	// ��һ������ת��Ϊstringֵ���
	public String toString() {
		String s = new String();
		for (int i = 0; i < values.size(); i++) {
			s += (values.get(i) + ",");
		}
		s += type;
		return s;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		int size = 0;
		values = new ArrayList<Double>();
		if((size = in.readInt()) != 0) {
			for (int i = 0; i < size; i++) {
				values.add(in.readDouble());
			}
		}
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(values.size());
		for (int i = 0; i < values.size(); i++) {
			out.writeDouble(values.get(i));
		}
	}
}
