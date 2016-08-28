package hadoop.mr.traffic;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * 
 * @author hzliyue1,2016年8月21日,下午5:01:53
 *
 */
public class TrafficBean implements WritableComparable<TrafficBean> {

	private long up_traffic;
	private long down_traffic;
	private long sum_traffic;

	/**
	 * @param up_traffic
	 * @param down_traffic
	 */
	public TrafficBean(long up_traffic, long down_traffic) {
		this.up_traffic = up_traffic;
		this.down_traffic = down_traffic;
		this.sum_traffic = up_traffic + down_traffic;
	}

	/**
	 * 
	 */
	public TrafficBean() {
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(up_traffic);
		out.writeLong(down_traffic);
		out.writeLong(sum_traffic);
		// So, the flow sequence is: --> sum_traffic --> down_traffic
		// -->up_traffic -->
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		up_traffic = in.readLong();
		down_traffic = in.readLong();
		sum_traffic = in.readLong();

	}

	/**
	 * @return the up_traffic
	 */
	public long getUp_traffic() {
		return up_traffic;
	}

	/**
	 * @return the down_traffic
	 */
	public long getDown_traffic() {
		return down_traffic;
	}

	/**
	 * @return the sum_traffic
	 */
	public long getSum_traffic() {
		return sum_traffic;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return up_traffic + "\t" + down_traffic + "\t" + sum_traffic;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(TrafficBean o) {
		// TODO Auto-generated method stub
		return this.sum_traffic > o.getSum_traffic() ? -1 : 1;
	}
}
