/**
 * 
 */
package hadoop.mr.traffic;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author hzliyue1,2016年8月27日,下午6:22:25
 *
 */
public class TrafficStatisticsMapper extends Mapper<LongWritable, Text, Text, TrafficBean>{

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String line = value.toString();
		String[] fields = StringUtils.split(line, '\t');
		super.map(key, value, context);
	}
	
	

	
}
