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
		//fetch a line content
		String line = value.toString();
		//split we need
		String[] fields = StringUtils.split(line, '\t');
		//get num,up,down traffice
		String telNumber = fields[1];
		long up_traffic = Long.parseLong(fields[fields.length - 3]);
		long down_traffic = Long.parseLong(fields[fields.length - 2]);
		//load to TrafficBean
		TrafficBean trafficBean = new TrafficBean(up_traffic,down_traffic);
		//output telNum and trafficbean as Key-Value
		context.write(new Text(telNumber), trafficBean);
	}
	
	

	
}
