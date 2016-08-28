/**
 * 
 */
package hadoop.mr.traffic;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author hzliyue1,2016年8月28日,下午2:24:16
 *
 */
public class TrafficStatisticRuuner {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.resourcemanager.hostname", "211.87.227.209");
		conf.set("fs.defaultFS", "hdfs://211.87.227.209:9000");
		Job job = Job.getInstance(conf);
		job.setJarByClass(TrafficStatisticRuuner.class);
		
		job.setMapperClass(TrafficStatisticsMapper.class);
		job.setReducerClass(TrafficStatisticsReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TrafficBean.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(TrafficBean.class);
		
		FileInputFormat.setInputPaths(job, new Path("/traffic-log_20130313143750.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/trafficOut/"));
		job.waitForCompletion(true);
	}
}
