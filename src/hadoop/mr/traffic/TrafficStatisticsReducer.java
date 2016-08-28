/**
 * 
 */
package hadoop.mr.traffic;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author hzliyue1,2016年8月28日,下午2:10:18
 *
 */
public class TrafficStatisticsReducer extends Reducer<Text, TrafficBean, Text, TrafficBean>{

	/**
	 * reduce输入的数据形如：<手机号，{trafficBean1, trafficBean2……}>
	 */
	@Override
	protected void reduce(Text key, Iterable<TrafficBean> values,
			Reducer<Text, TrafficBean, Text, TrafficBean>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//define up,down traffice counter
		long up_traffic_counter = 0 ;
		long down_traffice_counter = 0;
		for(TrafficBean trafficBean: values){
			up_traffic_counter += trafficBean.getUp_traffic();
			down_traffice_counter += trafficBean.getDown_traffic();
		}
		TrafficBean resultBean = new TrafficBean(up_traffic_counter,down_traffice_counter);
		context.write(key, resultBean);
	}
}
