/**
 * 
 */
package work.hbase;

import com.netease.recsys.recomhis.reuse_recom_his.ReuseHistoryFactory;
import com.netease.recsys.urs.util.UrsEncoder;

/**
 * @author hzliyue1,2016年8月25日,下午8:58:52
 *
 */
public class HbaseTest {

	
	public static void main(String[] args) {
		String useridE = UrsEncoder.encrypt("wadqse@126.com");
    	System.out.println(useridE);
    	//初始化HBase连接
    	ReuseHistoryFactory.getInstance().init();
    	
    	//传入userid或devid，id为经过UrsEncoder编码的数据，且不加‘_’区分userid和devid，查询时若两参数都不为空以userid为准
    	//获取用户复用文章推荐历史
		System.out.println(ReuseHistoryFactory.getInstance().getHistoryHbase().GetUserHis(useridE,""));
		
		//获取用户复用文章点击历史
		System.out.println(ReuseHistoryFactory.getInstance().getClkHistoryHbase().GetUserHis(useridE, ""));
	}
}
