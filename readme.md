# HISTORY

    1. 2014-06-30, 星期一 20:34 : 创建项目：美丽说同款服务。




# DESCRIPTION

    基于图片（及其他特征）进行同款识别（相同图片识别）。
	


# PROJECT DETAIL

1. 部署信息
   1. 机器： work@bi-test01.meilishuo.com
   2. 路径： /home/work/Gemini
   3. 入口脚本是 run.sh
	  1. crontab中： 05,25,45 * * * * cd /home/work/Gemini; ./run.sh >> log/run.stderr 2>&1
2. 程序目录
   1. run.sh ： crontab的调用脚本
   2. build_xxxx.py : 被run.sh调用，建立各种索引
   3. server :  提供同款服务的http server。
   4. plot_xxxxx.py : 输出html，检查算法效果的各种脚本。
   5. samelib： 项目的各种库文件
   6. feature： 从图片（url）提取特征（高维向量）的函数， 由荣国提供并封装。
   7. data：  图片的特征文件和索引文件所在目录。
   8. log： 日志
   9. conf: 配置。目前全在build.yaml中
3. run.sh 调用4个脚本
   1. fetch_verify_wait_in_mysql.py: 每20分钟（每小时05,25,45分）调用一次。
	  1. 从数据库dolphin中审核待审表（t_dolphin_twitter_verify_wait）表选出全部就绪的twitter（已经完成宝贝类目计算）。
	  2. 将所有推发给同款检测服务（http server，对应same_serer.py）
	  3. 将同款检测结果写回待审表。类似 update t_dolphin_twitter_verify_wait set same_twitter_id=-1 where twitter_id = 2981621993;
   2. build_label_daily.py： 每天凌晨4点左右调用一次。
	  1. 主要输入是hive的ods_dolphin_twitter_verify_operation表（每天1~2点就绪）。
	  2. 从operation表中选出 *上一个星期六* 到 *昨天* 的所有新审核数据，抽取特征， 建立索引。
	  3. 输出类似 data/index_label_daily/daily_20140720_20140720
   3. build_label_weekly.py： 每周日早上10点调用。
	  1. 主要输入是hive的ods_dolphin_twitter_verify_operation表（所有已审核数据）
	  2. 从operation表中选出 *全部数据* ，抽取特征， 建立索引。
	  3. 出入类似： data/index_label_weekly/label_util_20140720
   4. build_all_weekly.py： 每周日早上10点调用。
	  1. 主要输入是hive中ods_dolphin_stat_sell_pool表。
	  2. 从表中筛选上周瀑布流展现较多的商品（目前是5次）， 抽取特征，建立索引。
	  3. 根据索引，计算每件商品的近邻，将全部商品聚类
	  4. 输出类似： data/index_all_weekly/all_20140713_20140719
   5. build_label_xxx.py建立的索引为流行度审核服务（一个新推，继承同款推的流行度打分）。 build_all_xxx.py为线上排序使用，对同款推进行打压。
4. server/same_server.py
   1. 提供同款检测的http服务。不要直接启动或者杀死server。利用同目录下的server/server_control.sh
	  1. server_control.sh start
	  2. server_control.sh stop
	  3. server_control.sh restart
   2. 主要服务端口为 localhost:8873/result。 接口说明见same_server.py的文件头。
   3. same_server.py启动时会读取三个索引，一次请求中依次查询。
	  1. 大索引，上周六之前所有已经审核的数据
		 1. data/index_label_weekly/current
	  2. 小索引，上周日到昨天，所有已经审核数据
		 1. data/index_label_daily/current
	  3. 实时索引，最近5w个审核的数据
		 1. server/rt_index_dir
   4. same_server.py目前所有的请求都是fetch_verify_wait_in_mysql.py发起的。


# TODO


