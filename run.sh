#! /bin/bash

# By: pengtao@baidu.com
# Create date:    14 Jul 2014
# Last modified:  

###################################################
# purpose:
#      对所有的建库命令进行封装，每20分钟启动一次。
#      1. 每20分钟启动，从数据库t_dolphin_twitter_verify_wait中获取一审待审。
#          1. 发给same_server， 计算同款组
#          2. 将同款组数据写入t_dophin_twitter_verify_wait
#      2.  每天凌晨6点启动天级别建库。
#          1. build_label_daily.py
#          2. 输出目录类似 data/index_label_daily/current -> data/index_label_daily/daily_from_20140713
#      3.  每周日中午12点启动周级别建库
#          1. build_all_weekly.py
#          2. 输入目录类似：data/index_label_weekly/current -> data/index_label_weekly/label_util_20140714
#
###################################################

PWD=`pwd`
TEMP_LOCK_FILE='tmp.run.instance.lock'
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$PWD/feature
PYTHON_BIN=/home/work/taopeng/local/Python-2.7.8/bin/python

function lock_run_instance ()
{
    
    while [ 1 ]
    do
	ts=`date` 
	if [ -e $TEMP_LOCK_FILE  ];then
	    echo "[$ts] other intance is running!"
	    sleep 5m	# 1 minute, 另一种选择是退出，保证不要积累太多的任务。20分钟一次。
	else
	    break
	fi
    done
    touch $TEMP_LOCK_FILE
    echo "================================================"
    echo "[$ts] begin run.sh. create the lock file $TEMP_LOCK_FILE"
}
	    


RUNTIME=`date`
RUN_WEEKDAY=`date +%a` 		# Sun
RUN_HOUR=`date +%H`		# 0~23
RUN_MINUTE=`date +%M`		# 0~59

# RUN_HOUR=6
# RUN_MINUTE=1

lock_run_instance

# 执行周级脚本
if [ $RUN_WEEKDAY = 'Sun'  ] ; then 
#test
#if [ $RUN_WEEKDAY = 'Tue'  ] ; then 
  if [ $RUN_HOUR -eq 10 ] ; then # 保证sell_pool表生成
	if [ $RUN_MINUTE -lt 20 ] ; then
	    ts=`date`
	    echo "[$ts] stop same server"
	    $PWD/server/server_control.sh stop
	    echo ""

	    ts=`date`
	    echo "[$ts] $PYTHON_BIN build_all_weekly.py"
	    $PYTHON_BIN build_all_weekly.py >>log/build_all_weekly.stderr 2>&1

	    ts=`date`
	    echo "[$ts] $PYTHON_BIN build_label_weekly.py"
	    $PYTHON_BIN build_label_weekly.py  >>log/build_label_weekly.stderr 2>&1

	    ts=`date`
	    echo "[$ts] start same serer"
	    $PWD/server/server_control.sh start
	    echo ""
	    sleep 1m
        # 把输出靠给策略同学使用（to shenchongwei）
        cat $PWD/data/index_all_weekly/current/*tid2group > $PWD/data/index_all_weekly/current/same_all_weekly
        scp $PWD/data/index_all_weekly/current/same_* search@search01:/home/search/same_pic_result/
        scp $PWD/data/index_all_weekly//current/same_* search@search02:/home/search/same_pic_result/
        scp $PWD/data/index_all_weekly//current/same_* search@gz-search01:/home/search/same_pic_result/
        
        cat $PWD/data/index_label_weekly/current/*tid2group > $PWD/data/index_label_weekly/current/same_label_weekly
        scp $PWD/data/index_label_weekly/current/same_* search@search01:/home/search/same_pic_result/
        scp $PWD/data/index_label_weekly/current/same_* search@search02:/home/search/same_pic_result/
        scp $PWD/data/index_label_weekly/current/same_* search@gz-search01:/home/search/same_pic_result/
 
	fi
    fi
fi

# 执行天级建库脚本
if [ $RUN_HOUR -eq 4 ] ; then	# 保证sqoops的脚本导完了。
    if [ $RUN_MINUTE -lt 20 ] ; then

	ts=`date`
	echo "[$ts] stop same server"
	$PWD/server/server_control.sh stop
	echo ""
	
	ts=`date`
	echo "[$ts] $PYTHON_BIN build_label_daily.py"
	$PYTHON_BIN build_label_daily.py >> log/build_label_daily.stderr 2>&1
	
	ts=`date`
	echo "[$ts] start same server"
	$PWD/server/server_control.sh start
	sleep 1m
	echo ""
    #把数据发送给崇伟
    cat $PWD/data/index_label_daily/current/*tid2group > $PWD/data/index_label_daily/current/same_label_daily
    scp $PWD/data/index_label_daily/current/same_* search@search01:/home/search/same_pic_result/
    scp $PWD/data/index_label_daily/current/same_* search@search02:/home/search/same_pic_result/
    scp $PWD/data/index_label_daily/current/same_* search@gz-search01:/home/search/same_pic_result/
    fi
fi
    


# 每20分钟执行一次，dump数据
ts=`date`
$PWD/server/server_control.sh restart
echo ""
sleep 1m
echo "[$ts] fetch twitter and query same server"
$PYTHON_BIN fetch_verify_wait_in_mysql.py >> log/fetch_verify_wait_in_mysql.stderr 2>&1

ts=`date`
echo "[$ts] end run.sh. remove the lock file $TEMP_LOCK_FILE"
rm $TEMP_LOCK_FILE
