#! /bin/bash

# By: pengtao@baidu.com
# Create date:    14 Jul 2014
# Last modified:  

###################################################
# purpose:
#      启动，关闭same_server
###################################################


HOST=127.0.0.1
PORT=8081
ABS_PATH=`pwd`/$0
prefix=`expr substr $0 1 1`
if [ $prefix == '/' ] ; then
    ABS_PATH=$0
fi

ABS_DIR=`dirname $ABS_PATH`

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/$ABS_DIR/../feature
PYTHON_BIN=/home/work/taopeng/local/Python-2.7.8/bin/python

case  "$1"  in
    "start")
   	    echo "nohup $PYTHON_BIN $ABS_DIR/same_server.py 1>> $ABS_DIR/../log/same_server.stdout 2>> $ABS_DIR/../log/same_server.stderr &"
        nohup $PYTHON_BIN $ABS_DIR/same_server.py 1>> $ABS_DIR/../log/same_server.stdout 2>> $ABS_DIR/../log/same_server.stderr &
	    ;;
    "stop")
        echo "curl -d \"vkey=UkoJRlAIxsCNlAWO\" $HOST:$PORT/exit"
	    curl -d "vkey=UkoJRlAIxsCNlAWO" $HOST:$PORT/exit
	    echo ""
	    ;;
    "restart")
        echo "curl $HOST:$PORT/ping"
        curl $HOST:$PORT/ping
        if [ $? -eq 0 ]; then
            echo ""
            echo "SERVER  $HOST:$PORT is OK"
	    else
            echo ""
            echo "restart by \"nohup $PYTHON_BIN $ABS_DIR/same_server.py 1>> $ABS_DIR/../log/same_server.stdout 2>> $ABS_DIR/../log/same_server.stderr & \""
            nohup $PYTHON_BIN $ABS_DIR/same_server.py 1>> $ABS_DIR/../log/same_server.stdout 2>> $ABS_DIR/../log/same_server.stderr &
        fi
	    ;;
    *)
        echo "the command for server_control.sh are:"
        echo "   start:   launch the server "
        echo "   stop:    elegent exit the server"
        echo "   restart: if the server is not alive, restart it."        
esac 


