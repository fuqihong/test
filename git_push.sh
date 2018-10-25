#!/bin/sh

comment=$*
if [ ! -n "$1" ]; then
    echo -e "\033[41;33;1m请输入commit comment!!!!!!!!!!!!!!\033[0m"
else
    echo "开始提交代码"

	echo "检查更新"

	git pull
	git add .
	git commit -m "$*"
	git push -u origin master

	if [ $? -eq 0 ]; then
    	echo -e "\033[1;5;37;44m代码提交成功 SUCCESS\033[0m"
	else
    	echo -e "\033[41;33;1m代码提交失败 Fail!!!!!!!!!!!!!!\033[0m"
	fi
fi



