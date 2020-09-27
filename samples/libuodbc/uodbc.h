/***********************************************************
	FILE NAME	    	 	:	uodbc.h
	Copyright 2010 DataComo Corporation. All rights reserved.
	
	Created by	    	 	:	LiuLe
	Create Time	    	 	:	2010.05.20
	Last Modified Time  	:	2010.05.20 
 	Modified by	    	 	:	LiuLe
************************************************************/
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#ifdef _DEBUG
#define printf_log	printf
#else
#define printf_log
#endif 

class UODBC {
	void *handle;
public:
	UODBC();
	int 		DmConnect(char* root,char* passwd,char* database); //Dm连接函数
	int	 	DmSetSQL(char* sqltemp);	    //设置sql语句
	int 		DmBind(int num,char* param); //Dm绑定参数
	int	 	DmOpen();			  	   	  //Dm执行查询语句
	char*		DmGetData(int num);		  //取属性列的值
	bool	 	DmNext();			  	   //结果集游标下移
	~UODBC();      				 
};
