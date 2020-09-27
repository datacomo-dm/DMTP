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
	int 		DmConnect(char* root,char* passwd,char* database); //Dm���Ӻ���
	int	 	DmSetSQL(char* sqltemp);	    //����sql���
	int 		DmBind(int num,char* param); //Dm�󶨲���
	int	 	DmOpen();			  	   	  //Dmִ�в�ѯ���
	char*		DmGetData(int num);		  //ȡ�����е�ֵ
	bool	 	DmNext();			  	   //������α�����
	~UODBC();      				 
};
