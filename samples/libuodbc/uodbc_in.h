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
#include "sql.h"
#include "sqlext.h"
#include "sqltypes.h"
#define Maxsqllen 8000 
#define MaxBuffer 1500 

class UODBCIN{
	SQLHENV          	V_OD_Env;                 // Handle ODBC environment
	SQLHDBC         	V_OD_hdbc;              	// Handle connection
	long           		V_OD_erg;                  // result of functions
	SQLHSTMT        	V_OD_hstmt;	
	SQLHSTMT        	V_OD_hstmt1;	
//	int			hstmt_num;	
	char            	V_OD_msg[200],V_OD_buffer[200];
	char			sqltxt[Maxsqllen];		//��ѯ��sql���
	char			sqltxt2[Maxsqllen];		//��ѯ��sql���
	SQLINTEGER      	V_OD_err,V_OD_rowanz,V_OD_id;
	SQLINTEGER 		param_temp;		//�󶨲���ʹ�õ�һ�����ñ���
	char            	V_OD_stat[10];          	// Status SQL
	SQLSMALLINT 		V_OD_mlen,V_OD_colanz;
	int 			ParamNo;
	int 			num[10];
	char			BindParam[10][50];
	SQLINTEGER		ParamLEN[10];
	SQLINTEGER  		flag;    		//��ʾ�ǲ�����Ҫͨ������view�������в�ѯ
	char 			selet[Maxsqllen];
	char 			sqlnew[Maxsqllen]; 		//="select flag,view from dm_model where type = ";
	int 			getflag();
	char 			*fial;
	int 			strpos(char *string,char *substring);
	char* 			BindPhoneNo(char *sqltxt ,char *NO);
	int 			makesqltxt(char *oldsql,int len,char *fial,char *temp,char *type);
	int 			abc(char *fial,char *argv);
	void            	DmSQLJoin();
public:
	char                    sqlbuffer1[MaxBuffer];	//���ֻ��һ���󶨲�������Ҫ�������ȡ
	char                    sqlbuffer2[MaxBuffer];
	int			sqlno;
	int 			colanz;
	UODBCIN();
	int 		DmConnect(char* root,char* passwd,char* database); //Dm���Ӻ���
	int	 	DmSetSQL(char* sqltemp);	    //����sql���
	int 		DmBind(int num,char* param); //Dm�󶨲���
	int	 	DmOpen();			  	   	  //Dmִ�в�ѯ���
	char*		DmGetData(int num);		  //ȡ�����е�ֵ
	bool	 	DmNext();			  	   //������α�����
	~UODBCIN();      				 
};

