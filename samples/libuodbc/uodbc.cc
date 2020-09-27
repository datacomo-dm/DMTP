
#include "uodbc.h"
#include "uodbc_in.h"

UODBC::UODBC() {
//	printf("Alloc UODBCIN object,size %d.\n",sizeof(UODBCIN));
	handle=(UODBCIN *)new UODBCIN();
}

UODBC::~UODBC() {
	delete (UODBCIN *)handle;
}
	int 		UODBC::DmConnect(char* root,char* passwd,char* database) //Dm���Ӻ���
	{
		//printf("connect to dm ...\n");
		return ((UODBCIN*)handle)->DmConnect(root,passwd,database);
	}

	int	 	UODBC::DmSetSQL(char* sqltemp)	    //����sql���
	{
		return ((UODBCIN*)handle)->DmSetSQL(sqltemp);
	}

	int 		UODBC::DmBind(int num,char* param) //Dm�󶨲���
	{
		return ((UODBCIN*)handle)->DmBind(num,param);
	}

	int	 	UODBC::DmOpen()			  	   	  //Dmִ�в�ѯ���
	{
		return ((UODBCIN*)handle)->DmOpen();
	}

	char*		UODBC::DmGetData(int num)		  //ȡ�����е�ֵ
	{
		return ((UODBCIN*)handle)->DmGetData(num);
	}

	bool	 	UODBC::DmNext()			  	   //������α�����
	{
		return ((UODBCIN*)handle)->DmNext();
	}



/*****************************
	���캯������ʼ��
******************************/
UODBCIN::UODBCIN(){
	sqlno = 0;
	ParamNo = 0;
	colanz = 0;
	flag = 1;
	//printf("Build UODBCIN ,alloc %d bytes...\n",Maxsqllen);
	fial = (char *)malloc(Maxsqllen);
	
	strcpy(sqlnew,"select flag,view from dm_template where type = ");

	V_OD_erg=SQLAllocHandle(SQL_HANDLE_ENV,SQL_NULL_HANDLE,&V_OD_Env); 
	if ((V_OD_erg != SQL_SUCCESS) && (V_OD_erg != SQL_SUCCESS_WITH_INFO))
        {
                printf("Error AllocHandle\n");
                exit(0);
        }
        V_OD_erg=SQLSetEnvAttr(V_OD_Env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0); 
        if ((V_OD_erg != SQL_SUCCESS) && (V_OD_erg != SQL_SUCCESS_WITH_INFO))
        {
                printf("Error SetEnv\n");
                SQLFreeHandle(SQL_HANDLE_ENV, V_OD_Env);
                exit(0);
        }
        // 2. allocate connection handle, set timeout
        V_OD_erg = SQLAllocHandle(SQL_HANDLE_DBC, V_OD_Env, &V_OD_hdbc); 
        if ((V_OD_erg != SQL_SUCCESS) && (V_OD_erg != SQL_SUCCESS_WITH_INFO))
        {
                printf("Error AllocHDB %ld\n",V_OD_erg);
                SQLFreeHandle(SQL_HANDLE_ENV, V_OD_Env);
                exit(0);
	}
	SQLSetConnectAttr(V_OD_hdbc, SQL_LOGIN_TIMEOUT, (SQLPOINTER *)5, 0);
}

/***********************************************
	Dm���Ӻ���
	root		---�û���
	passwd	---����
	database	---���ݿ���,Ҳ����odbc.ini�ж�Ӧ��DSN
	����ֵ 0--�ɹ�1--ʧ��
************************************************/
int UODBCIN::DmConnect(char* root,char* passwd,char* database){

	// 1. allocate Environment handle and register version 
	printf_log("SQLConnect now ...\n");
	V_OD_erg = SQLConnect(V_OD_hdbc,(SQLCHAR*)database,SQL_NTS,(SQLCHAR*)root,SQL_NTS,(SQLCHAR*)passwd,SQL_NTS);
	if ((V_OD_erg != SQL_SUCCESS) && (V_OD_erg != SQL_SUCCESS_WITH_INFO))
        {
                printf_log("Error SQLConnect %ld\n",V_OD_erg);
                SQLGetDiagRec(SQL_HANDLE_DBC, V_OD_hdbc,1,(SQLCHAR*)V_OD_stat, &V_OD_err,(SQLCHAR*)V_OD_msg,100,&V_OD_mlen);
                printf_log("%s (%ld)\n",V_OD_msg,V_OD_err);
                SQLFreeHandle(SQL_HANDLE_ENV, V_OD_Env);
                return 1;
        }
        printf_log("Connected !\n");
		V_OD_hstmt=0;
		V_OD_hstmt1=0;
/*	V_OD_erg=SQLAllocHandle(SQL_HANDLE_STMT, V_OD_hdbc, &V_OD_hstmt1);
        if ((V_OD_erg != SQL_SUCCESS) && (V_OD_erg != SQL_SUCCESS_WITH_INFO))
        {
                //printf("Fehler im AllocStatement %ld\n",V_OD_erg);
                SQLGetDiagRec(SQL_HANDLE_DBC, V_OD_hdbc,1,(SQLCHAR*)V_OD_stat,&V_OD_err,(SQLCHAR*)V_OD_msg,100,&V_OD_mlen);
                //printf("%s (%ld)\n",V_OD_msg,V_OD_err);
                SQLFreeHandle(SQL_HANDLE_ENV, V_OD_Env);
                return 1;
        }
*/
	return 0;
}

/**********************************************
	Dm��sql���ִ�к���

	sqltxt		---��ѯ���
	V_OD_hstmt	---Dm���ӱ�ʾ,��Ų�ѯ���ͽ����
	
	���ؽ��:���û�в�ѯ���������0,
		  ���򷵻ز�ѯ������
**********************************************/
int  UODBCIN::DmOpen(){

	int   i = 0;
	printf_log("bind is %10s\n",BindParam[0]);
	BindPhoneNo(sqltxt,BindParam[0]);
	printf_log("DmOpen is running here\n");
	if(!V_OD_hstmt) SQLFreeHandle(SQL_HANDLE_STMT,V_OD_hstmt);
	V_OD_erg=SQLAllocHandle(SQL_HANDLE_STMT, V_OD_hdbc, &V_OD_hstmt);
        if ((V_OD_erg != SQL_SUCCESS) && (V_OD_erg != SQL_SUCCESS_WITH_INFO))
        {
                SQLGetDiagRec(SQL_HANDLE_DBC, V_OD_hdbc,1,(SQLCHAR*)V_OD_stat,&V_OD_err,(SQLCHAR*)V_OD_msg,100,&V_OD_mlen);
				V_OD_hstmt=0;
				colanz = 0;
                return 0;
        }
	//�󶨲�ѯ�����,���صĽ�������ڴ˱�����

	if(ParamNo<sqlno){
                printf("param is not enough,please make it again\n");
                return 0;
        }
	printf_log("NEXT running is DMSQLJION\n");
	DmSQLJoin();
	//ParamNo�ж��Ƿ��а󶨲�������������ڰ󶨲�������ֱ��ִ��SQLExecDirect
	printf_log("sqltx2222t is %s-----%d\n\n",sqltxt2,strlen(sqltxt2));
	V_OD_erg=SQLExecDirect(V_OD_hstmt,(SQLCHAR*)sqltxt2,SQL_NTS);
	//V_OD_erg = SQLExecute(V_OD_hstmt);
	ParamNo = 0; 
	if ((V_OD_erg != SQL_SUCCESS) && (V_OD_erg != SQL_SUCCESS_WITH_INFO))
    	{
       		printf_log("Error in Select %ld\n",V_OD_erg);
       		SQLGetDiagRec(SQL_HANDLE_DBC, V_OD_hdbc,1,(SQLCHAR*)V_OD_stat,&V_OD_err,(SQLCHAR*)V_OD_msg,100,&V_OD_mlen);
       		printf("%s (%ld)\n",V_OD_msg,V_OD_err);
			colanz = 0;
       		SQLFreeHandle(SQL_HANDLE_STMT,V_OD_hstmt);
			V_OD_hstmt=0;
       		return 0;
    	}
	
	V_OD_erg=SQLNumResultCols(V_OD_hstmt,&V_OD_colanz);
    printf_log("The colllll is %d\n",colanz);
	colanz = V_OD_colanz;	
	if ((V_OD_erg != SQL_SUCCESS) && (V_OD_erg != SQL_SUCCESS_WITH_INFO))
        {
         	SQLGetDiagRec(SQL_HANDLE_STMT, V_OD_hstmt,1,(SQLCHAR*)V_OD_stat,&V_OD_err,(SQLCHAR*)V_OD_msg,100,&V_OD_mlen);
       		printf("%s (%ld)---------1\n",V_OD_msg,V_OD_err);
		//SQLFreeHandle(SQL_HANDLE_STMT,V_OD_hstmt);
         	//SQLDisconnect(V_OD_hdbc);
         	//SQLFreeHandle(SQL_HANDLE_DBC,V_OD_hdbc);
         	//SQLFreeHandle(SQL_HANDLE_ENV, V_OD_Env);
         	return 0;
        }
    	return 1;
}

/***************************************
	Dm�󶨲�������

	V_OD_hstmt�������ݿ�ı�ʾ
	numΪ�󶨲����ĸ���
	param�󶨲�������ֵ����
	
****************************************/
int UODBCIN::DmBind(int no,char* param){
	
	num[ParamNo] = no;
	ParamLEN[ParamNo] = strlen(param);
	strcpy(BindParam[ParamNo],param);
	ParamNo++;	
	return 1;	
}

void UODBCIN::DmSQLJoin(){
        char *src;
        char *des,*temp;
        int i = 0;
        int j = 0;
        temp = (char* )malloc(8000);
	memset(temp,'\0',8000);
        src = (char* )malloc(8000);
	memset(src,'\0',8000);
        strcpy(src,sqltxt);
	src[strlen(sqltxt)]='\0';
	printf_log("src is %s-----%d\n\n",src,strlen(src));
	printf_log("sqltxttxt is %s-----%d\n\n",sqltxt,strlen(sqltxt));
        des  = temp;
        while(*src)
        {
                if(*src == '?'&&src)
                {
                        *temp++ = '\'';
                        while(j < strlen(BindParam[i]))
                                *temp++ = BindParam[i][j++];
                        *temp++ = '\'';
                        src++;
                        i++;
                        j = 0;
                        continue;
                }
                *temp++ = *src++;
        }
        strcpy(sqltxt2,des);
	sqltxt2[strlen(des)]='\0';
	printf_log("des is %s-----%d\n\n",des,strlen(des));
	free(src);
	free(temp);
}



/***************************************
	��ȡ���������

	V_OD_hstmt --���ӱ�ʾ

	���ؽ����0--����Ѿ�ȡ��,1--������н��
****************************************/
bool UODBCIN::DmNext(){

	V_OD_erg=SQLFetch(V_OD_hstmt);
	/*if ((V_OD_erg != SQL_SUCCESS) && (V_OD_erg != SQL_SUCCESS_WITH_INFO))
        {
        	SQLGetDiagRec(SQL_HANDLE_STMT, V_OD_hstmt,1,(SQLCHAR*)V_OD_stat,&V_OD_err,(SQLCHAR*)V_OD_msg,100,&V_OD_mlen);
       		printf("%s (%ld)--------2\n",V_OD_msg,V_OD_err);
         	SQLFreeHandle(SQL_HANDLE_STMT,V_OD_hstmt);
		return 0;
        }
*/
	if( V_OD_erg == SQL_NO_DATA )
	{
		printf_log("DmNext is running here!\n");
		SQLFreeHandle(SQL_HANDLE_STMT,V_OD_hstmt);
		V_OD_hstmt=0;
        	//SQLDisconnect(V_OD_hdbc);
        	//SQLFreeHandle(SQL_HANDLE_DBC,V_OD_hdbc);
        	//SQLFreeHandle(SQL_HANDLE_ENV, V_OD_Env);
		return 0;
	}
	return 1;
}

char*	UODBCIN::DmGetData(int num){
	
	SQLINTEGER sqlint;
	if(SQLGetData(V_OD_hstmt,num,SQL_C_CHAR,sqlbuffer2,1500,&sqlint)!=SQL_SUCCESS_WITH_INFO)
	{
		printf_log("the result is %s\n",sqlbuffer2);
		return sqlbuffer2;
	}
	return " ";
}

/***************************************
	����sql���
	
	sqltxt ---����ǰ��sql���

	����ֵchar* ���������Ժ��sql
****************************************/
int UODBCIN::DmSetSQL(char* sqltemp){
	char *sqltemp_temp;
	char *DMSS_temp;
        char *sql;
	char *fial_temp;
	sqltemp_temp=(char *)malloc(Maxsqllen);
	strcpy(sqltemp_temp,sqltemp);
	sqltemp_temp[strlen(sqltemp)]='\0';
	printf_log("sqltemp_temp is %s\n",sqltemp_temp);
	abc(fial,sqltemp_temp);
	DMSS_temp =(char* )malloc(Maxsqllen);
        sql = DMSS_temp;
	fial_temp = fial;
        strcpy(fial_temp,fial);
	fial_temp[strlen(fial)]='\0';
	while(*fial_temp)
        {
                if(*fial_temp != '@')
                        *DMSS_temp = *fial_temp;
                else
                {
                        *DMSS_temp = '?';
                        while(*fial_temp != ' ' && *fial_temp)
                                fial_temp++;
                        DMSS_temp++;
                        *DMSS_temp = ' ';
                }
                fial_temp++;
                DMSS_temp++;
        }
        *DMSS_temp = '\0';
	strcpy(sqltxt,sql);
	sqltxt[strlen(sql)]='\0';
	printf_log("sqltxt is %s\n",sqltxt);
	free(sqltemp_temp);
	free(DMSS_temp);
	  flag = 1;
	return 1;
}


//�����µ�sql����������
int UODBCIN::abc(char *fial,char *argv)
{
	char *temp;
	//s_gsmhomecdr
        if( (temp=strstr(argv,"s_gsmhomecdr"))!=NULL )
        {
                flag = makesqltxt(argv,22,fial,temp,"s_gsmhomecdr");
		printf_log("flag=%d\n",(int)flag);
        }
        //v_smshomecdr
        if( (temp=strstr(argv,"v_smshomecdr"))!=NULL )
        {
                flag = makesqltxt(argv,22,fial,temp,"v_smshomecdr");
				printf_log("flag=%d\n",(int)flag);
                //printf("2---fial is %s\n",fial);
        }
        //s_scpcdr
        if( (temp=strstr(argv,"s_scpcdr"))!=NULL )
        {
                flag = makesqltxt(argv,18,fial,temp,"s_scpcdr");
                printf_log("flag=%d\n",(int)flag);
				printf_log("3---fial is %s\n",fial);
        }
        //s_szx_smshomecdr
        if( (temp=strstr(argv,"s_szx_smshomecdr"))!=NULL )
        {
                flag = makesqltxt(argv,26,fial,temp,"s_szx_smshomecdr");
				printf_log("flag=%d\n",(int)flag);
                printf_log("4---fial is %s\n",fial);
        }
        //s_vpmnhomecdr
        if( (temp=strstr(argv,"s_vpmnhomecdr"))!=NULL )
        {
                flag = makesqltxt(argv,23,fial,temp,"s_vpmnhomecdr");
                printf_log("flag=%d\n",(int)flag);
				printf_log("5---fial is %s\n",fial);
        }
        //v_otherfee
        if( (temp=strstr(argv,"v_otherfee"))!=NULL )
        {
                flag = makesqltxt(argv,20,fial,temp,"v_otherfee");
                printf_log("flag=%d\n",(int)flag);
				printf_log("6---fial is %s\n",fial);
        }
        //v_ggsncdr
        if( (temp=strstr(argv,"v_ggsncdr"))!=NULL )
        {
                flag = makesqltxt(argv,19,fial,temp,"v_ggsncdr");
                printf_log("flag=%d\n",(int)flag);
				printf_log("7---fial is %s\n",fial);
        }
        //v_gprshomecdr
        if( (temp=strstr(argv,"v_gprshomecdr"))!=NULL )
        {
                flag = makesqltxt(argv,23,fial,temp,"v_gprshomecdr");
				printf_log("flag=%d\n",(int)flag);
                printf_log("8---fial is %s\n",fial);
        }
	return flag;
}

//�ж��ǲ�����Ҫ������װsql���
int UODBCIN::makesqltxt(char *oldsql,int len,char *fial,char *temp,char *type)
{
	 int strposition=0;
	 char *temp1;
         temp1 = (char*)malloc(Maxsqllen);
	 printf_log("len is %d\n",len);
	 strncpy(temp1,temp,len);
         temp1[len] = '\0';
	 printf_log("temp1 is %s\n",temp1);
         //sprintf(sqlnew,"%s%s%s%s%s",sqlnew,"'",temp1,"'",";");
	 strcat(sqlnew,"'");
	 strcat(sqlnew,temp1);
	 strcat(sqlnew,"'");
	 strcat(sqlnew,";");
	 sqlnew[strlen(sqlnew)]='\0';
	 printf_log("sqlnew is %s\n",sqlnew);
         getflag();
         if(flag==1)
         {
         	strcpy(fial,oldsql);
                fial[strlen(oldsql)]='\0';
         } 
         else
         {
               strposition=strpos(oldsql,type);
               strncpy(temp1,oldsql,strposition);
               temp1[strposition]='\0';
               //sprintf(fial,"%s%s%s%s%s",temp1,"(",selet,") t",(temp+len));
	       strcpy(fial,temp1);
	       strcat(fial,"(");
	       strcat(fial,selet);
               strcat(fial,") t");
	       strcat(fial,(temp+len));
	       printf_log("fial-2 is %s\n",fial);
          }
        strcpy(sqlnew,"select flag,view from dm_template where type = ");
	sqlnew[strlen(sqlnew)]='\0';
	printf_log("sqlnew strlen is %d\n",strlen(sqlnew));
	free(temp1);	
	return flag;
}

//��ȡ��ͼ���ڵ�λ��
int UODBCIN::strpos(char *string,char *substring) 
{ 
    int i,j,k,count=0; 
    for(i=0;string[i];i++) 
    {       
       count=i; 
       for(j=i,k=0;string[j]==substring[k];j++,k++)
	   if(!substring[k+1]) 
       		return count; 
    } 
    return 0;
}

char* UODBCIN::BindPhoneNo(char *sqlnew,char *NO)
{
	char *sqltemp;
	char *temp;
	sqltemp=(char *)malloc(Maxsqllen);
	temp=(char *)malloc(Maxsqllen);
	int pos=strpos(sqlnew,"#");
	printf_log("pos is %d\n",pos);
	strncpy(temp,sqltxt,pos);
	temp[pos]='\0';
	while(pos != 0)
	{
		//sprintf(sqltemp,"%s%s%s%s%s",temp,"'",NO,"'",(sqltxt+pos+1));
		strcpy(sqltemp,temp);
		strcat(sqltemp,"'");
		strcat(sqltemp,NO);
		strcat(sqltemp,"'");
		strcat(sqltemp,(sqlnew+pos+1));
		pos = strpos(sqltemp,"#");
	    printf_log("pos is %d\n",pos);	
		strncpy(temp,sqltemp,pos);
		temp[pos]='\0';
		strcpy(sqlnew,sqltemp);
        	sqlnew[strlen(sqltemp)]='\0';
		printf_log("sqlnew-2 is %s \n",sqlnew);
	}
	free(sqltemp);
	free(temp);
	return NULL;
}

//��ѯ���ݿ��ȡģ��ı�ʾ��select���
int UODBCIN::getflag()
{
	  if(V_OD_hstmt1) SQLFreeHandle(SQL_HANDLE_STMT,V_OD_hstmt1);
      V_OD_erg=SQLAllocHandle(SQL_HANDLE_STMT, V_OD_hdbc, &V_OD_hstmt1);
        if ((V_OD_erg != SQL_SUCCESS) && (V_OD_erg != SQL_SUCCESS_WITH_INFO))
        {
                SQLGetDiagRec(SQL_HANDLE_DBC, V_OD_hdbc,1,(SQLCHAR*)V_OD_stat,&V_OD_err,(SQLCHAR*)V_OD_msg,100,&V_OD_mlen);
                //SQLFreeHandle(SQL_HANDLE_ENV, V_OD_Env);
				SQLFreeHandle(SQL_HANDLE_STMT,V_OD_hstmt1);V_OD_hstmt1=0;
                return 1;
        }
        V_OD_erg=SQLExecDirect(V_OD_hstmt1,(SQLCHAR*)sqlnew,strlen(sqlnew));
        SQLExecute(V_OD_hstmt1);
        if ((V_OD_erg != SQL_SUCCESS) && (V_OD_erg != SQL_SUCCESS_WITH_INFO))
        {
                printf("Error in Select %ld\n",V_OD_erg);
                SQLGetDiagRec(SQL_HANDLE_DBC, V_OD_hdbc,1,(SQLCHAR*)V_OD_stat,&V_OD_err,(SQLCHAR*)V_OD_msg,100,&V_OD_mlen);
                printf("%s (%ld)\n",V_OD_msg,V_OD_err);
                SQLFreeHandle(SQL_HANDLE_STMT,V_OD_hstmt1);V_OD_hstmt1=0;
        }
        V_OD_erg=SQLFetch(V_OD_hstmt1);
        SQLINTEGER sqlint;
        if(SQLGetData(V_OD_hstmt1,1,SQL_C_LONG,&flag,sizeof(int),&sqlint)!=SQL_SUCCESS_WITH_INFO);
        if(SQLGetData(V_OD_hstmt1,2,SQL_C_CHAR,selet,8000,&sqlint)!=SQL_SUCCESS_WITH_INFO);
        SQLFreeHandle(SQL_HANDLE_STMT,V_OD_hstmt1);V_OD_hstmt1=0;
	return 0;
}

/***************************************
	Dm�ͷ������Լ���Դ�ĺ���
	
	V_OD_Env 	---���ӵĻ���
	V_OD_hdbc	---Dm������
	V_OD_hstmt	---Dm�е������Դ

	����ֵ0-�ɹ� 1-ʧ��
****************************************/
UODBCIN::~UODBCIN(){
	if(!V_OD_hstmt) SQLFreeHandle(SQL_HANDLE_STMT,V_OD_hstmt);
	if(!V_OD_hstmt1) SQLFreeHandle(SQL_HANDLE_STMT,V_OD_hstmt1);
      	SQLDisconnect(V_OD_hdbc);
      	SQLFreeHandle(SQL_HANDLE_DBC,V_OD_hdbc);
      	SQLFreeHandle(SQL_HANDLE_ENV, V_OD_Env);
}
