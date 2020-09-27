#ifdef WIN32
#include <process.h>
#else
#include <unistd.h>
#include <malloc.h>
#include <pthread.h>
#endif
#include <stdlib.h>
#include <stdio.h>
#include "AutoHandle.h"
#include "dt_svrlib.h" 
int Start(void *ptr);

     int ci=1;
     char *pidxcolarray[20];

int main(int argc,char *argv[]) {
    int nRetCode = 0;
    WOCIInit("dptest\batchquery");
    nRetCode=wociMainEntranceEx(Start,true,NULL,2,WDBI_ENT_INIT|WDBI_ENT_CLEAN);
    WOCIQuit(); 
    return nRetCode;
}

char * BuildQuery(char *sql,const char *tbname,AutoMt &indexmt,int row,bool ora) {
	sprintf(sql,"select * from %s where ",tbname);
	
	//int concol=wociGetColumnNumber(indexmt);
        for(int i=0;i<ci;i++) {
        	//char colname[300];
        	int c=wociGetColumnPosByName(indexmt,(const char*)pidxcolarray[i]);
        	//wociGetColumnName(indexmt,i,colname);
        	char cell[300];
        	wociGetCell(indexmt,row,c,cell,false);
        	switch(wociGetColumnType(indexmt,c)) {
        	case COLUMN_TYPE_CHAR :
        	{
        	  int sl=strlen(cell);
        	  while(cell[--sl]==' ') cell[sl]=0;
        	  sprintf(sql+strlen(sql)," %s='%s' ",pidxcolarray[i],cell);
        	  break;
        	}
        	case COLUMN_TYPE_FLOAT:
        	case COLUMN_TYPE_NUM:
        	case COLUMN_TYPE_INT:
        	case COLUMN_TYPE_BIGINT:
        	  sprintf(sql+strlen(sql)," %s=%s ",pidxcolarray[i],cell);
        	  break;
        	case COLUMN_TYPE_DATE:
        	 if(ora)
  	          sprintf(sql+strlen(sql)," %s=to_date('%s','yyyymmdd hh24:miss') ",pidxcolarray[i],cell);
  	         else
        	  sprintf(sql+strlen(sql)," %s='%s' ",pidxcolarray[i],cell);
        	 break;
        	}
        	if(i+1!=ci) strcat(sql," and ");
        }
        return sql;
}

int Execute(AutoMt &indexmt,const char *tabname,int dts,int irn ,int lrn,bool ora)
{
	char sql[1000];
	AutoMt mt(dts,lrn);
	mt.FetchFirst(BuildQuery(sql,tabname,indexmt,irn,ora));
	int rn=mt.Wait();
	if(rn==lrn) {
		wociGetLine(indexmt,irn,sql,false,NULL);
		printf("������ѯ���Ƽ�¼��%d,����У��:%s\n",sql);
	}
	else if(rn!=indexmt.GetLong("idx_rownum",irn)) {
		wociGetLine(indexmt,irn,sql,false,NULL);
		printf("����,��ѯ��¼������,����%ld,ʵ��%d :%s\n",indexmt.GetLong("idx_rownum",irn),rn,sql);
	}
	return rn;
}

int Start(void *ptr) { 
    wociSetEcho(FALSE);
    wociSetOutputToConsole(TRUE);
    AutoHandle dts;
    printf("dp_indexq�������ڲ��ԺͶԱ�" DBPLUS_STR " ��oracle������������ѯ����Ч��.\n���ӵ�" DBPLUS_STR "���ݿ�:\n");
    //int cmpdp=GetYesNo("��Ҫֱ����" DBPLUS_STR "��ѯ?<Y>",true);
    char und[100],psd[100],svd[100];
    //if(cmpdp) {
    	 getdbcstr(und,psd,svd,"");
         dts.SetHandle(wociCreateSession(und,psd,svd,DTDBTYPE_ODBC));
    //}
    mytimer tm;
    AutoHandle dtsora;
    char uno[100],pso[100],svo[100];
    int cmpora=GetYesNo("��ҪOracle�ԱȲ���?<N>",false);
    char oratabname[300];
    if(cmpora) {
        printf("���ӵ�Oracle���ݿ�...\n");
        getdbcstr(uno,pso,svo,"");
        dtsora.SetHandle(wociCreateSession(uno,pso,svo,DTDBTYPE_ORACLE));
        getString("����oracle�еı�(��ͼ)��:",NULL,oratabname);
    }
    char sql[10000];
    char dbname[300],tabname[300];
    while(true) {
     AutoMt selmt(dts,500);
     selmt.FetchAll("select databasename \"���ݿ�\", tabname \"����\",recordnum \"��¼��\" from dp.dp_table where recordnum>0 order by databasename,recordnum limit 500");
     selmt.Wait();
	wociSetColumnDisplay(selmt,NULL,0,"���ݿ�",-1,-1);
	wociSetColumnDisplay(selmt,NULL,1,"����",-1,-1);
	wociSetColumnDisplay(selmt,NULL,2,"��¼��",-1,0);

     wociMTCompactPrint(selmt,0,NULL);
     sql[0]=0;
     getString("�������ݿ���:",NULL,dbname);
     if(strlen(dbname)==0) break;
     getString("�������:",NULL,tabname);
     if(strlen(tabname)==0) break;
     selmt.FetchAll("select tabid,recordnum from dp.dp_table where databasename=lower('%s') and tabname=lower('%s')",dbname,tabname);
     if(selmt.Wait()<1) {
     	printf("����:��%s.%s������.\n",dbname,tabname);
     	continue;
     }
     if(selmt.GetDouble("recordnum",0)<1) {
     	printf("����:��%s.%s��û�м�¼.\n",dbname,tabname);
     	continue;
     }
     int tabid=selmt.GetInt("tabid",0);
     selmt.FetchAll("select * from dp.dp_index where tabid=%d",tabid);
     int indexrn=selmt.Wait();
     if(indexrn<1) {
     	printf("����:��%s.%sδ��������.\n",dbname,tabname);
     	continue;
     }
     printf("��ѡ��ʹ�õ�����:\n");
     int i;
     for(i=0;i<indexrn;i++)
     	printf("%d: %s.\n",i+1,selmt.PtrStr("columnsname",i));
     int selidx=getOption("�������<1>",1,1,indexrn)-1;
     
     //selmt.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>0",tabid);
     //int indexrn=selmt.Wait();
     //if(indexrn<1) {
     //     	printf("����:��%s.%sδ��������.\n",dbname,tabname);
     //	continue;
     //}
     //if(strstr(selmt.PtrStr("columnsname",0),"(")) {
     //	printf("����:��%s.%sʹ���˲�������,����У���ѯ���.\n",dbname,tabname);
     //	continue;
     //}
     printf("�����ֶ�:%s.\n",selmt.PtrStr("columnsname",selidx));
     static char icolsnm[1000];
     strcpy(icolsnm,selmt.PtrStr("columnsname",selidx));
     //�ֽ��ֶ�,ע�ⲿ������
     char *ptemp=icolsnm;
     while(*ptemp==' ') ++ptemp;
     
     pidxcolarray[ci-1]=ptemp;
     
     while(*ptemp) {
     	if(*ptemp=='(') {
     		*ptemp=0;
     		while(*ptemp!=')') ++ptemp;
     		++ptemp;
     	}
     	if(*ptemp==',') {
     		*ptemp++=0;
     		while(*ptemp==' ') ++ptemp;
     		ci++;
     		pidxcolarray[ci-1]=ptemp;
     	}  
     	if(*ptemp==' ') *ptemp=0;
     	ptemp++;
     }
     char icols[1000];
     strcpy(icols,pidxcolarray[0]);
     for(i=1;i<ci;i++) {
      strcat(icols,",");
      strcat(icols,pidxcolarray[i]);
     }
     
     char idxtbname[200];
     AutoMt indexmt(dts,500);
     if(strlen(selmt.PtrStr("indextabname",selidx))>0) 
     	sprintf(idxtbname,"%s.%s",dbname,selmt.PtrStr("indextabname",selidx));
     else
        sprintf(idxtbname,"%s.%sidx%d",dbname,tabname,selmt.GetInt("indexgid",selidx));
     indexmt.FetchFirst("select * from %s",idxtbname);
     if(indexmt.Wait()<1) {
     	sprintf(idxtbname,"%s.%sidx%d",dbname,tabname,selmt.GetInt("indexgid",selidx));
        indexmt.FetchFirst("select * from %s",idxtbname);
        if(indexmt.Wait()<1) {
     	  printf("����:�Ҳ���������¼.\n",dbname,tabname);
     	  continue;
     	}
     }
     int il=wociGetRowLen(indexmt)-sizeof(int)*6;
     int concol=wociGetColumnNumber(selmt)-6;
     
/*     int ci=0;
     char icols[1000];
     icols[0]=0;
     for(ci=0;ci<concol;ci++) {
     	char colname[100];
     	wociGetColumnName(indexmt,ci,colname);
     	strcat(icols,colname);
     	if(ci+1!=concol) strcat(icols,",");
     }
*/
     int idxlimitrn=getOption("�������Ƽ�¼��<100000>",100000,1000,5000000);
     int limitrn=getOption("���Ʒ��ؼ�¼��<10000>",10000,100,5000000);
     int connum=getOption("ͬʱ������<10>",10,1,600);
     int reprn=getOption("ÿ�������ϵĲ�ѯ����<300>",300,1,5000000);
     int reconn=getOption("���Ӵ���<10>",10,1,1000);
     int userand=GetYesNo("���˳��?<N>",false);
     indexmt.SetMaxRows(idxlimitrn);
     printf("������������...\n");
     indexmt.FetchFirst("select %s,sum(idx_rownum) idx_rownum from %s group by %s",icols,idxtbname,icols);
     //indexmt.FetchFirst("select distinct %s from %s ",icols,idxtbname);
     int irn=indexmt.Wait();
     printf("��������%d��.\n��ʼ��ѯ...\n",irn);
     indexmt.SetDBC(dts);
     int clp=0;
     sprintf(dbname+strlen(dbname),".%s",tabname);
     int rcc=0;
     int trn=0;
     int seed=(int)time(NULL);
     //if(cmpdp) {
     if(!userand) seed=30022;
     srand(seed);
      for(rcc=0;rcc<reconn;rcc++) {
     	lgprintf("����%d�����ݿ�����...",connum);
     	AutoHandle *pdts=new AutoHandle[connum];
     	for(int cc=0;cc<connum;cc++) {
         pdts[cc].SetHandle(wociCreateSession(und,psd,svd,DTDBTYPE_ODBC));
     	}
     	lgprintf("���ӽ����ɹ�����ʼ��ѯ...");
        tm.Clear();
        tm.Start();
     	for(int qc=0;qc<reprn*connum;qc++) {
 		int uoff=(int)(((double)rand())/(RAND_MAX)*(irn-1));//i;
 			trn+=Execute(indexmt,dbname,pdts[clp],uoff,limitrn,false);
		clp=(++clp)%connum;
     	}
	delete []pdts;
        tm.Stop();
     	lgprintf("��%d�����ӣ���ѯ����",rcc+1);
      }
     //}
     printf( DBPLUS_STR " Query %d times,Rows %d,time :%.2f.\n",reprn*connum*reconn,trn,tm.GetTime());
      tm.Clear();
     if(cmpora) {
      trn=0;
      srand(seed);
      for(rcc=0;rcc<reconn;rcc++) {
     	lgprintf("����%d��oracle���ݿ�����...",connum);
     	AutoHandle *pdts=new AutoHandle[connum];
     	for(int cc=0;cc<connum;cc++) {
         pdts[cc].SetHandle(wociCreateSession(uno,pso,svo,DTDBTYPE_ORACLE));
     	}
     	lgprintf("oracle���ӽ����ɹ�����ʼ��ѯ...");
        tm.Start();
     	for(int qc=0;qc<reprn*connum;qc++) {
 		int uoff=(int)(((double)rand())/RAND_MAX*(irn-1));//i;
		trn+=Execute(indexmt,oratabname,pdts[clp],uoff,limitrn,true);
		clp=(++clp)%connum;
     	}
	delete []pdts;
        tm.Stop();
     	lgprintf("��%d�����ӣ�oracle��ѯ����",rcc+1);
     }
     printf("Oracle Query %d times,Rows %d,time :%.2f.\n",reprn*connum*reconn,trn,tm.GetTime());
     }
     
    }
   return 0;
}
