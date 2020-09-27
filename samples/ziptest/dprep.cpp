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

int main(int argc,char *argv[]) {
    int nRetCode = 0;
    WOCIInit("samp/fetch/");
    nRetCode=wociMainEntrance(Start,true,NULL,2);
    WOCIQuit(); 
    return nRetCode;
}

int Start(void *ptr) { 
    wociSetEcho(FALSE);
    wociSetOutputToConsole(TRUE);
    AutoHandle dts;
    printf("dprep�������ڲ����ظ�ִ��ͬһ��sql.\n���ӵ����ݿ�...\n");
    dts.SetHandle(BuildConn(0));
    char sql[10000];
    while(true) {
     sql[0]=0;
     getString("REP_SQL",NULL,sql);
     if(sql[strlen(sql)-1]==';') sql[strlen(sql)-1]=0;
     if(strcmp(sql,"quit")==0 || strcmp(sql,"exit")==0) return 0;
     try {
     	int rn;
     //AutoMt mt(dts,2);
     //mt.FetchFirst(sql);
     //int rn=mt.Wait();
     //int l=wociGetRowLen(mt);
     //printf("���м�¼����%d�ֽ�.\n",l);
     int meml=getOption("���μ�¼��<5000>",5000,2,5000000);
     //mt.SetMaxRows(meml);
     int times=getOption("ִ�д���<10000>",10000,2,5000000);
     int flushtables=GetYesNo("��Ҫˢ�±�?<Y>",true);
     mytimer mtm;
     AutoStmt st(dts);
     mtm.Start();
     while(times-->0) {
      {
       AutoMt mt1(dts,meml);
       mt1.FetchFirst(sql);
       rn=mt1.Wait();
      }
      printf("%d: %drows.\n",times,rn);
      if(flushtables) st.DirectExecute("flush tables");
     }
     mtm.Stop();
     printf("ʱ��:%.2fs.\n",mtm.GetTime());
     }
     catch(...) {}
    }     
   return 0;
}

