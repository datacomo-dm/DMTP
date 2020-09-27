#ifdef WIN32
#include <process.h>
#define getch getchar
#else
#include <unistd.h>
#include <malloc.h>
#include <pthread.h>
#endif
#include <stdlib.h>
#include <stdio.h>
#include "AutoHandle.h"
#include "dt_common.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

int Start(void *ptr);
int main(int argc,char *argv[]) {
    int nRetCode = 0;
    WOCIInit("woci_ocp");
    wociSetOutputToConsole(TRUE);
    wociSetEcho(false);
    nRetCode=wociMainEntrance(Start,true,NULL,2);
    WOCIQuit(); 
    return nRetCode;
}

int Start(void *ptr) { 
    AutoHandle dts;
    mytimer tm;
    printf("ѡ��Դ���ݿ�...\n");
    dts.SetHandle(BuildConn(0));
    AutoHandle dtd;
    printf("ѡ��Ŀ�����ݿ�...\n");
    dtd.SetHandle(BuildConn(0));
    char sqlst[2000];
    getString("�������ѯ���(�м䲻Ҫ�س�)",NULL,sqlst);
    int fetchn=getOption("�ύ����",10000,100,5000);
    char desttb[200];
    getString("������Ŀ�������",NULL,desttb);
    AutoStmt srcst(dts);
    srcst.Prepare(sqlst);
    TradeOffMt mt(0,fetchn);
    mt.Cur()->Build(srcst);
    mt.Next()->Build(srcst);
    if(wociTestTable(dtd,desttb)) {
    	int sel=getOption("Ŀ����Ѵ��ڣ�׷��(1)/ɾ���ؽ�(2)/��պ����(3)/�˳�(4)",2,1,4);
    	if(sel==4) return -1;
    	if(sel==2) {
    		printf("ɾ����(drop table)'%s'...\n",desttb);
    		AutoStmt st(dtd);
		st.Prepare("drop table %s",desttb);
		st.Execute(1);
		st.Wait();
    		printf("����(create table)'%s'...\n",desttb);
		wociGeneTable(*mt.Cur(),desttb,dtd);
	}
	else if(sel==3) {
    		printf("��ձ�(truncate table)'%s'...\n",desttb);
    		AutoStmt st(dtd);
		st.Prepare("truncate table %s",desttb);
		st.Execute(1);
		st.Wait();
	}
    }
    else  {   	
    	printf("����(create table)'%s'...\n",desttb);
	wociGeneTable(*mt.Cur(),desttb,dtd);
    }
    tm.Start();
    mt.FetchFirst();
    int trn=0;
    printf("��ʼ����...\n");
    int rrn=mt.Wait();
    while(rrn>0) {
    	mt.FetchNext();
    	wociAppendToDbTable(*mt.Cur(),desttb,dtd,true);
    	trn+=rrn;
    	rrn=mt.Wait();
    	printf(".");fflush(stdout);
    }
    tm.Stop();
    printf("\n������%d��(ʱ��%.2f��)��\n",trn,tm.GetTime());
    return 1;
}
	
