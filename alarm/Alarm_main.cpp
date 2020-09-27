#ifdef WIN32
#include <process.h>
#define getch getchar
#else
#include <unistd.h>
#include <malloc.h>
#include <pthread.h>
#endif
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include "AutoHandle.h"
#include "dt_common.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/wait.h>
#include <sys/types.h>

#include "ProcessMsg.h"

int Start(void *ptr);

int main(int argc,char *argv[]) {
	int nRetCode = 0;
	WOCIInit("dpAlarm/dpAlarm");
	wociSetOutputToConsole(TRUE);
	wociSetEcho(false);
	nRetCode=wociMainEntrance(Start,true,NULL,2);
	WOCIQuit(); 
	return nRetCode;
}

ProcessMsg g_procMsgObj;  // ��Ϣ�������
int Start(void *ptr) { 
	if (g_procMsgObj.OnInit("SendAlarm.ctl") != 0)
	{
		lgprintf("��������ʧ��.");
		return -1;
	}
	if (g_procMsgObj.OnStart()!=0)
	{
		lgprintf("�澯���ʹ�������ʧ��.");
		return -1;
	}
	sleep(3);
	char szExit[32] = {0};
	while (1)
	{
		memset(szExit,0,32);
		scanf(szExit,"%s");
		if (strcasecmp(szExit,"exit") == 0 || strcasecmp(szExit,"quit") == 0)
		{
			lgprintf("�澯�����˳�.");
			break;
		}
		sleep(1);
	}
	g_procMsgObj.OnStop();
	sleep(1);
	return 1;
}

