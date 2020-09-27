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
    dts.SetHandle(wociCreateSession("root","dbplus03","dp5",DTDBTYPE_ODBC));
    {
      AutoHandle dts1;
      dts1.SetHandle(wociCreateSession("root","dbplus03","dp5",DTDBTYPE_ODBC));
    }
    mytimer tm;
    tm.Start();
    //mt.FetchFirst();
    //int rn=mt.Wait();
    tm.Stop();
    return 1;
}
	
