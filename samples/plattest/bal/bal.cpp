 #ifdef WIN32
 #include <windows.h>
 #include <process.h>
 #include <io.h>
 #else
 #include <unistd.h>
 #endif
 #include <stdlib.h>
 #include <stdio.h>
 #include "AutoHandle.h"
 int Start(void *ptr);
 int argc;
 char **argv; 	
 int main(int _argc,char *_argv[]) {
     WOCIInit("stat");
     argc=_argc;
     argv=_argv;
     int nRetCode=wociMainEntrance(Start,true,NULL,2);
     WOCIQuit(); 
     return nRetCode;
 }
 void Stati(const char *table_name,char *part_id);
 #define OUTFILENAME "subscrbGSM.txt"
 int Start(void *ptr) { 
     wociSetOutputToConsole(TRUE);
     char s_tab_name[1024];
     char s_part[100];
     int rn;
     if (argc != 2)
     {
         printf("�����в�����ʽ: bal ����\n���� : bal 200507");
         exit(0);
     }
     lgprintf("���ӵ��������ݿ�...");
     AutoHandle dts;
     dts.SetHandle(wociCreateSession("kmcrm","crmkm","//130.86.18.202:1521/kmcrm",DTDBTYPE_ORACLE));
     mytimer mt_1;
     mt_1.Start();
     //40���¼�Ĵ洢��
     wociSetDefaultPrec(10,0);
     AutoMt usrtab(dts,2400000);
     //wociAddColumn(usrtab,"acctid",NULL,COLUMN_TYPE_INT,10,0);
     wociAddColumn(usrtab,"prcplnid",NULL,COLUMN_TYPE_INT,10,0);
     wociAddColumn(usrtab,"ofee",NULL,COLUMN_TYPE_NUM,10,2);
     wociAddColumn(usrtab,"rfee",NULL,COLUMN_TYPE_NUM,10,2);
     wociAddColumn(usrtab,"thisbal",NULL,COLUMN_TYPE_NUM,10,2);
     wociAddColumn(usrtab,"rbal",NULL,COLUMN_TYPE_NUM,10,2);
     usrtab.SetAutoClear(false);
     usrtab.FetchAll("select subscrbid*1.0 subscrbid,statdate,svcnum,svcstat from tab_subscrb where trim(svcstat)<>'9' and svcid='10' and rownum<100000");
     usrtab.Wait();
     printf("Get %d rows.\n",usrtab.GetRows());
     wociSetSortColumn(usrtab,"subscrbid");
     wociSort(usrtab);
     //wociSetIKByName(usrtab,"subscrbid");
     //wociOrderByIK(usrtab);
     
     //ȡ�ײʹ���
     AutoMt srctab(dts,50000);
     srctab.FetchFirst("select prcplnid,subscrbid*1.0 subscrbid from tab_subscrbprcpln where begtime<sysdate and (endtime>sysdate or endtime is null) and svcid='10' and rownum<100000");
     rn=srctab.Wait();
     while(rn) {
     	wociValuesSet(usrtab,srctab,"subscrbid;prcplnid","subscrbid;prcplnid",true,VALUESET_SET);
     	srctab.FetchNext();
     	rn=srctab.Wait();
     }
     //ȡǷ��
     srctab.FetchFirst("select subscrbid*1.0 subscrbid,fee,dfee from tab_billdetail where recstat in ('00','04','05','06','07','p3') and rownum<100000");
     rn=srctab.Wait();
     while(rn) {
     	wociValuesSet(usrtab,srctab,"subscrbid;ofee","subscrbid;fee",true,VALUESET_ADD);
     	wociValuesSet(usrtab,srctab,"subscrbid;ofee","subscrbid;dfee",true,VALUESET_ADD);
     	srctab.FetchNext();
     	rn=srctab.Wait();
     }
 
     //ȡʵʱ����
     wociSetDefaultPrec(10,2);
     srctab.FetchFirst("select subscrbid,(fee+dfee)/1000.0 as rfee from tab_acc_gsm_sum_fee where month>=%s and rownum<100000",argv[1]);
     rn=srctab.Wait();
     while(rn) {
     	wociValuesSet(usrtab,srctab,"subscrbid;rfee","subscrbid;rfee",true,VALUESET_ADD);
     	srctab.FetchNext();
     	rn=srctab.Wait();
     }
 
     wociSetDefaultPrec(10,0);
     {
      //ȡ�ʺŶ�Ӧ��ϵ
      AutoMt acct(dts,6000000);
      wociAddColumn(acct,"thisbal",NULL,COLUMN_TYPE_NUM,10,2);
      acct.SetAutoClear(false);
      acct.FetchAll("select acctid,subscrbid*1.0 subscrbid from tab_acctrelation where rownum<100000");
      rn=acct.Wait();
      wociSetIKByName(acct,"acctid");
      wociOrderByIK(acct);
      //ȡԤ������
      srctab.FetchFirst("select acctid,thisbal from tab_accountpredeposit where rownum<100000");
      rn=srctab.Wait();
      while(rn) {
     	wociValuesSet(acct,srctab,"acctid;thisbal","acctid;thisbal",true,VALUESET_ADD);
     	srctab.FetchNext();
     	rn=srctab.Wait();
      }
      
      wociValuesSet(usrtab,acct,"subscrbid;thisbal","subscrbid;thisbal",true,VALUESET_ADD);
     }
     wociValuesSet(usrtab,usrtab,"subscrbid;rbal","subscrbid;thisbal",true,VALUESET_ADD);
     wociValuesSet(usrtab,usrtab,"subscrbid;rbal","subscrbid;ofee",true,VALUESET_SUB);
     wociValuesSet(usrtab,usrtab,"subscrbid;rbal","subscrbid;rfee",true,VALUESET_SUB);
     //������ݴ浽�ı��ļ�
     printf("Write to text file '%s'...\n",OUTFILENAME);
     wociSetMTName(usrtab,"GSM�������û��嵥");
     unlink(OUTFILENAME);
     wociMTToTextFile(usrtab,OUTFILENAME,0,NULL);
     //������� 
     usrtab.CreateAndAppend("tab_gsm_balance_fee",0/*use src mt's db connection*/,
                   true/*force drop and create table*/);
     mt_1.Stop();
     lgprintf("����ʱ��%f�� ��������",mt_1.GetTime());
 }