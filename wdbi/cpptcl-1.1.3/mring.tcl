#ͳ��ȫ���û��ĵ�ǰ�������
# �� tab_subscrb tab_subscrbprcpln tab_acctrelation �����û����ײ͡��˻�����
#   ȡtab_billdetail  tab_acc_gsm_sum_fee tab_acc_gsm_hot_rent ���û����ܼ���Ƿ�ѡ�ʵʱ���ѡ�Ԥ�����ࡢ���
# ���д���� tab_gsm_balance_fee �����ڵ�ǰĿ¼�����ı��ļ� subscrbGSM.txt
load ./myhello.so
dbSetEcho true
dbSetOutputToConsole true
#set obsbc [dbCon wanggsh wanggsh //130.86.1.2:1526/obs9i ]
set yncrm [dbCon wanggsh wanggsh //130.86.4.3/yncrm ]
 set obs   [dbCon wanggsh wanggsh //130.86.12.18:1522/obs9i ]
#set casbc   [dbCon wanggsh wanggsh //130.86.1.2:1526/cas9i ]
set usrtab [dbMt [$obs id]]
$usrtab SetMaxRows  2400000
#�����ֶΣ����ڴ�Ż��ֶһ���Ŀ
dbAddColumn [$usrtab id] svcnum string 20 0
dbAddColumn [$usrtab id] svcstat string 2 0
dbAddColumn [$usrtab id] statdate date 7 0
dbAddColumn [$usrtab id] opendate date 7 0


#���������ֶΣ���ִ��SQL��ʱ��������е��ֶ�
$usrtab SetAutoClear false
$usrtab FetchSQL "select subscrbid,orderdate,removedate,srvgstat,areaid from obs.tab_srvg a where  subsvcid='000000505' and orderdate<sysdate and nvl(removedate,sysdate+1)>sysdate and svcid='10' "
$usrtab Wait
$usrtab Sort subscrbid

#ȡ���ֶһ���¼
set midmt [dbMt [$obs id]]
$midmt SetMaxRows 50000
$midmt FetchFirstSQL "select subscrbid,svcnum,statdate,opendate,svcstat from obs.tab_subscrb"
$midmt Wait
set rn [$midmt Wait]
while {$rn>0}  {
 dbValuesSet [$usrtab id] "subscrbid;svcnum" [$midmt id] "subscrbid;svcnum" true set
 dbValuesSet [$usrtab id] "subscrbid;statdate" [$midmt id] "subscrbid;statdate" true set
 dbValuesSet [$usrtab id] "subscrbid;opendate" [$midmt id] "subscrbid;opendate" true set
 dbValuesSet [$usrtab id] "subscrbid;svcstat" [$midmt id] "subscrbid;svcstat" true set
  $midmt FetchNext
 set rn [$midmt Wait]
}

dbSetMTName [$usrtab id] ��Ч�����û�
dbMTToTextFile [$usrtab id] "mring.txt" 0 ""
$usrtab CreateAndAppend tab_mring_090417 [$yncrm id] true true
