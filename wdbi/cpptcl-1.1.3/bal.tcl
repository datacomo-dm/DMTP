#ͳ��ȫ���û��ĵ�ǰ�������
# �� tab_subscrb tab_subscrbprcpln tab_acctrelation �����û����ײ͡��˻�����
#   ȡtab_billdetail  tab_acc_gsm_sum_fee tab_acc_gsm_hot_rent ���û����ܼ���Ƿ�ѡ�ʵʱ���ѡ�Ԥ�����ࡢ���
# ���д���� tab_gsm_balance_fee �����ڵ�ǰĿ¼�����ı��ļ� subscrbGSM.txt
load ./myhello.so
dbSetOutputToConsole true
set dts [dbCon kmcrm "stat@))^" //130.86.18.202/kmcrm ]
set usrtab [dbMt [$dts id]]
$usrtab SetMaxRows  2240000
#�����ֶΣ��ֱ����ڴ�ţ� �ʺţ��ײͺţ�Ƿ�ѣ�ʵʱ���ѣ�Ԥ����࣬���
dbAddColumn [$usrtab id] acctid int 10 0
dbAddColumn [$usrtab id] prcplnid int 10 0
dbAddColumn [$usrtab id] ofee number 10 2
dbAddColumn [$usrtab id] rfee number 10 2
dbAddColumn [$usrtab id] thisbal number 10 2
dbAddColumn [$usrtab id] rbal number 10 2
#���������ֶΣ���ִ��SQL��ʱ��������е��ֶ�
$usrtab SetAutoClear false
$usrtab FetchSQL "select subscrbid,statdate,svcnum,svcstat from tab_subscrb where trim(svcstat)<>'9' and svcid='10'"
$usrtab Wait
$usrtab Sort subscrbid
set srctab [dbMt [$dts id] ]
$srctab SetMaxRows 50000
$srctab FetchFirstSQL "select prcplnid,subscrbid from tab_subscrbprcpln where begtime<sysdate and (endtime>sysdate or endtime is null) and svcid='10'"
set rn [$srctab Wait]
#ȡ�ײʹ���
while {$rn>0}  {
 dbValuesSet [$usrtab id] "subscrbid;prcplnid" [$srctab id] "subscrbid;prcplnid" true set
 $srctab FetchNext
 set rn [$srctab Wait]
}
#ȡǷ��
$srctab FetchFirstSQL "select subscrbid,fee,dfee from tab_billdetail where recstat in ('00','04','05','06','07','p3') "
set rn [$srctab Wait]
while {$rn>0}  {
 dbValuesSet [$usrtab id] "subscrbid;ofee" [$srctab id] "subscrbid;fee" true add
 dbValuesSet [$usrtab id] "subscrbid;ofee" [$srctab id] "subscrbid;dfee" true add
 $srctab FetchNext
 set rn [$srctab Wait ]
}
#ȡʵʱ����
dbSetDefaultPrec 10 2
$srctab FetchFirstSQL [format "select subscrbid,(fee+dfee)/1000.0 as rfee from tab_acc_gsm_sum_fee where month>=%s" 200902]
set rn [$srctab Wait]
while {$rn>0}  {
 dbValuesSet [$usrtab id] "subscrbid;rfee" [$srctab id] "subscrbid;rfee" true add
 $srctab FetchNext
 set rn [$srctab Wait ]
}
#ȡ�������
$srctab FetchFirstSQL [format "select subscrbid,(fee+dfee)/1000.0 as rfee from tab_acc_gsm_hot_rent where month>=%s" 200902]
set rn [$srctab Wait]
while {$rn>0}  {
 dbValuesSet [$usrtab id] "subscrbid;rfee" [$srctab id] "subscrbid;rfee" true add
 $srctab FetchNext
 set rn [$srctab Wait ]
}
dbSetDefaultPrec 10 0
set acct [dbMt [$dts id] ]
$acct SetMaxRows 6000000
dbAddColumn [$acct id] thisbal number 10 2
$acct SetAutoClear false
$acct FetchSQL "select acctid,subscrbid from tab_acctrelation where acctitmtypeid in(1,-1) and nvl(expdate,sysdate+1)>sysdate"
set rn [$acct Wait]
dbSetIKByName [$acct id] acctid
dbOrderByIK [$acct id]
dbValuesSet [$usrtab id] "subscrbid;acctid" [$acct id] "subscrbid;acctid" true set
$srctab FetchFirstSQL "select acctid,thisbal from tab_accountpredeposit "
set rn [$srctab Wait]
while {$rn>0} {
  dbValuesSet [$acct id] "acctid;thisbal" [$srctab id] "acctid;thisbal" true add
  $srctab FetchNext
  set rn [$srctab Wait]
}

dbValuesSet [$usrtab id] "subscrbid;thisbal" [$acct id] "subscrbid;thisbal" true add
dbValuesSet [$usrtab id] "subscrbid;rbal" [$acct id] "subscrbid;thisbal" true add
dbValuesSet [$usrtab id] "subscrbid;rbal" [$usrtab id] "subscrbid;ofee" true sub
dbValuesSet [$usrtab id] "subscrbid;rbal" [$usrtab id] "subscrbid;rfee" true sub
dbSetMTName [$usrtab id] GSM�������û��嵥
dbMTToTextFile [$usrtab id] "subscrbGSM.txt" 0 ""
$usrtab CreateAndAppend tab_gsm_balance_feexx 0 true true

