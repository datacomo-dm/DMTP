#include"uodbc.h"
UODBC test;
int main(int argc,char *argv[]){

	int rownum = 0;
	int i = 0;
	char *sqlaaaa;
	sqlaaaa = (char *)malloc(8000);
	printf("here\n");
	//strcpy(sqlaaaa,"select concat_ws('', cdrtype,',',substring(othertelnum,1,22),',',case substring(roamtype,1,1) when '0' then '1' else roamtype END,',',calltype,',',DATE_FORMAT(starttime, '%m/%d %H:%i:%S'),',','',',',fee*100,',','0',',','0',',',fee*100,',','0',',','0',',','0',',','0',',',getname,',',hregion,',',ifnull(mscid,''),',','',',','' ) from s_szx_smshomecdr311_201003 where  telnum=@telnum  and starttime>=DATE_FORMAT(@starttime ,'%Y-%m-%d %H:%i:%S')  and starttime<=DATE_FORMAT(@endtime ,'%Y-%m-%d %H:%i:%S')");
	printf("asdfsadf\n");
	//strcpy(sqlaaaa,"select concat_ws('', case substring(cdrtype,1,1) when 'O' then 'OS' else 'TS' END,',',substring(othertelnum,1,22),',',case substring(roamtype,1,1) when '0' then '1' else roamtype END,',',calltype,',',DATE_FORMAT(starttime, '%m/%d %H:%i:%S'),',','',',',fee*100,',','0',',','0',',',fee*100,',','0',',','0',',','0',',','0',',','',',',hregion,',',ifnull(mscid,''),',','',',','' ) from v_smshomecdr311_201003 where  telnum=@telnum  and starttime>=DATE_FORMAT(@starttime ,'%Y-%m-%d %H:%i:%S')  and starttime<=DATE_FORMAT(@endtime ,'%Y-%m-%d %H:%i:%S')");
//	strcpy(sqlaaaa,"select concat_ws('', cdrtype,',',case length(othershortnum) when 0 then othertelnum else (substring((concat(othershortnum,':',othertelnum)),1,22)) END,',',case substring(roamtype,1,1) when '0' then '1' else (case substring(roamtype,1,1) when '1' then '20' else '21' END) END,',',case substring(calltype,1,2) when '22' then '212' else calltype END,',',DATE_FORMAT(starttime, '%m/%d %H:%i:%S'),',',duration,',',localfee*100,',',IFNULL(tollfee,0)*100,',',0,',',(localfee+roamfee+tollfee+tolladdfee)*100,',',IFNULL(roamfee,0)*100,',','0',',','0',',',IFNULL(TOLLADDFEE,0)*100,',',getname,',',hregion,',',ifnull(mscid,''),',','',',','' ) from s_vpmnhomecdr311_201003 where  telnum=@telnum  and starttime>=DATE_FORMAT(@starttime ,'%Y-%m-%d %H:%i:%S')  and starttime<=DATE_FORMAT(@endtime ,'%Y-%m-%d %H:%i:%S')");
	//strcpy(sqlaaaa,"select concat_ws('', 'O',',',substring(othertelnum,1,22),',',case substring(roamtype,1,1) when '0' then '1' else (case substring(roamtype,1,1) when '1' then '20' else '21' END) END,',',calltype,',',DATE_FORMAT(starttime, '%m/%d %H:%i:%S'),',',duration,',','0',',','0',',','0',',',fee*100,',','0',',','0',',','0',',',fee*100,',',getname,',',hregion,',','0',',','',',','' ) from v_otherfee311_201003 where  telnum=@telnum  and starttime>=DATE_FORMAT(@starttime ,'%Y-%m-%d %H:%i:%S')  and starttime<=DATE_FORMAT(@endtime ,'%Y-%m-%d %H:%i:%S')");
//	strcpy(sqlaaaa,"select concat_ws('', case cdrtype when 'T' then 'T' else (case substring(servicecode,1,1) when '2' then 'R' else 'O' END) END,',',substring(othertelnum,1,22),',',case substring(roamtype,1,1) when '0' then '1' else (case substring(roamtype,1,1) when '1' then '20' else '21' END) END,',',case substring(specialtype,1,1) when '8' then '41' else (case substring(calltype,1,2) when '22' then '212' else (case sign(tollfee2) when sign(1) then (case substring(calltype,1,1) when '1' then '21' else calltype END) else calltype END) END) END,',',DATE_FORMAT(starttime, '%m/%d %H:%i:%S'),',',duration,',',(case substring(specialtype,1,1) when 'E' then (case substring(roamtype,1,1) when '0' then localfee+roamfee else 0 END) when 'Y' then (case substring(roamtype,1,1) when '0' then localfee+roamfee else 0 END) else localfee END)*100,',',(ifnull(tollfee,0)+ifnull(tollfee2,0))*100,',',0,',',(localfee+roamfee+tollfee+tolladdfee+tollfee2)*100,',',100* (case substring(specialtype,1,1) when 'E' then (case substring(roamtype,1,1) when '0' then localfee+roamfee+tolladdfee else 0 END) else IFNULL(roamfee,0) END),',','0',',','0',',',IFNULL(TOLLADDFEE,0)*100,',',getname,',',hregion,',',ifnull(mscid,''),',',cellid,',',lac ) from s_scpcdr311_201003 where  telnum=@telnum  and starttime>=DATE_FORMAT(@starttime ,'%Y-%m-%d %H:%i:%S')  and starttime<=DATE_FORMAT(@endtime ,'%Y-%m-%d %H:%i:%S')");
	strcpy(sqlaaaa,"select concat_ws('', case cdrtype when 'T' then (case freeformatdata when 'VPMN' then 'TV' else 'T' END) else (case freeformatdata when 'VPMN' then 'OV' else (case substring(servicecode,1,1) when '2' then 'R' else 'O' END) END) END,',',substring(othertelnum,1,22),',',case substring(roamtype,1,1) when '0' then '1' else (case substring(roamtype,1,1) when '1' then '20' else '21' END) END,',',case substring(specialtype,1,1) when '8' then '41' else (case substring(calltype,1,2) when '22' then '212' else (case sign(tollfee2) when sign(1) then (case substring(calltype,1,1) when '1' then '21' else calltype END) else calltype END) END) END,',',DATE_FORMAT(starttime, '%m/%d %H:%i:%S'),',',duration,',',(case substring(specialtype,1,1) when 'E' then (case substring(roamtype,1,1) when '0' then localfee+roamfee else 0 END) when 'Y' then (case substring(roamtype,1,1) when '0' then localfee+roamfee else 0 END) else localfee END)*100,',',(ifnull(tollfee,0)+ifnull(tollfee2,0))*100,',',0,',',(localfee+roamfee+tollfee+tolladdfee+tollfee2)*100,',',(case substring(specialtype,1,1) when 'E' then (case substring(roamtype,1,1) when '0' then localfee+roamfee+tolladdfee  else localfee+roamfee END) else IFNULL(roamfee,0) END)*100,',','0',',','0',',',IFNULL(TOLLADDFEE,0)*100,',',getname,',',hregion,',',ifnull(mscid,''),',',cellid,',',lac,',',IFNULL(system_type,'GSM'),',',case ifnull(viedoflag,'V') when 'M' then 'ÊÇ' else '·ñ' END ) from s_gsmhomecdr311_201003 where  telnum=@telnum  and starttime>=DATE_FORMAT(@starttime ,'%Y-%m-%d %H:%i:%S')  and starttime<=DATE_FORMAT(@endtime ,'%Y-%m-%d %H:%i:%S')");	
	
for(int l=0;l<10;l++)
{	if(test.DmConnect("root","dbplus03","dm") != 0) 
	{
		printf("connect is error!\n");
		//return 0;
	}
	printf("connect success!\n");	
	test.DmSetSQL(sqlaaaa);
        test.DmBind(1,"13933803074");
        test.DmBind(2,"2010-03-01 07:28:45");
        test.DmBind(3,"2010-05-01 07:28:45");
        int a = test.DmOpen();
        printf("a is -------%d\n",a);
        while(test.DmNext()!=0 && a!=0 )
                    printf("test.DmGetDataaaa is %s\n",test.DmGetData(1));
	printf("l is %d\n",l);
}
/*	test.DmSetSQL(sqlaaa);
	test.DmBind(1,"311");
	test.DmBind(1,"13933803074");
	test.DmBind(2,"2010-03-01 07:28:45");
	test.DmBind(3,"2010-05-10 07:28:45");
	printf("sqlaaaa is %s\n",sqlaaaa);
	printf("test.DMOpen is %d----\n",test.DmOpen());
	while(test.DmNext()!=0)
        {      printf("sql result is %10s\n",test.sqlbuffer1);
               for(i=1;i<test.colanz+1;i++)
                 {
                      test.DmGetData(i);
                      printf("the i is -------%d\n",i);
                      printf("test.DmGetDataaaa is %s\n",test.DmGetData(1));

                 }
         }
	*/
/*	for(int j=0;j<5;j++)
	{
		test.DmSetSQL(sqlaaaa);
		test.DmBind(1,"13933803074");
        	test.DmBind(2,"2010-01-01 07:28:45");
        	test.DmBind(3,"2010-03-10 07:28:45");
		a = test.DmOpen();
		printf("a is -------%d\n",a);
		while(test.DmNext()!=0 && a!=0 )
		{	printf("sql result is %10s\n",test.sqlbuffer1);
			for(i=1;i<test.colanz+1;i++)
			{
				test.DmGetData(i);
				printf("the j is -------%d\n",j);
				printf("test.DmGetDataaaa is %s\n",test.DmGetData(1));

			}
		}
	}*/
	return 1;
}

