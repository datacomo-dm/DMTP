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
#include "dt_common.h"
#include "mysqlconn.h"
#include "dt_svrlib.h"
#include "dt_cmd_base.h"
#include "dtio.h"
int Start(void *ptr);

int ci=1;
char *pidxcolarray[20];
int cmd_base::argc=0;
char **cmd_base::argv=(char **)NULL;

cmd_base cp;

int main(int argc,char *argv[]) {
	int nRetCode = 0;
	cmd_base::argc=argc;
        cmd_base::argv=(char **)argv;

	WOCIInit("dputil" PATH_SEP "dprmrows");
	nRetCode=wociMainEntrance(Start,true,NULL,2);
	WOCIQuit(); 
	return nRetCode;
}

char * BuildQuery(char *sql,const char *tbname,AutoMt &indexmt,int row,bool ora) {
	sprintf(sql,"select * from %s where ",tbname);
	for(int i=0;i<ci;i++) {
		//char colname[300];
		int c=i;//wociGetColumnPosByName(indexmt,(const char*)pidxcolarray[i]);
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
	else if(rn!=indexmt.GetInt("idx_rownum",irn)) {
		wociGetLine(indexmt,irn,sql,false,NULL);
		printf("����,��ѯ��¼������,����%d,ʵ��%d :%s\n",indexmt.GetInt("idx_rownum",irn),rn,sql);
	}
	return rn;
}
const char *GetDPLibVersion();
int Start(void *ptr) { 
	wociSetEcho(FALSE);
	wociSetOutputToConsole(TRUE);
	DbplusCert::initInstance();
	DbplusCert::getInstance()->printlogo();
	printf(DBPLUS_STR " ������汾 :%s \n",GetDPLibVersion());
    	cp.GetEnv();
	char date[30];
	wociGetCurDateTime(date);
	int y,m,d;
	DbplusCert::getInstance()->GetExpiredDate(y,m,d);
	if(y>0 && wociGetYear(date)*100+wociGetMonth(date)>y*100+m) 
		ThrowWith("���õ�" DBPLUS_STR "�汾̫�ϣ�����º�ʹ��(Your " DBPLUS_STR " is too old,please update it)!");
	printf("  dprmrows �������ڰ�������ֵɾ��Ŀ����е�����.\n"
	       " ��ǰ�汾ֻ֧��Ψһ���������Ҳ��������������ֶ�ɾ������.\n"
	       "!!!!!ǿ�ҽ�������ɾ������ǰ�ȱ���!!!!!!\n");
	if(!GetYesNo("�Ƿ����ִ��ɾ������?<N>",false)) return -1;
	printf("���ӵ�" DBPLUS_STR "���ݿ�:\n");
	AutoHandle dts;
	dts.SetHandle(wociCreateSession(cp.musrname,cp.mpswd,cp.mhost,DTDBTYPE_ODBC));
	SysAdmin sa (dts,cp.serverip,cp.musrname,cp.mpswd);
	lgprintf("ȡ���в���...");
	sa.Reload();
	char sql[10000];
	char dbname[300],tabname[300];
	char condsql[5000];
	condsql[0]=dbname[0]=tabname[0]=0;
	int selidx=0;
	while(true) {
		AutoMt selmt(dts,500);
		selmt.FetchAll("select databasename \"���ݿ�\", tabname \"����\",recordnum \"��¼��\" from dp.dp_table where recordnum>0 order by databasename,recordnum limit 500");
		selmt.Wait();
		wociSetColumnDisplay(selmt,NULL,0,"���ݿ�",-1,-1);
		wociSetColumnDisplay(selmt,NULL,1,"����",-1,-1);
		wociSetColumnDisplay(selmt,NULL,2,"��¼��",-1,0);
		
		wociMTCompactPrint(selmt,0,NULL);
		sql[0]=0;
		getString("�������ݿ���",dbname,dbname);
		//***strcpy(dbname,"test");
		if(strlen(dbname)==0) break;
		getString("�������",tabname,tabname);
		//***strcpy(tabname,"tab_subscrb");
		if(strlen(tabname)==0) break;
		selmt.FetchAll("select tabid,recordnum from dp.dp_table where databasename=lower('%s') and tabname=lower('%s')",dbname,tabname);
		if(selmt.Wait()<1) {
			lgprintf("����:��%s.%s������.",dbname,tabname);
			continue;
		}
		if(selmt.GetDouble("recordnum",0)<1) {
			lgprintf("����:��%s.%s��û�м�¼.",dbname,tabname);
			continue;
		}
		int tabid=selmt.GetInt("tabid",0);
		selmt.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>=1",tabid);
		if(selmt.Wait()>1) {
			lgprintf("����:��%s.%s�ж���������������һ������֧�ּ�¼ɾ��.",dbname,tabname);
			continue;
		}
		selmt.FetchAll("select * from dp.dp_index where tabid=%d",tabid);
		int indexrn=selmt.Wait();
		if(indexrn<1) {
			lgprintf("����:��%s.%sδ��������.",dbname,tabname);
			continue;
		}
		char fulltabname[300];
		sprintf(fulltabname,"%s.%s",dbname,tabname);
		printf("��ѡ��ʹ�õ�����:\n");
		int i;
		for(i=0;i<indexrn;i++)
			printf("%d: %s.\n",i+1,selmt.PtrStr("columnsname",i));
		
		selidx=getOption("�������",selidx+1,1,indexrn)-1;
		//***int selidx=2;
		
		lgprintf("�����ֶ�:%s.",selmt.PtrStr("columnsname",selidx));
		static char icolsnm[1000];
		strcpy(icolsnm,selmt.PtrStr("columnsname",selidx));
		//�ֽ��ֶ�,ע�ⲿ������
		char *ptemp=icolsnm;
		while(*ptemp==' ') ++ptemp;
		
		pidxcolarray[ci-1]=ptemp;
		
		while(*ptemp) {
			if(*ptemp=='(') {
				lgprintf("��֧�ְ����������ֶ�ɾ����¼��");
				return -1;
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
				lgprintf("����:�Ҳ���������¼.");
				continue;
			}
		}
		int il=wociGetRowLen(indexmt)-sizeof(int)*6;
		int concol=wociGetColumnNumber(selmt)-6;
		
		int condlimitrn=getOption("ɾ���������Ƽ�¼��",100000,100,5000000);
		int idxlimitrn=getOption("�������Ʒ��ؼ�¼��",100000,100,5000000);
		indexmt.SetMaxRows(idxlimitrn);
		lgprintf("����ɾ����������.\nѡ���ѯɾ���������ӵ����ݿ�:");
		AutoHandle dtsc;
		dtsc.SetHandle(BuildConn(0));
		//***dtsc.SetHandle(wociCreateSession(cp.musrname,cp.mpswd,cp.mhost,DTDBTYPE_ODBC));
		getString("����ɾ��������ѯ��䣬ע�ⷵ�����ݵ��ֶ����͡�������˳�򣬱���������ѡ��������ֶ�һ��:\n",condsql,condsql);
		//***strcpy(condsql,"select '13388829666' svcnum");
		AutoMt condmt(dtsc,condlimitrn);
		condmt.FetchAll(condsql);
		int condrn=condmt.Wait();
		indexmt.FetchFirst("select %s from %s limit 10",icols,idxtbname );
		indexmt.Wait();
		if(condmt.CompatibleMt(indexmt)) {
			lgprintf("�����ֶ��������ֶβ�һ��!");
			continue;
		}
		lgprintf("ȡ��ɾ����������%d��.\n",condrn);
		indexmt.FetchFirst("select * from %s limit 10",idxtbname );
		indexmt.Wait();
		AutoMt filemap(dts,2000);
		//ֻ���Ψһ�������������������Ҫ��ѯindexgid.
		filemap.FetchAll("select * from dp.dp_datafilemap where tabid=%d and fileflag=0",tabid);
		filemap.Wait();
		wociSetIKByName(filemap,"fileid");
		wociOrderByIK(filemap);
		//wociSearchIK
		int lid=0;
		wociReset(indexmt);
		lgprintf("��ɾ������ȡ��������...\n");
		for(lid=0;lid<condrn;lid++) {
			BuildQuery(sql,idxtbname,condmt,lid,false) ;
			indexmt.FetchAllAt(indexmt.GetRows(),sql);
			indexmt.Wait();
		}
		int irn=indexmt.GetRows();
		int destrn=(int)wociCalculate(indexmt,"idx_rownum",CAL_SUM);
		lgprintf("ȡ����������%d��,��ӦĿ������%d�С�\n",irn,destrn);
		wociSetSortColumn(indexmt,"dtfid,blockstart");
		wociSort(indexmt);
		
		printf("���������Ϣ�Ƿ���ȷ��\n\n");
		bool needback=GetYesNo("Ҫɾ����������Ҫ������?<Y>",true);
		{
			AutoMt bkmt(dts,50000);
			bkmt.FetchAll("select * from %s.%s limit 10",dbname,tabname);
			bkmt.Wait();
			wociReset(bkmt);
			AutoStmt bkst(dts);
			AutoHandle dtbk;
			char bktabname[300];
			if(needback) {
			 lgprintf("���ӵ��������ݿ�:\n");
			 dtbk.SetHandle(BuildConn(0));
			 getString("�������",NULL,bktabname);
			 if(wociTestTable(dtbk,bktabname)) {
				int sel=getOption("Ŀ����Ѵ��ڣ�׷��(1)/ɾ���ؽ�(2)/��պ����(3)/�˳�(4)",2,1,4);
				if(sel==4) return -1;
				if(sel==2) {
					lgprintf("ɾ����(drop table)'%s'...",bktabname);
					AutoStmt st(dtbk);
					st.DirectExecute("drop table %s",bktabname);
					lgprintf("����(create table)'%s'...",bktabname);
					wociGeneTable(bkmt,bktabname,dtbk);
				}
				else if(sel==3) {
					lgprintf("��ձ�(truncate table)'%s'...",bktabname);
					AutoStmt st(dtbk);
					st.DirectExecute("truncate table %s",bktabname);
				}
			 }
			 else  { 	
				lgprintf("����(create table)'%s'...",bktabname);
				wociGeneTable(bkmt,bktabname,dtbk);
			 }
			}			
			int totrows=0;
			for(lid=0;lid<condrn;lid++) {
				BuildQuery(sql,fulltabname,condmt,lid,false) ;
				bkst.Prepare(sql);
				wociReplaceStmt(bkmt,bkst);
				bkmt.FetchAppend();
				while(bkmt.Wait()==50000) // mt full
				{
					if(needback) 
						wociAppendToDbTable(bkmt,bktabname,dtbk,true);
					wociReset(bkmt);
					bkmt.FetchAppend();
					totrows+=50000;
				}
			}
			totrows+=bkmt.GetRows();
			if(needback) 
				wociAppendToDbTable(bkmt,bktabname,dtbk,true);
			//***wociMTPrint(bkmt,0,NULL);
			wociReset(bkmt);
			if(totrows!=destrn) {
				if(totrows<1) {
					errprintf("�Ҳ���ɾ������,�����Ƿ��ظ�ɾ��.");
					return -1;
				}
				errprintf("��������(%d)����������(%d)�еļ�¼������һ��.\n�����Ӧ����ֵ��������ǰ����ɾ��,����Լ���,��������ֹɾ������!!",totrows,destrn);
				if(GetYesNo("����ɾ��������?<N>",false)) {
					destrn=totrows;
				}
				else return -1;
			}
		}
		int havedel=0;
		for(lid=0;lid<irn;lid++) {
			int p=wociGetRawrnBySort(indexmt,lid);
			int dtfid=indexmt.GetInt("dtfid",p);
			int fpos=wociSearchIK(filemap,dtfid);
			if(fpos<0) {
				errprintf("�Ҳ��������ļ�,tabid:%d,dtfid:%d.",tabid,dtfid);
				return -1;
			}
			char *fn=NULL;
			fn=filemap.PtrStr("filename",fpos);
			file_mt dfile;
			dfile.Open(fn,0);
			char *tmp=NULL;
			if(dfile.ReadMtOrBlock(indexmt.GetInt("BLOCKSTART",p),indexmt.GetInt("BLOCKSIZE",p),1,&tmp)<0) {
				errprintf("read block from '%s' failed,off:%d,len:%d.",indexmt.GetInt("BLOCKSTART",p),indexmt.GetInt("BLOCKSIZE",p));
				return -1;
			}
			int startrow=indexmt.GetInt("startrow",p);
			int idxrownum=indexmt.GetInt("idx_rownum",p);
			// startrow: 0 based
			int sdel=dfile.deleterows(startrow,idxrownum);
			havedel+=sdel;
			printf("idxr:%d,sdel:%d,fnid:%d,blockstart:%d,startrow:%d\n",
				idxrownum,sdel,indexmt.GetInt("dtfid",p),indexmt.GetInt("BLOCKSTART",p),startrow);
			*filemap.PtrInt("recordnum",fpos)-=sdel;
		}
		if(havedel!=destrn) {
			errprintf("ɾ����¼����һ��! ��Ҫɾ��%d��,ʵ��ɾ��%d��,��ӱ��ݽ��ʻָ�����!",destrn,havedel);
		}
		lgprintf("�޸������ֵ�...");
		double newtotrows=wociCalculate(filemap,"recordnum",CAL_SUM);
		AutoStmt actst(dts);
		actst.DirectExecute("update dp.dp_table set recordnum=%.0f where tabid=%d",newtotrows,tabid);
		actst.DirectExecute("delete from dp.dp_datafilemap where tabid=%d",tabid);
		wociAppendToDbTable(filemap,"dp.dp_datafilemap",dts,true);
		sa.BuildDTP(fulltabname);
		lgprintf("��'%s'����ɾ���ɹ�,��ɾ��%d�м�¼.",fulltabname,destrn);
 	}
	return 0;
}
