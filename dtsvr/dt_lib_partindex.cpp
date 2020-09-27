#include "dt_common.h"
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include "dt_lib.h"
#include "zlib.h"
#include "dtio.h"
#include <sys/wait.h>
#define COMPRESSLEVEL 5

#define MAX_DUMPIDXBYTES	1024*1024
#define	SRCBUFLEN 2500000
#define DSTBUFLEN 2500000
#define PREPPARE_ONLINE_DBNAME "preponl"
#define FORDELETE_DBNAME "fordelete"
// 2005/08/27�޸ģ�partid��Ч��sub	

//�̶�Ϊ1MB
#define FIX_MAXINDEXRN 1*1024*1024

	extern char errfile[];
	extern char lgfile[];
// Ϊ����wdbi�����������(dtadmin��������),TestColumn�ڴ�ʵ�֡�����λ���ǵ�wdbi��ʵ��
bool TestColumn(int mt,const char *colname)
{
	int colct=wociGetColumnNumber(mt);
	char tmp[300];
	for(int i=0;i<colct;i++) {
		wociGetColumnName(mt,i,tmp);
		if(STRICMP(colname,tmp)==0) return true;
	}
	return false;
}

extern bool wdbi_kill_in_progress;
int CopyMySQLTable(const char *path,const char *sdn,const char *stn,
				   const char *ddn,const char *dtn)
{
	char srcf[300],dstf[300];
	// check destination directory
	sprintf(dstf,"%s%s",path,ddn);
	xmkdir(dstf);
	sprintf(srcf,"%s%s/%s%s",path,sdn,stn,".frm");
	sprintf(dstf,"%s%s/%s%s",path,ddn,dtn,".frm");
	mCopyFile(srcf,dstf);
	sprintf(srcf,"%s%s/%s%s",path,sdn,stn,".MYD");
	sprintf(dstf,"%s%s/%s%s",path,ddn,dtn,".MYD");
	mCopyFile(srcf,dstf);
	sprintf(srcf,"%s%s/%s%s",path,sdn,stn,".MYI");
	sprintf(dstf,"%s%s/%s%s",path,ddn,dtn,".MYI");
	mCopyFile(srcf,dstf);
	
	sprintf(srcf,"%s%s/%s%s",path,sdn,stn,".DTP");
	{
		FILE *fsrc=fopen(srcf,"rb");
		if(fsrc!=NULL) {
			fclose(fsrc) ;
			sprintf(dstf,"%s%s/%s%s",path,ddn,dtn,".DTP");
			mCopyFile(srcf,dstf);
		}	
	}
	return 1;
}

int MoveMySQLTable(const char *path,const char *sdn,const char *stn,
				   const char *ddn,const char *dtn)
{
	char srcf[300],dstf[300];
	// check destination directory
	sprintf(dstf,"%s%s",path,ddn);
	xmkdir(dstf);
	sprintf(srcf,"%s%s/%s%s",path,sdn,stn,".frm");
	sprintf(dstf,"%s%s/%s%s",path,ddn,dtn,".frm");
	rename(srcf,dstf);
	sprintf(srcf,"%s%s/%s%s",path,sdn,stn,".MYD");
	sprintf(dstf,"%s%s/%s%s",path,ddn,dtn,".MYD");
	rename(srcf,dstf);
	sprintf(srcf,"%s%s/%s%s",path,sdn,stn,".MYI");
	sprintf(dstf,"%s%s/%s%s",path,ddn,dtn,".MYI");
	rename(srcf,dstf);
	sprintf(srcf,"%s%s/%s%s",path,sdn,stn,".DTP");
	{
		FILE *fsrc=fopen(srcf,"rb");
		if(fsrc!=NULL) {
			fclose(fsrc) ;
			sprintf(dstf,"%s%s/%s%s",path,ddn,dtn,".DTP");
			rename(srcf,dstf);
		}
	}
	return 1;
}


long StatMySQLTable(const char *path,const char *fulltbn)
{
	char srcf[300],fn[300];
	strcpy(fn,fulltbn);
	char *psep=strstr(fn,".");
	if(psep==NULL) 
		  ThrowWith("Invalid table name format'%s',should be dbname.tbname.",fn);
	  *psep='/';
	struct stat st;
	long rt=0;
	// check destination directory
	sprintf(srcf,"%s%s%s",path,fn,".frm");
	stat(srcf,&st);
	rt+=st.st_size;
	sprintf(srcf,"%s%s%s",path,fn,".MYD");
	stat(srcf,&st);
	rt+=st.st_size;
	sprintf(srcf,"%s%s%s",path,fn,".MYI");
	stat(srcf,&st);
	rt+=st.st_size;
	return rt;
}

/*
int CreateMtFromFile(int maxrows,char *filename)
{
FILE *fp=fopen(filename,"rb");
if(!fp)
ThrowWith("CreateMt from file '%s' which could not open.",filename);
int cdlen=0,cdnum=0;
fread(&cdlen,sizeof(int),1,fp);
fread(&cdnum,sizeof(int),1,fp);
revInt(&cdnum);
revInt(&cdlen);
if(cdlen==0 || cdnum==0)
ThrowWith("Could not read columns info from file 's' !",filename);
char *pbf=new char[cdlen];
if(fread(pbf,1,cdlen,fp)!=cdlen) {
delete [] pbf;
ThrowWith("Could not read columns info from file 's' !",filename);
}
int mt=wociCreateMemTable();
wociImport(mt,NULL,0,pbf,cdnum,cdlen,maxrows,0);
delete []pbf;
return mt;
}
*/



void SysAdmin::CreateDT(int tabid) 
{
	wociSetTraceFile("cdt����Ŀ���ṹ/");
	lgprintf("����Դ��,�ع���ṹ,Ŀ�����%d.",tabid);
	int tabp=wociSearchIK(dt_table,tabid);
	
	int srcid=dt_table.GetInt("sysid",tabp);
	int srcidp=wociSearchIK(dt_srcsys,srcid);
	if(srcidp<0)
	  ThrowWith( "�Ҳ���Դϵͳ��������,��%d,Դϵͳ��%d",tabid,srcid) ;
	//����������Դ������
	AutoHandle srcsys;
	srcsys.SetHandle(BuildDBC(srcidp));
	//����Դ���ݵ��ڴ��ṹ
	AutoMt srcmt(srcsys,0);
	srcmt.SetHandle(GetSrcTableStructMt(tabp,srcsys));
	if(wociGetRowLen(srcmt)<1) 
		ThrowWith( "Դ���������,��¼����Ϊ%d",wociGetRowLen(srcmt)) ;
	char tbname[150],idxname[150];
	GetTableName(tabid,-1,tbname,idxname,TBNAME_DEST);
	int colct=srcmt.GetColumnNum();
	#define WDBI_DEFAULT_NUMLEN 16
	#define WDBI_DEFAULT_SCALE  2
	for(int ci=0;ci<colct;ci++) {
	  if(wociGetColumnType(srcmt,ci)==COLUMN_TYPE_NUM) {
	   if(wociGetColumnDataLenByPos(srcmt,ci)==WDBI_DEFAULT_NUMLEN && wociGetColumnScale(srcmt,ci)==WDBI_DEFAULT_SCALE) {
	    char sql_st[4000];
	    wociGetCreateTableSQL(srcmt,sql_st,tbname,false);
	    char *psub;
	    while(psub=strstr(sql_st,"16,2")) memcpy(psub,"????",4);
	    ThrowWith( "Դ���������,һЩ��ֵ�ֶ�ȱ����ȷ�ĳ��ȶ���,��ʹ�ø�ʽ����������.�ο��������: \n %s .",sql_st) ;
	   }
	  }
	}
		
	lgprintf("�ؽ�Ŀ���ṹ��CreateDP)��ǰ��Ҫ��ֹ�Ա�ķ���.�������ݻ������ṹ�����б仯,����ڽṹ�ؽ��������ݻָ�.");
	lgprintf("��¼������...");
	CloseTable(tabid,tbname,true);
	CreateTableOnMysql(srcmt,tbname,true);
	CreateAllIndexTable(tabid,srcmt,TBNAME_DEST,true);
	char sqlbf[300];
	sprintf(sqlbf,"update dp.dp_table set cdfileid=1 , recordlen=%d where tabid=%d",
		wociGetRowLen(srcmt), tabid);
	DoQuery(sqlbf);
	log(tabid,0,100,"�ṹ�ؽ�:�ֶ���%d,��¼����%d�ֽ�.",colct,wociGetRowLen(srcmt));
}


DataDump::DataDump(int dtdbc,int maxmem,int _blocksize):fnmt(dtdbc,300)
{
	this->dtdbc=dtdbc;
	//Create fnmt and build column structrues.
	//fnmt.FetchAll("select * from dt_middledatafile where rownum<1");
	fnmt.FetchAll("select * from dp.dp_middledatafile limit 3");
	fnmt.Wait();
	fnmt.Reset();
	indexid=0;
	memlimit=maxmem*1024*1000;
	maxblockrn=0;
	blocksize=_blocksize*1024;
}

int DataDump::BuildMiddleFilePath(int _fid) {
	int fid=_fid;
	sprintf(tmpfn,"%smddt_%d.dat",dp.tmppath[0]
		,fid);
	sprintf(tmpidxfn,"%smdidx_%d.dat",dp.tmppath[0],
		fid);
	while(true) {
		int freem=GetFreeM(tmpfn);
		if(freem<1024) {
			lgprintf("Available space on hard disk('%s') less then 1G : %dM,waitting 5 minutes for free...",tmpfn,freem);
			mSleep(300000);
		}
		else break;
	}
	return fid;
}

void DataDump::ProcBlock(int partid,AutoMt *pCur,int idx,AutoMt &blockmt,int _fid)
{
	freeinfo1("Start ProcBlock");
	int fid=BuildMiddleFilePath(_fid);
	blockmt.Reset();
	int cur=*pCur;
	char *idxcolsname=dp.idxp[idx].idxcolsname;
	int *ikptr=NULL;
	int strow=-1;
	int subrn=0;
	int blockrn=0;
	if(maxblockrn<MIN_BLOCKRN) ThrowWith("Ŀ�������ݿ��С(%d)�����ʣ�����Ϊ%d.",maxblockrn,MIN_BLOCKRN);
	if(maxblockrn>MAX_BLOCKRN) ThrowWith("Ŀ�������ݿ��С(%d)�����ʣ����ܳ���%d.",maxblockrn,MAX_BLOCKRN);
	AutoMt idxdt(0,10);
	wociCopyColumnDefine(idxdt,cur,idxcolsname);
	wociAddColumn(idxdt,"idx_blockoffset","",COLUMN_TYPE_INT,0,0);
	//	wociAddColumn(idxdt,"idx_storesize","",COLUMN_TYPE_INT,0,0);
	wociAddColumn(idxdt,"idx_startrow","",COLUMN_TYPE_INT,0,0);
	wociAddColumn(idxdt,"idx_rownum","",COLUMN_TYPE_INT,0,0);
	wociAddColumn(idxdt,"idx_fid","",COLUMN_TYPE_INT,0,0);
	int idxrnlmt=max(FIX_MAXINDEXRN/wociGetRowLen(idxdt),2);
	idxdt.SetMaxRows(idxrnlmt);
	idxdt.Build();
	freeinfo1("After Build indxdt mt");
	void *idxptr[20];
	int pidxc1[10];
	bool pkmode=false;
	sorttm.Start();
	int cn1=wociConvertColStrToInt(cur,idxcolsname,pidxc1);
	//����PKģʽ��ȫ������ͨģʽ����
	//if(cn1==1 && wociGetColumnType(cur,pidxc1[0])==COLUMN_TYPE_INT)
	//	pkmode=true;
	if(!pkmode) {
		wociSetSortColumn(cur,idxcolsname);
		wociSortHeap(cur);
	}
	else {
		wociSetIKByName(cur,idxcolsname);
		wociOrderByIK(cur);
		wociGetIntAddrByName(cur,idxcolsname,0,&ikptr);
	}
	sorttm.Stop();
	int idx_blockoffset=0,idx_store_size=0,idx_startrow=0,idx_rownum=0;
	int idxc=cn1;
	idxptr[idxc++]=&idx_blockoffset;
	//	idxptr[idxc++]=&idx_store_size;
	idxptr[idxc++]=&idx_startrow;
	idxptr[idxc++]=&idx_rownum;
	idxptr[idxc++]=&fid;
	idxptr[idxc]=NULL;
	dt_file df;
	df.Open(tmpfn,1,fid);
	idx_blockoffset=df.WriteHeader(cur);
	dt_file di;
	di.Open(tmpidxfn,1);
	di.WriteHeader(idxdt,wociGetMemtableRows(idxdt));
	idxdt.Reset();
	int totidxrn=0;
	int rn=wociGetMemtableRows(cur);
	adjtm.Start();
	for(int i=0;i<rn;i++) {
		int thisrow=pkmode?wociGetRawrnByIK(cur,i):wociGetRawrnBySort(cur,i);
		//int thisrow=wociGetRawrnByIK(cur,i);
		if(strow==-1) {
			strow=thisrow;
			idx_startrow=blockrn;
		}
		//�ӿ�ָ�
		else 
			if(pkmode?(ikptr[strow]!=ikptr[thisrow]):
			(wociCompareSortRow(cur,strow,thisrow)!=0) ){
				//if(ikptr[strow]!=ikptr[thisrow]) {
				for(int c=0;c<cn1;c++) {
					idxptr[c]=pCur->PtrVoid(pidxc1[c],strow);
				}
				idx_rownum=blockrn-idx_startrow;
				wociInsertRows(idxdt,idxptr,NULL,1);
				totidxrn++;
				int irn=wociGetMemtableRows(idxdt);
				if(irn>idxrnlmt-2) {
					int *pbo=idxdt.PtrInt("idx_blockoffset",0);
					int pos=irn-1;
					while(pos>=0 && pbo[pos]==idx_blockoffset) pos--;
					if(pos>0) {
						di.WriteMt(idxdt,COMPRESSLEVEL,pos+1,false);
						if(pos+1<irn) 
							wociCopyRowsTo(idxdt,idxdt,0,pos+1,irn-pos-1);
						else wociReset(idxdt);
					}
					else ThrowWith("����Ԥ�������,������%d,�����ֶ�'%s',�������������鳤��%d.",partid,idxcolsname,MAX_BLOCKRN);
				}
				strow=thisrow;
				idx_startrow=blockrn;
			}
			//blockmt.QuickCopyFrom(pcur,blockrn,thisrow);
			wociCopyRowsTo(cur,blockmt,-1,thisrow,1);
			blockrn++;//=wociGetMemtableRows(blockmt);
			//����ӿ�ķָ�
			if(blockrn>=maxblockrn) {
				adjtm.Stop();
				fiotm.Start();
				for(int c=0;c<cn1;c++) {
					idxptr[c]=pCur->PtrVoid(pidxc1[c],strow);
				}
				idx_rownum=blockrn-idx_startrow;
				wociInsertRows(idxdt,idxptr,NULL,1);
				totidxrn++;
				idx_blockoffset=df.WriteMt(blockmt,COMPRESSLEVEL,0,false);
				idx_startrow=0;
				strow=-1;blockrn=0;
				blockmt.Reset();
				fiotm.Stop();
				adjtm.Start();
			}
	}
	adjtm.Stop();
	//�������Ŀ�����
	if(blockrn) {
		for(int c=0;c<cn1;c++) {
			idxptr[c]=pCur->PtrVoid(pidxc1[c],strow);
		}
		idx_rownum=blockrn-idx_startrow;
		wociInsertRows(idxdt,idxptr,NULL,1);
		totidxrn++;
		idx_blockoffset=df.WriteMt(blockmt,COMPRESSLEVEL,0,false);
		idx_startrow=0;
		strow=-1;blockrn=0;
		blockmt.Reset();
	}
	//������������
	{
		di.WriteMt(idxdt,COMPRESSLEVEL,0,false);
		di.SetFileHeader(totidxrn,NULL);
	}
	//�����ļ�����
	{
		void *ptr[20];
		ptr[0]=&fid;
		ptr[1]=&partid;
		ptr[2]=&dp.tabid;
		int rn=df.GetRowNum();
		int fl=df.GetLastOffset();
		ptr[3]=&rn;ptr[4]=&fl;
		char now[10];
		wociGetCurDateTime(now);
		ptr[5]=tmpfn;ptr[6]=tmpidxfn;ptr[7]=now;
		int state=1;
		ptr[8]=&state;
		char nuldt[10];
		memset(nuldt,0,10);
		ptr[9]=now;//nuldt;
		ptr[10]=&dp.idxp[idx].idxid;
		ptr[11]=dp.idxp[idx].idxcolsname;
		ptr[12]=dp.idxp[idx].idxreusecols;
		ptr[13]=NULL;
		wociInsertRows(fnmt,ptr,NULL,1);
	}
	freeinfo1("End ProcBlock");
}


int DataDump::DoDump(SysAdmin &sp) {
	int tabid=0;
	sp.GetFirstTaskID(NEWTASK,tabid,datapartid);
	if(tabid<1) return 0;
	wociSetTraceFile("dump���ݵ���/");
	sorttm.Clear();
	fiotm.Clear();
	adjtm.Clear();
	sp.GetSoledIndexParam(datapartid,&dp,tabid);
	sp.OutTaskDesc("ִ�����ݵ�������: ",tabid,datapartid);
	if(xmkdir(dp.tmppath[0])) 
		ThrowWith("��ʱ��·���޷�����,��:%d,������:%d,·��:%s.",
		tabid,datapartid,dp.tmppath[0]);
	AutoHandle srcdbc;
	srcdbc.SetHandle(sp.BuildSrcDBC(tabid,datapartid));
	AutoMt srctstmt(0,10);
	int partoff=0;
	{	
		//�����ʽ����Ŀ���Ľṹ��һ��
		sp.BuildMtFromSrcTable(srcdbc,tabid,&srctstmt);
		srctstmt.AddrFresh();
		int srl=wociGetRowLen(srctstmt);
		char tabname[150];
		sp.GetTableName(tabid,-1,tabname,NULL,TBNAME_DEST);
		AutoMt dsttstmt(dtdbc,10);
		dsttstmt.FetchFirst("select * from dp.dp_datapart where tabid=%d and status=5 ",tabid);
		int ndmprn=dsttstmt.Wait();
		dsttstmt.FetchFirst("select * from %s",tabname);
		int tstrn=dsttstmt.Wait();
		if(srctstmt.CompareMt(dsttstmt)!=0 ) {
			if(tstrn>0 && ndmprn>0) 
				ThrowWith("��%s���Ѿ������ݣ���Դ��(��ʽ��)��ʽ���޸ģ����ܵ������ݣ��뵼���µ�(�յ�)Ŀ����С�",tabname);
			lgprintf("Դ�����ݸ�ʽ�����仯�����½���Դ��... ");
			if(tstrn==0) {
				sp.CreateDT(tabid);
				sp.Reload();
				sp.GetSoledIndexParam(datapartid,&dp,tabid);
			}
			else {
				//ȫ�����ݷ������µ�������,��������ṹ��ʱ��һ��
				//����Ŀ��������ݣ���ʱ���޸�dt_table.recordlen
				dp.rowlen=srl;
			}
		}
		else if(srl!=dp.rowlen) {
			lgprintf("Ŀ����еļ�¼���ȴ���%d�޸�Ϊ%d",dp.rowlen,srl);
			wociClear(dsttstmt);
			AutoStmt &st=dsttstmt.GetStmt();
			st.Prepare("update dp.dp_table set recordlen=%d where tabid=%d",srl,tabid);
			st.Execute(1);
			st.Wait();
		}
		if(ndmprn==0 && tstrn==0) {
			//��λ�ļ����,���Ŀ���ǿ�,���ļ���Ų���λ
			wociClear(dsttstmt);
			AutoStmt &st=dsttstmt.GetStmt();
			st.Prepare("update dp.dp_table set lstfid=0 where tabid=%d",srl,tabid);
			st.Execute(1);
			st.Wait();
		}			
	}
	
	int realrn=memlimit/dp.rowlen;
	lgprintf("��ʼ��������,���ݳ�ȡ�ڴ�%d�ֽ�(�ۺϼ�¼��:%d)",realrn*dp.rowlen,realrn);
	sp.log(tabid,datapartid,101,"��ʼ���ݵ���:���ݿ�%d�ֽ�(��¼��%d),��־�ļ� '%s'.",realrn*dp.rowlen,realrn,lgfile);
	//if(realrn>dp.maxrecnum) realrn=dp.maxrecnum;
	{
		lgprintf("����ϴε���������...");
		
		AutoMt clsmt(dtdbc,100);
		AutoStmt st(dtdbc);
		int clsrn=0;
		do {
			clsmt.FetchAll("select * from dp.dp_middledatafile where datapartid=%d and tabid=%d limit 100",datapartid,tabid);
			clsrn=clsmt.Wait();
			for(int i=0;i<clsrn;i++) {
				unlink(clsmt.PtrStr("datafilename",i));
				unlink(clsmt.PtrStr("indexfilename",i));
			}
			st.Prepare("delete from dp.dp_middledatafile where datapartid=%d and tabid=%d limit 100",datapartid,tabid);
			st.Execute(1);
			st.Wait();
		} while(clsrn>0);
	}
	//realrn=50000;
	//indexparam *ip=&dp.idxp[dp.psoledindex];
	maxblockrn=blocksize/dp.rowlen;
	{
		//�������ݵ���ʱ���ÿ��¼��,�Ժ�Ĵ���Ͳ�ѯ�Դ�Ϊ����
		//�ֶ�maxrecinblock��ʹ�÷������Ϊ:������ݺ�̨���õĲ����Զ�����,�������ֻ̨��
		lgprintf("����Ŀ�����ݿ�%d�ֽ�(��¼��:%d)",maxblockrn*dp.rowlen,maxblockrn);
		AutoStmt st(dtdbc);
		st.Prepare("update dp.dp_table set maxrecinblock=%d where tabid=%d",maxblockrn,dp.tabid);
		st.Execute(1);
		st.Wait();
	}
	sp.Reload();
	maxblockrn=sp.GetMaxBlockRn(tabid);
	AutoMt blockmt(0,maxblockrn);
	fnmt.Reset();
	//int partid=0;
	fnorder=0;
	try {
		sp.UpdateTaskStatus(DUMPING,tabid,datapartid);
	}
	catch(char *str) {
		lgprintf(str);
		return 0;
	}
	bool dumpcomplete=false;
	
	LONG64 srn=0;
	try {
		bool ec=wociIsEcho();
		wociSetEcho(TRUE);
		
		if(sp.GetDumpSQL(tabid,datapartid,dumpsql)!=-1) {
			//idxdt.Reset();
			sp.log(tabid,datapartid,131,"���ݳ�ȡsql:%s.",dumpsql);
			TradeOffMt dtmt(0,realrn);
			blockmt.Clear();
			sp.BuildMtFromSrcTable(srcdbc,tabid,&blockmt);
			//blockmt.Build(stmt);
			blockmt.AddrFresh();
			sp.BuildMtFromSrcTable(srcdbc,tabid,dtmt.Cur());
			sp.BuildMtFromSrcTable(srcdbc,tabid,dtmt.Next());
			AutoStmt stmt(srcdbc);
			stmt.Prepare(dumpsql);
			{
			 AutoMt tstmt(0,1);
			 tstmt.Build(stmt);
			 if(blockmt.CompatibleMt(tstmt)!=0 ) 
			 	ThrowWith("�������ݳ�ȡ���õ��ĸ�ʽ��Դ����ĸ�ʽ��һ��:\n%s.",dumpsql);
			}
			wociReplaceStmt(*dtmt.Cur(),stmt);
			wociReplaceStmt(*dtmt.Next(),stmt);
			dtmt.Cur()->AddrFresh();
			dtmt.Next()->AddrFresh();
			//			dtmt.Cur()->Build(stmt);
			//			dtmt.Next()->Build(stmt);
			//׼����������������������
			dtmt.FetchFirst();
			int rn=dtmt.Wait();
			srn=rn;
			while(rn>0) {
				dtmt.FetchNext();
				lgprintf("��ʼ���ݴ���");
				freeinfo1("before call prcblk");
				for(int i=0;i<dp.soledindexnum;i++) {
					ProcBlock(datapartid,dtmt.Cur(),i/*dp.psoledindex*/,blockmt,sp.NextTmpFileID());
				}
				lgprintf("���ݴ������");
				freeinfo1("after call prcblk");
				rn=dtmt.Wait();
				srn+=rn;
			}
			//delete dtmt;
			//delete stmt;
		}
		wociSetEcho(ec);
		dumpcomplete=true;
		wociAppendToDbTable(fnmt,"dp.dp_middledatafile",dtdbc,true);
		sp.UpdateTaskStatus(DUMPED,tabid,datapartid);
		
	}
	catch(...) {
		int frn=wociGetMemtableRows(fnmt);
		errprintf("���ݵ����쳣��ֹ����%d(%d),�м��ļ���:%d.",tabid,datapartid,frn);
		sp.log(tabid,datapartid,102,"���ݵ����쳣��ֹ���м��ļ���:%d.",frn);
		bool restored=false;
		if(dumpcomplete) {
			//��ǰ����ĵ�������ɣ����޸�DP����ʧ��.����10��,�����Ȼʧ��,�����.
			int retrytimes=0;
			while(retrytimes<10 &&!restored) {
				restored=true;
				try {
					wociAppendToDbTable(fnmt,"dp.dp_middledatafile",dtdbc,true);
					sp.UpdateTaskStatus(DUMPED,tabid,datapartid);
				}
				catch(...) {
					lgprintf("��%d(%d)���������,��д��dp������(dp_middledatafile)ʧ��,һ���Ӻ�����(%d)...",tabid,datapartid,++retrytimes);
					restored=false;
					mSleep(60000);
				}
			}
		}
		if(!restored) {
			int i;
			wociMTPrint(fnmt,0,NULL);
			//�����ָ�����״̬�Ĳ���,��Ϊ����״̬���˹�������Ϊ����.������ݿ�����һֱû�лָ�,
			//������״̬�ָ��������쳣,������ɾ���ļ��ͼ�¼�Ĳ������ᱻִ��,�������˹���ȷ���Ƿ�ɻָ�,��λָ�
			errprintf("�ָ�����״̬.");
		        sp.log(tabid,datapartid,103,"�ָ�����״̬.");
			sp.UpdateTaskStatus(NEWTASK,tabid,datapartid);
			errprintf("ɾ���м��ļ�...");
			for(i=0;i<frn;i++) {
				errprintf("\t %s \t %s",fnmt.PtrStr("datafilename",i),
					fnmt.PtrStr("indexfilename",i));
			}
			for(i=0;i<frn;i++) {
				unlink(fnmt.PtrStr("datafilename",i));
				unlink(fnmt.PtrStr("indexfilename",i));
			}
			errprintf("ɾ���м��ļ���¼...");
			AutoStmt st(dtdbc);
			st.Prepare("delete from dp.dp_middledatafile where tabid=%d and datapartid=%d",tabid,datapartid);
			st.Execute(1);
			st.Wait();
			throw;
		}
	}
	
	lgprintf("���ݳ�ȡ����,����״̬1-->2,tabid %d(%d)",tabid,datapartid);
	lgprintf("sort time :%11.6f file io time :%11.6f adjust data time:%11.6f",
		sorttm.GetTime(),fiotm.GetTime(),adjtm.GetTime());
	
	lgprintf("����");
	sp.log(tabid,datapartid,104,"���ݳ�ȡ���� ,��¼��%lld.",srn);
	//lgprintf("�����������...");
	//getchar();
	//MYSQL�е�MY_ISAM��֧����������MYSQL����޸Ĳ���Ҫ�ύ.
	return 0;
}

MiddleDataLoader::MiddleDataLoader(SysAdmin *_sp):
indexmt(0,0),mdindexmt(0,0),blockmt(0,0),mdf(_sp->GetDTS(),MAX_MIDDLE_FILE_NUM)
{
		  sp=_sp;
		  tmpfilenum=0;
		  pdtf=NULL;
		  pdtfid=NULL;
		  //dtfidlen=0;
}

  void StrToLower(char *str) {
  	    while(*str) *str=tolower(*str++);
	}
	
  int MiddleDataLoader::CreateLike(const char *dbn,const char *tbn,const char *nsrctbn,const char *ndsttbn,const char *taskdate)
  {
	  int tabid=0,srctabid=0;
	  AutoMt mt(sp->GetDTS(),10);
	  char tdt[21];
	  if(strcmp(taskdate,"now")==0) 
		  wociGetCurDateTime(tdt);
	  else {
		  mt.FetchAll("select adddate(cast('%s' AS DATETIME),interval 0 day) as tskdate",taskdate);
		  if(mt.Wait()!=1) 
			  ThrowWith("���ڸ�ʽ����:'%s'",taskdate);
		  memcpy(tdt,mt.PtrDate("tskdate",0),7);
		  if(wociGetYear(tdt)==0)
			  ThrowWith("���ڸ�ʽ����:'%s'",taskdate);
	  }
	  mt.FetchAll("select * from dp.dp_table where databasename=lower('%s') and tabname=lower('%s')",dbn,ndsttbn);
	  if(mt.Wait()>0) 
		  ThrowWith("�� %s.%s �Ѿ����ڡ�",dbn,ndsttbn);
	  
	  AutoMt tabmt(sp->GetDTS(),10);
	  tabmt.FetchAll("select * from dp.dp_table where databasename=lower('%s') and lower(tabname)='%s'",dbn,tbn);
	  if(tabmt.Wait()!=1) 
		  ThrowWith("�ο��� %s.%s �����ڡ�",dbn,tbn);
	  int reftabid=tabmt.GetInt("tabid",0);
	  //���Ŀ�����Ϣ
	  strcpy(tabmt.PtrStr("tabdesc",0),ndsttbn);
	  strcpy(tabmt.PtrStr("tabname",0),ndsttbn);
	  *tabmt.PtrInt("cdfileid",0)=0;
	  *tabmt.PtrDouble("recordnum",0)=0;
	  *tabmt.PtrInt("firstdatafileid",0)=0;
	  *tabmt.PtrInt("datafilenum",0)=0;
	  *tabmt.PtrDouble("totalbytes",0)=0;
	  const char *prefsrctbn=tabmt.PtrStr("srctabname",0);
	  StrToLower((char*)prefsrctbn);
	  //�ο�Դ����滹Ҫ����,��ʱ���滻
	  //strcpy(tabmt.PtrStr("srctabname",0),nsrctbn);
	  tabid=sp->NextTableID();
	  *tabmt.PtrInt("tabid",0)=tabid;
	  // ��Ӧ��Դ���Ժ��޸�
	  mt.FetchAll("select * from dp.dp_table where tabid=%d",tabid);
	  if(mt.Wait()>0)
		ThrowWith("����ظ�: ���%d��Ŀ���'%s.%s'�Ѿ�����!",mt.GetInt("tabid",0),mt.PtrStr("databasename",0),mt.PtrStr("tabname",0));
	  mt.FetchAll("select * from dp.dp_index where tabid=%d",tabid);
	  if(mt.Wait()>0)
		ThrowWith("���ֲ���ȷ����������(��%d)��",mt.GetInt("tabid",0));
	  mt.FetchAll("select * from dp.dp_datafilemap where tabid=%d and fileflag!=2",tabid);
	  if(mt.Wait()>0) 
		ThrowWith("���ֲ���ȷ�������ļ���¼(��%d)!",mt.GetInt("tabid",0));
	  mt.FetchAll("select * from dp.dp_datapart where tabid=%d",tabid);
	  if(mt.Wait()>0) 
		ThrowWith("���ֲ���ȷ�����ݷ������(��%d)!",mt.GetInt("tabid",0));

	  AutoMt indexmt(sp->GetDTS(),200);
	  indexmt.FetchAll("select * from dp.dp_index where tabid=%d",reftabid);
	  int irn=indexmt.Wait();
	  if(irn<1)
		  ThrowWith("�ο��� %s.%s û�н���������",dbn,tbn);
	  int soledct=1;
	  //���������Ϣ���ؽ����ù�ϵ
	  for(int ip=0;ip<irn;ip++) {
	  	*indexmt.PtrInt("tabid",ip)=tabid;
	  }
	  
	  AutoMt taskmt(sp->GetDTS(),500);
	  taskmt.FetchAll("select * from dp.dp_datapart where tabid=%d",reftabid);
	  int trn=taskmt.Wait();
	  if(trn<1) 
		  ThrowWith("�ο��� %s.%s û�����ݷ�����Ϣ��",dbn,tbn);
	  
	  //�����ݳ�ȡ�������Сд���е�Դ�������滻,��������������:
	  // 1. ����ֶ�������������Դ��������ͬ,������滻ʧ��
	  // 2. ���Դ�����ƴ�Сд��һ��,������滻ʧ��
	  for(int tp=0;tp<trn;tp++) {
	   char sqlbk[5000];
	   char *psql=taskmt.PtrStr("extsql",tp);
	   strcpy(sqlbk,psql);
	   if(strcmp(prefsrctbn,nsrctbn)!=0) {
		    char tmp[5000];
	      strcpy(tmp,psql);
  	    char extsql[5000];
			 	StrToLower(tmp);
		    char *sch=strstr(tmp," from ");
		    if(sch) {
		    	sch+=6;
		    	strncpy(extsql,psql,sch-tmp);
		    	extsql[sch-tmp]=0;
		    	char *sch2=strstr(sch,prefsrctbn);
		    	if(sch2) {
		        strncpy(extsql+strlen(extsql),psql+(sch-tmp),sch2-sch);
		        strcat(extsql,nsrctbn);
		        strcat(extsql,psql+(sch2-tmp)+strlen(prefsrctbn));
		        strcpy(psql,extsql);
		      }
		    }
		  }
		  if(strcmp(sqlbk,psql)==0) 
		    lgprintf("���ݷ���%d�����ݳ�ȡ���δ���޸ģ��������Ҫ�ֹ�����.",taskmt.GetInt("datapartid",tp));
		  else
		    lgprintf("���ݷ���%d�����ݳ�ȡ����Ѿ��޸ģ�\n%s\n--->\n%s\n�������Ҫ�ֹ�����.",taskmt.GetInt("datapartid",tp),sqlbk,psql);
		    
		  *taskmt.PtrInt("tabid",tp)=tabid; 
		  memcpy(taskmt.PtrDate("begintime",tp),tdt,7);
		  *taskmt.PtrInt("status",tp)=0;
		  //sprintf(taskmt.PtrStr("partdesc",tp),"%s.%s",dbn,ndsttbn);

	  }
	  strcpy(tabmt.PtrStr("srctabname",0),nsrctbn);
	  try {
		  wociAppendToDbTable(tabmt,"dp.dp_table",sp->GetDTS(),true);
		  wociAppendToDbTable(indexmt,"dp.dp_index",sp->GetDTS(),true);
		  wociAppendToDbTable(taskmt,"dp.dp_datapart",sp->GetDTS(),true);
	  }
	  catch(...) {
		  //�ָ����ݣ��������в���
		  AutoStmt st(sp->GetDTS());
		  st.DirectExecute("delete from dp.dp_table where tabid=%d",tabid);
		  st.DirectExecute("delete from dp.dp_index where tabid=%d",tabid);
		  st.DirectExecute("delete from dp.dp_datapart where tabid=%d",tabid);
		  errprintf("�����ƴ���ʱ�����ύʧ�ܣ���ɾ�����ݡ�");
		  throw;
	  }
	  {
		  char dtstr[100];
		  wociDateTimeToStr(tdt,dtstr);
		  lgprintf("�����ɹ�,Դ��'%s',Ŀ���'%s',��ʼʱ��'%s'.",nsrctbn,ndsttbn,dtstr);
	  }
	  return 0;
}

//DT data&index File Check
int MiddleDataLoader::dtfile_chk(const char *dbname,const char *tname) {
	//Check deserved temporary(middle) fileset
	//���״̬Ϊ1������
	mdf.FetchAll("select * from dp.dp_table where databasename=lower('%s') and tabname=lower('%s')",dbname,tname);
	int rn=mdf.Wait();
	if(rn<1) ThrowWith("DP�ļ����:��dp_table��'%s.%s'Ŀ����޼�¼��",dbname,tname);
	int firstfid=mdf.GetInt("firstdatafileid",0);
	int tabid=*mdf.PtrInt("tabid",0);
	int blockmaxrn=*mdf.PtrInt("maxrecinblock",0);
	double totrc=mdf.GetDouble("recordnum",0);
	sp->OutTaskDesc("Ŀ����� :",0,tabid);
	char *srcbf=new char[SRCBUFLEN];//ÿһ�δ����������ݿ飨��ѹ���󣩡�
	int errct=0;
	mdf.FetchAll("select * from dp.dp_datafilemap where tabid=%d and fileflag!=2 order by indexgid,fileid",tabid);
	int irn=mdf.Wait();
	if(irn<1) {
		ThrowWith("DP�ļ����:��dp_datafilemap��%dĿ����������ļ��ļ�¼��",tabid);
	}
	
	{
		AutoMt datmt(sp->GetDTS(),100);
		datmt.FetchAll("select * from dp.dp_datafilemap where tabid=%d and fileid=%d",tabid,firstfid);
		if(datmt.Wait()!=1) 
			ThrowWith("��ʼ�����ļ�(���%d)��ϵͳ��û�м�¼.",firstfid);
		char linkfn[300];
		strcpy(linkfn,datmt.PtrStr("filename",0));
		datmt.FetchAll("select * from dp.dp_datafilemap where tabid=%d and fileflag!=2 and isfirstindex=1 order by datapartid,fileid",tabid);
		int rn1=datmt.Wait();
		if(firstfid!=datmt.GetInt("fileid",0)) 
			ThrowWith("��ʼ�����ļ�(���%d)�����ô���Ӧ����%d..",firstfid,datmt.GetInt("fileid",0));
		int lfn=0;
		while(true) {
			file_mt dtf;
			lfn++;
			dtf.Open(linkfn,0);
			const char *fn=dtf.GetNextFileName();
			if(fn==NULL) {
				printf("%s==>����.\n",linkfn);
				break;
			}
			printf("%s==>%s\n",linkfn,fn);
			strcpy(linkfn,fn);
		}
		if(lfn!=rn1) 
			ThrowWith("�ļ����Ӵ���ȱʧ%d���ļ�.",rn1-lfn);
	}
	
	mytimer chktm;
	chktm.Start();
	try {
		int oldidxid=-1;
		for(int iid=0;iid<irn;iid++) {
			//ȡ��������
			int indexid=mdf.GetInt("indexgid",iid);
			
			AutoMt idxsubmt(sp->GetDTS(),100);
			idxsubmt.FetchAll("select * from dp.dp_index where tabid=%d and indexgid=%d ",tabid,indexid);
			rn=idxsubmt.Wait();
			if(rn<1) {
				ThrowWith("DP�ļ����:��dp.dp_index��%dĿ�����%d�����ļ�¼��",tabid,indexid);
			}
			
			printf("����ļ�%s--\n",mdf.PtrStr("filename",iid));
			fflush(stdout);
			
			//����ȫ����������
			char dtfn[300];
			dt_file idxf;
			idxf.Open(mdf.PtrStr("idxfname",iid),0);
			AutoMt idxmt(0);
			idxmt.SetHandle(idxf.CreateMt(1));
			idxmt.SetHandle(idxf.CreateMt(FIX_MAXINDEXRN/wociGetRowLen(idxmt)));
			int brn=0;//idxf.ReadMt(-1,0,idxmt,false);
			int sbrn=0;
			while( (sbrn=idxf.ReadMt(-1,0,idxmt,true))>0) brn+=sbrn;
			int thiserrct=errct;
			//�����������ļ��ĺ�׺��idx�滻Ϊdat���������ļ�.
			AutoMt destmt(0);
			strcpy(dtfn,mdf.PtrStr("idxfname",iid));
			strcpy(dtfn+strlen(dtfn)-3,"dat");
			//�������ļ�ȡ�ֶνṹ���ڴ���СΪĿ����ÿ���ݿ����������
			//destmt.SetHandle(dtf.CreateMt(blockmaxrn));
			FILE *fp=fopen(dtfn,"rb");
			if(fp==NULL)
				ThrowWith("DP�ļ����:�ļ�'%s'����.",dtfn);
			fseek(fp,0,SEEK_END);
			unsigned int flen=ftell(fp);
			fseek(fp,0,SEEK_SET);
			block_hdr *pbhdr=(block_hdr *)srcbf;
			
			int oldblockstart=-1;
			int dspct=0;
			int totct=0;
			int blockstart,blocksize;
			int rownum;
			int bcn=wociGetColumnPosByName(idxmt,"dtfid");
			int bkf=0;
			sbrn=idxf.ReadMt(0,0,idxmt,true);
			int ist=0;
			for(int i=0;i<brn;i++) {
				//ֱ��ʹ���ֶ����ƻ����idx_rownum�ֶε����Ʋ�ƥ�䣬���ڵ�idx�����ļ��е��ֶ���Ϊrownum.
				//dtfid=*idxmt.PtrInt(bcn,i);
				if(i>=sbrn) {
					ist=sbrn;
					sbrn+=idxf.ReadMt(-1,0,idxmt,true);
				}
				blockstart=*idxmt.PtrInt(bcn+1,i-ist);
				blocksize=*idxmt.PtrInt(bcn+2,i-ist);
				//blockrn=*idxmt.PtrInt(bcn+3,i);
				//startrow=*idxmt.PtrInt(bcn+4,i);
				rownum=*idxmt.PtrInt(bcn+5,i-ist);
				if(oldblockstart!=blockstart) {
					try {
						//dtf.ReadMt(blockstart,blocksize,mdf,1,1,srcbf);
						fseek(fp,blockstart,SEEK_SET);
						if(fread(srcbf,1,blocksize,fp)!=blocksize) {
							errprintf("�����ݴ���λ��:%d,����:%d,�������:%d.\n",blockstart,blocksize,i);
							throw -1;
						}
						pbhdr->ReverseByteOrder();
						if(!dt_file::CheckBlockFlag(pbhdr->blockflag))
						{
							errprintf("����Ŀ��ʶ��λ��:%d,����:%d,�������:%d.\n",blockstart,blocksize,i);
							throw -1;
						}
						if(pbhdr->blockflag!=bkf) {
							bkf=pbhdr->blockflag;
							//if(bkf==BLOCKFLAG) printf("���ݿ�����:WOCI.\n");
							//else if(bkf==MYSQLBLOCKFLAG)
							//  printf("���ݿ�����:MYISAM.\n");
							printf("���ݿ�����: %s .\n",dt_file::GetBlockTypeName(pbhdr->blockflag));
							printf("ѹ������:%d.\n",pbhdr->compressflag);
						}
						if(pbhdr->storelen!=blocksize-sizeof(block_hdr))
						{
							errprintf("����Ŀ鳤�ȣ�λ��:%d,����:%d,�������:%d.\n",blockstart,blocksize,i);
							throw -1;
						}
					}
					catch (...) {
						if(errct++>100) {
							errprintf("̫��Ĵ����ѷ�����飡");
							throw;
						}	
					}
					//int mt=dtf.ReadBlock(blockstart,0,1,true);
					//destmt.SetHandle(mt,true);
					oldblockstart=blockstart;
				}
				totct+=rownum;
				if(totct-dspct>200000) {
					printf("%d/%d    --- %d%%\r",i,brn,i*100/brn);
					fflush(stdout);
					dspct=totct;
				}
			} // end of for(...)
			if(ftell(fp)!=flen) {
				errprintf("�ļ����Ȳ�ƥ�䣬�����ļ�����:%d,�����ļ�ָʾ�Ľ���λ��:%d\n",flen,ftell(fp));
				errct++;
			}
			printf("�ļ������ϣ������� ��%d.    \n",errct-thiserrct);
			fclose(fp);
		}// end of for
	} // end of try
	catch (...) {
		errprintf("DP�ļ��������쳣��tabid:%d.",tabid);
	}
	delete []srcbf;
	if(errct>0)
		errprintf("DP�ļ������ִ��󣬿�����Ҫʹ���칹�ؽ���ʽ��������.");
	printf("\n");
	chktm.Stop();
	lgprintf("DP�ļ�������,������%d���ļ�,����%d������(%.2fs).",irn,errct,chktm.GetTime());
	return 1;
  }
  
  
  void MiddleDataLoader::CheckEmptyDump() {
	  mdf.FetchAll("select * from dp.dp_datapart where status=2 limit 100");
	  int rn=mdf.Wait();
	  if(rn>0) {
		  while(--rn>=0) {
			  AutoMt tmt(sp->GetDTS(),10);
			  tmt.FetchAll("select * from dp.dp_middledatafile where tabid=%d and datapartid=%d and procstate!=3 limit 10",
			      mdf.GetInt("tabid",rn),mdf.GetInt("datapartid",rn));
			  if(tmt.Wait()<1) {
				  AutoStmt st(sp->GetDTS());
				  st.Prepare(" update dp.dp_datapart set status=3 where tabid=%d and datapartid=%d",
			      		mdf.GetInt("tabid",rn),mdf.GetInt("datapartid",rn));
				  st.Execute(1);
				  st.Wait();
				  lgprintf("��%d(%d)������Ϊ�գ��޸�Ϊ������",mdf.GetInt("tabid",rn),mdf.GetInt("datapartid",rn));
			  }
		  }
	  }
  }
  // MAXINDEXRN ȱʡֵ500MB,LOADIDXSIZEȱʡֵ1600MB
  // MAXINDEXRN Ϊ���������ļ�,��¼���ȶ�Ӧ������.
  // LOADIDXSIZE �洢��ʱ����,��¼���ȴ����ݵ���ʱ�������ļ����ڴ�����.
  
  //2005/08/24�޸ģ� MAXINDEXRN����ʹ��
  //2005/11/01�޸ģ� װ��״̬��Ϊ1(��װ�룩��2����װ��),3����װ��);ȡ��10״̬(ԭ�������ֵ�һ�κ͵ڶ���װ��)
  //2005/11/01�޸ģ� ��������װ���ڴ�ʱ�Ĳ��Ը�Ϊ�������ֶΡ�
  //2006/02/15�޸ģ� һ��װ�������������ݣ���Ϊ�ֶ�װ��
  //			һ��ֻװ��ÿ���ļ��е�һ���ڴ��(��ѹ����1M,������¼��50�ֽڳ�,����Լ2������¼,1G�ڴ��������1000/(1*1.2)=800�������ļ�)
  int MiddleDataLoader::Load(int MAXINDEXRN,int LOADTIDXSIZE,bool useOldBlock) {
	  //Check deserved temporary(middle) fileset
	  //���״̬Ϊ1������1Ϊ��ȡ�����ȴ�װ��.
	  CheckEmptyDump();
	  mdf.FetchAll("select * from dp.dp_middledatafile where procstate<=1 order by tabid,datapartid,indexgid limit 100");
	  int rn=mdf.Wait();
	  //�����������LOADIDXSIZE,��Ҫ��������,��ֻ�ڴ����һ������ʱ���DT_DATAFILEMAP/DT_INDEXFILEMAP��
	  //���ֵ�һ�κ͵ڶ���װ������壺���һ�������Ӽ���ָ�����ݼ�-������(datapartid)->����)���������ڿ�ʼװ����ǰ��Ҫ
	  //  ��ɾ���ϴ�����װ������ݣ�ע�⣺�����ߵ����ݲ�������ɾ��������װ�������ɾ��).
	  bool firstbatch=true;
	  if(rn<1) return 0;
	  
	  //���������Ӽ��Ƿ��һ��װ��
	  int tabid=mdf.GetInt("tabid",0);
	  int indexid=mdf.GetInt("indexgid",0);
	  int datapartid=mdf.GetInt("datapartid",0);
	  mdf.FetchAll("select procstate from dp.dp_middledatafile where tabid=%d and datapartid=%d and indexgid=%d and procstate>1 limit 10",
		  tabid,datapartid,indexid);
	  firstbatch=mdf.Wait()<1;//�����Ӽ�û���������������ļ�¼��
	  
	  //ȡ���м��ļ���¼
	  mdf.FetchAll("select * from dp.dp_middledatafile where  tabid=%d and datapartid=%d and indexgid=%d and procstate<=1 order by mdfid limit %d",
		  tabid,datapartid,indexid,MAX_MIDDLE_FILE_NUM);
	  rn=mdf.Wait();
	  if(rn<1) 
		  ThrowWith("MiddleDataLoader::Load : ȷ�������Ӽ����Ҳ����м����ݼ�¼(δ����)��");
	  
	  
	  //ȡ��������
	  int idxtlimit=0,idxdlimit=0;//��ʱ��(�����ȡ�����ļ���Ӧ)��Ŀ����(����Ŀ�������ļ���Ӧ)����������¼��.
	  wociSetTraceFile("dr��������/");
	  tabid=mdf.GetInt("tabid",0);
	  indexid=mdf.GetInt("indexgid",0);
	  datapartid=mdf.GetInt("datapartid",0);
	  int compflag=5;
	  bool col_object=false; // useOldBlock=trueʱ������col_object
	  //ȡѹ�����ͺ����ݿ�����
	  {
		  AutoMt tmt(sp->GetDTS(),10);
		  tmt.FetchAll("select compflag from dp.dp_datapart where tabid=%d and datapartid=%d",tabid,datapartid);
		  if(tmt.Wait()>0)
			compflag=tmt.GetInt("compflag",0);
		  tmt.FetchAll("select * from dp.dp_table where tabid=%d",tabid);
		  if(tmt.Wait()>0) {
			  if(TestColumn(tmt,"blocktype")) 
				  col_object=tmt.GetInt("blocktype",0)&1; // bit 1 means column object block type.
		  }
	  }
	  
	  //��dt_datafilemap(��blockmt�ļ���)��dt_indexfilemap(��indexmt�ļ���)
	  //�����ڴ��ṹ
	  char fn[300];
	  AutoMt fnmt(sp->GetDTS(),MAX_DST_DATAFILENUM);
	  
	  fnmt.FetchAll("select * from dp.dp_datafilemap limit 2");
	  fnmt.Wait();
	  wociReset(fnmt);
	  LONG64 dispct=0,lstdispct=0,llt=0;
	  sp->OutTaskDesc("�������� :",tabid,datapartid,indexid);
	  sp->log(tabid,datapartid,105,"��������,������%d,��־�ļ� '%s' .",indexid,lgfile);
	  int start_mdfid=0,end_mdfid=0;
	  char sqlbf[200];
	  LONG64 extrn=0,lmtextrn=-1;
	  LONG64 adjrn=0;
	  try {	
		  tmpfilenum=rn;
		  //���������ļ��������ۼ�����������
		  LONG64 idxrn=0;
		  int i;
		  int mdrowlen=0;
		  //ȡ������¼�ĳ���(��ʱ�������ݼ�¼)
		  {
			  dt_file df;
			  df.Open(mdf.PtrStr("indexfilename",0),0);
			  mdrowlen=df.GetRowLen();
		  }
		  
		  lgprintf("��ʱ�������ݼ�¼����:%d",mdrowlen);  
		  int lmtrn=-1,lmtfn=-1;
		  //�����ʱ�������ڴ�����,�жϵ�ǰ�Ĳ��������Ƿ����һ��װ��ȫ��������¼
		  for( i=0;i<rn;i++) {
			  dt_file df;
			  df.Open(mdf.PtrStr("indexfilename",i),0);
			  if(mdrowlen==0) 
				  mdrowlen=df.GetRowLen();
			  extrn+=mdf.GetInt("recordnum",i);
			  idxrn+=df.GetRowNum();
			  llt=idxrn;
			  llt*=mdrowlen;

			  llt/=(1024*1024); //-->(MB)
			  if(llt>LOADTIDXSIZE && lmtrn==-1) { //ʹ�õ���ʱ���������ڴ�������������ޣ���Ҫ���
				  if(i==0) 
					  ThrowWith("MLoader:�ڴ����DP_LOADTIDXSIZE����̫��:%dMB��\n"
					  "������װ������һ����ʱ��ȡ��:%dMB��\n",LOADTIDXSIZE,(int)llt);
				  lmtrn=idxrn-df.GetRowNum();
				  lmtfn=i;
				  lmtextrn=extrn-mdf.GetInt("recordnum",i);
			  }
		  }
		  if(lmtrn!=-1) { //ʹ�õ���ʱ���������ڴ�������������ޣ���Ҫ���
			  lgprintf("MLoader:�����������ڴ�����%dMB,��Ҫ�����ļ���%d,������%d,��ʼ��:%d,������:%d,�ļ���:%d .",LOADTIDXSIZE,rn,datapartid,mdf.GetInt("mdfid",0),mdf.GetInt("mdfid",lmtfn-1),lmtfn);
			  lgprintf("������Ҫ�ڴ�%dM ",idxrn*mdrowlen/(1024*1024));
			  sp->log(tabid,datapartid,106,"MLoader:�����������ڴ�����%dMB,��Ҫ�����ļ���%d,��ʼ��:%d,������:%d,�ļ���:%d ,��Ҫ�ڴ�%dMB.",LOADTIDXSIZE,rn,mdf.GetInt("mdfid",0),mdf.GetInt("mdfid",lmtfn-1),lmtfn,idxrn*mdrowlen/(1024*1024));
			  idxrn=lmtrn;
			  //fix a bug
			  rn=lmtfn;
		  }
		  if(lmtextrn==-1) lmtextrn=extrn;
		  lgprintf("��������ʵ��ʹ���ڴ�%dM,�����п�%d.",idxrn*mdrowlen/(1024*1024),mdrowlen);
		  start_mdfid=mdf.GetInt("mdfid",0);
		  end_mdfid=mdf.GetInt("mdfid",rn-1);
		  lgprintf("������¼��:%d",idxrn);
		  //Ϊ��ֹ��������,�м��ļ�״̬�޸�.
		  lgprintf("�޸��м��ļ��Ĵ���״̬(tabid:%d,datapartid:%d,indexid:%d,%d���ļ�)��1-->2",tabid,datapartid,indexid,rn);
		  sprintf(sqlbf,"update dp.dp_middledatafile set procstate=2 where tabid=%d  and datapartid=%d and indexgid=%d and procstate<=1 and mdfid>=%d and mdfid<=%d ",
			  tabid,datapartid,indexid,start_mdfid,end_mdfid);
		  int ern=sp->DoQuery(sqlbf);
		  if(ern!=rn) {
			  if(ern>0) {  //�����UPdate����޸���һ���ּ�¼״̬,�Ҳ��ɻָ�,��Ҫ�������Ӽ�������װ��.
				  ThrowWith("MLoader�޸��м��ļ��Ĵ���״̬�쳣�����������������̳�ͻ��\n"
					  " �����ļ��Ĵ���״̬��һ�£�������ֹͣ���е��������������������������������\n"
					  "  tabid:%d,datapartid:%d,indexid:%d.\n",
					  tabid,datapartid,indexid);
			  }
			  else //�����update���δ����ʵ���޸Ĳ���,�������̿��Լ�������.
				  ThrowWith("MLoader�޸��м��ļ��Ĵ���״̬�쳣�����������������̳�ͻ��\n"
					  "  tabid:%d,datapartid:%d,indexid:%d.\n",
					  tabid,datapartid,indexid);
		  }
		  
		  	//ThrowWith("������ֹ---%d������.",dispct);
	if(firstbatch) {
		lgprintf("ɾ�����ݷ���%d,����%d �����ݺ�������¼(��:%d)...",datapartid,indexid,tabid);
		sp->log(tabid,datapartid,107,"ɾ��������%d��ȥ���ɵ����ݺ�������¼...",indexid);
		AutoMt dfnmt(sp->GetDTS(),MAX_DST_DATAFILENUM);
		dfnmt.FetchAll("select * from dp.dp_datafilemap where tabid=%d and datapartid=%d and indexgid=%d and fileflag=1",tabid,datapartid,indexid);
		int dfn=dfnmt.Wait();
		if(dfn>0) {
			AutoStmt st(sp->GetDTS());
			for(int di=0;di<dfn;di++)
			{
				lgprintf("ɾ��'%s'/'%s'�͸��ӵ�depcp,dep5�ļ�",dfnmt.PtrStr("filename",di),dfnmt.PtrStr("idxfname",di));
				unlink(dfnmt.PtrStr("filename",di));
				unlink(dfnmt.PtrStr("idxfname",di));
				char tmp[300];
				sprintf(tmp,"%s.depcp",dfnmt.PtrStr("filename",di));
				unlink(tmp);
				sprintf(tmp,"%s.dep5",dfnmt.PtrStr("filename",di));
				unlink(tmp);
				sprintf(tmp,"%s.depcp",dfnmt.PtrStr("idxfname",di));
				unlink(tmp);
				sprintf(tmp,"%s.dep5",dfnmt.PtrStr("idxfname",di));
				unlink(tmp);
			}
			st.Prepare(" delete from dp.dp_datafilemap where tabid=%d and datapartid=%d and indexgid=%d and fileflag=1",tabid,datapartid,indexid);
			st.Execute(1);
			st.Wait();
		}
	}

		  //�����м�����(�м��ļ����ݿ�����)�ڴ��mdindexmt��Ŀ�����ݿ��ڴ��blockmt
		  int maxblockrn=sp->GetMaxBlockRn(tabid);
		  {
			  dt_file idf;
			  idf.Open(mdf.PtrStr("indexfilename",0),0);
			  mdindexmt.SetHandle(idf.CreateMt(idxrn));
			  //wociAddColumn(idxmt,"dtfileid",NULL,COLUMN_TYPE_INT,4,0);
			  //idxmt.SetMaxRows(idxrn);
			  //mdindexmt.Build();
			  idf.Open(mdf.PtrStr("datafilename",0),0);
			  blockmt.SetHandle(idf.CreateMt(maxblockrn));
			  //mdblockmt.SetHandle(idf.CreateMt(maxblockrn));
		  }
		  LONG64 crn=0;
		  //	wociGetIntAddrByName(idxmt,"dtfileid",0,&pdtfid);
		  // pdtfidΪһ���ַ����飬ƫ��Ϊx��ֵ��ʾ�м������ڴ���x�е��ļ����(Base0);
		  if(pdtfid)
			  delete [] pdtfid;
		  pdtfid=new unsigned short [idxrn];
		  //dtfidlen=idxrn;
		  //pdtfΪfile_mt��������顣��������ļ�����
		  if(pdtf) delete [] pdtf;
		  pdtf=new file_mt[rn];
		  //mdindexmt.SetMaxRows(idxrn);
		  //����ȫ���������ݵ�mdindexmt(�м������ڴ��),����ȫ�������ļ�
		  //pdtfidָ���Ӧ���ļ���š�
		  lgprintf("����������...");
		  for(i=0;i<rn;i++) {
			  dt_file df; 
			  df.Open(mdf.PtrStr("indexfilename",i),0);
			  int brn=0;
			  int sbrn=0;
			  while( (sbrn=df.ReadMt(-1,0,mdindexmt,false))>0) brn+=sbrn;
			  
			  for(int j=crn;j<crn+brn;j++)
				  pdtfid[j]=(unsigned short )i;
			  crn+=brn;
			  
			  //pdtf[i].SetParalMode(true);
			  pdtf[i].Open(mdf.PtrStr("datafilename",i),0);
			  //		if(crn>10000000) break; ///DEBUG
		  }
		  lgprintf("��������:%d.",crn);
		  if(crn!=idxrn) {
		  	sp->log(tabid,0,108,"���������ļ����ܼ�¼��%lld,��ָʾ��Ϣ��һ��:%lld",crn,idxrn);
		  	ThrowWith("���������ļ����ܼ�¼��%lld,��ָʾ��Ϣ��һ��:%lld",crn,idxrn);
		  }
		  //��mdindexmt(�м������ڴ��)������
		  //���������漰�ڴ����������У������½���¼˳�����ˣ�
		  // pdtfid��Ϊ�ڴ����ĵ�Ч�ڴ���ֶΣ�����������
		  lgprintf("����('%s')...",mdf.PtrStr("soledindexcols",0));
		  {
			  char sort[300];
			  sprintf(sort,"%s,idx_fid,idx_blockoffset",mdf.PtrStr("soledindexcols",0));
			  wociSetSortColumn(mdindexmt,sort);
			  wociSortHeap(mdindexmt);
		  }
		  lgprintf("�������.");
		  //ȡ��ȫ�����������ṹ
		  sp->GetSoledIndexParam(datapartid,&dp,tabid);
		  //�����Ҫ������м������Ƿ�ʹ������������������ǣ�isfirstidx=1.
		  int isfirstidx=0;
		  indexparam *ip;
		  {
			  int idxp=dp.GetOffset(indexid);
			  ip=&dp.idxp[idxp];
			  if(idxp==dp.psoledindex) isfirstidx=1;
		  }
		  //�ӽṹ�����ļ�����indexmt,indexmt��Ŀ�������ڴ���ǽ���Ŀ�������������Դ��
		  //indexmt.SetHandle(CreateMtFromFile(MAXINDEXRN,ip->cdsfilename));
		  int pblockc[20];
		  char colsname[500];
		  void *indexptr[40];
		  indexmt.SetMaxRows(1);
		  int stcn=sp->CreateIndexMT(indexmt,blockmt,tabid,indexid,pblockc,colsname,true),bcn=stcn;
		  llt=FIX_MAXINDEXRN;
		  llt/=wociGetRowLen(indexmt); //==> to rownum;
		  idxdlimit=(int)llt;
		  indexmt.SetMaxRows(idxdlimit);
		  sp->CreateIndexMT(indexmt,blockmt,tabid,indexid,pblockc,colsname,true);
		  bool pkmode=false;
		  //ȡ����������mdindexmt(�м��������)�ṹ�е�λ�á�
		  //���ö�indexmt�����¼��Ҫ�Ľṹ�ͱ�����
		  int pidxc1[20];
		  int cn1=wociConvertColStrToInt(mdindexmt,ip->idxcolsname,pidxc1);
		  int dtfid,blockstart,blocksize,blockrn=0,startrow,rownum;
		  indexptr[stcn++]=&dtfid;
		  indexptr[stcn++]=&blockstart;
		  indexptr[stcn++]=&blocksize;
		  indexptr[stcn++]=&blockrn;
		  indexptr[stcn++]=&startrow;
		  indexptr[stcn++]=&rownum;
		  indexptr[stcn]=NULL;
		  //indexmt�е�blocksize,blockstart?,blockrownum��Ҫ�ͺ�д�룬
		  //�����Ҫȡ����Щ�ֶε��׵�ַ��
		  int *pblocksize;
		  int *pblockstart;
		  int *pblockrn;
		  wociGetIntAddrByName(indexmt,"blocksize",0,&pblocksize);
		  wociGetIntAddrByName(indexmt,"blockstart",0,&pblockstart);
		  wociGetIntAddrByName(indexmt,"blockrownum",0,&pblockrn);
		  //mdindexmt�������ֶ��Ƕ��м������ļ��Ĺؼ��
		  int *poffset,*pstartrow,*prownum;
		  wociGetIntAddrByName(mdindexmt,"idx_blockoffset",0,&poffset);
		  wociGetIntAddrByName(mdindexmt,"idx_startrow",0,&pstartrow);
		  wociGetIntAddrByName(mdindexmt,"idx_rownum",0,&prownum);
		  
		  //indexmt ��¼����������λ
		  int indexmtrn=0;
		  
		  //����Ŀ�������ļ���Ŀ�������ļ�����(dt_file).
		  // Ŀ�������ļ���Ŀ�������ļ�һһ��Ӧ��Ŀ�������ļ��а��ӿ鷽ʽ�洢�ڴ��
		  //  Ŀ�������ļ���Ϊһ����һ���ڴ���ļ�ͷ�ṹ��д���ڴ��ʱ����
		  dtfid=sp->NextDstFileID(tabid);
		  char tbname[150];
		  sp->GetTableName(tabid,-1,tbname,NULL,TBNAME_DEST);
		  dp.usingpathid=0;
		  sprintf(fn,"%s%s_%d_%d_%d.dat",dp.dstpath[0],tbname,datapartid,indexid,dtfid);
		  {
		  	FILE *fp;
		  	fp=fopen(fn,"rb");
		  	if(fp!=NULL) {
		  		fclose(fp);
		  		ThrowWith("�ļ�'%s'�Ѿ����ڣ����ܼ����������ݡ�",fn);
		  	}
		  }
		  dt_file dstfile;
		  dstfile.Open(fn,1);
		  blockstart=dstfile.WriteHeader(blockmt,0,dtfid);
		  char idxfn[300];
		  sprintf(idxfn,"%s%s_%d_%d_%d.idx",dp.dstpath[0],tbname,datapartid,indexid,dtfid);
		  dt_file idxf;
		  idxf.Open(idxfn,1);
		  idxf.WriteHeader(indexmt,0,dtfid);
		  
		  startrow=0;
		  rownum=0;
		  blockrn=0;
		  int subtotrn=0;
		  int idxtotrn=0;
		  lgprintf("��ʼ���ݴ���(MiddleDataLoading)....");
		  
		  /*******����Sort˳�����mdindexmt(�м������ڴ��)***********/
		  //
		  //
		  lgprintf("�����ļ�,���:%d...",dtfid);
		  int firstrn=wociGetRawrnBySort(mdindexmt,0);
		  mytimer arrtm;
		  arrtm.Start();
		  for(i=0;i<idxrn;i++) {
			  int thisrn=wociGetRawrnBySort(mdindexmt,i);
			  int rfid=pdtfid[thisrn];
			  int sbrn=prownum[thisrn];
			  int sbstart=pstartrow[thisrn];
			  int sameval=mdindexmt.CompareRows(firstrn,thisrn,pidxc1,cn1);
			  if(sameval!=0) {
				  //Ҫ��������ݿ���ϴε����ݿ鲻��һ���ؼ��֣�
				  // ���ԣ��ȱ����ϴεĹؼ����������ؼ��ֵ�ֵ��blockmt����ȡ��
				  // startrow����ʼ�ձ����һ��δ�洢�ؼ������������ݿ鿪ʼ�кš�
				  int c;
				  for(c=0;c<bcn;c++) {
					  indexptr[c]=blockmt.PtrVoid(pblockc[c],startrow);
				  }
				  if(rownum>0) {
					  wociInsertRows(indexmt,indexptr,NULL,1);
					  idxtotrn++;
				  }
				  firstrn=thisrn;
				  //�������startrow��ʱָ����Ч�е����(����δ���).
				  startrow=blockrn;
				  rownum=0;
			  }
			  //�������ļ��ж������ݿ�
			  int mt=pdtf[rfid].ReadBlock(poffset[thisrn],0,true);
			  //2005/08/24 �޸ģ� ���������ļ�Ϊ�������˳��洢
			  
			  //���ڽ�����ǰ�ļ��п��ܻ�Ҫ����һ������,����ж��ڴ����������Ϊ(idxdlimit-1)
			  int irn=wociGetMemtableRows(indexmt);
			  if(irn>=(idxdlimit-2))
			  {
				  int pos=irn-1;
				  //���blocksize��blockrn���ֶλ�δ���ã������
				  while(pos>=0 && pblockstart[pos]==blockstart) pos--;
				  if(pos>0) {
					  //�����Ѿ�������������������,false������ʾ����Ҫɾ��λͼ��.
					  idxf.WriteMt(indexmt,COMPRESSLEVEL,pos+1,false);
					  if(pos+1<irn) 
						  wociCopyRowsTo(indexmt,indexmt,0,pos+1,irn-pos-1);
					  else wociReset(indexmt);
				  }
				  else 
				  ThrowWith("Ŀ���%d,������%d,װ��ʱ�����������һ�������¼��%d",tabid,indexid,idxdlimit);
			  }
			  //���ݿ��� 
			  //������ݿ��Ƿ���Ҫ���
			  // ����һ��ѭ�������ڴ���mt�е�sbrn�������maxblockrn���쳣�����
			  //	�����쳣ԭ������ʱ����������������ݿ飬�������ۺ�����ʱ������ֲ���һ������������ʱ��¼
			  while(true) {
				  if(blockrn+sbrn>maxblockrn ) {
					  //ÿ�����ݿ�������Ҫ�ﵽ���ֵ��80%��
					  if(blockrn<maxblockrn*.8 ) {
						  //�������80%���ѵ�ǰ��������ݿ���
						  int rmrn=maxblockrn-blockrn;
						  wociCopyRowsTo(mt,blockmt,-1,sbstart,rmrn);
						  rownum+=rmrn;
						  sbrn-=rmrn;
						  sbstart+=rmrn;
						  blockrn+=rmrn;
					  }
					  
					  //���������
					  if(useOldBlock) 
						  blocksize=dstfile.WriteMt(blockmt,compflag,0,false)-blockstart;
					  else if(col_object)
						  blocksize=dstfile.WriteMySQLMt(blockmt,compflag)-blockstart;
					  else 
						  blocksize=dstfile.WriteMySQLMt(blockmt,compflag,false)-blockstart;
					  adjrn+=wociGetMemtableRows(blockmt);
					  //�����ӿ�����
					  if(startrow<blockrn) {
						  int c;
						  for(c=0;c<bcn;c++) {
							  indexptr[c]=blockmt.PtrVoid(pblockc[c],startrow);
						  }
						  if(rownum>0) {
							  wociInsertRows(indexmt,indexptr,NULL,1);
							  dispct++;
							  idxtotrn++;
						  }
					  }
					  int irn=wociGetMemtableRows(indexmt);
					  int irn1=irn;
					  while(--irn>=0) {
						  if(pblockstart[irn]==blockstart) {
							  pblocksize[irn]=blocksize;
							  pblockrn[irn]=blockrn;
						  }
						  else break;
					  }
					  
					  blockstart+=blocksize;
					  subtotrn+=blockrn;
					  //�����ļ����ȳ���2Gʱ���
					  if(blockstart>2000000000 ) {
						  //�����ļ����ձ��¼(dt_datafilemap)
						  {
							  void *fnmtptr[20];
  							  int idxfsize=idxf.WriteMt(indexmt,COMPRESSLEVEL,0,false);
							  fnmtptr[0]=&dtfid;
							  fnmtptr[1]=fn;
							  fnmtptr[2]=&datapartid;
							  fnmtptr[3]=&dp.dstpathid[dp.usingpathid];
							  fnmtptr[4]=&dp.tabid;
							  fnmtptr[5]=&ip->idxid;
							  fnmtptr[6]=&isfirstidx;
							  fnmtptr[7]=&blockstart;
							  fnmtptr[8]=&subtotrn;
							  int procstatus=0;
							  fnmtptr[9]=&procstatus;
							  //int compflag=COMPRESSLEVEL;
							  fnmtptr[10]=&compflag;
							  int fileflag=1;
							  fnmtptr[11]=&fileflag;
							  fnmtptr[12]=idxfn;
							  fnmtptr[13]=&idxfsize;
							  fnmtptr[14]=&idxtotrn;
							  fnmtptr[15]=NULL;
							  wociInsertRows(fnmt,fnmtptr,NULL,1);
						  }
						  //
						  dtfid=sp->NextDstFileID(tabid);
		  				  sprintf(fn,"%s%s_%d_%d_%d.dat",dp.dstpath[0],tbname,datapartid,indexid,dtfid);
		  				  {
		  					FILE *fp;
		  					fp=fopen(fn,"rb");
		  					if(fp!=NULL) {
		  						fclose(fp);
		  						ThrowWith("�ļ�'%s'�Ѿ����ڣ����ܼ����������ݡ�",fn);
		  					}
		  				  }
						  dstfile.SetFileHeader(subtotrn,fn);
						  dstfile.Open(fn,1);
						  blockstart=dstfile.WriteHeader(blockmt,0,dtfid);
						  printf("\r                                                                            \r");
						  lgprintf("�����ļ�,���:%d...",dtfid);
		  				  sprintf(idxfn,"%s%s_%d_%d_%d.idx",dp.dstpath[0],tbname,datapartid,indexid,dtfid);
						  
						  idxf.SetFileHeader(idxtotrn,idxfn);
						  // create another file
						  idxf.Open(idxfn,1);
						  idxf.WriteHeader(indexmt,0,dtfid);
						  indexmt.Reset();
						  subtotrn=0;
						  blockrn=0;
						  idxtotrn=0;
						  //lgprintf("�����ļ�,���:%d...",dtfid);
						  
					  } // end of IF blockstart>2000000000)
					  blockmt.Reset();
					  blockrn=0;
					  firstrn=thisrn;
					  startrow=blockrn;
					  rownum=0;
					  dispct++;
				    if(wdbi_kill_in_progress) {
				    	wdbi_kill_in_progress=false;
					  	ThrowWith("�û�ȡ������!");
					  }
					  if(dispct-lstdispct>=200) {
						  lstdispct=dispct;
						  arrtm.Stop();
						  double tm1=arrtm.GetTime();
						  arrtm.Start();
						  printf("  ������%lld���ݿ�,��ʱ%.0f��,Ԥ�ƻ���Ҫ%.0f��          .\r",dispct,tm1,(tm1*(idxrn-i))/i);
						  fflush(stdout);
					  }
			} //end of blockrn+sbrn>maxblockrn
			if(blockrn+sbrn>maxblockrn) {
				int rmrn=maxblockrn-blockrn;
				wociCopyRowsTo(mt,blockmt,-1,sbstart,rmrn);
				rownum+=rmrn;
				sbrn-=rmrn;
				sbstart+=rmrn;
				blockrn+=rmrn;
			}
			else {
				wociCopyRowsTo(mt,blockmt,-1,sbstart,sbrn);
				rownum+=sbrn;
				blockrn+=sbrn;
				break;
			}
		} // end of while(true)
	} // end of for(...)
	if(blockrn>0) {
		
		//�����ӿ�����
		int c;
		for( c=0;c<bcn;c++) {
			indexptr[c]=blockmt.PtrVoid(pblockc[c],startrow);
		}
		//for(c=0;c<bcn2;c++) {
		//	indexptr[bcn1+c]=blockmt.PtrVoid(pblockc2[c],startrow);
		//}
		if(rownum>0) {
			wociInsertRows(indexmt,indexptr,NULL,1);
			dispct++;
		}
		//���������
		//���������
		if(useOldBlock)
			blocksize=dstfile.WriteMt(blockmt,compflag,0,false)-blockstart;
		else if(col_object)
			blocksize=dstfile.WriteMySQLMt(blockmt,compflag)-blockstart;
		else 
			blocksize=dstfile.WriteMySQLMt(blockmt,compflag,false)-blockstart;
		int irn=wociGetMemtableRows(indexmt);
		adjrn+=wociGetMemtableRows(blockmt);
		while(--irn>=0) {
			if(pblockstart[irn]==blockstart) {
				pblocksize[irn]=blocksize;
				pblockrn[irn]=blockrn;
			}
			else break;
		}
		blockstart+=blocksize;
		subtotrn+=blockrn;
		//�����ļ����ձ��¼(dt_datafilemap)
		{
			void *fnmtptr[20];
			int idxfsize=idxf.WriteMt(indexmt,COMPRESSLEVEL,0,false);
			fnmtptr[0]=&dtfid;
			fnmtptr[1]=fn;
			fnmtptr[2]=&datapartid;
			fnmtptr[3]=&dp.dstpathid[dp.usingpathid];
			fnmtptr[4]=&dp.tabid;
			fnmtptr[5]=&ip->idxid;
			fnmtptr[6]=&isfirstidx;
			fnmtptr[7]=&blockstart;
			fnmtptr[8]=&subtotrn;
			int procstatus=0;
			fnmtptr[9]=&procstatus;
			//int compflag=COMPRESSLEVEL;
			fnmtptr[10]=&compflag;
			int fileflag=1;
			fnmtptr[11]=&fileflag;
		        fnmtptr[12]=idxfn;
			fnmtptr[13]=&idxfsize;
			fnmtptr[14]=&idxtotrn;
			fnmtptr[15]=NULL;
			wociInsertRows(fnmt,fnmtptr,NULL,1);
		}
		
		//
		dstfile.SetFileHeader(subtotrn,NULL);
		idxf.SetFileHeader(idxtotrn,NULL);
		indexmt.Reset();
		blockmt.Reset();
		blockrn=0;
		startrow=blockrn;
		rownum=0;
	}
	
	//��¼��У�顣�����У����Ǳ����������飬�п�����һ���������ĳ���������һ���֡������У������һ�������飬У������������
	if(adjrn!=lmtextrn) {
		sp->log(tabid,datapartid,109,"��������Ҫ����������%lld�У���ʵ������%lld��! ������%d.",lmtextrn,adjrn,indexid);
		ThrowWith("��������Ҫ����������%lld�У���ʵ������%lld��! ��%d(%d),������%d.",lmtextrn,adjrn,tabid,datapartid,indexid);
	}
	wociAppendToDbTable(fnmt,"dp.dp_datafilemap",sp->GetDTS(),true);
	lgprintf("�޸��м��ļ��Ĵ���״̬(��%d,����%d,������:%d,%d���ļ�)��2-->3",tabid,indexid,datapartid,rn);
	sprintf(sqlbf,"update dp.dp_middledatafile set procstate=3 where tabid=%d and datapartid=%d and indexgid=%d and procstate=%d",tabid,datapartid,indexid,2);
	sp->DoQuery(sqlbf);
	sp->log(tabid,datapartid,110,"��������,������:%d,��¼��%lld.",indexid,lmtextrn);
	}
	catch (...) {
		int frn=wociGetMemtableRows(fnmt);
		errprintf("������������쳣����:%d,������:%d.",tabid,datapartid);
		sp->log(tabid,datapartid,111,"������������쳣");
		errprintf("�ָ��м��ļ��Ĵ���״̬(������:%d,%d���ļ�)��2-->1",datapartid,rn);
		sprintf(sqlbf,"update dp.dp_middledatafile set procstate=1 where tabid=%d and datapartid=%d and indexgid=%d and mdfid>=%d and mdfid<=%d",tabid,datapartid,indexid,start_mdfid,end_mdfid);
		sp->DoQuery(sqlbf);
		errprintf("ɾ������������ݺ������ļ�.");
		errprintf("ɾ�������ļ�...");
		int i;
		for(i=0;i<frn;i++) {
			errprintf("\t %s ",fnmt.PtrStr("filename",i));
			errprintf("\t %s ",fnmt.PtrStr("idxfname",i));
		}
		for(i=0;i<frn;i++) {
			unlink(fnmt.PtrStr("filename",i));
			unlink(fnmt.PtrStr("idxfname",i));
		}
		errprintf("ɾ���Ѵ��������ļ��������ļ���¼...");
		AutoStmt st(sp->GetDTS());
		for(i=0;i<frn;i++) {
			st.Prepare("delete from dp.dp_datafilemap where tabid=%d and datapartid=%d and indexgid=%d and fileflag=1 and fileid=%d",tabid,datapartid,indexid,fnmt.PtrInt("fileid",i));
			st.Execute(1);
			st.Wait();
		}
		wociCommit(sp->GetDTS());
		
		sp->log(tabid,datapartid,112,"״̬�ѻָ�");
		throw ;
	}
	
	lgprintf("���ݴ���(MiddleDataLoading)����,���������ݰ�%d��.",dispct);
	lgprintf("����%d�������ļ�,�Ѳ���dp.dp_datafilemap��.",wociGetMemtableRows(fnmt));
	sp->log(tabid,datapartid,113,"�����������,���������ݰ�%d��,�����ļ�%d��.",dispct,wociGetMemtableRows(fnmt));
	//wociMTPrint(fnmt,0,NULL);
	//����Ƿ�����ݷ�������һ������
	// ��������һ�����ݣ���˵����
	//  1. ��ִ�������һ�����������Ѵ����ꡣ
	//  2. ���и÷����Ӧ��һ������������������������ɡ�
	//���������²�ɾ����������ʱ���ݡ�
	try
	{
		//�����������������ͬһ���������������ε����ݣ����������Ӽ����Ƿ�������ϡ�
		mdf.FetchAll("select * from dp.dp_middledatafile where procstate!=3 and tabid=%d and datapartid=%d ",
			tabid,datapartid);
		int rn=mdf.Wait();
		if(rn==0) {
			mdf.FetchAll("select sum(recordnum) rn from dp.dp_middledatafile where tabid=%d and datapartid=%d and indexgid=%d",
			  tabid,datapartid,indexid);
			mdf.Wait();
			LONG64 trn=mdf.GetLong("rn",0);
			mdf.FetchAll("select sum(recordnum) rn from dp.dp_datafilemap where tabid=%d and datapartid=%d and indexgid=%d and fileflag=1 and procstatus=0",
			  tabid,datapartid,indexid);
			mdf.Wait();
			//����У��
			if(trn!=mdf.GetLong("rn",0)) {
				//δ֪ԭ������Ĵ��󣬲���ֱ�ӻָ�״̬. ��ͣ�����ִ��
				sp->log(tabid,datapartid,114,"����У�����,����%lld�У���������%lld��(��֤������%d),����Ǩ�ƹ����ѱ���ͣ��",trn,mdf.GetLong("rn",0),indexid);
				sprintf(sqlbf,"update dp.dp_datapart set oldstatus=status,status=70 where tabid=%d",tabid);
				sp->DoQuery(sqlbf);
				ThrowWith("����У�����,��%d(%d),����%lld�У���������%lld��(��֤������%d),����Ǩ�ƹ����ѱ���ͣ",tabid,datapartid,trn,mdf.GetLong("rn",0),indexid);
			}
			if(mdf.GetInt("rn",0))
			lgprintf("���һ�������Ѵ�����,����״̬2-->3,��%d(%d)",tabid,datapartid);
			//����ǵ������������񣬱�����������ͬ���ݼ�������״̬Ϊ3������������һ���Ĳ���������װ�룩��
			sprintf(sqlbf,"update dp.dp_datapart set status=3 where tabid=%d and datapartid=%d",
				tabid,datapartid);
			sp->DoQuery(sqlbf);
			//���´�����ṹ��
			//sp->CreateDT(tabid);
			//}
			lgprintf("ɾ���м���ʱ�ļ�...");
			mdf.FetchAll("select * from dp.dp_middledatafile where tabid=%d and datapartid=%d and procstate=3",tabid,datapartid);
			int dfn=mdf.Wait();
			{
				for(int di=0;di<dfn;di++) {
					lgprintf("ɾ���ļ�'%s'",mdf.PtrStr("datafilename",di));
					unlink(mdf.PtrStr("datafilename",di));
					lgprintf("ɾ���ļ�'%s'",mdf.PtrStr("indexfilename",di));
					unlink(mdf.PtrStr("indexfilename",di));
				} 
				lgprintf("ɾ����¼...");
				AutoStmt st(sp->GetDTS());
				st.Prepare("delete from dp.dp_middledatafile where tabid=%d and datapartid=%d and procstate=3",tabid,datapartid);
				st.Execute(1);
				st.Wait();
			}
		}
		
	}
	catch(...) {
		errprintf("����������������ɣ�������״̬��������ʱ�м��ļ�ɾ��ʱ���ִ�����Ҫ�˹�������\n��%d(%d)��",
			tabid,datapartid);
		throw;
	}
	return 1;
	//Load index data into memory table (indexmt)
  }
  
  //�������ݱ����,��������ṹ�������ṹ
  bool SysAdmin::EmptyIndex(int tabid)
  {
	  AutoMt indexmt(dts,MAX_DST_INDEX_NUM);
	  indexmt.FetchAll("select databasename from dp.dp_table where tabid=%d",tabid);
	  int rn=indexmt.Wait();
	  char dbn[150];
	  char tbname[150],idxtbn[150];
	  strcpy(dbn,indexmt.PtrStr("databasename",0));
	  if(rn<1) ThrowWith("���������ʱ�Ҳ���%dĿ���.",tabid);
	  indexmt.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>0",tabid);
	  rn=indexmt.Wait();
	  if(rn<1) ThrowWith("���������ʱ�Ҳ�������������¼(%dĿ���).",tabid);
	  char sqlbf[300];
	  for(int i=0;i<rn;i++) {
	  	  int ilp=0;
	  	  while(GetTableName(tabid,indexmt.GetInt("indexgid",i),tbname,idxtbn,TBNAME_DEST,ilp++)) {
		   lgprintf("���������%s...",idxtbn);
		   sprintf(sqlbf,"truncate table %s",idxtbn);
		   DoQuery(sqlbf);
		  } 
	  }
	  return true;
  }
  
  //���ص�mtֻ�ܲ����ڴ��,���ܲ������ݿ�(fetch),��Ϊ�������ں����˳�ʱ�Ѿ��ͷ�
  int SysAdmin::BuildMtFromSrcTable(int srcsys,int tabid,AutoMt *mt) {
	  int tabp=wociSearchIK(dt_table,tabid);
	  const char *srctbn=dt_table.PtrStr("srctabname",tabp);
  	  AutoStmt srcst(srcsys);
	  srcst.Prepare("select * from %s.%s",dt_table.PtrStr("srcowner",tabp),
		  srctbn);
	  wociBuildStmt(*mt,srcst,mt->GetMaxRows());
	  return 0;
  }
  
  int SysAdmin::GetSrcTableStructMt(int tabp, int srcsys)
  {
	  AutoStmt srcst(srcsys);
	  srcst.Prepare("select * from %s.%s",dt_table.PtrStr("srcowner",tabp),dt_table.PtrStr("srctabname",tabp));
	  int mt=wociCreateMemTable();
	  wociBuildStmt(mt,srcst,10);
	  return mt;
  }
  
  //���ȥ���ֶ������ļ���֧��,������ĺ��������
  bool SysAdmin::CreateTableOnMysql(int srcmt,const char *tabname,bool forcecreate)
  {
	  //���Ŀ����Ѵ��ڣ���ɾ��
	    	char sqlbf[3000];
			bool exist=conn.TouchTable(tabname);
			if(exist && !forcecreate) 
				ThrowWith("Create MySQL Table '%s' failed,table already exists.",tabname);
			if(exist) {
				printf("table %s has exist,dropped.\n",tabname);
				sprintf(sqlbf,"drop table %s",tabname);
				conn.DoQuery(sqlbf);
			}
			//����Ŀ��꼰���ṹ�������ļ�
			wociGetCreateTableSQL(srcmt,sqlbf,tabname,true);
			strcat(sqlbf," PACK_KEYS = 1");
			//printf("%s.\n",sqlbf);
			conn.DoQuery(sqlbf);
			mSleep(300);			
			return true;
  }
  
  void SysAdmin::CloseTable(int tabid,char *tbname,bool cleandt) {
	  char tabname[150];
	  //AutoStmt st(dts);
	  //if(cleandt)
	  //	  st.Prepare("update dp.dp_table set recordnum=0,firstdatafileid=0,totalbytes=0 where tabid=%d",tabid);
	  //else
	  //	  st.Prepare("update dp.dp_table set recordnum=0 where tabid=%d",tabid);
	  //st.Execute(1);
	  //st.Wait();
	  //wociCommit(dts);
	  if(tbname==NULL) {
		  GetTableName(tabid,-1,tabname,NULL,TBNAME_DEST);
	  }
	  else strcpy(tabname,tbname);
	  lgprintf("�ر�'%s'��...",tabname);
	  {
		  lgprintf("ɾ��DP�����ļ�.");
		  char basedir[300];
		  char streamPath[300];
		  char tbntmp[200];
		  strcpy(basedir,GetMySQLPathName(0,"msys"));
		  strcpy(tbntmp,tabname);
		  char *psep=strstr(tbntmp,".");
		  if(psep==NULL) 
			  ThrowWith("Invalid table name format'%s',should be dbname.tbname.",tbname);
		  *psep='/';
		  sprintf(streamPath,"%s%s.DTP",basedir,tbntmp);
		  unlink(streamPath);
	  }
    	  conn.FlushTables(tabname);
		 lgprintf("��'%s'�����...",tabname);
  }	
  
//���������¸����ֶ�ֵ(dt_index.reusecols)
//�������������¼������
//1. �����������������ļ�,��Ҫ����ԭ�еı�����ݼ�¼.
//2. ����ǰ����������,����Ҫ����ԭ�еı�����ݼ�¼.
//3. ����ǰ��ʹ��ͬһ�������ļ�,��Ҫʹ���µı�������ṹ�滻ԭ����,��ʱҲ����Ҫ����ԭ�еı�����ݼ�¼.
//
//   �п���ֻ�ǲ�����������(һ���򼸸�����)
//
//  ��������������ǰ��Ҫȷ�����ݵ�������:
//    1.������������,�������������Ѿ���.
//    2.ȫ����������,�����е����ݶ���ȱʧ.
void SysAdmin::DataOnLine(int tabid) {
	char tbname[150],idxname[150];
	char tbname_p[150],idxname_p[150];
	char sql[3000];
	bool ec=wociIsEcho();
	wociSetEcho(FALSE);
	lgprintf("%d������...",tabid);
	
	AutoMt mtp(dts,200);
	mtp.FetchAll("select distinct datapartid,oldstatus from dp.dp_datapart where tabid=%d and status=21 order by datapartid",tabid);
	int rn=mtp.Wait();
	if(rn<1)
		return ;
	AutoMt mti(dts,200);
	mti.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>0",tabid);
	AutoStmt st(dts);
	int rni=mti.Wait();
	if(rni<1)
		ThrowWith("�Ҳ�����Ӧ��%d����κ�������",tabid);
	
	double idxtbsize=0;
	int datapartid=-1;
	AutoMt mt(dts,200);
	mt.FetchAll("select * from dp.dp_datapart where tabid=%d and status!=21 limit 10",tabid);
	bool onlypart=mt.Wait()>0;
	int pi;
	sprintf
	try {
		CloseTable(tabid,NULL,false);
		for(pi=0;pi<rn;pi++) {
			//����Ƿ���Ҫ����ԭ�е�����,���������װ��,����ѹ�������,����ͬһ�������ϲ���,����Ҫ����.
			bool replace=(mtp.GetInt("oldstatus",pi)==4);
			datapartid=mtp.GetInt("datapartid",pi);
			if(replace) {
				st.DirectExecute("update dp.dp_datafilemap set fileflag=2 where tabid=%d and fileflag=0 and datapartid=%d",tabid,datapartid);
				st.DirectExecute("update dp.dp_datafilemap set fileflag=0 where tabid=%d and fileflag=1 and datapartid=%d",tabid,datapartid);
			}
			mt.FetchAll("select sum(recordnum) rn  from dp.dp_datafilemap "
				" where tabid=%d and fileflag=0 and isfirstindex=1 ",tabid);
			mt.Wait();
			double sumrn=mt.GetDouble("rn",0);
			for(int idi=0;idi<rni;idi++) {
				int indexgid=mti.GetInt("indexgid",idi);
				GetTableName(tabid,indexgid,tbname,idxname,TBNAME_DEST,-1,datapartid) ;
				if(replace && conn.TouchTable(idxname)) {
					GetTableName(tabid,indexgid,tbname_p,idxname_p,TBNAME_FORDELETE,-1,datapartid);
					conn.RenameTable(idxname,idxname_p,true);
				}
				GetTableName(tabid,indexgid,tbname_p,idxname_p,TBNAME_PREPONL,-1,datapartid);
				//���������,���ʾ�÷�������Ϊ��
				if(conn.TouchTable(idxname_p)) {
					conn.RenameTable(idxname_p,idxname,true);
					idxtbsize+=StatMySQLTable(GetMySQLPathName(0,"msys"),idxname);
				}
				else if(sumrn>0)
					ThrowWith("��%d�ķ���%d,ָʾ��¼��Ϊ%.0f,���Ҳ���������'%s'",tabid,datapartid,sumrn,idxname_p);
			}
		}
		
		for(int idi=0;idi<rni;idi++) {
		 int indexgid=mti.GetInt("indexgid",idi);
		 GetTableName(tabid,indexgid,tbname,idxname,TBNAME_DEST) ;
		 sprintf(sql,"create table %s 	
		//���ֻ�ǲ��ַ���װ��,���޸�Ŀ���.
		if(!onlypart) {
			GetTableName(tabid,-1,tbname_p,idxname_p,TBNAME_FORDELETE);
			if(conn.TouchTable(tbname))
				conn.RenameTable(tbname,tbname_p,true);
			GetTableName(tabid,-1,tbname_p,idxname_p,TBNAME_PREPONL);
			conn.RenameTable(tbname_p,tbname,true);
		}
		//�����ļ�����
		mt.FetchAll("select * from dp.dp_datafilemap where tabid=%d and (fileflag=0 or fileflag is null) and isfirstindex=1",tabid);
		rn=mt.Wait();
		int k;
		for(k=0;k<rn;k++) {
		 //Build table data file link information.
		 if(k+1==rn) {
		  dt_file df;
		  df.Open(mt.PtrStr("filename",k),2,mt.GetInt("fileid",k));
		  df.SetFileHeader(0,NULL);
		  df.Open(mt.PtrStr("idxfname",k),2,mt.GetInt("fileid",k));
		  df.SetFileHeader(0,NULL);
		 }
		 else {
		  dt_file df;
		  df.Open(mt.PtrStr("filename",k),2,mt.GetInt("fileid",k));
		  df.SetFileHeader(0,mt.PtrStr("filename",k+1));
		  df.Open(mt.PtrStr("idxfname",k),2,mt.GetInt("fileid",k));
		  df.SetFileHeader(0,mt.PtrStr("idxfname",k+1));
		 }
		}

		mt.FetchAll("select sum(recordnum) rn from dp.dp_datafilemap where tabid=%d and (fileflag=0 or fileflag is null) and isfirstindex=1",tabid);
		mt.Wait();
		double sumrn=mt.GetDouble("rn",0);
		mt.FetchAll("select sum(recordnum) rn,sum(filesize) tsize,sum(idxfsize) itsize ,count(*) fnum from dp.dp_datafilemap where tabid=%d and (fileflag=0 or fileflag is null) ",tabid);
		mt.Wait();
		double tsize=mt.GetDouble("tsize",0);
		double itsize=mt.GetDouble("itsize",0);
		int fnum=mt.GetInt("fnum",0);
		mt.FetchAll("select fileid from dp.dp_datafilemap where tabid=%d and (fileflag=0 or fileflag is null) and isfirstindex=1 order by datapartid ,fileid ",tabid);
		mt.Wait();
		st.DirectExecute("update dp.dp_table set recordnum=%.0f,firstdatafileid=%d,totalbytes=%15.0f,"
			"datafilenum=%d where tabid=%d",
			sumrn,mt.GetInt("fileid",0),tsize,fnum,tabid);
		CreateMergeIndexTable(tabid);
		BuildDTP(tbname);
		lgprintf("��'%s'�ɹ�����,��¼��%.0f,����%.0f,����%.0f. MySQLˢ��...",tbname,sumrn,tsize+itsize,idxtbsize);
		conn.FlushTables(tbname);
		wociSetEcho(ec);
		log(tabid,0,115,"���ѳɹ�����,��¼��%.0f,����%.0f,����%.0f. ",sumrn,tsize+itsize,idxtbsize);
	}
	catch(...) {
		//�ָ������ļ���������
		//ֻ��������װ��ʱ�����Ҫ�ָ�����Ŀ,��װ������������Ҫ�ָ�.
		mt.FetchAll("select distinct datapartid from dp.dp_datafilemap "
			" where fileflag=2 and tabid=%d",tabid);
		rn=mt.Wait();
		char deltbname[150];
		for(pi=0;pi<rn;pi++) {
			//�ָ�ԭ�е�����.
			datapartid=mt.GetInt(0,pi);
			st.DirectExecute("update dp.dp_datafilemap set fileflag=1 where tabid=%d and fileflag=0 and datapartid=%d",tabid,datapartid);
			st.DirectExecute("update dp.dp_datafilemap set fileflag=0 where tabid=%d and fileflag=2 and datapartid=%d",tabid,datapartid);
			for(int idi=0;idi<rni;idi++) {
				//�ָ�������
				int indexgid=mti.GetInt("indexgid",idi);
				GetTableName(tabid,indexgid,tbname_p,deltbname,TBNAME_FORDELETE,-1,datapartid);
				if(conn.TouchTable(deltbname)) {
					GetTableName(tabid,indexgid,tbname_p,idxname_p,TBNAME_PREPONL,-1,datapartid);
					GetTableName(tabid,indexgid,tbname,idxname,TBNAME_DEST,-1,datapartid) ;
					if(conn.TouchTable(idxname)) 
						conn.RenameTable(idxname,idxname_p,true);
					conn.RenameTable(deltbname,idxname,true);
				}
			}
		}
		if(!onlypart) {
			GetTableName(tabid,-1,deltbname,idxname_p,TBNAME_FORDELETE);
			if(conn.TouchTable(deltbname)) {
				GetTableName(tabid,-1,tbname_p,idxname_p,TBNAME_PREPONL);
				if(conn.TouchTable(tbname)) 
					conn.RenameTable(tbname,tbname_p,true);
				conn.RenameTable(deltbname,tbname,true);
			}
		}
		throw;
	}		
  }
  
  void SysAdmin::BuildDTP(const char *tbname)
  {
	  lgprintf("����DP�����ļ�.");
	  char basedir[300];
	  char streamPath[300];
	  char tbntmp[200];
	  strcpy(basedir,GetMySQLPathName(0,"msys"));
	  
	  dtioStream *pdtio=new dtioStreamFile(basedir);
	  strcpy(tbntmp,tbname);
	  char *psep=strstr(tbntmp,".");
	  if(psep==NULL) 
		  ThrowWith("Invalid table name format'%s',should be dbname.tbname.",tbname);
	  *psep=0;
	  psep++;
#ifdef WIN32
	  sprintf(streamPath,"%s%s\\%s.DTP",basedir,tbntmp,psep);
#else
	  sprintf(streamPath,"%s%s/%s.DTP",basedir,tbntmp,psep);
#endif
	  pdtio->SetStreamName(streamPath);
	  pdtio->SetWrite(false);
	  pdtio->StreamWriteInit(DTP_BIND);
	  
	  pdtio->SetOutDir(basedir);
	  {
		  dtioDTTable dtt(tbntmp,psep,pdtio,false);
		  dtt.FetchParam(dts);
		  dtt.SerializeParam();
	  }
	  pdtio->SetWrite(true);
	  pdtio->StreamWriteInit(DTP_BIND);
	  pdtio->SetOutDir(basedir); 
	  {
		  dtioDTTable dtt(tbntmp,psep,pdtio,false);
		  dtt.FetchParam(dts);
		  dtt.SerializeParam();
	  }
	  delete pdtio;
  }
  
  //����������ȫ��(�����ݿ���)
  void SysAdmin::GetPathName(char *path,const char *tbname,const char *surf) {
	  char dbname[150];
	  strcpy(dbname,tbname);
	  char *dot=strstr(dbname,".");
	  if(dot==NULL) 
		  ThrowWith("����������ȫ��('%s')",tbname);
	  char *mtbname=dot+1;
	  *dot=0;
	  const char *pathval=GetMySQLPathName(0,"msys");
#ifdef WIN32
	  sprintf(path,"%s\\%s\\%s.%s",pathval,dbname,mtbname,surf);
#else
	  sprintf(path,"%s/%s/%s.%s",pathval,dbname,mtbname,surf);
#endif
  }
  
  //type TBNAME_DEST: destination name
  //type TBNAME_PREPONL: prepare for online
  //type TBNAME_FORDELETE: fordelete 
  
  //ʹ�����datapartoff��datapartidָ��������,�������
  // �������ָ��(��Ϊ-1),�򷵻ز�������������������(�ϵĸ�ʽ).
  bool SysAdmin::GetTableName(int tabid,int indexid,char *tbname,char *idxname,int type,int datapartoff,int datapartid) {
	  char tbname1[150],idxname1[150],dbname[130];
	  idxname1[0]=0;
	  if(tabid==-1) 
		  ThrowWith("Invalid tabid parameter on call SysAdmin::GetTableName.");
	  AutoMt mt(dts,300);
	  mt.FetchAll("select * from dp.dp_table where tabid=%d",tabid);
	  int rn=mt.Wait();
	  if(rn<1)
		  ThrowWith("Tabid is invalid  :%d",tabid);
	  strcpy(tbname1,mt.PtrStr("tabname",0));
	  strcpy(dbname,mt.PtrStr("databasename",0));
	  if(indexid!=-1) {
	  	  if(datapartoff>=0 || datapartid>0) {
	  	   if(datapartoff>=0) 
	  	    mt.FetchAll("select datapartid from dp.dp_datapart where tabid=%d order by datapartid",tabid);
	  	   else 
	  	    mt.FetchAll("select datapartid from dp.dp_datapart where tabid=%d and datapartid=%d",tabid,datapartid);
	  	   rn=mt.Wait();
	  	   if(rn<1)
	  	    ThrowWith("tabid:%d,indexid:%d,datapartoff:%d,datapartid:%d ��Ч.",tabid,indexid,datapartoff,datapartid);
	  	   if(datapartoff>=0) {
	  	    if(datapartoff>=rn) return false;
	  	     datapartid=mt.GetInt(0,datapartoff);
	  	   }
	  	  }
	  	  mt.FetchAll("select * from dp.dp_index where tabid=%d and indexgid=%d and issoledindex>0",tabid,indexid);
		  rn=mt.Wait();
		  if(rn<1)
		  	 ThrowWith("tabid:%d,indexid:%d ��Ч.",tabid,indexid);
		  
		  //���ش������򲻴�����(�ϸ�ʽ)������������
		  if(datapartid>=0) 
		     sprintf(idxname1,"%sidx%d_p_%d",tbname1,indexid,datapartid);
		  else if(strlen(mt.PtrStr("indextabname",0))>0)
		    strcpy(idxname1,mt.PtrStr("indextabname",0));
		  else sprintf(idxname1,"%sidx%d",tbname1,indexid);
	  }
//	  if(STRICMP(idxname1,tbname1)==0) 
//	   ThrowWith("��������'%s'��Ŀ�����ظ�:indexid:%d,tabid:%d!",indexid,tabid);
	  if(type!=TBNAME_DEST) {
		  if(indexid!=-1) {
			  strcat(idxname1,"_");
			  strcat(idxname1,dbname);
		  }
		  strcat(tbname1,"_");
		  strcat(tbname1,dbname);
		  if(type==TBNAME_PREPONL)
			  strcpy(dbname,PREPPARE_ONLINE_DBNAME);
		  else if(type==TBNAME_FORDELETE)
			  strcpy(dbname,FORDELETE_DBNAME);
		  else ThrowWith("Invalid table name type :%d.",type);
	  }
	  if(tbname) sprintf(tbname,"%s.%s",dbname,tbname1);
	  if(indexid!=-1 && idxname)
		  sprintf(idxname,"%s.%s",dbname,idxname1);
	  return true;
  }
  
  void SysAdmin::CreateAllIndex(int tabid,int nametype,bool forcecreate,int ci_type,int datapartid)
  {
	  AutoMt mt(dts,MAX_DST_INDEX_NUM);
	  mt.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>0 order by seqindattab",tabid);
	  int rn=mt.Wait();
	  if(rn<1)
		  ThrowWith("�Ҳ���%d��Ķ���������",tabid);
	  for(int i=0;i<rn;i++)
		  CreateIndex(tabid,mt.GetInt("indexgid",i),nametype,forcecreate,ci_type,datapartid);
  }
  
  void SysAdmin::RepairAllIndex(int tabid,int nametype,int datapartid)
  {
	  AutoMt mt(dts,MAX_DST_INDEX_NUM);
	  mt.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>0 order by seqindattab",tabid);
	  int rn=mt.Wait();
	  if(rn<1)
		  ThrowWith("�Ҳ���%d��Ķ���������",tabid);
	  for(int i=0;i<rn;i++) {
		  int indexid=mt.GetInt("indexgid",i);
		  char tbname[100],idxname[100];
		  //int ilp=0;
		  if(GetTableName(tabid,indexid,tbname,idxname,nametype,-1,datapartid)) {
		   if(conn.TouchTable(idxname)) {
		    lgprintf("������ˢ��...");
		    FlushTables(idxname);
		    lgprintf("�������ؽ�:%s...",idxname);
		  
		    char fn[500];
		    GetPathName(fn,idxname,"MYI");
		    char cmdline[500];
		    // -n ѡ������ǿ��ʹ������ʽ�޸�
		    sprintf(cmdline,"myisamchk -rqnv --tmpdir=\"%s\" %s ",GetMySQLPathName(0,"msys"),fn);
		    int rt=system(cmdline);
		    wait(&rt);
		    if(rt)
			  ThrowWith("�����ؽ�ʧ��!");
		   }
		  }
		  //char sqlbf[3000];
		  //sprintf(sqlbf,"repair table %s quick",idxname);
		  //conn.DoQuery(sqlbf);
	  }
  }
  
  void SysAdmin::CreateIndex(int tabid,int indexid,int nametype,bool forcecreate,int ci_type,int datapartoff,int datapartid)
  {
	  AutoMt mt(dts,10);
	  mt.FetchAll("select * from dp.dp_index where tabid=%d and indexgid=%d and issoledindex>0",tabid,indexid);
	  int rn=mt.Wait();
	  if(rn<1)
		  ThrowWith("Indexgid is invalid or not a soled :%d",indexid);
	  char colsname[300];
	  strcpy(colsname,mt.PtrStr("columnsname",0));
	  char tbname[100],idxname[100];
	  int ilp=0;
	  if(GetTableName(tabid,indexid,tbname,idxname,nametype,datapartoff,datapartid)) {
	   //��������������
	   if(ci_type==CI_ALL || ci_type==CI_IDX_ONLY)
		  CreateIndex(idxname,mt.GetInt("seqinidxtab",0),colsname,forcecreate);
	   if(ci_type==CI_ALL || ci_type==CI_DAT_ONLY)
		  CreateIndex(tbname,mt.GetInt("seqindattab",0),colsname,forcecreate);
	  
	  //��������������
	  //���Ҹö������������ķǶ������������Դ�Ϊ���ݽ���������
	   mt.FetchAll("select * from dp.dp_index where tabid=%d and indexgid=%d and issoledindex=0 order by seqinidxtab",
		  tabid,indexid);
	   int srn=mt.Wait();
	   for(int j=0;j<srn;j++) {
		  strcpy(colsname,mt.PtrStr("columnsname",j));
		  if(ci_type==CI_ALL || ci_type==CI_IDX_ONLY)
			  CreateIndex(idxname,mt.GetInt("seqinidxtab",j),colsname,forcecreate);
		  if(ci_type==CI_ALL || ci_type==CI_DAT_ONLY)
			  CreateIndex(tbname,mt.GetInt("seqindattab",j),colsname,forcecreate);
	   }
	}
  }
  
  void SysAdmin::CreateIndex(const char *dtname, int id, const char *colsname, bool forcecreate)
  {
	  //create index����е��������Ʋ���������ݿ�����
	  const char *tbname=strstr(dtname,".");
	  if(tbname==NULL) tbname=dtname;
	  else tbname++;
	  char sqlbf[300];
	  if(forcecreate) {
		  sprintf(sqlbf,"drop index %s_%d on %s",tbname,
			  id,dtname);
		  conn.DoQuery(sqlbf);
	  }
	  sprintf(sqlbf,"create index %s_%d on %s(%s)",
		  tbname,id,
		  dtname,colsname);
	  lgprintf("��������:%s.",sqlbf);
	  conn.DoQuery(sqlbf);
  }
#ifdef WORDS_BIGENDIAN
#define revlint(v) v
#else
#define revlint(V)   { char def_temp[8];\
	((mbyte*) &def_temp)[0]=((mbyte*)(V))[7];\
	((mbyte*) &def_temp)[1]=((mbyte*)(V))[6];\
	((mbyte*) &def_temp)[2]=((mbyte*)(V))[5];\
	((mbyte*) &def_temp)[3]=((mbyte*)(V))[4];\
	((mbyte*) &def_temp)[4]=((mbyte*)(V))[3];\
	((mbyte*) &def_temp)[5]=((mbyte*)(V))[2];\
	((mbyte*) &def_temp)[6]=((mbyte*)(V))[1];\
	((mbyte*) &def_temp)[7]=((mbyte*)(V))[0];\
  memcpy(V,def_temp,sizeof(LLONG)); }
#endif                          
  
  int DestLoader::Load(bool directIOSkip) {
	  //Check deserved temporary(middle) fileset
	  AutoMt mdf(psa->GetDTS(),MAX_DST_DATAFILENUM);
	  mdf.FetchAll("select * from dp.dp_datapart where (status=3 or status=30) limit 2");
	  int rn=mdf.Wait();
	  if(rn<1) {
		  printf("û�з��ִ�����ɵȴ�װ�������(����״̬=3).\n");
		  return 0;
	  }
	  char sqlbf[1000];
	  tabid=mdf.GetInt("tabid",0);
	  datapartid=mdf.GetInt("datapartid",0);
	  bool preponl=mdf.GetInt("status",0)==3;
	  lgprintf("װ������:%s.",preponl?"��װ��":"��װ��");
	  /*mdf.FetchAll("select * from dp.dp_datapart where status<3 and tabid=%d limit 2",tabid);
	  rn=mdf.Wait();
	  if(rn>0) {
		  printf("����װ��ʱ����%d��һЩ���ݷ��黹δ����,�������%d(״̬%d).\n",tabid,
		          mdf.GetInt("datapartid",0),mdf.GetInt("status",0));
		  return 0;
	  }
	  
	  mdf.FetchAll("select * from dp.dp_datapart where status>%d and tabid=%d limit 2",preponl?3:30,tabid);
	  rn=mdf.Wait();
	  if(rn>0) {
		  if(!preponl) {
			  lgprintf("����װ��ʱ��������״̬Ϊ����װ��(30)��Ӧ��������װ��(3),��%d,�����%d,(״̬%d).\n",tabid,
				  mdf.GetInt("datapartid",0),mdf.GetInt("status",0));
			  return 0;
		  }
		  // 2005/08/27�޸ģ�partid��Ч��subdatasetid
		  mdf.FetchAll("select count(*) fn,sum(recordnum) rn from dp.dp_datafilemap where tabid=%d and datapartid=%d and fileflag=1 and isfirstindex=1 ",tabid,datapartid);
		  mdf.Wait();
		  if(mdf.GetInt("fn",0)<1) {
			  lgprintf("����װ��ʱ����������װ�������Ӽ������Ҳ��������ļ�, ������%d(%d).",tabid,datapartid);
			  //char ans[100];
			  //sprintf(choose,"������(Y/N)?");
			  //if(!GetYesNo(choose,false)) {
				AutoStmt st(psa->GetDTS());
				st.DirectExecute(" update dp.dp_datapart set status=5 where tabid=%d and datapartid=%d",tabid,datapartid); 
				lgprintf("ȡ��װ�룬������%d(%d)�ָ�Ϊ����ɡ� ",tabid,datapartid);
				return 0;
			  //}
		  }
		  lgprintf("������%d(%d)�������滻��ԭ�����ݼ�¼����Ϊ��ɾ��(fileflag=2).",tabid,datapartid);
		  lgprintf("�������������%d���ļ���%15.f����¼.",mdf.GetInt("fn",0),mdf.GetDouble("rn",0));
		  mdf.FetchAll("select count(*) fn,sum(recordnum) rn from dp.dp_datafilemap where tabid=%d and datapartid=%d and fileflag=0 ",tabid,datapartid);
		  mdf.Wait();
		  lgprintf("�ϵķ���������������%d���ļ���%15.f����¼.",mdf.GetInt("fn",0),mdf.GetDouble("rn",0));
		  
		  AutoStmt st(psa->GetDTS());
		  st.DirectExecute(" update dp.dp_datafilemap set fileflag=2 where tabid=%d and datapartid=%d and fileflag=0 ",tabid,datapartid);
		 
		  st.DirectExecute(" update dp.dp_datafilemap set fileflag=0 where tabid=%d and datapartid=%d and fileflag=1 ",tabid,datapartid);
		  st.DirectExecute(" update dp.dp_datapart set status=30 where tabid=%d",tabid);
		  lgprintf("��ķ����ѱ任Ϊ����װ��(30).",tabid);
		  preponl=false;
	  }
	  */
	  wociSetTraceFile("dl����װ��/");
	  psa->OutTaskDesc("����װ�� :",tabid);
	  
	  mdf.FetchAll("select * from dp.dp_datafilemap where tabid=%d and procstatus=0 and fileflag=%d and datapartid=%d limit 2",
		  tabid,preponl?1:0,datapartid);
	  if(mdf.Wait()<1) {
		  //errprintf("������%d(%d)ָʾ����������������Ҳ�����Ӧ�����ݼ�¼��\n�����������ļ���¼�����ڻ�״̬�ǿ���(0).\n",
		  //  tabid,datapartid);
		  AutoStmt st(psa->GetDTS());
		  st.DirectExecute(" update dp.dp_datapart set status=21 ,oldstatus=%d where tabid=%d and datapartid=%d",
		     preponl?4:40,datapartid);
	          psa->CreateIndexTable(tabid,indexid,indexmt,-1,TBNAME_PREPONL,TRUE,CI_IDX_ONLY,datapartid);
		  return 0;
	  }
	  tabid=mdf.GetInt("tabid",0);
	  datapartid=mdf.GetInt("datapartid",0);
	  
	  //mdf.FetchAll("select * from dp.dp_table where tabid=%d",tabid);
	  //if(mdf.Wait()<1) {
	  //	  errprintf("�Ҳ��������ļ���Ӧ��Ŀ���(%d).\n",tabid);
	  //	  return 0;
	  //}
	  //char dbname[300];
	  //strcpy(dbname,mdf.PtrStr("databasename",0));
	  //conn.SelectDB(dbname);
	  //partid=mdf.GetInt("partid",0);
	  //dumpparam dpsrc;
	  //psa->GetSoledIndexParam(srctabid,&dpsrc);
	  
	  psa->GetSoledIndexParam(-1,&dp,tabid);
	  AutoMt idxmt(psa->GetDTS(),10);
	  idxmt.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>0",tabid);
	  int idxrn=idxmt.Wait();
	  int totdatrn=0;
	  psa->log(tabid,0,116,"����װ��,����������%d,��־�ļ� '%s' .",idxrn,lgfile);
	  try {
		  //Ϊ��ֹ��������,�����ļ�״̬�޸�.
		  mdf.FetchAll("select * from dp.dp_datafilemap where tabid=%d and fileflag=%d and datapartid=%d order by indexgid,fileid",
			  tabid,preponl?1:0,datapartid);
		  totdatrn=rn=mdf.Wait();
		  lgprintf("�޸������ļ��Ĵ���״̬(tabid:%d,%d���ļ�)��0-->1",tabid,rn);
		  sprintf(sqlbf,"update dp.dp_datafilemap set procstatus=1 where tabid=%d and "
			  "procstatus=0 and fileflag=%d and datapartid=%d",tabid,preponl?1:0,datapartid);
		  if(psa->DoQuery(sqlbf)!=rn) 
			  ThrowWith("�޸������ļ��Ĵ���״̬�쳣�����������������̳�ͻ��\n"
			  "   tabid:%d.\n",tabid);
	  for(int i=0;i<idxrn;i++) {
		  indexid=idxmt.GetInt("indexgid",i);
		  mdf.FetchAll("select * from dp.dp_datafilemap where tabid=%d  and indexgid=%d and fileflag=%d and datapartid=%d order by fileid",
			  tabid,indexid,preponl?1:0,datapartid);
		  rn=mdf.Wait();
		  if(rn<1) ThrowWith("�Ҳ��������ļ�(indexgid:%d,tabid:%d,datapartid:%d,fileflag:%d).",indexid,tabid,datapartid,preponl?1:0);
		  //�������ݱ�Ľṹ���ļ���ȡ���ȴ�template����ȡ��׼ȷ��
		  int off=dp.GetOffset(indexid);
		  //{
		  dt_file idxf;
		  idxf.Open(mdf.PtrStr("idxfname",0),0);
		  AutoMt indexmt(0);
		  indexmt.SetHandle(idxf.CreateMt(10));
		  /*
		  if(conn.TouchTable(dp.idxp[off].idxtbname)) {
		  printf("table %s has exist,dropped.\n",dp.idxp[off].idxtbname);
		  sprintf(sqlbf,"drop table %s",dp.idxp[off].idxtbname);
		  conn.DoQuery(sqlbf);
		  }
		  //����Ŀ��꼰���ṹ�������ļ�
		  wociGetCreateTableSQL(idxmt,sqlbf,dp.idxp[off].idxtbname,true);
		  conn.DoQuery(sqlbf);
		  */
		  //}
		 
		  AutoMt datmt(psa->GetDTS(),MAX_DST_DATAFILENUM);
		  datmt.FetchAll("select * from dp.dp_datafilemap where tabid=%d "
			  " and indexgid=%d and procstatus=1 and fileflag=%d and datapartid=%d order by fileid",
			  tabid,indexid,preponl?1:0,datapartid);
		  int datrn=datmt.Wait();
		  //��ֹ���������������ʱ���ظ�ִ��
		  if(datrn<1) continue;
		  char fn[300];
		  bool isfixed=false;
		  int k=0;
		  const char *pathval=psa->GetMySQLPathName(0,"msys");		
			  lgprintf("��ʼ����װ��(DestLoading),�ļ���:%d,tabid:%d,indexid:%d ...",
				  rn,tabid,indexid);
			  //ԭ���ķ�ʽΪ����������(FALSE����)
			  //2005/12/01�޸�Ϊ�����������ڿձ�֮��),�������ؽ�������(::RecreateIndex,taskstaus 4->5),
			  //  ʹ��repair table ... quick�����ٽ��������ṹ��
			  // BUG FIXING
			  //**
			  psa->CreateIndexTable(tabid,indexid,indexmt,-1,TBNAME_PREPONL,TRUE,CI_IDX_ONLY,datapartid);
			  //psa->CreateIndexTable(indexid,indexmt,-1,TBNAME_PREPONL,FALSE);
			  char tbname[150],idxname[150];
			  psa->GetTableName(tabid,indexid,tbname,idxname,TBNAME_PREPONL,-1,datapartid);
			  psa->GetPathName(fn,idxname,"MYD");
			  //struct _finddata_t ft;
			  FILE *fp =NULL;
			  if(!directIOSkip) {
				  fp=fopen(fn,"wb");
				  if(fp==NULL) 
					  ThrowWith("Open file %s for writing failed!",fn);
			  }
			  LLONG totidxrn=0;
			  //lgprintf("�����ļ���%s",mdf.PtrStr("filename",0));
			  for(k=0;k<rn;k++) {
				  file_mt idxf;
				  idxf.Open(datmt.PtrStr("idxfname",k),0);
				  int rn_fromtab=datmt.GetInt("idxrn",k);
				  int mt=idxf.ReadBlock(0,0);
				  isfixed=wociIsFixedMySQLBlock(mt);
				  LLONG startat=totidxrn;
				  try {
					  AutoStmt st(psa->GetDTS());
					  st.DirectExecute("use preponl");
				    if(wdbi_kill_in_progress) {
				    	wdbi_kill_in_progress=false;
					  	ThrowWith("�û�ȡ������!");
					  }
					  lgprintf("������������...");
					  while(mt) {
						  if(!directIOSkip)
							  wociCopyToMySQL(mt,0,0,fp);
						  else wociAppendToDbTable(mt,idxname,psa->GetDTS(),true);
						  totidxrn+=wociGetMemtableRows(mt);
						  if(totidxrn-startat<rn_fromtab)
							  mt=idxf.ReadBlock(-1,0);
						  else break;
					  }
				  }
				  catch(...) {
					  AutoStmt st(psa->GetDTS());
					  st.DirectExecute("use dp");
					  throw;
				  }
				  {
					  AutoStmt st(psa->GetDTS());
					  st.DirectExecute("use dp");
					  lgprintf("��������:%lld��.",totidxrn);
				  }
			  }
			  if(!directIOSkip) {
				  fclose(fp);
				  fp=fopen(fn,"rb");
				  fseeko(fp,0,SEEK_END);
				  LONG64 fsz=ftello(fp);//_filelength(_fileno(fp));
				  fclose(fp);
				  //�������ݱ�Ľṹ���ļ���ȡ���ȴ�template����ȡ��׼ȷ��
				  psa->GetPathName(fn,idxname,"MYI");
				  char tmp[20];
				  memset(tmp,0,20);
				  
				  revlint(&totidxrn);
				  revlint(&fsz);
				  fp=fopen(fn,"r+b");
				  if(fp==NULL) 
					  ThrowWith("�޷����ļ�'%s'������Ŀ¼��������(dt_path)�Ƿ���ȷ��",fn);
				  fseek(fp,28,SEEK_SET);
				  dp_fwrite(&totidxrn,1,8,fp);
				  // reset deleted records count.
				  dp_fwrite(tmp,1,8,fp);
				  fseek(fp,68,SEEK_SET);
				  dp_fwrite(&fsz,1,8,fp);
				  fseek(fp,0,SEEK_END);
				  fclose(fp); 
				  lgprintf("������ˢ��...");
				  psa->FlushTables(idxname);
				  revlint(&fsz);
				  // BUG FIXING
				  //**ThrowWith("�����ж�");
				  
				  if(fsz>1024*1024) {
					  lgprintf("ѹ��������:%s....",idxname);
					  char cmdline[300];
					  strcpy(fn+strlen(fn)-3,"TMD");
					  unlink(fn);
					  strcpy(fn+strlen(fn)-3,"MYI");
					  printf("pack:%s\n",fn);
					  sprintf(cmdline,"myisampack -v %s",fn);
					  //sprintf(cmdline,"gdb myisampack",fn);
					  int rt=system(cmdline) ;
					  wait (&rt);
					  if(rt)
						  ThrowWith("������%sѹ��ʧ��.",idxname);
					  lgprintf("ѹ���ɹ���");
				  }
			  }
		  }
	}
	catch(...) {
		  lgprintf("�ָ������ļ��Ĵ���״̬(tabid %d(%d),%d���ļ�)��1-->0",tabid,datapartid,totdatrn);
		  sprintf(sqlbf,"update dp.dp_datafilemap set procstatus=0 where tabid=%d  and fileflag=%d and datapartid=%d",
			  tabid,preponl?1:0,datapartid);
		  psa->DoQuery(sqlbf);
		  char tbname[150],idxname[150];
	  	  psa->log(tabid,0,123,"����װ������쳣����,�ѻָ�����״̬");
		  // BUG FIXING
		  /*
		  psa->GetTableName(tabid,indexid,tbname,idxname,TBNAME_PREPONL);
		  char fn[300];
		  psa->GetPathName(fn,idxname,"MYD");
		  unlink(fn);
		  psa->GetPathName(fn,idxname,"MYI");
		  unlink(fn);
		  psa->GetPathName(fn,idxname,"frm");
		  unlink(fn);
		  */
		  throw;
	}
	lgprintf("����װ��(DestLoading)���� ...");
	AutoStmt updst(psa->GetDTS());
	updst.DirectExecute("update dp.dp_datapart set status=%d where tabid=%d and datapartid=%d",
		preponl?4:40,tabid,datapartid);
	updst.DirectExecute("update dp.dp_datafilemap set procstatus=0 where tabid=%d and datapartid=%d",
		tabid,datapartid);
	lgprintf("����״̬����,3(MLoaded)--->4(DLoaded),��:%d,����:%d.",tabid,datapartid);
	psa->log(tabid,0,117,"����װ��������ȴ���������.");
	//ThrowWith("DEBUG_BREAK_HEAR.");
	return 1;
}

// Դ��ΪDBPLUS�����Ŀ����Ҽ�¼���ǿա�
int DestLoader::MoveTable(const char *srcdbn,const char *srctabname,const char * dstdbn,const char *dsttabname)
{
	char dtpath[300];
	lgprintf("Ŀ������(ת��) '%s.%s -> '%s.%s'.",srcdbn,srctabname,dstdbn,dsttabname);
	sprintf(dtpath,"%s.%s",srcdbn,srctabname);
	if(!psa->TouchTable(dtpath))
	  ThrowWith("Դ��û�ҵ�");
	sprintf(dtpath,"%s.%s",dstdbn,dsttabname);
	if(psa->TouchTable(dtpath)) {
		sprintf(dtpath,"��%s.%s�Ѵ��ڣ�Ҫ������?(Y/N)",dstdbn,dsttabname);
		if(!GetYesNo(dtpath,false)) {
			lgprintf("ȡ�������� ");
			return 0;
		}			
	}
	AutoMt mt(psa->GetDTS(),MAX_DST_DATAFILENUM);
	int rn;
	//mt.FetchAll("select pathval from dp.dp_path where pathtype='msys'");
	//int rn=mt.Wait();
	//i/f(rn<1) 
	//	ThrowWith("�Ҳ���MySQL����Ŀ¼(dt_path.pathtype='msys'),����ת���쳣��ֹ.");
	strcpy(dtpath,psa->GetMySQLPathName(0,"msys"));
	if(STRICMP(srcdbn,dstdbn)==0 && STRICMP(srctabname,dsttabname)==0) 
		ThrowWith("Դ���Ŀ������Ʋ�����ͬ.");
	mt.FetchAll("select * from dp.dp_table where tabname=lower('%s') and databasename=lower('%s')",dsttabname,dstdbn);
	rn=mt.Wait();
	if(rn>0) {
		ThrowWith("��'%s.%s'�Ѵ���(��¼��:%d)������ʧ��!",dstdbn,dsttabname,mt.GetInt("recordnum",0));
	}
	mt.FetchAll("select * from dp.dp_table where tabname=lower('%s') and databasename=lower('%s')",srctabname,srcdbn);
	rn=mt.Wait();
	if(rn<1) {
		ThrowWith("Դ��'%s.%s'������.",srcdbn,srctabname);
	}
	int dsttabid=psa->NextTableID();
	tabid=mt.GetInt("tabid",0);
	int recordnum=mt.GetInt("recordnum",0);
	int firstdatafileid=mt.GetInt("firstdatafileid",0);
	double totalbytes=mt.GetDouble("totalbytes",0);
	int datafilenum=mt.GetInt("datafilenum",0);
	//if(recordnum<1) {
	//	lgprintf("Դ��'%s'����Ϊ�գ�����ת��ʧ�ܡ�",srctabname);
	//	return 0;
	//}
	lgprintf("Դ��'%s.%s' id:%d,��¼��:%d,��ʼ�����ļ��� :%d",
		srcdbn,srctabname,tabid,recordnum,firstdatafileid);
	
	//�±����ڣ���dt_table���½�һ����¼
	*mt.PtrInt("tabid",0)=dsttabid;
	strcat(mt.PtrStr("tabdesc",0),"_r");
	strcpy(mt.PtrStr("tabname",0),dsttabname);
	strcpy(mt.PtrStr("databasename",0),dstdbn);
	wociAppendToDbTable(mt,"dp.dp_table",psa->GetDTS(),true);
	psa->CloseTable(tabid,NULL,false);
	CopyMySQLTable(dtpath,srcdbn,srctabname,dstdbn,dsttabname);
	//��ʱ�ر�Դ������ݷ��ʣ���¼���Ѵ��ڱ��ر���recordnum��
	//Ŀ����.DTP�ļ��Ѿ�����,��ʱ���η���
	psa->CloseTable(dsttabid,NULL,false);
	char sqlbuf[1000];
	mt.FetchAll("select * from dp.dp_datapart where tabid=%d order by datapartid ",tabid);
	rn=mt.Wait();
	//����������¼���������޸������ļ��������ļ���tabid ָ��
	int i=0;
	for(i=0;i<rn;i++) 
		*mt.PtrInt("tabid",i)=dsttabid;
	wociAppendToDbTable(mt,"dp.dp_datapart",psa->GetDTS(),true);

	mt.FetchAll("select * from dp.dp_index where tabid=%d order by seqindattab ",tabid);
	rn=mt.Wait();
	//����������¼���������޸������ļ��������ļ���tabid ָ��
	for(i=0;i<rn;i++) {
		*mt.PtrInt("tabid",i)=dsttabid;
		strcpy(mt.PtrStr("indextabname",i),"");
	}
	wociAppendToDbTable(mt,"dp.dp_index",psa->GetDTS(),true);
	
	for(i=0;i<rn;i++) {
		if(mt.GetInt("issoledindex",i)>0) {
			char tbn1[300],tbn2[300];
			//������ϵ�����������,���ٴ������������.
			psa->GetTableName(tabid,mt.GetInt("indexgid",i),NULL,tbn1,TBNAME_DEST);
			if(psa->TouchTable(tbn1)) {
			 psa->GetTableName(dsttabid,mt.GetInt("indexgid",i),NULL,tbn2,TBNAME_DEST);
			 lgprintf("������ '%s'-->'%s...'",tbn1,tbn2);
			 MoveMySQLTable(dtpath,srcdbn,strstr(tbn1,".")+1,dstdbn,strstr(tbn2,".")+1);
			}
			else {
				int ilp=0;
				while(psa->GetTableName(tabid,mt.GetInt("indexgid",i),NULL,tbn1,TBNAME_DEST,ilp)) {
			         psa->GetTableName(dsttabid,mt.GetInt("indexgid",i),NULL,tbn2,TBNAME_DEST,ilp++);
			         lgprintf("������ '%s'-->'%s...'",tbn1,tbn2);
			         MoveMySQLTable(dtpath,srcdbn,strstr(tbn1,".")+1,dstdbn,strstr(tbn2,".")+1);
				}
			}
		}
	}
	mt.FetchAll("select * from dp.dp_datafilemap where tabid=%d order by fileid",tabid);
	rn=mt.Wait();
	AutoMt idxmt(psa->GetDTS(),MAX_DST_DATAFILENUM);

	for(i=0;i<rn;i++) {
		  char fn[300];
		  psa->GetMySQLPathName(mt.GetInt("pathid",i));
		  sprintf(fn,"%s%s.%s_%d_%d_%d.dat",psa->GetMySQLPathName(mt.GetInt("pathid",i)),
		    dstdbn,dsttabname,mt.GetInt("datapartid",i),mt.GetInt("indexgid",i),mt.GetInt("fileid",i));
		  
		  FILE *fp;
		  fp=fopen(mt.PtrStr("filename",i),"rb");
		  if(fp==NULL) ThrowWith("�Ҳ����ļ�'%s'.",mt.PtrStr("filename",i));
		  fclose(fp);
		  fp=fopen(fn,"rb");
		  if(fp!=NULL) ThrowWith("�ļ�'%s'�Ѿ�����.",fn);
      		  rename(mt.PtrStr("filename",i),fn);
		  strcpy(mt.PtrStr("filename",i),fn);
		  *mt.PtrInt("tabid",i)=dsttabid;

		  //psa->GetMySQLPathName(idxmt.GetInt("pathid",i));
		  sprintf(fn,"%s%s.%s_%d_%d_%d.idx",psa->GetMySQLPathName(mt.GetInt("pathid",i)),
		    dstdbn,dsttabname,mt.GetInt("datapartid",i),mt.GetInt("indexgid",i),mt.GetInt("fileid",i));
		  rename(mt.PtrStr("idxfname",i),fn);
		  strcpy(mt.PtrStr("idxfname",i),fn);
	}
	wociAppendToDbTable(mt,"dp.dp_datafilemap",psa->GetDTS(),true);
	
	sprintf(sqlbuf,"delete from dp.dp_datafilemap where tabid=%d",tabid);
	psa->DoQuery(sqlbuf);
	sprintf(sqlbuf,"update dp.dp_log set tabid=%d where tabid=%d",dsttabid,tabid);
	psa->DoQuery(sqlbuf);
	sprintf(sqlbuf,"update dp.dp_table set recordnum=0,cdfileid=0  where tabid=%d ",tabid);
	psa->DoQuery(sqlbuf);
	psa->log(dsttabid,0,118,"���ݴ�%s.%s��ת�ƶ���.",srcdbn,srctabname);
	//Move��������,��Ŀ���
	lgprintf("MySQLˢ��...");
	char tbn[300];
	sprintf(tbn,"%s.%s",dstdbn,dsttabname);
	psa->BuildDTP(tbn);
	psa->FlushTables(tbn);
	lgprintf("ɾ��Դ��..");
	RemoveTable(srcdbn,srctabname,false);
	lgprintf("�����Ѵӱ�'%s'ת�Ƶ�'%s'��",srctabname,dsttabname);
	return 1;
}


// 7,10������״̬����:
//   1. ���ļ�ϵͳ��ȡ����ѹ������ļ���С.
//   2. ����״̬�޸�Ϊ(8,11) (?? ����ʡ��)
//   3. �ر�Ŀ���(unlink DTP file,flush table).
//   4. �޸�����/����ӳ����е��ļ���С��ѹ������.
//   5. ����/���� �ļ��滻.
//   6. ����״̬�޸�Ϊ30(�ȴ�����װ��).
int DestLoader::ReLoad() {
	
	AutoMt mdf(psa->GetDTS(),MAX_DST_DATAFILENUM);
	mdf.FetchAll("select * from dp.dp_datapart where status in (7,10) limit 2");
	int rn=mdf.Wait();
	if(rn<1) {
		printf("û�з�������ѹ����ɵȴ�װ�������.\n");
		return 0;
	}
	wociSetTraceFile("drcl����ѹ����װ��/");
	bool dpcp=mdf.GetInt("status",0)==7;
	int compflag=mdf.GetInt("compflag",0);
	tabid=mdf.GetInt("tabid",0); 
	mdf.FetchAll("select * from dp.dp_datafilemap where tabid=%d and (fileflag=0 or fileflag is null) order by indexgid",tabid);
	rn=mdf.Wait();
	if(rn<1) {
		lgprintf("װ�����ѹ������ʱ�Ҳ��������ļ���¼��");
		return 1;
	}
	mdf.FetchAll("select * from dp.dp_table where tabid=%d",tabid);
	rn=mdf.Wait();
	if(rn<1) {
		lgprintf("װ�����ѹ������ʱ�Ҳ���dp.dp_table��¼(tabid:%d).",tabid);
		return 1;
	}
	char dbname[100],tbname[100];
	strcpy(dbname,mdf.PtrStr("databasename",0));
	strcpy(tbname,mdf.PtrStr("tabname",0));
	AutoMt datmt(psa->GetDTS(),MAX_DST_DATAFILENUM);
	datmt.FetchAll("select * from dp.dp_datafilemap where tabid=%d and (fileflag=0 or fileflag is null) order by datapartid,indexgid,fileid",
	               tabid);
	int datrn=datmt.Wait();
	rn=datrn;
	AutoStmt updst(psa->GetDTS());
	char tmpfn[300];
	int k;
	unsigned long dtflen[MAX_DST_DATAFILENUM];
	unsigned long idxflen[MAX_DST_DATAFILENUM];
	//�ȼ��
	for(k=0;k<datrn;k++) {
		sprintf(tmpfn,"%s.%s",datmt.PtrStr("filename",k),dpcp?"depcp":"dep5");
		dt_file df;
		df.Open(tmpfn,0);
		dtflen[k]=df.GetFileSize();
		if(dtflen[k]<1) 
			ThrowWith("file '%s' is empty!",tmpfn);
	}
	for(k=0;k<rn;k++) {
		sprintf(tmpfn,"%s.%s",datmt.PtrStr("idxfname",k),dpcp?"depcp":"dep5");
		dt_file df;
		df.Open(tmpfn,0);
		idxflen[k]=df.GetFileSize();
		if(idxflen[k]<1) 
			ThrowWith("file '%s' is empty!",tmpfn);
	}
	char sqlbf[200];
	sprintf(sqlbf,"update dp.dp_datapart set status=%d where tabid=%d", dpcp?8:11,tabid);
	if(psa->DoQuery(sqlbf)<1) 
		ThrowWith("����ѹ����������װ������޸�����״̬�쳣�����������������̳�ͻ(tabid:%d)��\n",tabid);
	
	//��ֹ�������룬�޸�����״̬
	//�����޸Ľ��漰�����ļ����滻,��������ǰ�ȹرձ�
	psa->CloseTable(tabid,NULL,false);
	lgprintf("�����ѹر�.");
	//���µ������ļ��滻ԭ�����ļ�����ɾ��ԭ�ļ������ļ����Ƹ���Ϊԭ�ļ����޸��ļ���¼�е��ļ���С�ֶΡ�
	lgprintf("��ʼ���ݺ������ļ��滻...");
	for(k=0;k<datrn;k++) {
		updst.Prepare("update dp.dp_datafilemap set filesize=%d,compflag=%d,idxfsize=%d where tabid=%d and fileid=%d and fileflag=0",
			dtflen[k],compflag,idxflen[k],tabid,datmt.GetInt("fileid",k));
		updst.Execute(1);
		updst.Wait();
		const char *filename=datmt.PtrStr("filename",k);
		unlink(filename);
		sprintf(tmpfn,"%s.%s",filename,dpcp?"depcp":"dep5");
		rename(tmpfn,filename);
		lgprintf("rename file '%s' as '%s'",tmpfn,filename);
		filename=datmt.PtrStr("idxfname",k);
		unlink(filename);
		sprintf(tmpfn,"%s.%s",filename,dpcp?"depcp":"dep5");
		rename(tmpfn,filename);
		lgprintf("rename file '%s' as '%s'",tmpfn,filename);
	}
	lgprintf("���ݺ������ļ��ѳɹ��滻...");
	sprintf(sqlbf,"update dp.dp_datapart set status=30 where tabid=%d", tabid);
	psa->DoQuery(sqlbf);
	sprintf(sqlbf,"update dp.dp_datafilemap set procstatus=0 where tabid=%d and fileflag=0",tabid);
	lgprintf("����״̬�޸�Ϊ�����������(3),�����ļ�����״̬��Ϊδ����(0).");
	Load();
	return 1;
}


int DestLoader::RecreateIndex(SysAdmin *_Psa) 
{
	AutoMt mdf(psa->GetDTS(),MAX_MIDDLE_FILE_NUM);
	
	//�����Ҫ���ߵ�����
	mdf.FetchAll("select * from dp.dp_datapart where status=21");
	if(mdf.Wait()>0) {
	   tabid=mdf.GetInt("tabid",0);
	   int newld=mdf.GetInt("oldstatus",0)==4?1:0;
	   mdf.FetchAll("select * from dp.dp_datapart where tabid=%d and status!=21 and status!=5",
		  tabid);
	   if(mdf.Wait()<1) {
	    char tbname[150],idxname[150];
	    psa->GetTableName(tabid,-1,tbname,idxname,TBNAME_PREPONL);
	    AutoMt destmt(0,10);
	    psa->CreateDataMtFromFile(destmt,0,tabid,newld);
	    psa->CreateTableOnMysql(destmt,tbname,true);
	    psa->CreateAllIndex(tabid,TBNAME_PREPONL,true,CI_DAT_ONLY,-1);
	    psa->DataOnLine(tabid);
	    AutoStmt st(psa->GetDTS());
	    st.Prepare("update dp.dp_datapart set status=5 where tabid=%d and status=21",
		tabid);
	    st.Execute(1);
	    st.Wait();
	    st.Prepare("update dp.dp_datafilemap set procstatus=0 where tabid=%d and fileflag=0 ",
		tabid);
	    st.Execute(1);
	    st.Wait();
	    lgprintf("״̬4(DestLoaded)-->5(Complete),tabid:%d.",tabid);
	
	    lgprintf("ɾ���м���ʱ�ļ�...");
	    mdf.FetchAll("select * from dp.dp_middledatafile where tabid=%d",tabid);
	    {
		int dfn=mdf.Wait();
		for(int di=0;di<dfn;di++) {
			lgprintf("ɾ���ļ�'%s'",mdf.PtrStr("datafilename",di));
			unlink(mdf.PtrStr("datafilename",di));
			lgprintf("ɾ���ļ�'%s'",mdf.PtrStr("indexfilename",di));
			unlink(mdf.PtrStr("indexfilename",di));
		}
		lgprintf("ɾ����¼...");
		st.Prepare("delete from dp.dp_middledatafile where tabid=%d",tabid);
		st.Execute(1);
		st.Wait();
	    } 
	    psa->CleanData(false);
	  }
	}
	
	mdf.FetchAll("select * from dp.dp_datapart where (status =4 or status=40 ) limit 2");
	int rn=mdf.Wait();
	if(rn<1) {
		return 0;
	}
	datapartid=mdf.GetInt("datapartid",0);
	tabid=mdf.GetInt("tabid",0);
	bool preponl=mdf.GetInt("status",0)==4;
	//if(tabid<1) ThrowWith("�Ҳ��������:%d��Tabid",taskid);
	
	//check intergrity.
	mdf.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>0",tabid);
	rn=mdf.Wait();
	if(rn<1) 
		ThrowWith("Ŀ���%dȱ����������¼.",tabid);
	
	mdf.FetchAll("select distinct indexgid from dp.dp_datafilemap where tabid=%d and fileflag=%d",tabid,preponl?1:0);
	rn=mdf.Wait();
	mdf.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>0",tabid);
	int rni=mdf.Wait();
	if(rni!=rn) 
	{
		lgprintf("���ִ���: װ�����������(%d)�������������е�ֵ(%d)����.",
			rn,rni);
		return 0; //dump && destload(create temporary index table) have not complete.
	}
	char sqlbf[300];
	try {
		sprintf(sqlbf,"update dp.dp_datapart set oldstatus=status,status=%d where tabid=%d and datapartid=%d", 20,tabid,datapartid,preponl?4:40);
		if(psa->DoQuery(sqlbf)<1) 
			ThrowWith("����װ���ؽ����������޸�����״̬�쳣�����������������̳�ͻ��\n"
			"  tabid:%d.\n",
			tabid);
		
		lgprintf("��ʼ�����ؽ�,tabid:%d,�������� :%d",
			tabid,rn);
		psa->log(tabid,0,119,"��ʼ��������.");
		//2005/12/01 ������Ϊ�����������ؽ�(�޸�)��
		lgprintf("���������Ĺ��̿�����Ҫ�ϳ���ʱ�䣬�����ĵȴ�...");
		psa->RepairAllIndex(tabid,TBNAME_PREPONL,datapartid);
		lgprintf("�����������.");
		psa->log(tabid,0,120,"�����������.");
		AutoStmt st(psa->GetDTS());
		st.DirectExecute("update dp.dp_datapart set status=21 where tabid=%d and datapartid=%d",
		     tabid,datapartid);
		mdf.FetchAll("select * from dp.dp_datafilemap where tabid=%d and isfirstindex=1 and fileflag!=2 order by datapartid,indexgid,fileid",
			tabid);
		rn=mdf.Wait();
	}
	catch (...) {
		errprintf("���������ṹʱ�����쳣����,�ָ�����״̬...");
		sprintf(sqlbf,"update dp.dp_datapart set status=%d where tabid=%d and datapartid=%d", preponl?3:30,tabid,datapartid);
		psa->DoQuery(sqlbf);
		sprintf(sqlbf,"update dp.dp_datafilemap set procstatus=0 where tabid=%d and "
			"procstatus=1 and fileflag=%d and datapartid=%d",tabid,preponl?1:0,datapartid);
		psa->DoQuery(sqlbf);
  	        psa->log(tabid,0,124,"���������ṹʱ�����쳣����,�ѻָ�����״̬.");
		throw;
	}
	return 1;
}

thread_rt LaunchWork(void *ptr) 
{
	((worker *) ptr)->work();
	thread_end;
}

//���´����д���
//up to 2005/04/13, the bugs of this routine continuous produce error occursionnaly .
//   ReCompress sometimes give up last block of data file,but remain original index record in idx file.
int DestLoader::ReCompress(int threadnum)
{
	AutoMt mdt(psa->GetDTS(),MAX_DST_DATAFILENUM);
	AutoMt mdf(psa->GetDTS(),MAX_DST_DATAFILENUM);
	mdt.FetchAll("select distinct tabid,datapartid,status,compflag from dp.dp_datapart where (status=6 or status=9)");
	int rn1=mdt.Wait();
	int rn;
	int i=0;
	bool deepcmp;
	if(rn1<1) {
		return 0;
	}
	wociSetTraceFile("ddc����ѹ��/");
	for(i=0;i<rn1;i++) {

		tabid=mdt.GetInt("tabid",i);
		datapartid=mdt.GetInt("datapartid",i);
		deepcmp=mdt.GetInt("status",i)==6;
		mdf.FetchAll("select * from dp.dp_datafilemap where tabid=%d and procstatus =0 and (fileflag=0 or fileflag is null) order by datapartid,indexgid,fileid",tabid);
		rn=mdf.Wait();
		if(rn<1) {
			mdf.FetchAll("select * from dp.dp_datafilemap where tabid=%d and procstatus <>2 and (fileflag=0 or fileflag is null) order by datapartid,indexgid,fileid",tabid);
			rn=mdf.Wait();
			if(rn<1) {
				AutoStmt st1(psa->GetDTS());
				st1.Prepare("update dp.dp_datapart set status=%d where tabid=%d",
					deepcmp?7:10,tabid);
				st1.Execute(1);
				st1.Wait();
				st1.Prepare("update dp.dp_datafilemap set procstatus =0 where tabid=%d and fileflag=0",tabid);
				st1.Execute(1);
				st1.Wait();
				lgprintf("����ѹ����������ɣ�����״̬���޸�Ϊ%d,�����ļ�����״̬�޸�Ϊ����(0)",deepcmp?7:10);
			}
			else lgprintf("��%d(%d)---����ѹ������δ���,����û�еȴ�ѹ��������",tabid,datapartid);
		}
		else break;
	}
	if(i==rn1) return 0;
	
	//��ֹ���룬�޸������ļ�״̬��
	psa->OutTaskDesc("��������ѹ������",mdt.GetInt("tabid",0),mdt.GetInt("datapartid",0));
	int compflag=mdt.GetInt("compflag",0);
	lgprintf("ԭѹ������:%d, �µ�ѹ������:%d .",mdf.GetInt("compflag",0),compflag);
	int fid=mdf.GetInt("fileid",0);
	psa->log(tabid,0,121,"����ѹ�������ͣ� %d-->%d ,�ļ���%d,��־�ļ� '%s' .",mdf.GetInt("compflag",0),compflag,fid,lgfile);

	char srcfn[300];
	strcpy(srcfn,mdf.PtrStr("filename",0));
	int origsize=mdf.GetInt("filesize",0);
	char dstfn[300];
	sprintf(dstfn,"%s.%s",srcfn,deepcmp?"depcp":"dep5");
	tabid=mdf.GetInt("tabid",0);
	mdf.FetchAll("select filename,idxfname from dp.dp_datafilemap where tabid=%d and fileid=%d and fileflag!=2",
		tabid,fid);
	if(mdf.Wait()<1)
		ThrowWith(" �Ҳ��������ļ���¼,dp_datafilemap�еļ�¼����,����.\n"
		" ��Ӧ�������ļ�Ϊ:'%s',�ļ����: '%d'",srcfn,fid);
	char idxdstfn[300];
	sprintf(idxdstfn,"%s.%s",mdf.PtrStr("idxfname",0),deepcmp?"depcp":"dep5");
	double dstfilelen=0;
	try {
		AutoStmt st(psa->GetDTS());
		st.Prepare("update dp.dp_datafilemap set procstatus=1 where tabid=%d and fileid=%d and procstatus=0 and fileflag!=2",
			tabid,fid);
		st.Execute(1);
		st.Wait();
		if(wociGetFetchedRows(st)!=1) {
			lgprintf("�����ļ�ѹ��ʱ״̬�쳣,tabid:%d,fid:%d,�������������̳�ͻ��"
				,tabid,fid);
			return 1;
		}
		file_mt idxf;
		lgprintf("���ݴ��������ļ�:'%s',�ֽ���:%d,�����ļ�:'%s'.",srcfn,origsize,mdf.PtrStr("idxfname",0));
		idxf.Open(mdf.PtrStr("idxfname",0),0);
		
		dt_file srcf;
		srcf.Open(srcfn,0,fid);
		dt_file dstf;
		dstf.Open(dstfn,1,fid);
		mdf.SetHandle(srcf.CreateMt());
		int lastoffset=dstf.WriteHeader(mdf,0,fid,srcf.GetNextFileName());
		
		dt_file idxdstf;
		idxdstf.Open(idxdstfn,1,fid);
		mdf.SetHandle(idxf.CreateMt());
		int idxrn=idxf.GetRowNum();
		idxdstf.WriteHeader(mdf,idxf.GetRowNum(),fid,idxf.GetNextFileName());
		if(idxf.ReadBlock(-1,0)<0)
			ThrowWith("�����ļ���ȡ����: '%s'",mdf.PtrStr("filename",0));
		AutoMt *pidxmt=(AutoMt *)idxf;
		//lgprintf("�������ļ�����%d����¼.",wociGetMemtableRows(*pidxmt));
		int *pblockstart=pidxmt->PtrInt("blockstart",0);
		int *pblocksize=pidxmt->PtrInt("blocksize",0);
		blockcompress bc(compflag);
		for(i=1;i<threadnum;i++) {
			bc.AddWorker(new blockcompress(compflag));
		}
#define BFNUM 10
		char *srcbf=new char[SRCBUFLEN];//ÿһ�δ����������ݿ飨��ѹ���󣩡�
		char *dstbf=new char[DSTBUFLEN*BFNUM];//���ۻ����������(ѹ����).
		int dstseplen=DSTBUFLEN;
		bool isfilled[BFNUM];
		int filledlen[BFNUM];
		int filledworkid[BFNUM];
		char *outcache[BFNUM];
		for(i=0;i<BFNUM;i++) {
			isfilled[i]=false;
			filledworkid[i]=0;
			outcache[i]=dstbf+i*DSTBUFLEN;
			filledlen[i]=0;
		}
		int workid=0;
		int nextid=0;
		int oldblockstart=pblockstart[0];
		int lastrow=0;
		int slastrow=0;
		bool iseof=false;
		bool isalldone=false;
		int lastdsp=0;
		mytimer tmr;
		tmr.Start();
		while(!isalldone) {//�ļ��������˳�
			if(srcf.ReadMt(-1,0,mdf,1,1,srcbf,false,true)<0) {
				iseof=true;
			}
				    if(wdbi_kill_in_progress) {
				    	wdbi_kill_in_progress=false;
					  	ThrowWith("�û�ȡ������!");
					  }
			block_hdr *pbh=(block_hdr *)srcbf;
			int doff=srcf.GetDataOffset(pbh);
			if(pbh->origlen+doff>SRCBUFLEN) 
				ThrowWith("Decompress data exceed buffer length. dec:%d,bufl:%d",
				pbh->origlen+sizeof(block_hdr),SRCBUFLEN);
			bool deliverd=false;
			while(!deliverd) { //���񽻸����˳�
				worker *pbc=NULL;
				if(!iseof) {
					pbc=bc.GetIdleWorker();
					if(pbc) {
						
						//pbc->Do(workid++,srcbf,pbh->origlen+sizeof(block_hdr),
						//	pbh->origlen/2); //Unlock internal
						pbc->Do(workid++,srcbf,pbh->origlen+doff,doff,
							pbh->origlen/2); //Unlock internal
						deliverd=true;
					}
				}
				pbc=bc.GetDoneWorker();
				while(pbc) {
					char *pout;
					int dstlen=pbc->GetOutput(&pout);//Unlock internal;
					int doneid=pbc->GetWorkID();
					if(dstlen>dstseplen) 
						ThrowWith("Ҫѹ��������:%d,������������:%d.",dstlen,dstseplen);
					//get empty buf:
					for(i=0;i<BFNUM;i++) if(!isfilled[i]) break;
					if(i==BFNUM) ThrowWith("Write cache buffer fulled!.");
					memcpy(outcache[i],pout,dstlen);
					filledworkid[i]=doneid;
					filledlen[i]=dstlen;
					isfilled[i]=true;
					pbc=bc.GetDoneWorker();
					//lgprintf("Fill to cache %d,doneid:%d,len:%d",i,doneid,dstlen);
				}
				bool idleall=bc.isidleall();
				for(i=0;i<BFNUM;i++) {
					if(isfilled[i] && filledworkid[i]==nextid) {
						int idxrn1=wociGetMemtableRows(*pidxmt);
						for(;pblockstart[lastrow]==oldblockstart;) {
							pblockstart[lastrow]=lastoffset;
							pblocksize[lastrow++]=filledlen[i];
							slastrow++;
							if(lastrow==idxrn1) {
								idxdstf.WriteMt(*pidxmt,compflag,0,false);
								pidxmt->Reset();
								lastrow=0;
								if(idxf.ReadBlock(-1,0)>0) {
									//ThrowWith("�����ļ���ȡ����: '%s'",mdf.PtrStr("filename",0));
									pidxmt=(AutoMt *)idxf;
									//lgprintf("�������ļ�����%d����¼.",wociGetMemtableRows(*pidxmt));
									pblockstart=pidxmt->PtrInt("blockstart",0);
									pblocksize=pidxmt->PtrInt("blocksize",0);
								}
							}
							else if(lastrow>idxrn1) 
								ThrowWith("�����ļ���ȡ����: '%s'",mdf.PtrStr("filename",0));
							
						}
						lastoffset=dstf.WriteBlock(outcache[i],filledlen[i],0,true);
						oldblockstart=pblockstart[lastrow];
						dstfilelen+=filledlen[i];
						filledworkid[i]=0;
						filledlen[i]=0;
						isfilled[i]=false;
						nextid++;
						tmr.Stop();
						double tm1=tmr.GetTime();
						if(nextid-lastdsp>=50) { 
							printf("�Ѵ���%d�����ݿ�(%d%%),%.2f(MB/s) %.0f--%.0f.\r",nextid,slastrow*100/idxrn,lastoffset/tm1/1024/1024,tm1,tm1/slastrow*(idxrn-slastrow));
							fflush(stdout);
							lastdsp=nextid;
						}
						i=-1; //Loop from begining.
					}
				}
				if(idleall && iseof) {
					//					if(bc.isidleall()) {
					isalldone=true;
					break;
					//					}
				}
				if(!pbc) 
					mSleep(20);
			}
		}
		if(lastrow!=wociGetMemtableRows(*pidxmt)) 
			ThrowWith("�쳣���󣺲����������ݶ��������Ѵ���%d,Ӧ����%d.",lastrow,wociGetMemtableRows(*pidxmt));
		if(wociGetMemtableRows(*pidxmt)>0)
		  idxdstf.WriteMt(*pidxmt,compflag,0,false);
		dstf.Close();
		idxdstf.Close();
		st.Prepare("update dp.dp_datafilemap set procstatus=2 where tabid=%d and fileid=%d and procstatus=1 and fileflag=0",
			tabid,fid);
		st.Execute(1);
		st.Wait();
		delete []srcbf;
		delete []dstbf;
	}
	catch(...) {
		errprintf("���ݶ���ѹ�������쳣���ļ�����״̬�ָ�...");
		AutoStmt st(psa->GetDTS());
		st.Prepare("update dp.dp_datafilemap set procstatus=0 where tabid=%d and fileid=%d and procstatus=1 and fileflag=0",
			tabid,fid);
		st.Execute(1);
		st.Wait();
		errprintf("ɾ�������ļ��������ļ�");
		unlink(dstfn);
		unlink(idxdstfn);
		throw;
	}
	
	psa->log(tabid,0,122,"����ѹ������,�ļ�%d����С%d->%d",fid,origsize,dstfilelen);
	lgprintf("�ļ�ת������,Ŀ���ļ�:'%s',�ļ�����(�ֽ�):%f.",dstfn,dstfilelen);
	return 1;
}


int DestLoader::ToMySQLBlock(const char *dbn, const char *tabname)
{
	lgprintf("��ʽת�� '%s.%s' ...",dbn,tabname);
	AutoMt mt(psa->GetDTS(),100);
	mt.FetchAll("select * from dp.dp_table where tabname=lower('%s') and databasename=lower('%s')",tabname,dbn);
	int rn=mt.Wait();
	if(rn<1) {
		printf("��'%s'������!",tabname);
		return 0;
	}
	tabid=mt.GetInt("tabid",0);
	int recordnum=mt.GetInt("recordnum",0);
	int firstdatafileid=mt.GetInt("firstdatafileid",0);
	if(recordnum<1) {
		lgprintf("Դ��'%s'����Ϊ��.",tabname);
		return 0;
	}
	AutoMt mdt(psa->GetDTS(),MAX_DST_DATAFILENUM);
	AutoMt mdf(psa->GetDTS(),MAX_DST_DATAFILENUM);
	wociSetTraceFile("rawBlock��ʽת��/");
	mdf.FetchAll("select * from dp.dp_datafilemap where tabid=%d and procstatus =0 and (fileflag=0 or fileflag is null) order by datapartid,indexgid,fileid",tabid);
	rn=mdf.Wait();
	//��ֹ���룬�޸������ļ�״̬��
	int fid=mdf.GetInt("fileid",0);
	char srcfn[300];
	strcpy(srcfn,mdf.PtrStr("filename",0));
	int origsize=mdf.GetInt("filesize",0);
	char dstfn[300];
	sprintf(dstfn,"%s.%s",srcfn,"dep5");
	tabid=mdf.GetInt("tabid",0);
	
	mdf.FetchAll("select idxfname as filename from dp.dp_datafilemap where tabid=%d and fileid=%d and fileflag!=2",
		tabid,fid);
	rn=mdf.Wait();
	char idxdstfn[300];
	sprintf(idxdstfn,"%s.%s",mdf.PtrStr("filename",0),"dep5");
	double dstfilelen=0;
	try {
		AutoStmt st(psa->GetDTS());
		st.Prepare("update dp.dp_datafilemap set procstatus=1 where tabid=%d and fileid=%d and procstatus=0 and fileflag!=2",
			tabid,fid);
		st.Execute(1);
		st.Wait();
		if(wociGetFetchedRows(st)!=1) {
			lgprintf("�����ļ�ת��ʱ״̬�쳣,tabid:%d,fid:%d,�������������̳�ͻ��"
				,tabid,fid);
			return 1;
		}
		file_mt idxf;
		lgprintf("���ݴ��������ļ�:'%s',�ֽ���:%d,�����ļ�:'%s'.",srcfn,origsize,mdf.PtrStr("filename",0));
		idxf.Open(mdf.PtrStr("filename",0),0);
		if(idxf.ReadBlock(-1,0)<0)
			ThrowWith("�����ļ���ȡ����: '%s'",mdf.PtrStr("filename",0));
		
		file_mt srcf;
		srcf.Open(srcfn,0,fid);
		dt_file dstf;
		dstf.Open(dstfn,1,fid);
		mdf.SetHandle(srcf.CreateMt());
		int lastoffset=dstf.WriteHeader(mdf,0,fid,srcf.GetNextFileName());
		
		AutoMt *pidxmt=(AutoMt *)idxf;
		int idxrn=wociGetMemtableRows(*pidxmt);
		lgprintf("�������ļ�����%d����¼.",idxrn);
		int *pblockstart=pidxmt->PtrInt("blockstart",0);
		int *pblocksize=pidxmt->PtrInt("blocksize",0);
		int lastrow=0;
		int oldblockstart=pblockstart[0];
		int dspct=0;
		while(true) {//�ļ��������˳�
				    if(wdbi_kill_in_progress) {
				    	wdbi_kill_in_progress=false;
					  	ThrowWith("�û�ȡ������!");
					  }
			int srcmt=srcf.ReadBlock(-1,0,1);
			if(srcmt==0) break;
			int tmpoffset=dstf.WriteMySQLMt(srcmt,COMPRESSLEVEL);
			int storesize=tmpoffset-lastoffset;
			for(;pblockstart[lastrow]==oldblockstart;) {
				pblockstart[lastrow]=lastoffset;
				pblocksize[lastrow++]=storesize;
			}
			if(++dspct>1000) {
				dspct=0;
				printf("\r...%d%% ",lastrow*100/idxrn);
				fflush(stdout);
				//			break;
			}
			lastoffset=tmpoffset;
			oldblockstart=pblockstart[lastrow];
		}
		dt_file idxdstf;
		idxdstf.Open(idxdstfn,1,fid);
		//mdf.SetHandle(idxf.CreateMt());
		idxdstf.WriteHeader(*pidxmt,idxrn,fid,idxf.GetNextFileName());
		dstfilelen=lastoffset;
		idxdstf.WriteMt(*pidxmt,COMPRESSLEVEL,0,false);
		dstf.Close();
		idxdstf.Close();
		st.Prepare("update dp.dp_datafilemap set procstatus=2 where tabid=%d and fileid=%d and procstatus=1 and fileflag=0",
			tabid,fid);
		st.Execute(1);
		st.Wait();
	}
	catch(...) {
		errprintf("����ת�������쳣���ļ�����״̬�ָ�...");
		AutoStmt st(psa->GetDTS());
		st.Prepare("update dp.dp_datafilemap set procstatus=0 where tabid=%d and fileid=%d and procstatus=1 and fileflag=0",
			tabid,fid);
		st.Execute(1);
		st.Wait();
		errprintf("ɾ�������ļ��������ļ�");
		unlink(dstfn);
		unlink(idxdstfn);
		throw;
	}
	
	lgprintf("�ļ�ת������,Ŀ���ļ�:'%s',�ļ�����(�ֽ�):%f.",dstfn,dstfilelen);
	return 1;
}
int DestLoader::RemoveTable(const char *dbn, const char *tabname,bool prompt)
{
	char sqlbuf[1000];
	char choose[200];
	wociSetEcho(FALSE); 
	lgprintf("remove table '%s.%s ' ...",dbn,tabname);
	AutoMt mt(psa->GetDTS(),MAX_DST_DATAFILENUM);
	AutoStmt st(psa->GetDTS());
	mt.FetchAll("select * from dp.dp_table where tabname=lower('%s') and databasename=lower('%s')",tabname,dbn);
	int rn=mt.Wait();
	if(rn<1) 
		ThrowWith("��%s.%s��dp_table���Ҳ���!.",dbn,tabname);
	tabid=mt.GetInt("tabid",0);
	double recordnum=mt.GetDouble("recordnum",0);
	int firstdatafileid=mt.GetInt("firstdatafileid",0);
	{
		char fulltbname[300];
		sprintf(fulltbname,"%s.%s",dbn,tabname);

		sprintf(sqlbuf,"drop table %s.%s",dbn,tabname);
		lgprintf(sqlbuf);
		if(psa->TouchTable(fulltbname)) psa->DoQuery(sqlbuf);
		{
			lgprintf("ɾ��DP�����ļ�.");
			char streamPath[300];
			sprintf(streamPath,"%s%s/%s.DTP",psa->GetMySQLPathName(0,"msys"),dbn,tabname);
			unlink(streamPath);
		}
	}
	if(rn<1) {
		lgprintf("��'%s.%s'��ɾ��.",dbn,tabname);
		return 0;
	}
	
	if(prompt) {
		if(recordnum<1)
			sprintf(choose,"DP��'%s.%s'����ɾ��������Ϊ�գ�������(Y/N)",dbn,tabname);
		else
			sprintf(choose,"DP��'%s.%s'����ɾ������¼��:%.0f��(Y/N)",dbn,tabname,recordnum);
		if(!GetYesNo(choose,false)) {
			lgprintf("ȡ��ɾ���� ");
			return 0;
		}			
	}
	psa->CloseTable(tabid,NULL,true);
	
	
	//����Ĳ�ѯ����Ҫ��fileflag!=2����������������ȫ��ɾ����
	mt.FetchAll("select * from dp.dp_datafilemap where tabid=%d ",
		tabid);
	rn=mt.Wait();
	int i=0;
	for(i=0;i<rn;i++) {
		char tmp[300];
		lgprintf("ɾ��'%s'�͸��ӵ�depcp,dep5�ļ�",mt.PtrStr("filename",i));
		unlink(mt.PtrStr("filename",i));
		sprintf(tmp,"%s.depcp",mt.PtrStr("filename",i));
		unlink(tmp);
		sprintf(tmp,"%s.dep5",mt.PtrStr("filename",i));
		unlink(tmp);
		lgprintf("ɾ��'%s'�͸��ӵ�depcp,dep5�ļ�",mt.PtrStr("idxfname",i));
		unlink(mt.PtrStr("idxfname",i));
		sprintf(tmp,"%s.depcp",mt.PtrStr("idxfname",i));
		unlink(tmp);
		sprintf(tmp,"%s.dep5",mt.PtrStr("idxfname",i));
		unlink(tmp);
	}
	//�������䲻��Ҫ��fileflag!=2����������������ȫ��ɾ����
	st.Prepare(" delete from dp.dp_datafilemap where tabid=%d ",tabid);
	st.Execute(1);
	st.Wait();
	
	st.Prepare(" delete from dp.dp_middledatafile where tabid=%d",tabid);
	st.Execute(1);
	st.Wait();
	
	bool forcedel=false;
	//if(prompt) {
		sprintf(choose,"��'%s.%s'�����ò���ҲҪɾ����?(Y/N)",dbn,tabname);
		forcedel=GetYesNo(choose,false);
	//}
	if(forcedel) {
		mt.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>0",tabid);
		rn=mt.Wait();
		for(i=0;i<rn;i++) {
			char tmp[300];
			psa->GetTableName(tabid,mt.GetInt("indexgid",i),NULL,tmp,TBNAME_DEST);
			sprintf(sqlbuf,"drop table %s",tmp);
			lgprintf(sqlbuf);
			if(psa->TouchTable(tmp)) psa->DoQuery(sqlbuf);
		}
		st.Prepare(" delete from dp.dp_index where tabid=%d",tabid);
		st.Execute(1);
		st.Wait();
		st.Prepare(" delete from dp.dp_datapart where tabid=%d",tabid);
		st.Execute(1);
		st.Wait();
		st.Prepare(" delete from dp.dp_table where tabid=%d",tabid);
		st.Execute(1);
		st.Wait();
	}
	else {
		st.Prepare(" update dp.dp_table set recordnum=0,lstfid=0,cdfileid=0,firstdatafileid=0 where tabid=%d",tabid);
		st.Execute(1);
		st.Wait();
	}//psa->EmptyIndex(tabid);
	
	lgprintf("��'%s.%s'��ɾ��%s.",dbn,tabname,forcedel?"":",����������");
	return 1;
}

// �����ļ�״̬Ϊ2���ȴ�ɾ��������ȫ��������������ݳɹ����ߺ�ִ�С�
int SysAdmin::CleanData(bool prompt)
{
	AutoMt mt(dts,100);
	AutoStmt st(dts);
	mt.FetchAll("select tabid,indexgid,sum(recordnum) recordnum from dp.dp_datafilemap where fileflag=2 group by tabid,indexgid");
	int rn=mt.Wait();
	if(rn<1) {
		printf("û��������Ҫ���!");
		return 0;
	}
	lgprintf("����ɾ������...");
	//for(int i=0;i<rn;i++) {
	int tabid=mt.GetInt("tabid",0);
	//	int recordnum=mt.GetInt("recordnum",0);
	char tbname[150],idxname[150];
	GetTableName(tabid,-1,tbname,idxname,TBNAME_FORDELETE);
	AutoMt dtmt(dts,MAX_DST_DATAFILENUM);
	dtmt.FetchAll("select filename from dp.dp_datafilemap where tabid=%d  and fileflag=2",
		tabid);
	int frn=dtmt.Wait();
	AutoMt idxmt(dts,200);
	idxmt.FetchAll("select idxfname as filename from dp.dp_datafilemap where tabid=%d  and fileflag=2",
		tabid);
	idxmt.Wait();
	//int firstdatafileid=mt.GetInt("firstdatafileid",0);
	//int srctabid=mt.GetInt("srctabid",0);
	if(prompt)
	{
		while(true) {
			printf("\n�� '%s': ����ɾ�������:%d,����?(Y/N)?",
				tbname,tabid);
			char ans[100];
			fgets(ans,100,stdin);
			if(tolower(ans[0])=='n') {
				lgprintf("ȡ��ɾ���� ");
				return 0;
			}
			if(tolower(ans[0])=='y') break;
		}
	}
	
	for(int j=0;j<frn;j++) {
		lgprintf("ɾ��'%s'�͸��ӵ�depcp,dep5�ļ�",dtmt.PtrStr("filename",j));
		unlink(dtmt.PtrStr("filename",j));
		char tmp[300];
		sprintf(tmp,"%s.depcp",dtmt.PtrStr("filename",j));
		unlink(tmp);
		sprintf(tmp,"%s.dep5",dtmt.PtrStr("filename",j));
		unlink(tmp);
		lgprintf("ɾ��'%s'�͸��ӵ�depcp,dep5�ļ�",idxmt.PtrStr("filename",j));
		unlink(idxmt.PtrStr("filename",j));
		sprintf(tmp,"%s.depcp",idxmt.PtrStr("filename",j));
		unlink(tmp);
		sprintf(tmp,"%s.dep5",idxmt.PtrStr("filename",j));
		unlink(tmp);
	}
	st.Prepare(" delete from dp.dp_datafilemap where tabid=%d  and fileflag=2",tabid);
	st.Execute(1);
	st.Wait();
	//ǧ����ɾ!!!!!!!!!!!
	//st.Prepare(" delete from dt_table where tabid=%d",tabid);
	//st.Execute(1);
	//st.Wait();
	DropDTTable(tabid,TBNAME_FORDELETE);
	lgprintf("��'%s'�����ݡ������ļ���ɾ��.",tbname);
	return 1;
}

//�������Ͳ�������������ɾ��(ֻҪ����)
void SysAdmin::DropDTTable(int tabid,int nametype) {
	char sqlbf[300];
	AutoMt mt(dts,MAX_DST_INDEX_NUM);
	char tbname[150],idxname[150];
	mt.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>0",tabid);
	int rn=mt.Wait();
	if(rn<1) ThrowWith("�Ҳ���%d��Ķ�������.",tabid);
	for(int i=0;i<rn;i++) {
		GetTableName(tabid,mt.GetInt("indexgid",i),tbname,idxname,nametype);
		sprintf(sqlbf,"drop table %s",idxname);
		if(conn.TouchTable(idxname))
			DoQuery(sqlbf);
		int ilp=0;
		while(GetTableName(tabid,mt.GetInt("indexgid",i),tbname,idxname,nametype,ilp++)) {
		  sprintf(sqlbf,"drop table %s",idxname);
		  if(conn.TouchTable(idxname))
			DoQuery(sqlbf);
		}
	}
	sprintf(sqlbf,"drop table %s",tbname);
	if(conn.TouchTable(tbname))
		DoQuery(sqlbf);
}

//		
//���ز��������ֶε������ֶ���
//destmt:Ŀ�����ڴ��,���ֶθ�ʽ��Ϣ
//indexid:�������
int SysAdmin::CreateIndexMT(AutoMt &idxtarget,int destmt,int tabid,int indexid,int *colidx,char *colsname,bool update_idxtb) {
	bool ec=wociIsEcho();
	wociSetEcho(FALSE);
	AutoMt idxsubmt(dts,10);
	idxsubmt.FetchAll("select * from dp.dp_index where tabid=%d and indexgid=%d and issoledindex>0",tabid,indexid);
	int rn=idxsubmt.Wait();
	if(rn<1) {
		ThrowWith("���������ṹ:��dp.dp_index����%d���������ļ�¼��",indexid);
	}
	wociClear(idxtarget);
	strcpy(colsname,idxsubmt.PtrStr("columnsname",0));
	wociCopyColumnDefine(idxtarget,destmt,colsname);
	//���Ҹö������������ķǶ������������Դ�Ϊ���ݽ���������
	idxsubmt.FetchAll("select * from dp.dp_index where tabid=%d and indexgid=%d and issoledindex=0 order by seqinidxtab",
		tabid,indexid);
	int srn=idxsubmt.Wait();
	for(int j=0;j<srn;j++) {
		//�ظ����ֶ��Զ��޳�
		wociCopyColumnDefine(idxtarget,destmt,idxsubmt.PtrStr("columnsname",j));
	}
	//�ع��������ö�
	char reusedcols[300];
	int cn1;
	cn1=wociConvertColStrToInt(destmt,colsname,colidx);
	reusedcols[0]=0;
	int tcn=wociGetMtColumnsNum(idxtarget);
	if(tcn>cn1) {
		for(int i=cn1;i<tcn;i++) {
			if(i!=cn1) strcat(reusedcols,",");
			wociGetColumnName(idxtarget,i,reusedcols+strlen(reusedcols));
		}
		strcat(colsname,",");
		strcat(colsname,reusedcols);
	}
	if(update_idxtb) {
		lgprintf("�޸�����%d�ĸ����ֶ�Ϊ'%s'.",indexid,reusedcols);
		AutoStmt st(dts);
		if(strlen(reusedcols)>0)
			st.Prepare("update dp.dp_index set reusecols='%s' where tabid=%d and indexgid=%d and issoledindex>0",
			reusedcols,tabid,indexid);
		else
			st.Prepare("update dp.dp_index set reusecols=null where tabid=%d and indexgid=%d and issoledindex>0",
			 tabid,indexid);
		st.Execute(1);
		st.Wait();
	}
	
	//���������ֶ�
	wociAddColumn(idxtarget,"dtfid",NULL,COLUMN_TYPE_INT,10,0);
	wociAddColumn(idxtarget,"blockstart",NULL,COLUMN_TYPE_INT,10,0);
	wociAddColumn(idxtarget,"blocksize",NULL,COLUMN_TYPE_INT,10,0);
	wociAddColumn(idxtarget,"blockrownum",NULL,COLUMN_TYPE_INT,10,0);
	wociAddColumn(idxtarget,"startrow",NULL,COLUMN_TYPE_INT,10,0);
	wociAddColumn(idxtarget,"idx_rownum",NULL,COLUMN_TYPE_INT,10,0);
	idxtarget.Build();
	//ȡ���������͸���������blockmt(Ŀ�����ݿ��ڴ��)�ṹ�е�λ�ã�
	// ���ṹ�����ļ������������Ƿ��ϵͳ��������ָ�����ֶ�����ͬ��
	int bcn=cn1;//wociConvertColStrToInt(destmt,colsname,colidx);(colsname��reusedcols��������ͬ��
	bcn+=wociConvertColStrToInt(destmt,reusedcols,colidx+bcn);
	if(wociGetColumnNumber(idxtarget)!=bcn+6) {
		ThrowWith("Column number error,colnum:%d,deserved:%d",
			wociGetColumnNumber(idxtarget),bcn+6);
	}
	//����dt_index�е�idxfieldnum
	if(update_idxtb) {
		lgprintf("�޸�%d�����������������ֶ�����Ϊ%d.",indexid,bcn);
		AutoStmt st(dts);
		st.Prepare("update dp.dp_index set idxfieldnum=%d where tabid=%d and indexgid=%d ",
			bcn,tabid,indexid);
		st.Execute(1);
		st.Wait();
	}
	wociSetEcho(ec);
	return bcn;
}

//datapartid==-1:ȱʡֵ,��ʾ�����з����Ͻ���������
//  !=-1: ֻ��ָ�������Ͻ���������
void SysAdmin::CreateAllIndexTable(int tabid,int destmt,int nametype,bool createidx,int ci_type,int datapartid) {
	AutoMt mt(dts,100);
	mt.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>0 order by seqindattab",tabid);
	int rn=mt.Wait();
	for(int i=0;i<rn;i++) 
		CreateIndexTable(tabid,mt.GetInt("indexgid",i),-1,destmt,nametype,createidx,datapartid);
}


void SysAdmin::CreateMergeIndexTable(int tabid) {
	AutoMt mtidx(dts,MAX_DST_DATAFILENUM);
	mtidx.FetchAll("select indexgid from dp.dp_index where tabid=%d and issoledindex>0 order by indexgid",tabid);
	int irn=mtidx.Wait();
	AutoMt mt(dts,MAX_DST_DATAFILENUM);
	mt.FetchAll("select * from dp.dp_datapart where tabid=%d order by datapartid",tabid);
	int dpn=mt.Wait();
	char tbname[300],idxname[300],sqlbf[3000];
	for(int idx=0;idx<irn;idx++) {
	 int indexid=mtidx.GetInt("indexgid",idx);
	 GetTableName(tabid,indexid,tbname,idxname,TBNAME_DEST,mt.GetInt("datapartid",0));
	 if(!conn.Touch(idxname))
	  ThrowWith("����������%s������,�޷�����Ŀ��������. tabid:%d,indexid:%d,datapartid:%d",
	   idxname,tabid,indexid,mt.GetInt("datapartid",0));
	 AutoMt idxmt(dts,100);
	 idxmt.FetchAll("select * from %s limit 10",idxname);
	 idxmt.Wait();
	 GetTableName(tabid,indexid,tbname,idxname,TBNAME_DEST);
	 if(conn.Touch(idxname)) {
		sprintf(sqlbf,"drop table %s",idxname);
		conn.DoQuery(sqlbf);
	 }
	 wociGetCreateTableSQL(idxmt,sqlbf,idxname,true);
	 strcat(sqlbf,"  TYPE=MERGE UNION=( ");
	 for(int part=0;part<dpn;part++) {
	   GetTableName(tabid,indexid,tbname,idxname,TBNAME_DEST,mt.GetInt("datapartid",part));
	   strcat(sqlbf,idxname);
	   strcat(sqlbf,(part+1)==dpn?") INSERT_METHOD=LAST;":",");
	 }
	 printf("\nmerge syntax :\n%s.\n",sqlbf);
	 conn.DoQuery(sqlbf);
	 CreateIndex(tabid,indexid,TBNAME_DEST,true,CI_IDX_ONLY,-1,-1)
	}
}

//���indexmtΪ-1����destmt������Ч��
// datapartid==-1,�������з���������
//  datapartid!=-1,ֻ�����ƶ�����������
void SysAdmin::CreateIndexTable(int tabid,int indexid,int indexmt,int destmt,int nametype,bool createidx,int ci_type,int datapartid) {
	AutoMt targetidxmt(dts,10);
	if(indexmt==-1) {
		int colidx[50];
		char colsname[600];
		int cn=CreateIndexMT(targetidxmt,destmt,tabid,indexid,colidx,colsname,false);
		indexmt=targetidxmt;
	}
	char tbname[300],idxname[300];
	int ilp=0;
	while(true) {
	 if(datapartid==-1 && ! GetTableName(tabid,indexid,tbname,idxname,nametype,ilp++)) break;
	 if(datapartid>=0)
	   GetTableName(tabid,indexid,tbname,idxname,nametype,-1,datapartid);
	 
	 CreateTableOnMysql(indexmt,idxname,true);
	 if(createidx) {
		//2005/12/01�޸�,���Ӵ���������/Ŀ������
		CreateIndex(tabid,indexid,nametype,true,ci_type,datapartid==-1?ilp-1:-1,datapartid);//tabname,idxtabname,0,conn,tabid,
		//indexid,true);
	 }
	 if(datapartid!=-1) break;
        }
}


int SysAdmin::CreateDataMtFromFile(AutoMt &destmt,int rownum,int tabid,int fileflag) {
	AutoMt mt(dts,10);
	mt.FetchAll("select filename from dp.dp_datafilemap where tabid=%d and fileflag=%d limit 2",tabid,fileflag);
	int rn=mt.Wait();
	if(rn<1) ThrowWith("����Ŀ���ṹʱ�Ҳ��������ļ���");
	dt_file idf;
	idf.Open(mt.PtrStr("filename",0),0);
	destmt.SetHandle(idf.CreateMt(rownum));
	return wociGetColumnNumber(destmt);
}

int SysAdmin::CreateIndexMtFromFile(AutoMt &indexmt,int rownum,int tabid,int indexid) {
	AutoMt mt(dts,10);
	mt.FetchAll("select idxfname as filename from dp.dp_datafilemap where tabid=%d and indexgid=%d  and (fileflag=0 or fileflag is null) limit 2",tabid,indexid);
	int rn=mt.Wait();
	if(rn<1) ThrowWith("����������ṹʱ�Ҳ��������ļ���");
	dt_file idf;
	idf.Open(mt.PtrStr("filename",0),0);
	indexmt.SetHandle(idf.CreateMt(rownum));
	return wociGetColumnNumber(indexmt);
}

void SysAdmin::OutTaskDesc(const char *prompt,int tabid,int datapartid,int indexid)
{
	AutoMt mt(dts,100);
	char tinfo[300];
	tinfo[0]=0;
	lgprintf(prompt);
	if(datapartid>0 && tabid>0) {
		mt.FetchAll("select * from dp.dp_datapart where tabid=%d and datapartid=%d",tabid,datapartid);
		if(mt.Wait()<1) ThrowWith("��������ݷ���:%d->%d.",tabid,datapartid);
		lgprintf("�������� : %s.",mt.PtrStr("partdesc",0));
		lgprintf("���ݷ��� : %d.",datapartid);
		lgprintf("Դϵͳ : %d.",mt.GetInt("srcsysid",0));
		lgprintf("���ݳ�ȡ : \n%s.",mt.PtrStr("extsql",0));
	}
	if(indexid>0 && tabid>0) {
	  mt.FetchAll("select * from dp.dp_index where tabid=%d and indexgid=%d and issoledindex>0",tabid,indexid);
	  if(mt.Wait()<1) ThrowWith("������������:%d->%d.",tabid,indexid);
	  lgprintf("�������%d,�����ֶ�: %s.",indexid,mt.PtrStr("columnsname",0));
	}		
	if(tabid>0) {
		mt.FetchAll("select concat(databasename,'.',tabname) as dstdesc,srcowner,srctabname from dp.dp_table where tabid=%d",tabid);
		if(mt.Wait()>0) 
			lgprintf("Ŀ��� :%s,Դ��:%s.%s.",mt.PtrStr("dstdesc",0),mt.PtrStr("srcowner",0),mt.PtrStr("srctabname",0));
	}
}
