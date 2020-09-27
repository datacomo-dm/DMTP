#include "dt_common.h"
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include "dt_lib.h"
#include "zlib.h"
#include "dtio.h"
//>> Begin: JIRA:DM 198 , modify by liujs 
#include "IDumpfile.h" 
#include "DumpFileWrapper.h"
#include <vector>
#include <iostream>
//<< End:modify by liujs
#ifndef min
 #define min(a,b) ((a)>(b)?(b):(a))
#endif

DllExport bool wdbi_kill_in_progress;
#ifndef WIN32
#include <sys/wait.h> 
#endif
// 2005/08/27�޸ģ�partid��Ч��sub	

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
	FILE *fsrc=fopen(srcf,"rb");
	if(fsrc!=NULL) {
		fclose(fsrc) ;
		sprintf(dstf,"%s%s/%s%s",path,ddn,dtn,".DTP");
		mCopyFile(srcf,dstf);
	}	
	sprintf(srcf,"%s%s/%s%s",path,sdn,stn,".MRG");
	fsrc=fopen(srcf,"rb");
	if(fsrc!=NULL) {
		fclose(fsrc) ;
		sprintf(dstf,"%s%s/%s%s",path,ddn,dtn,".MRG");
		mCopyFile(srcf,dstf);
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
	FILE *fsrc=fopen(srcf,"rb");
	if(fsrc!=NULL) {
		fclose(fsrc) ;
		sprintf(dstf,"%s%s/%s%s",path,ddn,dtn,".DTP");
		rename(srcf,dstf);
	}
	sprintf(srcf,"%s%s/%s%s",path,sdn,stn,".MRG");
	fsrc=fopen(srcf,"rb");
	if(fsrc!=NULL) {
		fclose(fsrc) ;
		sprintf(dstf,"%s%s/%s%s",path,ddn,dtn,".MRG");
		rename(srcf,dstf);
	}
	
	return 1;
}

/**************************************************************************************************
  Function    : RemoveContinueSpace(char * sqlText)
  DateTime    : 2013/1/20 22:11	
  Description : ɾ���ַ����������ظ��Ŀո�create by liujs
  param       : sqlText[input/output]
**************************************************************************************************/
void  RemoveContinueSpace(char * sqlText)
{	
	//--------- trim right
	for (int i=strlen(sqlText)-1;i>0;i--)
	{
		if (sqlText[i] == ' ' || sqlText[i] == '\r' || sqlText[i] == '\n' || sqlText[i] == '\t' )  sqlText[i] = 0;
		else  break;
	}

	//---------- trim middle
	int validLen = 0;
	while(*sqlText) 
	{
		if (*sqlText == ' ' && *(sqlText+1) == ' '){
			strcpy(sqlText,sqlText+1);
			continue;
		}
		else{ validLen++;}
		sqlText++;
	}
	sqlText-=validLen;

	//--------- trim left
	int trimLeft = 0;
	int len = strlen(sqlText);
	while (*sqlText == ' ' || *sqlText == '\r' || *sqlText == '\n' || *sqlText == '\t')
	{
		sqlText++;
		trimLeft++;
	}
	strcpy(sqlText-trimLeft,sqlText);
}


DataDump::DataDump(int dtdbc,int maxmem,int _blocksize):fnmt(dtdbc,MAX_MIDDLE_FILE_NUM)
{
	this->dtdbc=dtdbc;
	//Create fnmt and build column structrues.
	//fnmt.FetchAll("select * from dt_middledatafile where rownum<1");
	fnmt.FetchAll("select * from dp.dp_middledatafile limit 3");
	fnmt.Wait();
	fnmt.Reset();
	indexid=0;
	memlimit=maxmem*(long)1024*1000;
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
#define max(a,b) (a>b?a:b)
void DataDump::ProcBlock(SysAdmin &sp,int partid,AutoMt *pCur,int idx,AutoMt &blockmt,int _fid)
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
	if(maxblockrn<MIN_BLOCKRN) {
        sp.log(dp.tabid,partid,DUMP_DST_TABLE_DATA_BLOCK_SIZE_ERROR,"��%d��Ŀ�������ݿ��С(%d)�����ʣ�����Ϊ%d.",dp.tabid,maxblockrn,MIN_BLOCKRN);
	    ThrowWith("Ŀ�������ݿ��С(%d)�����ʣ�����Ϊ%d.",maxblockrn,MIN_BLOCKRN);  
	}
	if(maxblockrn>MAX_BLOCKRN){
        sp.log(dp.tabid,partid,DUMP_DST_TABLE_DATA_BLOCK_SIZE_ERROR,"��%d��Ŀ�������ݿ��С(%d)�����ʣ����ܳ���%d.",dp.tabid,maxblockrn,MAX_BLOCKRN);
        ThrowWith("Ŀ�������ݿ��С(%d)�����ʣ����ܳ���%d.",maxblockrn,MAX_BLOCKRN);
	}
	AutoMt idxdt(0,10);
	wociCopyColumnDefine(idxdt,cur,idxcolsname);
	wociAddColumn(idxdt,"idx_blockoffset","",COLUMN_TYPE_INT,0,0);
	//	wociAddColumn(idxdt,"idx_storesize","",COLUMN_TYPE_INT,0,0);
	wociAddColumn(idxdt,"idx_startrow","",COLUMN_TYPE_INT,0,0);
	wociAddColumn(idxdt,"idx_rownum","",COLUMN_TYPE_INT,0,0);
	wociAddColumn(idxdt,"idx_fid","",COLUMN_TYPE_INT,0,0);

	//>> Begin: fix dm-249
	int idxrnlmt=min(max(FIX_MAXINDEXRN/wociGetRowLen(idxdt),2),MAX_BLOCKRN);
	//<< End:fix dm-249

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
	try {
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
					else
					{
					     sp.log(dp.tabid,partid,DUMP_INDEX_BLOCK_SIZE_ERROR,"��%d,����Ԥ�������,������%d,�����ֶ�'%s',�������������鳤��%d.",dp.tabid,partid,idxcolsname,MAX_BLOCKRN);
						 ThrowWith("����Ԥ�������,������%d,�����ֶ�'%s',�������������鳤��%d.",partid,idxcolsname,MAX_BLOCKRN);
					}
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
					else 
					{                                      
						sp.log(dp.tabid,partid,DUMP_INDEX_BLOCK_SIZE_ERROR,"��%d,����Ԥ�������,������%d,�����ֶ�'%s',�������������鳤��%d.",dp.tabid,partid,idxcolsname,MAX_BLOCKRN);					        	 
						ThrowWith("����Ԥ�������,������%d,�����ֶ�'%s',�������������鳤��%d.",partid,idxcolsname,MAX_BLOCKRN);
					}
				}
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
		// ��ֹ�������ʣ�
		int state=2;
		ptr[8]=&state;
		char nuldt[10];
		memset(nuldt,0,10);
		ptr[9]=now;//nuldt;
		ptr[10]=&dp.idxp[idx].idxid;
		ptr[11]=dp.idxp[idx].idxcolsname;
		ptr[12]=dp.idxp[idx].idxreusecols;
		int blevel=0;
		ptr[13]=&blevel;
		ptr[14]=NULL;
		wociInsertRows(fnmt,ptr,NULL,1);
	}
	}
	catch(...) {
		unlink(tmpidxfn);
		unlink(tmpfn);
		throw;
	}
	freeinfo1("End ProcBlock");
}
#ifndef strcasestr
/* hp-ux has not this function ,copy from linux define. */
 char *
 strcasestr(const char *s, const char *find)
 {
         char c, sc;
         size_t len;
 
         if ((c = *find++) != 0) {
                 c = (char)tolower((unsigned char)c);
                 len = strlen(find);
                 do {
                         do {
                                 if ((sc = *s++) == 0)
                                         return (NULL);
                         } while ((char)tolower((unsigned char)sc) != c);
                 } while (strncasecmp(s, find, len) != 0);
                 s--;
         }
         return ((char *)s);
 }

#endif

int DataDump::DoDump(SysAdmin &sp,const char *systype) {
        int tabid=0;
	AutoMt mdf(sp.GetDTS(),MAX_DST_DATAFILENUM);
	  //CMNET:�����ļ����������������������һ�������ĵ������Զ�����̲���
	//  mdf.FetchAll("select * from dp.dp_datapart where (status=0 or (status=72 and lower(extsql) like 'load %')) and begintime<now() %s order by blevel,touchtime,tabid,datapartid limit 2",sp.GetNormalTaskDesc());
        if(systype==NULL)
          mdf.FetchAll("select * from dp.dp_datapart where (status=0 or (status=72 and lower(extsql) like 'load %%')) and begintime<now() %s order by blevel,touchtime,tabid,datapartid limit 2",sp.GetNormalTaskDesc());
        else {
           printf("�������ͣ�%s\n",systype);
           mdf.FetchAll("select * from dp.dp_datapart where (status=0 or (status=72 and lower(extsql) like 'load %%')) and exists(select 1 from dp.dp_datasrc a where a.sysid=dp_datapart.srcsysid and a.systype in ( %s) ) and begintime<now() %s order by blevel,touchtime,tabid,datapartid limit 2",systype,sp.GetNormalTaskDesc ());
        }
        if(mdf.Wait()<1) return 0;
	//sp.GetFirstTaskID(NEWTASK,tabid,datapartid);
	sp.Reload();
	//CMNET:����������
	bool keepfiles=mdf.GetInt("status",0)==72;
	tabid=mdf.GetInt("tabid",0);
	datapartid=mdf.GetInt("datapartid",0);
	if(tabid<1) return 0;
	sp.SetTrace("dump",tabid);
	sorttm.Clear();
	fiotm.Clear();
	adjtm.Clear();
	sp.GetSoledIndexParam(datapartid,&dp,tabid);
	sp.OutTaskDesc("ִ�����ݵ�������: ",tabid,datapartid);
	if(xmkdir(dp.tmppath[0])) 
	{
	    sp.log(tabid,datapartid,DUMP_CREATE_PATH_ERROR,"��ʱ��·���޷�����,��:%d,������:%d,·��:%s.",tabid,datapartid,dp.tmppath[0]);
		ThrowWith("��ʱ��·���޷�����,��:%d,������:%d,·��:%s.",tabid,datapartid,dp.tmppath[0]);
	}
	AutoHandle srcdbc;
	AutoHandle fmtdbc;
	// datapartid��Ӧ��Դϵͳ����һ���Ǹ�ʽ���Ӧ��Դϵͳ
	// Jira:DM-48
	try {
	  srcdbc.SetHandle(sp.BuildSrcDBC(tabid,datapartid));
		fmtdbc.SetHandle(sp.BuildSrcDBC(tabid,-1));
	}catch(...) {
		sp.log(tabid,datapartid,DUMP_CREATE_DBC_ERROR,"Դ(Ŀ��)����Դ����ʧ��,��:%d,����:%d",tabid,datapartid);
		AutoStmt st(dtdbc);
		st.DirectExecute("update dp.dp_datapart set blevel=ifnull(blevel,0)+100 where tabid=%d and datapartid=%d",
		  tabid,datapartid);
		throw;
	}

	AutoMt srctstmt(0,10);
	int partoff=0;
	try {	
		//�����ʽ����Ŀ���Ľṹ��һ��
		sp.BuildMtFromSrcTable(fmtdbc,tabid,&srctstmt);
		srctstmt.AddrFresh();
		int srl=sp.GetMySQLLen(srctstmt);//wociGetRowLen(srctstmt);
		char tabname[150];
		sp.GetTableName(tabid,-1,tabname,NULL,TBNAME_DEST);
		AutoMt dsttstmt(dtdbc,10);
		dsttstmt.FetchFirst("select * from dp.dp_datapart where tabid=%d and status=5 ",tabid);
		int ndmprn=dsttstmt.Wait();
                //����ƶ����ֵ����⣺�����������е������ڶ��������ڵ�һ����ʼ���������������dp_table.lstfid����λ������������ļ���Ŵ���
                // ���������5�д���
                if(ndmprn==0) {
                  dsttstmt.FetchFirst("select * from dp.dp_middledatafile where tabid=%d and procstate>1 limit 10",tabid);
                  ndmprn=dsttstmt.Wait();
                  dsttstmt.FetchFirst("select * from dp.dp_datafilemap where tabid=%d limit 10",tabid);
                  ndmprn+=dsttstmt.Wait();
                }
		int tstrn=0;
		try {
			dsttstmt.FetchFirst("select * from %s",tabname);
			tstrn=dsttstmt.Wait();
		}
		catch(...)
		{
			sp.log(tabid,datapartid,DUMP_DST_TABLE_ERROR,"Ŀ���%d�����ڻ�ṹ����,��Ҫ���¹���,�Ѿ��������ع����.",tabid);   
			AutoStmt st(dtdbc);
			st.DirectExecute("update dp.dp_table set cdfileid=0 where tabid=%d",tabid);
			lgprintf("Ŀ���%d�����ڻ�ṹ����,��Ҫ���¹���,�Ѿ��������ع����.",tabid);
			throw;
		}
		if(srctstmt.CompareMt(dsttstmt)!=0 ) {
			if(tstrn>0 && ndmprn>0) 
			{
			    sp.log(tabid,datapartid,DUMP_DST_TABLE_FORMAT_MODIFIED_ERROR,"��%s���Ѿ������ݣ���Դ��(��ʽ��)��ʽ���޸ģ����ܵ������ݣ��뵼���µ�(�յ�)Ŀ����С�",tabname);   
				ThrowWith("��%s���Ѿ������ݣ���Դ��(��ʽ��)��ʽ���޸ģ����ܵ������ݣ��뵼���µ�(�յ�)Ŀ����С�",tabname);
			}
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
			dp.rowlen=srl;
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
	catch(...) {
		AutoStmt st(dtdbc);
		st.DirectExecute("update dp.dp_datapart set blevel=ifnull(blevel,0)+100 where tabid=%d and datapartid=%d",
		  tabid,datapartid);
		throw;
	}
	
	long realrn=memlimit/dp.rowlen;
	//>> Begin:DMA-458,�ڴ��¼�������ֵ����,20130129
	if(realrn > MAX_ROWS_LIMIT-8)
	{
	    realrn = MAX_ROWS_LIMIT - 8;	
	    lgprintf("������¼�����Ѿ�����2G�����ֽ����%d���޸�Ϊ%d��,�������̼���ִ��",memlimit/dp.rowlen,realrn);
	}
	//<< End:
	lgprintf("��ʼ��������,���ݳ�ȡ�ڴ�%ld�ֽ�(�ۺϼ�¼��:%d)",realrn*dp.rowlen,realrn);
	sp.log(tabid,datapartid,DUMP_BEGIN_DUMPING_NOTIFY,"��ʼ���ݵ���:���ݿ�%d�ֽ�(��¼��%d),��־�ļ� '%s'.",realrn*dp.rowlen,realrn,wociGetLogFile());
	sp.log(tabid,datapartid,DUMP_BEGIN_DUMPING_NOTIFY,"��ʼ������%d,����%d����",tabid,datapartid);
	//if(realrn>dp.maxrecnum) realrn=dp.maxrecnm;
	//CMNET: ����ʱ��ɾ������
	 {
		lgprintf("����ϴε���������...");
		
		AutoMt clsmt(dtdbc,100);
		AutoStmt st(dtdbc);
		int clsrn_tmp_files=0,clsrn_data_files=0;
		do {
			int i;
			
			clsmt.FetchAll("select * from dp.dp_middledatafile where datapartid=%d and tabid=%d %s limit 100",
			 datapartid,tabid,keepfiles?" and procstate>1 ":"");
			clsrn_tmp_files=clsmt.Wait();
			for(i=0;i<clsrn_tmp_files;i++) {
				unlink(clsmt.PtrStr("datafilename",i));
				unlink(clsmt.PtrStr("indexfilename",i));
			}
			st.Prepare("delete from dp.dp_middledatafile where datapartid=%d and tabid=%d %s limit 100",
			  datapartid,tabid,keepfiles?" and procstate>1 ":"");
			st.Execute(1);
			st.Wait();

			st.Prepare("delete from dp.dp_filelog where tabid=%d and datapartid=%d %s",tabid,datapartid,
			keepfiles? " and status<2":"");
			st.Execute(1);
			st.Wait();
			if(keepfiles) { //�������һ���� 
				 clsmt.FetchAll("select sum(recordnum) srn from dp.dp_middledatafile where tabid=%d and datapartid=%d and procstate<=3",
				   tabid,datapartid);
				 clsmt.Wait();
				 double srn=clsmt.GetDouble("srn",0);
				 clsmt.FetchAll("select sum(recordnum) srn from dp.dp_filelog where tabid=%d and datapartid=%d and status=2",
				   tabid,datapartid);
				 clsmt.Wait();
				 if(srn!=clsmt.GetDouble("srn",0)) {
				 	sp.log(tabid,datapartid,DUMP_FILE_LINES_ERROR,"�м�����%.0f��,�����ļ�%.0f�У���һ�£����ܼ�����һ�����ݳ�ȡ���������µ��롣",srn,clsmt.GetDouble("srn",0));
					ThrowWith("�м�����%.0f��,�����ļ�%.0f�У���һ�£����ܼ�����һ�����ݳ�ȡ���������µ��롣",
					   srn,clsmt.GetDouble("srn",0));
				 }

				//add by ljs:ɾ����һ�βɼ���û���γ���ʱ�ļ���Դ�����ļ�
				{
					AutoStmt st(dtdbc);
					st.Prepare("delete from dp.dp_filelog where  tabid=%d and datapartid=%d and status<2",tabid,datapartid);
					st.Execute(1);
					st.Wait();
				}
		     }
			
			clsmt.FetchAll("select * from dp.dp_datafilemap where fileflag=1 and datapartid=%d and tabid=%d limit 100",datapartid,tabid);
			clsrn_data_files=clsmt.Wait();
			for(i=0;i<clsrn_data_files;i++) {
				unlink(clsmt.PtrStr("filename",i));
				unlink(clsmt.PtrStr("idxfname",i));
			}
			st.Prepare("delete from dp.dp_datafilemap where fileflag=1 and datapartid=%d and tabid=%d limit 100",datapartid,tabid);
			st.Execute(1);
			st.Wait();
		} while(clsrn_data_files>0 || clsrn_tmp_files>0);
	}

	//realrn=50000;
	//indexparam *ip=&dp.idxp[dp.psoledindex];
	maxblockrn=blocksize/dp.rowlen;
	if(maxblockrn<MIN_BLOCKRN) {
		blocksize=MIN_BLOCKRN*dp.rowlen;
		maxblockrn=blocksize/dp.rowlen;
		if(blocksize<0 || blocksize>1024*1024*1024) {
		 		 sp.log(tabid,datapartid,DUMP_RECORD_NUM_ERROR,"��%d,����%d ��¼����̫��,�޷�Ǩ��(��¼����%d)��",tabid,datapartid,dp.rowlen);
		 return 0;
		}
		lgprintf("���ڼ�¼����̫��,���С����Ϊ%d,Ŀ���Ļ�����ƽ�ʧЧ��(��¼��:%d,��¼����%d)",blocksize,maxblockrn,dp.rowlen);
	}
	else if(maxblockrn>MAX_BLOCKRN) {
		blocksize=(MAX_BLOCKRN-1)*dp.rowlen;
		maxblockrn=blocksize/dp.rowlen;
		lgprintf("���ڼ�¼����̫С�����С����Ϊ%d�� (��¼��:%d,��¼����%d)",blocksize,maxblockrn,dp.rowlen);
	}

	//CMNET:�������봦��
	if(!keepfiles){
		//�������ݵ���ʱ���ÿ��¼��,�Ժ�Ĵ���Ͳ�ѯ�Դ�Ϊ����
		//�ֶ�maxrecinblock��ʹ�÷������Ϊ:������ݺ�̨���õĲ����Զ�����,�������ֻ̨��
		lgprintf("����Ŀ�����ݿ�%d�ֽ�(��¼��:%d)",maxblockrn*dp.rowlen,maxblockrn);
		AutoStmt st(dtdbc);
		st.Prepare("update dp.dp_table set maxrecinblock=%d where tabid=%d",maxblockrn,dp.tabid);
		st.Execute(1);
		st.Wait();
	}
	sp.Reload();

	//>> Begin: fix dm-249
	maxblockrn=min(sp.GetMaxBlockRn(tabid),MAX_BLOCKRN);
    //<< End: fix dm-249
    
	AutoMt blockmt(0,maxblockrn);
	fnmt.Reset();
	//int partid=0;
	fnorder=0;
	bool dumpcomplete=false;
	try {
		//CMNET:�������봦��,
		//if(!keepfiles)
			 sp.UpdateTaskStatus(DUMPING,tabid,datapartid);
		// ���������ֶ� touchtime,pid,hostname,��������ֻ���ظ�ִ�е�һ��������ı�����
		/*else 
		{
		 AutoStmt st(dtdbc);
		 st.Prepare("update dp.dp_datapart set touchtime=now() where tabid=%d and datapartid=%d",
		  tabid,datapartid);
		 st.Execute(1);
		 st.Wait();
		}*/
	}
	catch(char *str) {
		sp.log(tabid,datapartid,DUMP_UPDATE_TASK_STATUS_ERROR,str);
		lgprintf(str);
		return 0;
	}
	
	bool filesmode=false;
	LONG64 srn=0;
	char reg_backfile[300];
	try {
		bool ec=wociIsEcho();
		wociSetEcho(TRUE);
		
		if(sp.GetDumpSQL(tabid,datapartid,dumpsql)!=-1) {
            //>> Begin:fix jira dma-470,dma-471,dma-472,20130121
            RemoveContinueSpace(dumpsql);
	        //<< End 
		
			//idxdt.Reset();
			sp.log(tabid,datapartid,99,"���ݳ�ȡsql:%s.",dumpsql);
			TradeOffMt dtmt(0,realrn);
			blockmt.Clear();
			sp.BuildMtFromSrcTable(fmtdbc,tabid,&blockmt);
			//blockmt.Build(stmt);
			blockmt.AddrFresh();
			sp.BuildMtFromSrcTable(fmtdbc,tabid,dtmt.Cur());
			sp.BuildMtFromSrcTable(fmtdbc,tabid,dtmt.Next());
			//CMNET :�ļ�ģʽ�жϣ�����dumpsql������
			//  Load data from files using cmnet.ctl [exclude backup]
			// ��̬���ļ����в��ܰ����ո�
			char libname[300];
			bool withbackup=true;//default to true
			if(strcasestr(dumpsql,"load data from files")!=NULL) {
				filesmode=true;
				char *plib=strcasestr(dumpsql,"using ");
				if(!plib) 
				{
				    sp.log(tabid,datapartid,DUMP_FILE_ERROR,"��%d,����%d,�ļ���������ȱ��using�ؼ���.",tabid,datapartid);
					ThrowWith("�ļ���������ȱ��using�ؼ���.");
				}
				plib+=strlen("using ");
				strcpy(libname,plib);
				plib=libname;
				//end by blank or null
				while(*plib) {
					if(*plib==' ') {
						*plib=0;break;
					}
					else plib++;
				}
				if(strcasestr(dumpsql,"exclude backup")!=NULL) withbackup=false;
			}
			
			AutoStmt stmt(srcdbc);
			//CMNET :�ļ�ģʽ
			if(!filesmode) {

				//>> Begin: fix dm-230 , ���¹���Դ��mt
                sp.IgnoreBigSizeColumn(srcdbc,dumpsql);
				//<< End: fix dm-230
				
				stmt.Prepare(dumpsql);


				/* // try to fix dm-356
			 	AutoMt tstmt(0,1);
			 	tstmt.Build(stmt);
			 	if(blockmt.CompatibleMt(tstmt)!=0 ) {
			 		sp.log(tabid,datapartid,DUMP_SQL_ERROR,"��%d,����%d,���ݳ�ȡ��� %s �õ��ĸ�ʽ��Դ����ĸ�ʽ��һ��.\n",tabid,datapartid,dumpsql);
			 		ThrowWith("�������ݳ�ȡ��� %s �õ��ĸ�ʽ��Դ����ĸ�ʽ��һ��.\n",dumpsql);
				}
				*/
				
				wociReplaceStmt(*dtmt.Cur(),stmt);
				wociReplaceStmt(*dtmt.Next(),stmt);
		  }
			dtmt.Cur()->AddrFresh();
			dtmt.Next()->AddrFresh();

			//>> Begin: fix dm-230
		    char cfilename[256];
            strcpy(cfilename,getenv("DATAMERGER_HOME"));
	        strcat(cfilename,"/ctl/");
	        strcat(cfilename,MYSQL_KEYWORDS_REPLACE_LIST_FILE);
            sp.ChangeMtSqlKeyWord(*dtmt.Cur(),cfilename);
            sp.ChangeMtSqlKeyWord(*dtmt.Next(),cfilename);			
			//<< End: fix dm-230
			
			//			dtmt.Cur()->Build(stmt);
			//			dtmt.Next()->Build(stmt);
			//׼����������������������
			//CMNET :�ļ�ģʽ
			int rn;
			bool filecp=false;
			bool needcp=false;
			int uncheckfct=0,uncheckbct=0;//�������ļ��������ݿ���
			
			//>> Begin: DM-201 , modify by liujs
			IDumpFileWrapper *dfw;
			IFileParser *fparser;
			//<< End: modify by liujs

			bool filehold=false;//�ļ������У��ڴ����)
			if(filesmode) {
				dfw=new DumpFileWrapper(libname);
				fparser=dfw->getParser();
				fparser->SetTable(dtdbc,tabid,datapartid,dfw);
				//Get file and parser
				//����ϴε��ļ�δ������(�ڴ����),���������ļ��������ֽ����
				while(true) {
					if(!filehold) {
						 int gfstate=fparser->GetFile(withbackup,sp,tabid,datapartid);
						 if(gfstate==0) {
						 	//û��������Ҫ����
						 	if(fparser->ExtractEnd()) break;
						 	//if(dtmt.Cur()->GetRows()>0) break;
							mySleep(fparser->GetIdleSeconds());
						 	continue;
						}
						else if(gfstate==2) //�ļ����󣬵�Ӧ�ú���
							continue;
						uncheckfct++;
					}
					int frt=fparser->DoParse(*dtmt.Cur(),sp,tabid,datapartid);
					filehold=false;
					if(frt==-1) {
						  sp.log(tabid,datapartid,DUMP_FILE_ERROR,"��%d,����%d,�ļ��������.",tabid,datapartid); 
							ThrowWith("�ļ��������.");
							break;
					}
					else if(frt==0) {
							//memory table is full
							filehold=true;
							break;
					}
				}
				rn=dtmt.Cur()->GetRows();
			}
			else	{
				dtmt.FetchFirst();
			  rn=dtmt.Wait();
			}
			
		srn=rn;
		//�ļ������Ƿ��ѵ���
		while(rn>0) {
				if(!filesmode)
					dtmt.FetchNext();
				lgprintf("��ʼ���ݴ���");
				int retryct=0;
				while(true) {
				 try{
				  freeinfo1("before call prcblk");
				  for(int i=0;i<dp.soledindexnum;i++) {
						ProcBlock(sp,datapartid,dtmt.Cur(),i/*dp.psoledindex*/,blockmt,sp.NextTmpFileID());
				  }
				  lgprintf("���ݴ������");
				  if(fnmt.GetRows()>0){
				  			wociSetEcho(false);
		 						wociAppendToDbTable(fnmt,"dp.dp_middledatafile",dtdbc,true);
		 						wociReset(fnmt);
		 						wociSetEcho(true);
					}
				  //�ļ�ģʽĿǰֻ����һ���ڴ���д��������ٿ�������ͬʱ�á�
				  if(filesmode) {
				  	double fillrate=(double)dtmt.Cur()->GetRows()/dtmt.Cur()->GetMaxRows();
				  	wociReset(*dtmt.Cur());
				  	uncheckbct++;
					
					//������ύ����
					//������һ���ļ���������������������ļ��Ĵ����м�����ύ
					//�ﵽ5�����ݿ���߷��������Ѿ�������������ύ
					//���ߴﵽ�򳬹�2�����ݿ�δ�ύ�������һ�����ݿ��Ѿ�����80%--���߳���10���ļ�δ�ύ							
					//������ļ������ֻ���Ҫ��һ�µĵ���
					if(!filehold && (uncheckbct>=5 || (uncheckbct>1 && fillrate>0.6) || fparser->ExtractEnd()))
					{			
				  	 	lgprintf("�ύ���㣺���ݿ�%d,�ļ���%d,�����:%.1f%%.",uncheckfct,uncheckbct,fillrate*100);
					  	 uncheckbct=uncheckfct=0;
					  	 AutoStmt st(dtdbc);
					  	 wociSetEcho(false);
						 st.DirectExecute("update dp.dp_filelog set status=2 where tabid=%d and datapartid=%d and status=1"
						    ,tabid,datapartid);
						 st.DirectExecute("update dp.dp_middledatafile set procstate=1 where tabid=%d and datapartid=%d and procstate=2"
						    ,tabid,datapartid);
						 wociSetEcho(true);
				  	}
				  }
				  break;
				 }
				 catch(...) {
					if(retryct++>20) 
					{
	    				 sp.log(tabid,datapartid,DUMP_WRITE_FILE_ERROR,"��%d,����%d,д����ʧ��.",tabid,datapartid);
						throw;
					}
					lgprintf("д����ʧ�ܣ�����...");
				 }
				}
				freeinfo1("after call prcblk");
			  if(filesmode) {
				//Get file and parser
				//����ϴε��ļ�δ������(�ڴ����),���������ļ��������ֽ����
				while(true) {
					if(!filehold) {
						 int gfstate=fparser->GetFile(withbackup,sp,tabid,datapartid);
						 if(gfstate==0) {
						 	//û��������Ҫ����
						 	if(fparser->ExtractEnd()) break;
						 	//if(dtmt.Cur()->GetRows()>0) break;
							mySleep(fparser->GetIdleSeconds());
						 	continue;
						}
						else if(gfstate==2) //�ļ����󣬵�Ӧ�ú���
							continue;
						
						uncheckfct++;
					}
					int frt=fparser->DoParse(*dtmt.Cur(),sp,tabid,datapartid);
					filehold=false;
					if(frt==-1) {
						    sp.log(tabid,datapartid,DUMP_FILE_ERROR,"��%d,����%d,�ļ��������.",tabid,datapartid); 						    
							ThrowWith("�ļ��������.");
							break;
					}
					else if(frt==0) {
							//memory table is full
							filehold=true;
							break;
					}
					
					double fillrate=(double)dtmt.Cur()->GetRows()/dtmt.Cur()->GetMaxRows();
					if(!filehold &&  (uncheckbct>=5 ||  (uncheckbct>1 && fillrate>0.6) ) ) break;
				}
				rn=dtmt.Cur()->GetRows();
			  }
			  else	{
				  rn=dtmt.Wait();
			  }
				srn+=rn;
			//delete dtmt;
			//delete stmt;
		}
		wociSetEcho(ec);
		if(fnmt.GetRows()>0){
		 wociAppendToDbTable(fnmt,"dp.dp_middledatafile",dtdbc,true);
		 wociReset(fnmt);
		}
		{
		 AutoStmt st(dtdbc);
		 st.DirectExecute("update dp.dp_middledatafile set procstate=1 where tabid=%d and datapartid=%d and procstate=2"
						    ,tabid,datapartid);
		}
		if(!filesmode || (filesmode && fparser->ExtractEnd())){
			dumpcomplete=true;
		  sp.UpdateTaskStatus(DUMPED,tabid,datapartid);
  	}
  	if(filesmode) {
  		
  		delete dfw;
  	}
		}

	}
	catch(...) {
		int frn=wociGetMemtableRows(fnmt);
		errprintf("���ݵ����쳣��ֹ����%d(%d),�м��ļ���:%d.",tabid,datapartid,frn);
		AutoStmt st(dtdbc);
		st.DirectExecute("unlock tables");
		sp.log(tabid,datapartid,DUMP_EXCEPTION_ERROR,"��%d,����%d ���ݵ����쳣��ֹ���м��ļ���:%d.",tabid,datapartid,frn);
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
					sp.log(tabid,datapartid,DUMP_FINISHED_NOTIFY,"��%d(%d)���������,��д��dp������(dp_middledatafile)ʧ��,һ���Ӻ�����(%d)...",tabid,datapartid,++retrytimes);
					lgprintf("��%d(%d)���������,��д��dp������(dp_middledatafile)ʧ��,һ���Ӻ�����(%d)...",tabid,datapartid,++retrytimes);
					restored=false;
					mSleep(60000);
				}
			}
		}
		if(!restored) {
			int i;
			wdbi_kill_in_progress=false;
			wociMTPrint(fnmt,0,NULL);
			//�����ָ�����״̬�Ĳ���,��Ϊ����״̬���˹�������Ϊ����.������ݿ�����һֱû�лָ�,
			//������״̬�ָ��������쳣,������ɾ���ļ��ͼ�¼�Ĳ������ᱻִ��,�������˹���ȷ���Ƿ�ɻָ�,��λָ�
			errprintf("�ָ�����״̬.");
		    sp.log(tabid,datapartid,DUMP_RECOVER_TAST_STATUS_NOTIFY,"�ָ�����״̬.");
		  // 2010-12-01 ��������״̬ 72�����ڳ����ļ�װ��
			st.DirectExecute("update dp.dp_datapart set status=%d,blevel=ifnull(blevel,0)+100 "
			  "where tabid=%d and datapartid=%d",
			   filesmode?72:0,tabid,datapartid);
			//sp.UpdateTaskStatus(NEWTASK,tabid,datapartid);
			if(filesmode) {
				if(fnmt.GetRows()>0){
		 			wociAppendToDbTable(fnmt,"dp.dp_middledatafile",dtdbc,true);
		 			wociReset(fnmt);
				}
				throw;
			}
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
			st.Prepare("delete from dp.dp_middledatafile where tabid=%d and datapartid=%d",tabid,datapartid);
			st.Execute(1);
			st.Wait();
			st.Prepare("delete from dp.dp_filelog where tabid=%d and datapartid=%d",tabid,datapartid);
			st.Execute(1);
			st.Wait();
		/* on a busy statement,wociSetTerminate will failed and throw a exception,so the last chance to
		    restore enviriement is lost. so move to here from begining or this catch block.hope this can process more stable.
		  LOGS:
		  [2007:11:02 10:40:31] ��ʼ���ݴ���
		  [2007:11:02 10:40:42] Write file failed! filename:/dbsturbo/dttemp/cas/mddt_340652.dat,blocklen:218816,offset:3371426
		  [2007:11:02 10:40:42]  ErrorCode: -9.  Exception : Execute(Query) or Delete on a busy statement.
		*/
			wociSetTerminate(dtdbc,false);
			wociSetTerminate(sp.GetDTS(),false);
			throw;
		}
	}
	if(dumpcomplete) {
	 lgprintf("���ݳ�ȡ����,����״̬1-->2,tabid %d(%d)",tabid,datapartid);
	 lgprintf("sort time :%11.6f file io time :%11.6f adjust data time:%11.6f",
		sorttm.GetTime(),fiotm.GetTime(),adjtm.GetTime());
		lgprintf("����");
		sp.log(tabid,datapartid,DUMP_FINISHED_NOTIFY,"��%d,����%d,���ݳ�ȡ���� ,��¼��%lld.",tabid,datapartid,srn);
  }	
	//lgprintf("�����������...");
	//getchar();
	//MYSQL�е�MY_ISAM��֧����������MYSQL����޸Ĳ���Ҫ�ύ.
	return 1; 
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
  	    while(*str!=0) 
  	    {
  	    	*str=tolower(*str);
  	    	str++;
  	    }
	}

  
  //>> Begin:dm-228
  // �ж�ָ�����Ƿ����������ļ��б�IndexListFile�У����ָ����columnName���򷵻أ�true,
  // ���򷵻أ�false
  bool GetColumnIndex(const char* IndexListFile,const char* columnName)
  {
	  FILE *fp = NULL;
	  fp = fopen(IndexListFile,"rt");
	  if(fp==NULL) 
	  {
			ThrowWith("�������б��ļ�ʧ��.");
	  }
	  char lines[300];
	  while(fgets(lines,300,fp)!=NULL)
	  {
		  int sl = strlen(lines);
		  if(lines[sl-1]=='\n') lines[sl-1]=0;
		  if(strcasecmp(lines,columnName) == 0)
		  {
			  fclose(fp);
			  fp = NULL;
			  return true;		  
		  }
	  }
	  fclose(fp);
	  fp=NULL;
	  return false;
  }
  
  // �������������ͨ��Դ�������������
  int MiddleDataLoader::CreateSolidIndexTable(const char* orasvcname,const char * orausrname,const char* orapswd,
          const char* srcdbname,const char* srctabname,const char* dstdbname,const char* dsttabname,
		  const int  cmptype,const char* tmppath,const char* backpath,const char *taskdate,
		  const char* solid_index_list_file,char* ext_sql)
  {
      int ret = 0;
	  
	  // 1-- �ж�Ŀ����Ƿ��Ѿ�����
	  AutoMt dst_table_mt(sp->GetDTS(),10);
	  dst_table_mt.FetchAll("select * from dp.dp_table where databasename=lower('%s') and tabname=lower('%s')",dstdbname,dsttabname);
	  if(dst_table_mt.Wait()>0) 
		  ThrowWith("�� %s.%s �Ѿ����ڡ�",dstdbname,dsttabname);

      // 2-- ����Դ�����ݿ�,�ж�ORACLEԴ���Ƿ����
      AutoHandle ora_dts;
	  ora_dts.SetHandle(wociCreateSession(orausrname,orapswd,orasvcname,DTDBTYPE_ORACLE));
      try 
      {
          AutoMt ora_src_test_mt(ora_dts,10);
		  char sql[1000];
		  sprintf(sql,"select count(1) from %s.%s where rownum < 2",srcdbname,srctabname);
		  //>> begin: fix dm-252
		  sp->IgnoreBigSizeColumn(ora_dts,sql);
		  //>> end: fix dm-252
		  ora_src_test_mt.FetchFirst(sql);
		  ora_src_test_mt.Wait();
	  }
	  catch(...)
	  {
		  ThrowWith("Դ��:%s.%s �������޷�������:%s.%s��",srcdbname,srctabname,dstdbname,dsttabname);
	  }

      // 3-- �ж�dp�����е�����Դ�Ƿ����,����ȡ������Դ���������������Դ
      AutoMt data_src_mt(sp->GetDTS(),10);
	  int  data_src_id = 0;
	  #define ORACLE_TYPE 1
	  data_src_mt.FetchFirst("select sysid from dp.dp_datasrc where systype = %d and jdbcdbn = '%s' and username = '%s' and pswd = '%s'",
	  	ORACLE_TYPE,orasvcname,orausrname,orapswd);
	  int trn=data_src_mt.Wait();
	  if(trn<1) 
	  {
	      // dp���ݿ��в���������Դ����Ҫ���
          AutoStmt tmp_data_src_mt(sp->GetDTS());
		  ret = tmp_data_src_mt.DirectExecute("INSERT INTO dp.dp_datasrc(sysid,sysname,svcname,username,pswd,systype,jdbcdbn) "
		  	" select max(sysid)+1,'%s','CreateSolidIndexTable add','%s','%s',%d,'%s' from dp.dp_datasrc",
            orasvcname,orausrname,orapswd,ORACLE_TYPE,orasvcname);

		  if(ret != 1)
		  {             
		      ThrowWith("dp ���ݿ����Oracle����Դ[%s],����ʧ��!",orasvcname);
		  }

		  // ��ȡ�²����sysid
		  data_src_mt.FetchFirst("select max(sysid) as sysid from dp.dp_datasrc");
		  trn = data_src_mt.Wait();
          data_src_id = data_src_mt.GetInt("sysid",0);
	  }
	  else
	  {
          data_src_id = data_src_mt.GetInt("sysid",0);
	  }

	  // 4-- �ж�dp.dp_path��·��ID�Ƿ����
      // 4.1--��ѯ��ʱ·��
	  AutoMt path_mt(sp->GetDTS(),10);
	  int  data_tmp_pathid = 0,data_backup_pathid=0;
	  path_mt.FetchFirst("SELECT pathid FROM dp.dp_path where pathval = '%s' and pathtype = 'temp'",tmppath);
	  if(1 > path_mt.Wait()){
          AutoStmt tmp_path_mt(sp->GetDTS());
          ret = tmp_path_mt.DirectExecute("INSERT INTO dp.dp_path(pathid,pathtype,pathdesc,pathval) values(%d,'temp','CreateSolidIndexTable add','%s')",
		  sp->NextTableID(),tmppath);
		  if(-1 == ret) ThrowWith("dp���ݿ������ʱ·��[%s]ʧ��!",tmppath);
		  path_mt.FetchFirst("select pathid from dp.dp_path where pathval = '%s' and pathtype= 'temp'", tmppath);
		  path_mt.Wait();
		  data_tmp_pathid = path_mt.GetInt("pathid",0);
	  }
	  else
	  {
	      data_tmp_pathid =  path_mt.GetInt("pathid",0);
	  }

	  // 4.2--��ѯ����·��
	  path_mt.FetchFirst("SELECT pathid FROM dp.dp_path where pathval = '%s' and pathtype = 'data'",backpath);
	  if(1 > path_mt.Wait()){
	      AutoStmt tmp_path_mt(sp->GetDTS());
		  ret = tmp_path_mt.DirectExecute("INSERT INTO dp.dp_path(pathid,pathtype,pathdesc,pathval) values(%d,'data','CreateSolidIndexTable add','%s')",
		  sp->NextTableID(),backpath);
		  if(-1 == ret) ThrowWith("dp���ݿ���뱸��·��[%s]ʧ��!",backpath);
		  path_mt.FetchFirst("select pathid from dp.dp_path where pathval = '%s' and pathtype= 'data'", backpath);
		  path_mt.Wait();
		  data_backup_pathid = path_mt.GetInt("pathid",0);
	  }
	  else
	  {
	  	  data_backup_pathid =  path_mt.GetInt("pathid",0);
	  }
	  
	  // 5-- �ж�ʱ���ʽ�Ƿ���ȷ
	  AutoMt mt(sp->GetDTS(),100);
	  char tdt[30];
	  if(strcmp(taskdate,"now()")==0)
	  {
		  wociGetCurDateTime(tdt);
	  }
	  else
	  {
		  mt.FetchAll("select adddate(cast('%s' AS DATETIME),interval 0 day) as tskdate",taskdate);
		  if(mt.Wait()!=1) {
			  ThrowWith("���ڸ�ʽ����:'%s'",taskdate);
		  }
		  memcpy(tdt,mt.PtrDate("tskdate",0),7);
		  if(wociGetYear(tdt)==0){
			  ThrowWith("���ڸ�ʽ����:'%s'",taskdate);
		  }
	  }
	  
      // 6-- ����dp_table����������Ϣ
      int table_id = 0;
      AutoMt table_mt(sp->GetDTS(),100);
	  table_mt.FetchFirst("select * from dp.dp_table limit 1");
	  if(table_mt.Wait() != 1)
	  {
          ThrowWith("���ݿ��dp_table��û�м�¼������ͨ��web����ƽ̨����һ��������ٽ��иò���.");
	  }
	
	  strcpy(table_mt.PtrStr("databasename",0),dstdbname);
	  strcpy(table_mt.PtrStr("tabdesc",0),dsttabname);
	  strcpy(table_mt.PtrStr("tabname",0),dsttabname);
	  strcpy(table_mt.PtrStr("srcowner",0),srcdbname);
	  strcpy(table_mt.PtrStr("srctabname",0),srctabname);
	  *table_mt.PtrInt("sysid",0)=data_src_id;
	  *table_mt.PtrInt("dstpathid",0)=data_backup_pathid;
	  
	  *table_mt.PtrInt("cdfileid",0)=0;
	  *table_mt.PtrDouble("recordnum",0)=0;
	  *table_mt.PtrInt("firstdatafileid",0)=0;
	  *table_mt.PtrInt("datafilenum",0)=0;
	  *table_mt.PtrInt("lstfid",0)=0;
	  *table_mt.PtrDouble("totalbytes",0)=0;
	  *table_mt.PtrInt("recordlen",0)=0;
	  *table_mt.PtrInt("maxrecinblock",0)=0;

	  
	  // ��ȡtabid
	  table_id = sp->NextTableID();
	  *table_mt.PtrInt("tabid",0)=table_id;

	  // 7-- ����id�Ƿ���ڼ�¼
	  AutoMt check_mt(sp->GetDTS(),10);
	  // ��Ӧ��Դ���Ժ��޸�
	  check_mt.FetchAll("select * from dp.dp_table where tabid=%d",table_id);
	  if(check_mt.Wait()>0)
		ThrowWith("����ظ�: ���%d��Ŀ���'%s.%s'�Ѿ�����!",check_mt.GetInt("tabid",0),check_mt.PtrStr("databasename",0),
		    check_mt.PtrStr("tabname",0));
	  check_mt.FetchAll("select * from dp.dp_index where tabid=%d",table_id);
	  if(check_mt.Wait()>0)
		ThrowWith("���ֲ���ȷ����������(��%d)��",check_mt.GetInt("tabid",0));
	  check_mt.FetchAll("select * from dp.dp_datafilemap where tabid=%d and fileflag!=2",table_id);
	  if(check_mt.Wait()>0) 
		ThrowWith("���ֲ���ȷ�������ļ���¼(��%d)!",check_mt.GetInt("tabid",0));
	  check_mt.FetchAll("select * from dp.dp_datapart where tabid=%d",table_id);
	  if(check_mt.Wait()>0) 
		ThrowWith("���ֲ���ȷ�����ݷ������(��%d)!",check_mt.GetInt("tabid",0));
	  	

	  // 8-- ����dp_index����������Ϣ
	  AutoMt  src_table_mt(ora_dts,5);
	  // ��������
      char idx_col_name[256] = {0}; 
	  try
	  {
	      // 8.1 -- ��ѯoracleԴ���ݿ����ȡ����Ϣ,��ȡsql���
          sp->IgnoreBigSizeColumn(ora_dts,srcdbname,srctabname,ext_sql);

          // 8.2 -- ��ȡԴ��ṹ
		  src_table_mt.FetchFirst(ext_sql);
	      src_table_mt.Wait();

	      // 8.3 -- ���Դ��ṹ�д���mysql�ؼ��֣������滻��
	      char cfilename[256];
          strcpy(cfilename,getenv("DATAMERGER_HOME"));
	      strcat(cfilename,"/ctl/");
	      strcat(cfilename,MYSQL_KEYWORDS_REPLACE_LIST_FILE);
	      sp->ChangeMtSqlKeyWord(src_table_mt,cfilename);

		  // 8.4 -- ����������ж�
		  bool get_column_index_flag = false;
	      for(int i=0;i<src_table_mt.GetColumnNum();i++)
	      {
	          wociGetColumnName(src_table_mt,i,idx_col_name);
              if(GetColumnIndex(solid_index_list_file,idx_col_name))
              {
	              get_column_index_flag = true;
			      break;
              }
	      }
	      if(!get_column_index_flag)  // �б��ļ���û���ҵ����������õ�һ������Ϊ����
	      {  
	          wociGetColumnName(src_table_mt,0,idx_col_name);
	      }
	  }
	  catch(...)
	  {
         ThrowWith("Դ��:%s.%s �������޷�������:%s.%s��",srcdbname,srctabname,dstdbname,dsttabname);
	  }
	  
	 
      // 8.3 -- ����dp_index���м�¼
      AutoMt index_mt(sp->GetDTS(),10);
	  index_mt.FetchFirst("select * from dp.dp_index limit 1");
	  if(index_mt.Wait() != 1)
	  {
          ThrowWith("���ݿ��dp.dp_index��û�м�¼������ͨ��web����ƽ̨����һ��������ٽ��иò���.");
	  }
	  *index_mt.PtrInt("indexgid",0)=1;  
	  *index_mt.PtrInt("tabid",0)=table_id;
	  strcpy(index_mt.PtrStr("indextabname",0),"");//dsttabname);	 
	  *index_mt.PtrInt("seqindattab",0)=1;
	  *index_mt.PtrInt("seqinidxtab",0)=1;
	  *index_mt.PtrInt("issoledindex",0)=1;
	  strcpy(index_mt.PtrStr("columnsname",0),idx_col_name);	
	  *index_mt.PtrInt("idxfieldnum",0)=1;
	  
	  // 9-- ����dp_datapart����������Ϣ
      AutoMt datapart_mt(sp->GetDTS(),10);
	  datapart_mt.FetchFirst("select * from dp.dp_datapart limit 1");
	  if(datapart_mt.Wait() != 1)
	  {
          ThrowWith("���ݿ��dp.dp_datapart��û�м�¼������ͨ��web����ƽ̨����һ��������ٽ��иò���.");
	  }

      // 9.1 -- �ж�Դ�����Ƿ���ڴ��ֶΣ�������ڴ��ֶξͽ�����˵�
      sp->IgnoreBigSizeColumn(ora_dts,srcdbname,srctabname,ext_sql);

	  // 9.2 -- ����������Ϣ��dp_datapart����
	  *datapart_mt.PtrInt("datapartid",0)=1;
	  memcpy(datapart_mt.PtrDate("begintime",0),tdt,7);
	  *datapart_mt.PtrInt("istimelimit",0)=0;
	  *datapart_mt.PtrInt("status",0)=0;
	  sprintf(datapart_mt.PtrStr("partdesc",0),"%s:%s.%s->%s.%s",orasvcname,srcdbname,srctabname,dstdbname,dsttabname);
	  *datapart_mt.PtrInt("tabid",0)=table_id;
	  *datapart_mt.PtrInt("compflag",0)=cmptype;
	  *datapart_mt.PtrInt("oldstatus",0)=0;
	  *datapart_mt.PtrInt("srcsysid",0)=data_src_id;
	  strcpy(datapart_mt.PtrStr("extsql",0),ext_sql);
	  *datapart_mt.PtrInt("tmppathid",0)=data_tmp_pathid;
	  *datapart_mt.PtrInt("blevel",0)=0;

	  try {
		  wociAppendToDbTable(table_mt,"dp.dp_table",sp->GetDTS(),true);
		  wociAppendToDbTable(index_mt,"dp.dp_index",sp->GetDTS(),true);
		  wociAppendToDbTable(datapart_mt,"dp.dp_datapart",sp->GetDTS(),true);
	  }
	  catch(...) {
		  //�ָ����ݣ��������в���
		  AutoStmt st(sp->GetDTS());
		  st.DirectExecute("delete from dp.dp_table where tabid=%d",table_id);
		  st.DirectExecute("delete from dp.dp_index where tabid=%d",table_id);
		  st.DirectExecute("delete from dp.dp_datapart where tabid=%d",table_id);
		  errprintf("�����ƴ���ʱ�����ύʧ�ܣ���ɾ�����ݡ�");
		  throw;
	  }
	  {
		  char dtstr[100];
		  wociDateTimeToStr(tdt,dtstr);
		  lgprintf("�����ɹ�,Ŀ���'%s.%s:%d',��ʼʱ��'%s'.",dstdbname,dsttabname,table_id,dtstr);
	  }
	  sp->Reload();
	  return 0;        
  }

  
  int MiddleDataLoader::CreateLike(const char *dbn,const char *tbn,const char *nsrcowner,const char *nsrctbn,const char * ndstdbn,const char *ndsttbn,const char *taskdate,bool presv_fmt)
  {
	  int tabid=0,srctabid=0;
	  AutoMt mt(sp->GetDTS(),100);
	  char tdt[30];
	  char ndbn[300];
	  strcpy(ndbn,ndstdbn);
	  StrToLower(ndbn);
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
	  mt.FetchAll("select * from dp.dp_table where databasename=lower('%s') and tabname=lower('%s')",ndbn,ndsttbn);
	  if(mt.Wait()>0) 
		  ThrowWith("�� %s.%s �Ѿ����ڡ�",dbn,ndsttbn);
	  
	  AutoMt tabmt(sp->GetDTS(),100);
	  tabmt.FetchAll("select * from dp.dp_table where databasename=lower('%s') and lower(tabname)='%s'",dbn,tbn);
	  if(tabmt.Wait()!=1) 
		  ThrowWith("�ο��� %s.%s �����ڡ�",dbn,tbn);
	  int reftabid=tabmt.GetInt("tabid",0);
	  //���Ŀ�����Ϣ
	  strcpy(tabmt.PtrStr("databasename",0),ndbn);
	  strcpy(tabmt.PtrStr("tabdesc",0),ndsttbn);
	  strcpy(tabmt.PtrStr("tabname",0),ndsttbn);
	  *tabmt.PtrInt("cdfileid",0)=0;
	  *tabmt.PtrDouble("recordnum",0)=0;
	  *tabmt.PtrInt("firstdatafileid",0)=0;
	  *tabmt.PtrInt("datafilenum",0)=0;
	  *tabmt.PtrInt("lstfid",0)=0;
	  *tabmt.PtrDouble("totalbytes",0)=0;
	  const char *prefsrctbn=tabmt.PtrStr("srctabname",0);
	  StrToLower((char*)prefsrctbn);
	  const char *prefsrcowner=tabmt.PtrStr("srcowner",0);
	  StrToLower((char*)prefsrcowner);
	  //�ο�Դ����滹Ҫ����,��ʱ���滻
	  //strcpy(tabmt.PtrStr("srcowner",0),srcowner);
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
	  	//set indextabname to null 
	  	*indexmt.PtrStr("indextabname",ip)=0;
	  }
	  
	  AutoMt taskmt(sp->GetDTS(),500);
	  taskmt.FetchAll("select * from dp.dp_datapart where tabid=%d",reftabid);
	  int trn=taskmt.Wait();
	  if(trn<1) 
		  ThrowWith("�ο��� %s.%s û�����ݷ�����Ϣ��",dbn,tbn);
	  
	  //�����ݳ�ȡ�������Сд���е�Դ�������滻,��������������:
	  // 1. ����ֶ�������������Դ��������ͬ,������滻ʧ��
	  // 2. ���Դ�����ƴ�Сд��һ��,������滻ʧ��
	   char tsrcowner[300],tsrctbn[300],tsrcfull[300],treffull[300];
	   strcpy(tsrcowner,nsrcowner);strcpy(tsrctbn,nsrctbn);
	   StrToLower(tsrcowner);StrToLower(tsrctbn);
	   sprintf(tsrcfull,"%s.%s",tsrcowner,tsrctbn);
	   sprintf(treffull,"%s.%s",prefsrcowner,prefsrctbn);

	  for(int tp=0;tp<trn;tp++) {
	   char sqlbk[MAX_STMT_LEN];
	   char *psql=taskmt.PtrStr("extsql",tp);
	   strcpy(sqlbk,psql);
	   char tmp[MAX_STMT_LEN];
	   strcpy(tmp,psql);
	   StrToLower(tmp);
	   if(strcmp(prefsrctbn,tsrctbn)!=0 || strcmp(prefsrcowner,tsrcowner)!=0 || presv_fmt) {
  	        char extsql[MAX_STMT_LEN];
		char *sch=strstr(tmp," from ");
		char *schx=strstr(tmp," where ");
		if(presv_fmt) {
			if(sch) {
				sch+=6;
				strncpy(extsql,psql,sch-tmp);
			        extsql[sch-tmp]=0;
			        strcat(extsql,tsrcfull);
			        if(schx)
			          strcat(extsql,psql+(schx-tmp));
		        	strcpy(psql,extsql);
			}
		}
		else if(sch) {
		   if(schx) *schx=0;
		   sch+=6;
		   //���� from ֮ǰ�����(�� from );
		   strncpy(extsql,psql,sch-tmp);
		   extsql[sch-tmp]=0;
		   bool fullsrc=true; 
		   int tablen=strlen(treffull);
		   char *sch2=strstr(sch,treffull);
		   if(sch2==NULL) {
		        //�ο����sql����в�����ʽ���������ʽ
		        //����Ƿ񺬲��֣�ֻ�б�����û�п���)
		   	sch2=strstr(sch,prefsrctbn);
		   	fullsrc=false;
		   	// other database name than prefsrcowner
		   	//if(sch2!=NULL && sch2[-1]!=' ') {
		   	//	tablen=sch2-sch;
		   	//	sch2=sch;
		   	//}
		   	//else 
		   	 tablen=strlen(prefsrctbn);
		   }
		   //����Ҫ�������򲿷���ʽ�ĸ�ʽ�����滻
		   if(sch2) {
		   	//ֻ��from ... where ֮������滻�����Ĳ����滻�������ı���
		   	// any chars between 'from' and tabname ?
		        strncpy(extsql+strlen(extsql),psql+(sch-tmp),sch2-sch);
		        // replace new tabname
		        strcat(extsql,tsrcfull);
		        //padding last part 
		        strcat(extsql,psql+(sch2-tmp)+tablen);
		        strcpy(psql,extsql);
		   }
		}
		}
		if(strcmp(sqlbk,psql)==0) 
	   	lgprintf("���ݷ���%d�����ݳ�ȡ���δ���޸ģ��������Ҫ�ֹ�����.",taskmt.GetInt("datapartid",tp));
	    else
	     lgprintf("���ݷ���%d�����ݳ�ȡ����Ѿ��޸ģ�\n%s\n--->\n%s\n�������Ҫ�ֹ�����.",taskmt.GetInt("datapartid",tp),sqlbk,psql);
	    
	    *taskmt.PtrInt("tabid",tp)=tabid; 
	    *taskmt.PtrInt("blevel",tp)=0; 
	    memcpy(taskmt.PtrDate("begintime",tp),tdt,7);
	    *taskmt.PtrInt("status",tp)=0;
	    //sprintf(taskmt.PtrStr("partdesc",tp),"%s.%s",dbn,ndsttbn);
	  }
	  if(presv_fmt) {
	    strcpy(tabmt.PtrStr("srctabname",0),prefsrctbn);
	    strcpy(tabmt.PtrStr("srcowner",0),prefsrcowner);
	  }
	  else {	  	
	    strcpy(tabmt.PtrStr("srctabname",0),tsrctbn);
	    strcpy(tabmt.PtrStr("srcowner",0),tsrcowner);
	  }
	  StrToLower(ndbn);

	  //check database on mysql,if not exists,create it
	  sp->TouchDatabase(ndbn,true);
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
			printf("�����ļ���%s.\n",mdf.PtrStr("idxfname",iid));
			AutoMt idxmt(0);
			idxmt.SetHandle(idxf.CreateMt(1));
			idxmt.SetHandle(idxf.CreateMt(FIX_MAXINDEXRN/wociGetRowLen(idxmt)));
			idxf.Open(mdf.PtrStr("idxfname",iid),0);
			int brn=0;//idxf.ReadMt(-1,0,idxmt,false);
			int sbrn=0;
			while( (sbrn=idxf.ReadMt(-1,0,idxmt,true,1))>0) brn+=sbrn;
			printf("����%d�С�\n",brn);
			int thiserrct=errct;
			//�����������ļ��ĺ�׺��idx�滻Ϊdat���������ļ�.
			AutoMt destmt(0);
			strcpy(dtfn,mdf.PtrStr("idxfname",iid));
			strcpy(dtfn+strlen(dtfn)-3,"dat");
			//�������ļ�ȡ�ֶνṹ���ڴ���СΪĿ����ÿ���ݿ����������
			//destmt.SetHandle(dtf.CreateMt(blockmaxrn));
			int myrowlen=0;
			{
				dt_file datf;
				datf.Open(dtfn,0);
				myrowlen=datf.GetMySQLRowLen();
			}
			FILE *fp=fopen(dtfn,"rb");
			printf("��������ļ���%s.\n",dtfn);
			if(fp==NULL)
				ThrowWith("DP�ļ����:�ļ�'%s'����.",dtfn);
			fseek(fp,0,SEEK_END);
			unsigned int flen=ftell(fp);
			fseek(fp,0,SEEK_SET);
			file_hdr fhdr;
			fread(&fhdr,sizeof(file_hdr),1,fp);
			fhdr.ReverseByteOrder();
			fseek(fp,0,SEEK_SET);
			printf("file flag:%x, rowlen:%x myrowlen:%d.\n",fhdr.fileflag,fhdr.rowlen,myrowlen);
			block_hdr *pbhdr=(block_hdr *)srcbf;
			
			int oldblockstart=-1;
			int dspct=0;
			int totct=0;
			int blockstart,blocksize,blockrn;
			int rownum;
			int bkf=0;
			sbrn=idxf.ReadMt(0,0,idxmt,true,1);
			int bcn=wociGetColumnPosByName(idxmt,"dtfid");
			int dtfid=*idxmt.PtrInt(bcn,0);
			int ist=0;
			for(int i=0;i<brn;i++) {
				//ֱ��ʹ���ֶ����ƻ����idx_rownum�ֶε����Ʋ�ƥ�䣬���ڵ�idx�����ļ��е��ֶ���Ϊrownum.
				//dtfid=*idxmt.PtrInt(bcn,i);
				if(i>=sbrn) {
					ist=sbrn;
					sbrn+=idxf.ReadMt(-1,0,idxmt,true,1);
				}
				blockstart=*idxmt.PtrInt(bcn+1,i-ist);
				blocksize=*idxmt.PtrInt(bcn+2,i-ist);
				blockrn=*idxmt.PtrInt(bcn+3,i-ist);
				//startrow=*idxmt.PtrInt(bcn+4,i);
				rownum=*idxmt.PtrInt(bcn+5,i-ist);
				if(oldblockstart!=blockstart) {
					try {
						//dtf.ReadMt(blockstart,blocksize,mdf,1,1,srcbf);
						//if(blockstart<65109161) continue;
						fseek(fp,blockstart,SEEK_SET); 
						if(fread(srcbf,1,blocksize,fp)!=blocksize) {//+sizeof(block_hdr)
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
						//JIRA DM-13 . add block uncompress test
						int dml=(blockrn+7)/8;
						int rcl=pbhdr->storelen-dml-sizeof(delmask_hdr);
						char *pblock=srcbf
						  +sizeof(block_hdr)
						   +dml
						    +sizeof(delmask_hdr);
						//bzlib2
						char destbuf[1024*800];//800KB as buffer
						uLong dstlen=1024*800;
						int rt=0;
						if(pbhdr->compressflag==10) {
							unsigned int dstlen2=dstlen;
							rt=BZ2_bzBuffToBuffDecompress(destbuf,&dstlen2,pblock,rcl,0,0);
							dstlen=dstlen2;
						}			
						/***********UCL decompress ***********/
						else if(pbhdr->compressflag==8) {
								unsigned int dstlen2=dstlen;
							#ifdef USE_ASM_8
							rt = ucl_nrv2d_decompress_asm_fast_8((Bytef *)pblock,rcl,(Bytef *)destbuf,(unsigned int *)&dstlen2,NULL);
							#else
							rt = ucl_nrv2d_decompress_8((Bytef *)pblock,rcl,(Bytef *)destbuf,(unsigned int *)&dstlen2,NULL);
							#endif
							dstlen=dstlen2;
						}
						/******* lzo compress ****/
						else if(pbhdr->compressflag==5) {
							lzo_uint dstlen2=dstlen;
							#ifdef USE_ASM_5
							rt=lzo1x_decompress_asm_fast((unsigned char*)pblock,rcl,(unsigned char *)destbuf,&dstlen2,NULL);
							#else
							rt=lzo1x_decompress((unsigned char*)pblock,rcl,(unsigned char *)destbuf,&dstlen2,NULL);
							#endif
							dstlen=dstlen2;
						}
		    			/*** zlib compress ***/
						else if(pbhdr->compressflag==1) {
			 					rt=uncompress((Bytef *)destbuf,&dstlen,(Bytef *)pblock,rcl);
						}
						else 
								ThrowWith("Invalid uncompress flag %d",pbhdr->compressflag);
		        if(rt!=Z_OK) {
		        	printf("blockrn%d, buffer head len:%d.pblock-srcbf:%d\n",blockrn,sizeof(block_hdr)
						   +dml
						    +sizeof(delmask_hdr),pblock-srcbf);
		        	for (int trc=0;trc<256;trc++) {
		        		printf("%02x ",(unsigned char)srcbf[trc]);
		        		if((trc+1)%16==0) printf("\n");
		          }
							ThrowWith("Decompress failed,fileid:%d,off:%d,blocksize:%d,datalen:%d,compress flag%d,errcode:%d",
								dtfid,blockstart,blocksize,pbhdr->storelen,pbhdr->compressflag,rt);
						}
						else if(dstlen!=pbhdr->origlen) {
										 ThrowWith("Decompress failed,datalen %d should be %d.fileid:%d,off:%d,datalen:%d,compress flag%d,errcode:%d",
								dstlen,pbhdr->origlen,bcn,blockstart,pbhdr->storelen,pbhdr->compressflag,rt);
						}
						//for test only
						//printf("%d:%d:%d:%d.\n",blockstart,pbhdr->origlen,dstlen,rcl);
						//if(i>10) ThrowWith("debug stop");
					}
					catch (...) {
						if(errct++>20) {
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
		errprintf("DP�ļ������ִ���������װ��������³�ȡ���ݣ�");
	printf("\n");
	chktm.Stop();
	lgprintf("DP�ļ�������,������%d���ļ�,����%d������(%.2fs).",irn,errct,chktm.GetTime());
	return 1;
  }
  
  
  void MiddleDataLoader::CheckEmptyDump() {
	  mdf.FetchAll("select * from dp.dp_datapart where status=2 %s limit 100",sp->GetNormalTaskDesc());
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
  //procstate״̬˵����
  //  >1 :���ڵ�������
  //  <=1:��������ݵ���
  int MiddleDataLoader::Load(int MAXINDEXRN,long LOADTIDXSIZE,bool useOldBlock) {
	  //Check deserved temporary(middle) fileset
	  //���״̬Ϊ1������1Ϊ��ȡ�����ȴ�װ��.
	  CheckEmptyDump();
	  //2010-12-01 ���ӷ�������״̬������2��ѡ��
	  mdf.FetchAll("select mf.* from dp.dp_middledatafile mf,dp.dp_datapart dp "
	     " where mf.procstate<=1 and mf.tabid=dp.tabid and mf.datapartid=dp.datapartid  "
	     " and ifnull(dp.blevel,0)%s and dp.status=2 "
	     " order by mf.blevel,mf.tabid,mf.datapartid,mf.indexgid limit 100",
	     sp->GetNormalTask()?"<100":">=100");
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
	  mdf.FetchAll("select procstate from dp.dp_middledatafile "
	    " where tabid=%d and datapartid=%d and indexgid=%d and procstate>1 limit 10",
		  tabid,datapartid,indexid);
	  firstbatch=mdf.Wait()<1;//�����Ӽ�û���������������ļ�¼��
	  
	  //ȡ���м��ļ���¼
	  mdf.FetchAll("select * from dp.dp_middledatafile "
	    " where  tabid=%d and datapartid=%d and indexgid=%d and procstate<=1 order by mdfid limit %d",
		  tabid,datapartid,indexid,MAX_MIDDLE_FILE_NUM);
	  rn=mdf.Wait();
	  if(rn<1) 
	  {
		  sp->log(tabid,datapartid,MLOAD_CAN_NOT_FIND_MIDDLEDATA_ERROR,"��%d,����%d,ȷ�������Ӽ����Ҳ����м����ݼ�¼(δ����)��",tabid,datapartid);
		  ThrowWith("MiddleDataLoader::Load : ȷ�������Ӽ����Ҳ����м����ݼ�¼(δ����)��");
	  }
	  
	  //ȡ��������
	  long idxtlimit=0,idxdlimit=0;//��ʱ��(�����ȡ�����ļ���Ӧ)��Ŀ����(����Ŀ�������ļ���Ӧ)����������¼��.
	  tabid=mdf.GetInt("tabid",0);
	  sp->SetTrace("datatrim",tabid);
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
	  sp->log(tabid,datapartid,MLOAD_DATA_RECOMBINATION_NOTIFY,"��%d,����%d,��������,������%d,��־�ļ� '%s' .",tabid,datapartid,indexid,wociGetLogFile());
	  int start_mdfid=0,end_mdfid=0;
	  char sqlbf[200];
	  LONG64 extrn=0,lmtextrn=-1;
	  LONG64 adjrn=0;
	  try {	
		  tmpfilenum=rn;
		  //���������ļ��������ۼ�����������
		  LONG64 idxrn=0;
		  long i;
		  long mdrowlen=0;
		  //ȡ������¼�ĳ���(��ʱ�������ݼ�¼)
		  {
			  dt_file df;
			  df.Open(mdf.PtrStr("indexfilename",0),0);
			  mdrowlen=df.GetRowLen();
		  }
		  
		  lgprintf("��ʱ�������ݼ�¼����:%d",mdrowlen);  
		  long lmtrn=-1,lmtfn=-1;
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
				  {
				  	  sp->log(tabid,datapartid,MLOAD_DP_LOADTIDXSIZE_TOO_LOW_ERROR,"�ڴ����DP_LOADTIDXSIZE����̫��:%dMB��������װ������һ����ʱ��ȡ��:%dMB��\n",LOADTIDXSIZE,(int)llt);				  
					  ThrowWith("MLoader:�ڴ����DP_LOADTIDXSIZE����̫��:%dMB��\n"
					  "������װ������һ����ʱ��ȡ��:%dMB��\n",LOADTIDXSIZE,(int)llt);
				  }
				  lmtrn=idxrn-df.GetRowNum();
				  lmtfn=i;
				  lmtextrn=extrn-mdf.GetInt("recordnum",i);
			  }
			  if(idxrn > (MAX_ROWS_LIMIT-8) && lmtrn==-1)
			  {
				  lmtrn=idxrn-df.GetRowNum();
				  lmtfn=i;
				  lmtextrn=extrn-mdf.GetInt("recordnum",i);
			  }	
		  }
		  if(lmtrn!=-1) { //ʹ�õ���ʱ���������ڴ�������������ޣ���Ҫ���
			  lgprintf("MLoader:�����������ڴ�����%dMB,��Ҫ�����ļ���%d,������%d,��ʼ��:%d,������:%d,�ļ���:%d .",LOADTIDXSIZE,rn,datapartid,mdf.GetInt("mdfid",0),mdf.GetInt("mdfid",lmtfn-1),lmtfn);
			  lgprintf("������Ҫ�ڴ�%dM ",idxrn*mdrowlen/(1024*1024));
			  sp->log(tabid,datapartid,MLOAD_DATA_NOTIFY,"MLoader:�����������ڴ�����%dMB,��Ҫ�����ļ���%d,��ʼ��:%d,������:%d,�ļ���:%d ,��Ҫ�ڴ�%dMB.",LOADTIDXSIZE,rn,mdf.GetInt("mdfid",0),mdf.GetInt("mdfid",lmtfn-1),lmtfn,idxrn*mdrowlen/(1024*1024));
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
			  	  sp->log(tabid,datapartid,MLOAD_UPDATE_MIDDLE_FILE_STATUS_ERROR,"��%d,����%d,�޸��м��ļ��Ĵ���״̬�쳣�����������������̳�ͻ.�����ļ��Ĵ���״̬��һ�£�������ֹͣ���е��������������������������������",
			  	  tabid,datapartid);
			  				  	
				  ThrowWith("MLoader�޸��м��ļ��Ĵ���״̬�쳣�����������������̳�ͻ��\n"
					  " �����ļ��Ĵ���״̬��һ�£�������ֹͣ���е��������������������������������\n"
					  "  tabid:%d,datapartid:%d,indexid:%d.\n",
					  tabid,datapartid,indexid);
			  }
			  else //�����update���δ����ʵ���޸Ĳ���,�������̿��Լ�������.
			  {
				 //��Ҫ��ThrowWith,���������catch�лָ�dp_middledatafile��.
				  errprintf("MLoader�޸��м��ļ��Ĵ���״̬�쳣�����������������̳�ͻ��\n"
					  "  tabid:%d,datapartid:%d,indexid:%d.\n",
					  tabid,datapartid,indexid);
				  return 0;	  
			  }
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
		  int maxblockrn=min(sp->GetMaxBlockRn(tabid),MAX_BLOCKRN);
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
		  	sp->log(tabid,0,MLOAD_INDEX_DATA_FILE_RECORD_NUM_ERROR,"��%d,���������ļ����ܼ�¼��%lld,��ָʾ��Ϣ��һ��:%lld",tabid,crn,idxrn);
		  	ThrowWith("���������ļ����ܼ�¼��%lld,��ָʾ��Ϣ��һ��:%lld",crn,idxrn);
		  }
		  /* DM-55 check value of USER_ID */
		  /*if(indexid==2) {
		   LONG64 *pchk;
		   wociGetLongAddrByName(mdindexmt,"USER_ID",0,&pchk) ;
		   int chkoff;
		   int nullct=0;
		   for(chkoff=0;chkoff<idxrn;chkoff++)
		   {
		   	if(wociIsNull(mdindexmt,0,chkoff)) nullct++;
		   }
		   printf("Check done,null %d.\n",nullct);
		   //if(chkoff==idxrn) ThrowWith("Got all null value,so exit to check.");
		 }*/
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

		  //>> Binge: fix  dm-249
		  idxdlimit=min((int)llt,MAX_BLOCKRN);
		  //<< End:fix dm-249
		  
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
		  		sp->log(tabid,datapartid,MLOAD_CAN_NOT_MLOAD_DATA_ERROR,"��%d,����%d,�ļ�'%s'�Ѿ����ڣ����ܼ����������ݡ�",tabid,datapartid,fn);
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
				  {
				  	  sp->log(tabid,datapartid,MLOAD_INDEX_NUM_OVER_ERROR,"Ŀ���%d,������%d,װ��ʱ�����������һ�������¼��%d",tabid,indexid,idxdlimit);
				      ThrowWith("Ŀ���%d,������%d,װ��ʱ�����������һ�������¼��%d",tabid,indexid,idxdlimit);
				  }
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
		  						sp->log(tabid,datapartid,MLOAD_FILE_EXISTS_ERROR,"��%d,����%d,�ļ�'%s'�Ѿ����ڣ����ܼ����������ݡ�",tabid,datapartid,fn);
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
		sp->log(tabid,datapartid,MLOAD_CHECK_RESULT_ERROR,"��%d,����%d,��������Ҫ����������%lld�У���ʵ������%lld��! ������%d.",tabid,datapartid,lmtextrn,adjrn,indexid);
		ThrowWith("��������Ҫ����������%lld�У���ʵ������%lld��! ��%d(%d),������%d.",lmtextrn,adjrn,tabid,datapartid,indexid);
	}
	wociAppendToDbTable(fnmt,"dp.dp_datafilemap",sp->GetDTS(),true);
	lgprintf("�޸��м��ļ��Ĵ���״̬(��%d,����%d,������:%d,%d���ļ�)��2-->3",tabid,indexid,datapartid,rn);
	sprintf(sqlbf,"update dp.dp_middledatafile set procstate=3 where tabid=%d and datapartid=%d and indexgid=%d and procstate=2 and mdfid>=%d and mdfid<=%d ",
	  tabid,datapartid,indexid,start_mdfid,end_mdfid);
	sp->DoQuery(sqlbf);
	sp->log(tabid,datapartid,MLOAD_DATA_NOTIFY,"��%d,����%d,��������,������:%d,��¼��%lld.",tabid,datapartid,indexid,lmtextrn);
	}
	catch (...) {
		int frn=wociGetMemtableRows(fnmt);
		errprintf("������������쳣����:%d,������:%d.",tabid,datapartid);
		sp->log(tabid,datapartid,111,"������������쳣");
		errprintf("�ָ��м��ļ��Ĵ���״̬(������:%d,%d���ļ�)��2-->1",datapartid,rn);
		sprintf(sqlbf,"update dp.dp_middledatafile set procstate=1,blevel=ifnull(blevel,0)+1 where tabid=%d and datapartid=%d and indexgid=%d and mdfid>=%d and mdfid<=%d",tabid,datapartid,indexid,start_mdfid,end_mdfid);
		sp->DoQuery(sqlbf);
		sprintf(sqlbf,"update dp.dp_datapart set blevel=ifnull(blevel,0)+100 where tabid=%d and datapartid=%d ",tabid,datapartid);
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
		sp->log(tabid,datapartid,MLOAD_DATA_NOTIFY,"��%d,����%d,��������д���״̬�ѻָ�.",tabid,datapartid);
		throw ;
	}
	
	lgprintf("���ݴ���(MiddleDataLoading)����,���������ݰ�%lld��.",dispct);
	lgprintf("����%d�������ļ�,�Ѳ���dp.dp_datafilemap��.",wociGetMemtableRows(fnmt));
	sp->log(tabid,datapartid,MLOAD_DATA_NOTIFY,"��%d,����%d,�����������,���������ݰ�%lld��,�����ļ�%d��.",tabid,datapartid,dispct,wociGetMemtableRows(fnmt));
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
				sp->log(tabid,datapartid,MLOAD_CHECK_RESULT_ERROR,"��%d,����%d,����У�����,����%lld�У���������%lld��(��֤������%d),����Ǩ�ƹ����ѱ���ͣ��",tabid,datapartid,trn,mdf.GetLong("rn",0),indexid);
				sprintf(sqlbf,"update dp.dp_datapart set oldstatus=status,status=70,blevel=ifnull(blevel,0)+100 where tabid=%d and datapartid=%d",tabid,datapartid);
				sp->DoQuery(sqlbf);
				ThrowWith("����У�����,��%d(%d),����%lld�У���������%lld��(��֤������%d),����Ǩ�ƹ����ѱ���ͣ",tabid,datapartid,trn,mdf.GetLong("rn",0),indexid);
			}
			if(mdf.GetLong("rn",0))
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
		sp->log(tabid,datapartid,MLOAD_EXCEPTION_ERROR,"����������������ɣ�������״̬��������ʱ�м��ļ�ɾ��ʱ���ִ�����Ҫ�˹�������\n��%d(%d)��",tabid,datapartid);
		throw;
	}
	return 1;
	//Load index data into memory table (indexmt)
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
  memcpy(V,def_temp,sizeof(LONG64)); }
#endif       

  // ���ڶ����Ĳ�������װ��
  #define DATALOAD_NUM 100

  int DestLoader::Load(bool directIOSkip) {
	  //Check deserved temporary(middle) fileset
	  AutoMt mdf_task(psa->GetDTS(),DATALOAD_NUM);
	  mdf_task.FetchFirst("select * from dp.dp_datapart where (status=3 or status=30) and begintime<now() %s order by blevel,tabid,datapartid limit %d",psa->GetNormalTaskDesc(),DATALOAD_NUM);
	  int rn_task=mdf_task.Wait();
	  if(rn_task<1) {
		  printf("û�з��ִ�����ɵȴ�װ�������(����״̬=3).\n");
		  return 0;
	  }

	  // ��¼����װ��ı��id
	  std::vector<int> loading_tabid_arry;
	  int load_part_index = 0;
	  
	  char sqlbf[1000];
start_dataload:	  
	  // �ж�id�Ƿ����
	  if(load_part_index<rn_task-1)
	  {
		  for(int i=0;i<loading_tabid_arry.size();i++)
		  {
		  	  // ����ñ�������Ѿ���װ�룬������һ�����������װ��
	          if(mdf_task.GetInt("tabid",load_part_index) == loading_tabid_arry[i]){
			  	  if(load_part_index < rn_task-1){
				  	load_part_index++;
					goto start_dataload;
			  	  }
				  else{ // �Ѿ������һ����¼
	                 break;
				  }
	          }
		  }
	  }
	  
	  int rn = 0;
	  tabid=mdf_task.GetInt("tabid",load_part_index);
	  psa->SetTrace("dataload",tabid);
	  datapartid=mdf_task.GetInt("datapartid",load_part_index);
	  bool preponl=mdf_task.GetInt("status",load_part_index)==3;
	  lgprintf("װ������:%s.",preponl?"��װ��":"��װ��");
	  psa->OutTaskDesc("����װ�� :",tabid);

	  AutoMt mdf(psa->GetDTS(),MAX_DST_DATAFILENUM);
	  mdf.FetchAll("select * from dp.dp_datafilemap where tabid=%d and procstatus=0 and fileflag=%d and datapartid=%d limit 2",
		  tabid,preponl?1:0,datapartid);
	  if(mdf.Wait()<1) {
		  //errprintf("������%d(%d)ָʾ����������������Ҳ�����Ӧ�����ݼ�¼��\n�����������ļ���¼�����ڻ�״̬�ǿ���(0).\n",
		  //  tabid,datapartid);
	 	  mdf.FetchAll("select * from dp.dp_datafilemap where tabid=%d and procstatus=1 and fileflag=%d and datapartid=%d limit 2",
		   tabid,preponl?1:0,datapartid);
		  if(mdf.Wait()>0) {
		    lgprintf("������%d(%d)������װ�����������������̴���.\n",tabid,datapartid);

			// û�е����һ��װ�������¼��������һ��װ������
			if(load_part_index < rn_task-1){
			  	load_part_index++;
				if(load_part_index < rn_task-1) // ��ֹԽ��rn_task
				{
					loading_tabid_arry.push_back(tabid);
					goto start_dataload;
				}
		  	}

		    return 0;
		  }
		  lgprintf("������%d(%d)ָʾ����������������Ҳ�����Ӧ�����ݼ�¼��\n����������ļ���¼״̬�쳣�������������װ��.\n���ռ�¼����",
		    tabid,datapartid);
		  
		  //JIRA DM-61 �ձ������ڴ�����ɾ�����������к�̨��������
		  /* ȥ���ձ���ؽ�����,ʹ���±����Ľṹ
		     �ָ���ע�ͺ��ܼ�����������߹���
		  */
		  sprintf(sqlbf," update dp.dp_datapart set status=21 ,istimelimit=0,oldstatus=%d where tabid=%d and datapartid=%d and status<>21",
		    preponl?4:40,tabid,datapartid);
		  if(psa->DoQuery(sqlbf)!=1) {
		    				  lgprintf("�޸Ŀձ�װ�봦��״̬�쳣�����������������̳�ͻ��\n"
			  "   tabid:%d.\n",tabid);
			  return 0;
		  }
		  AutoMt srctstmt(0,10);
		  //�ȴ������ļ���Ŀ������,���ʧ��,��Դϵͳ��
		  if(psa->CreateDataMtFromFile(srctstmt,10,tabid,0)==0) {
		   AutoHandle srcdbc;
		   srcdbc.SetHandle(psa->BuildSrcDBC(tabid,-1));
		   psa->BuildMtFromSrcTable(srcdbc,tabid,&srctstmt);
		  }
		  
		  psa->CreateAllIndexTable(tabid,srctstmt,TBNAME_PREPONL,true,CI_IDX_ONLY,datapartid);
		  
		  return 1;
	  }
	  tabid=mdf.GetInt("tabid",0);
	  datapartid=mdf.GetInt("datapartid",0);
	  
	  psa->GetSoledIndexParam(-1,&dp,tabid);
	  AutoMt idxmt(psa->GetDTS(),10);
	  idxmt.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>0",tabid);
	  int idxrn=idxmt.Wait();
	  int totdatrn=0;
	  psa->log(tabid,0,DLOAD_DATA_NOTIFY,"��%d,����%d,����װ��,����������%d,��־�ļ� '%s' .",tabid,datapartid,idxrn,wociGetLogFile());
		  //Ϊ��ֹ��������,�����ļ�״̬�޸�.
		  mdf.FetchAll("select * from dp.dp_datafilemap where tabid=%d and fileflag=%d and datapartid=%d order by indexgid,fileid",
			  tabid,preponl?1:0,datapartid);
		  totdatrn=rn=mdf.Wait();
		  lgprintf("�޸������ļ��Ĵ���״̬(tabid:%d,%d���ļ�)��0-->1",tabid,rn);
		  sprintf(sqlbf,"update dp.dp_datafilemap set procstatus=1 where tabid=%d and "
			  "procstatus=0 and fileflag=%d and datapartid=%d",tabid,preponl?1:0,datapartid);
		  if(psa->DoQuery(sqlbf)!=rn) {
			  lgprintf("�޸������ļ��Ĵ���״̬�쳣�����������������̳�ͻ.tabid:%d.\n",tabid);
			   psa->log(tabid,datapartid,DLOAD_DATA_NOTIFY,"�޸������ļ��Ĵ���״̬�쳣�����������������̳�ͻ.tabid:%d.\n",tabid);
			   return 0;
		  }
	  try {
	  for(int i=0;i<idxrn;i++) {
		  indexid=idxmt.GetInt("indexgid",i);
		  mdf.FetchAll("select * from dp.dp_datafilemap where tabid=%d  and indexgid=%d and fileflag=%d and datapartid=%d order by fileid",
			  tabid,indexid,preponl?1:0,datapartid);
		  rn=mdf.Wait();
		  if(rn<1) 
		  {
		      psa->log(tabid,datapartid,DLOAD_CAN_NOT_FIND_DATA_FILE_ERROR,"�Ҳ��������ļ�(indexgid:%d,tabid:%d,datapartid:%d,fileflag:%d).",indexid,tabid,datapartid,preponl?1:0);
		      ThrowWith("�Ҳ��������ļ�(indexgid:%d,tabid:%d,datapartid:%d,fileflag:%d).",indexid,tabid,datapartid,preponl?1:0);
		   }
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
				  {
					  psa->log(tabid,datapartid,DLOAD_OPEN_WRITE_FILE_ERROR,"��%d,����%d,���ļ�%sд��ʧ��.",tabid,datapartid,fn);
					  ThrowWith("Open file %s for writing failed!",fn);
			      }
			  }
			  LONG64 totidxrn=0;
			  //lgprintf("�����ļ���%s",mdf.PtrStr("filename",0));
			  for(k=0;k<rn;k++) {
				  file_mt idxf;
				  idxf.Open(datmt.PtrStr("idxfname",k),0);
				  int rn_fromtab=datmt.GetInt("idxrn",k);
				  int mt=idxf.ReadBlock(0,0);
				  		  /* DM-55 check value of USER_ID */
		  /*if(indexid==2) {
		  LONG64 *pchk;
		   wociGetLongAddrByName(mt,"USER_ID",0,&pchk) ;
		   for(int chkoff=0;chkoff<idxrn;chkoff++)
		   {
		   	if(!wociIsNull(mt,0,chkoff)) {
		   		printf("Got a not null value at %d,value %ld.\n",chkoff,pchk[chkoff]);
		   		break;
		   	}
		   }
		   printf("Check done.\n");
		 }*/
				  isfixed=wociIsFixedMySQLBlock(mt);
				  LONG64 startat=totidxrn;
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
						  {
							  wociCopyToMySQL(mt,0,0,fp);
						  }
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
				  {
				  	  psa->log(tabid,datapartid,DLOAD_OPEN_READ_FILE_ERROR,"��%d,����%d,���ļ�%s��ȡʧ��.",tabid,datapartid,fn);
					  ThrowWith("�޷����ļ�'%s'������Ŀ¼��������(dt_path)�Ƿ���ȷ��",fn);
				  }
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
					  //wait (&rt);
					  if(rt)
					  {
						  psa->log(tabid,datapartid,DLOAD_COMPRESS_INDEX_TABLE_ERROR,"��%d,����%d,������%sѹ��ʧ��.",tabid,datapartid,idxname);
						  ThrowWith("������%sѹ��ʧ��.",idxname);
					   }
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
		  psa->log(tabid,datapartid,DLOAD_UPDATE_FILE_STATUS_NOTIFY,"��%d,����%d,װ���ļ��쳣���ѻָ�����״̬.",tabid,datapartid);
	
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
	updst.DirectExecute("update dp.dp_datapart set status=%d,istimelimit=0 where tabid=%d and datapartid=%d",
		preponl?4:40,tabid,datapartid);
	updst.DirectExecute("update dp.dp_datafilemap set procstatus=0 where tabid=%d and datapartid=%d",
		tabid,datapartid);
	lgprintf("����״̬����,3(MLoaded)--->4(DLoaded),��:%d,����:%d.",tabid,datapartid);
	psa->log(tabid,0,DLOAD_DATA_NOTIFY,"����װ��������ȴ���������.");
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
		  lgprintf(dtpath,"��%s.%s�Ѵ���,����ִ�и�������!",dstdbn,dsttabname);
//		if(!GetYesNo(dtpath,false)) {
			lgprintf("ȡ�������� ");
			return 0;
//		}			
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
	mt.FetchAll("select * from dp.dp_datapart where tabid=%d and status not in(0,5)",mt.GetInt("tabid",0));
	rn=mt.Wait();
	if(rn>0) {
		ThrowWith("Դ��'%s.%s'Ǩ�ƹ���δ��ɣ����ܸ���.",srcdbn,srctabname);
	}
	mt.FetchAll("select * from dp.dp_table where tabname=lower('%s') and databasename=lower('%s')",srctabname,srcdbn);
	rn=mt.Wait();
	lgprintf("���ò���ת��.");
	int dsttabid=psa->NextTableID();
	tabid=mt.GetInt("tabid",0);
	long recordnum=mt.GetLong("recordnum",0);
	int firstdatafileid=mt.GetInt("firstdatafileid",0);
	double totalbytes=mt.GetDouble("totalbytes",0);
	int datafilenum=mt.GetInt("datafilenum",0);
	if(recordnum<1) {
		lgprintf("Դ��'%s.%s'����Ϊ�գ������ʧ�ܡ�",srcdbn,srctabname);
		return 0;
	}
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
	char sqlbuf[MAX_STMT_LEN];
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
	
	lgprintf("����ת��.");
	for(i=0;i<rn;i++) {
		if(mt.GetInt("issoledindex",i)>0) {
			char tbn1[300],tbn2[300];
			//������ϵ�����������,���ٴ������������.
			psa->GetTableName(tabid,mt.GetInt("indexgid",i),NULL,tbn1,TBNAME_DEST);
			if(psa->TouchTable(tbn1)) {
			 psa->GetTableName(dsttabid,mt.GetInt("indexgid",i),NULL,tbn2,TBNAME_DEST);
			 lgprintf("������ '%s'-->'%s...'",tbn1,tbn2);
		 	 psa->FlushTables(tbn1);
			 MoveMySQLTable(dtpath,srcdbn,strstr(tbn1,".")+1,dstdbn,strstr(tbn2,".")+1);
			}
			else
				ThrowWith("�Ҳ���������'%s',�����쳣��ֹ,��Ҫ�ֹ���鲢�޸�����!",tbn1);
			int ilp=0;
			psa->GetTableName(tabid,mt.GetInt("indexgid",i),NULL,tbn1,TBNAME_DEST,ilp);
			if(psa->TouchTable(tbn1)) {
			 //3.10���¸�ʽ����.
			char srcf[300];
			//����MERGE�ļ�
			//
			sprintf(srcf,"%s%s/%s.MRG",dtpath,dstdbn,strstr(tbn2,".")+1);
			FILE *fp=fopen(srcf,"w+t");
			 while(psa->GetTableName(tabid,mt.GetInt("indexgid",i),NULL,tbn1,TBNAME_DEST,ilp)) {
			         psa->GetTableName(dsttabid,mt.GetInt("indexgid",i),NULL,tbn2,TBNAME_DEST,ilp++);
			         lgprintf("������ '%s'-->'%s...'",tbn1,tbn2);
		 	 	 psa->FlushTables(tbn1);
			         MoveMySQLTable(dtpath,srcdbn,strstr(tbn1,".")+1,dstdbn,strstr(tbn2,".")+1);
			         fprintf(fp,"%s\n",strstr(tbn2,".")+1);
			 }
			 fprintf(fp,"#INSERT_METHOD=LAST\n");
			 fclose(fp);
			}
		}
	}
	mt.FetchAll("select * from dp.dp_datafilemap where tabid=%d order by fileid",tabid);
	rn=mt.Wait();
	AutoMt idxmt(psa->GetDTS(),MAX_DST_DATAFILENUM);
	lgprintf("�����ļ�ת��.");
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
	//�����ļ�����
	mt.FetchAll("select * from dp.dp_datafilemap where tabid=%d and (fileflag=0 or fileflag is null) and isfirstindex=1 order by datapartid ,fileid",dsttabid);
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
	mdf.FetchAll("select * from dp.dp_datapart where status in (7,10) and begintime<now() %s order by blevel,tabid,datapartid limit 2",psa->GetNormalTaskDesc());
	int rn=mdf.Wait();
	if(rn<1) {
		printf("û�з�������ѹ����ɵȴ�װ�������.\n");
		return 0;
	}
	bool dpcp=mdf.GetInt("status",0)==7;
	int compflag=mdf.GetInt("compflag",0);
	tabid=mdf.GetInt("tabid",0); 
	psa->SetTrace("reload",tabid);
	mdf.FetchAll("select * from dp.dp_datafilemap where tabid=%d and (fileflag=0 or fileflag is null) order by indexgid",tabid);
	rn=mdf.Wait();
	if(rn<1) {
		lgprintf("װ�����ѹ������ʱ�Ҳ��������ļ���¼,���ձ���");
		char sqlbf[MAX_STMT_LEN];
		sprintf(sqlbf,"update dp.dp_datapart set status=30 where tabid=%d", tabid);
		if(psa->DoQuery(sqlbf)<1) 
			ThrowWith("����ѹ����������װ������޸�����״̬�쳣�����������������̳�ͻ(tabid:%d)��\n",tabid);
		return 0;
	}
	mdf.FetchAll("select * from dp.dp_table where tabid=%d",tabid);
	rn=mdf.Wait();
	if(rn<1) {
		lgprintf("װ�����ѹ������ʱ�Ҳ���dp.dp_table��¼(tabid:%d).",tabid);
		return 0;
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
	char sqlbf[MAX_STMT_LEN];
	sprintf(sqlbf,"update dp.dp_datapart set status=%d where tabid=%d", dpcp?8:11,tabid);
	if(psa->DoQuery(sqlbf)<1) 
		ThrowWith("����ѹ����������װ������޸�����״̬�쳣�����������������̳�ͻ(tabid:%d)��\n",tabid);
	
	//��ֹ�������룬�޸�����״̬
	//�����޸Ľ��漰�����ļ����滻,��������ǰ�ȹرձ�
	// ����رղ�������ס��ķ��ʣ�ֱ�����߳ɹ���
	//TODO  �ļ�����������ʱ����Ϊ����
	psa->CloseTable(tabid,NULL,false,true);
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
	sprintf(sqlbf,"update dp.dp_datapart set status=30,istimelimit=0 where tabid=%d", tabid);
	psa->DoQuery(sqlbf);
	sprintf(sqlbf,"update dp.dp_datafilemap set procstatus=0 where tabid=%d and fileflag=0",tabid);
	psa->DoQuery(sqlbf);
	lgprintf("����״̬�޸�Ϊ�����������(3),�����ļ�����״̬��Ϊδ����(0).");
	Load();
	return 1;
}


int DestLoader::RecreateIndex(SysAdmin *_Psa) 
{
	AutoMt mdf(psa->GetDTS(),MAX_MIDDLE_FILE_NUM);
	AutoMt mdf1(psa->GetDTS(),MAX_MIDDLE_FILE_NUM);
	char sqlbf[MAX_STMT_LEN];
	//�����Ҫ���ߵ�����
	// ʹ���Ӳ�ѯ������ʹ�߼�����
	//   select * from dp.dp_datapart a where status=21 and istimelimit!=22 and begintime<now() %s and tabid not in (
   	//select tabid from dp_datapart b where b.tabid=a.tabid and b.status!=21 and b.status!=5 and b.begintime<now())
	//order by blevel,tabid,datapartid
	
	mdf1.FetchAll("select * from dp.dp_datapart where status=21 and begintime<now() %s order by blevel,tabid,datapartid",psa->GetNormalTaskDesc());
	if(mdf1.Wait()>0) {
   	   int mrn=mdf1.GetRows();
	   for(int i=0;i<mrn;i++) {
	   //�����ߴ���
	   tabid=mdf1.GetInt("tabid",i);
   	   psa->SetTrace("dataload",tabid);
	   datapartid=mdf1.GetInt("datapartid",i);
   	   int newld=mdf1.GetInt("oldstatus",i)==4?1:0;
   	   int oldstatus=mdf1.GetInt("oldstatus",i);
   	   if(mdf1.GetInt("istimelimit",i)==22)
   	   //���������ڴ�������
   	   {
	   	mdf.FetchAll("select * from dp.dp_table where tabid=%d ",
		  tabid);
		if(mdf.Wait()>0)
		 lgprintf("�� %s.%s ���߹������������̴���������������쳣�˳���������װ��",
		 mdf.PtrStr("databasename",0),mdf.PtrStr("tabname",0));
		else
		 lgprintf("tabidΪ%d�ı����߹������������̴���������������쳣�˳���������װ��",tabid);
		continue;
	   }
	   mdf.FetchAll("select * from dp.dp_datapart where tabid=%d and status!=21 and status!=5 and begintime<now()",
		  tabid);
	   if(mdf.Wait()<1) 
	   {
	    //��Դ��(��ʽ��)����Ŀ���ṹ,����������ݳ�ȡΪ�յ����
	    char tbname[150],idxname[150];
	    psa->GetTableName(tabid,-1,tbname,idxname,TBNAME_PREPONL);
	    sprintf(sqlbf,"update dp.dp_datapart set istimelimit=22 where tabid=%d and status =21 and begintime<now()",tabid);
	    if(psa->DoQuery(sqlbf)<1)  {
		lgprintf("��%d����ʱ�޸�����״̬�쳣�����������������̳�ͻ��\n",
	        tabid);
	        continue;
	    }
	    try {
	    AutoMt destmt(0,10);
	    //����Ҳ��������ļ�,���Դ������ṹ
	    if(psa->CreateDataMtFromFile(destmt,0,tabid,newld)==0)
	    {
	     AutoHandle srcdbc;
	     srcdbc.SetHandle(psa->BuildSrcDBC(tabid,-1));
	     psa->BuildMtFromSrcTable(srcdbc,tabid,&destmt);
	    }
	    psa->CreateTableOnMysql(destmt,tbname,true);
	     
	    psa->CreateAllIndex(tabid,TBNAME_PREPONL,true,CI_DAT_ONLY,-1);
	    psa->DataOnLine(tabid);
	    AutoStmt st(psa->GetDTS());
	    st.DirectExecute("update dp.dp_datapart set istimelimit=0,blevel=mod(blevel,100) where tabid=%d  and blevel>=100 and begintime<now()",tabid);
	    st.DirectExecute("update dp.dp_datafilemap set blevel=0 where tabid=%d ",tabid);
	
	    lgprintf("ɾ���м���ʱ�ļ�...");
	    mdf.FetchAll("select * from dp.dp_middledatafile where tabid=%d",tabid);
	    
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
	    psa->CleanData(false,tabid);
	    return 1;
	    }
	    catch(...) {
			errprintf("��%d ����ʱ�����쳣����,�ָ�����״̬...",tabid);
			//
			//sprintf(sqlbf,"update dp.dp_datapart set status=21,istimelimit=0,blevel=if(ifnull(blevel,0)>=100,blevel,ifnull(blevel,0)+100),oldstatus=%d where tabid=%d and istimelimit=22", oldstatus,tabid);
			sprintf(sqlbf,"update dp.dp_datapart set istimelimit=0,blevel=if(ifnull(blevel,0)>=100,blevel,ifnull(blevel,0)+100),oldstatus=%d where tabid=%d and istimelimit=22", oldstatus,tabid);
			//sprintf(sqlbf,"update dp.dp_datapart set status=21 where tabid=%d and istimelimit=22", oldstatus,tabid);
			psa->DoQuery(sqlbf);
  	        	psa->log(tabid,0,124,"����ʱ�����쳣����,�ѻָ�����״̬.");
			throw;
	    }		
	  }
	  //else {
	  //  AutoStmt st(psa->GetDTS());
	  //  st.DirectExecute("update dp.dp_datapart set blevel=if(ifnull(blevel,0)>=100,blevel,ifnull(blevel,0)+100) where tabid=%d and datapartid=%d and ifnull(blevel,0)<100",tabid,datapartid);
	  //}
	  }
	}
	
	// ��װ�������ݵ��������ϣ��޸����������޸�����״̬Ϊ21
	mdf.FetchAll("select * from dp.dp_datapart where (status =4 or status=40 ) %s order by blevel,tabid,datapartid limit 2",psa->GetNormalTaskDesc());
	int rn=mdf.Wait();
	if(rn<1) {
		return 0;
	}
	datapartid=mdf.GetInt("datapartid",0);
	tabid=mdf.GetInt("tabid",0);
  	psa->SetTrace("dataload",tabid);
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
		lgprintf("���ִ���: �ؽ�����ʱ�������ļ��еĶ���������(%d)�������������е�ֵ(%d)����,tabid:%d,datapartid:%d.",
			rn,rni,tabid,datapartid);
		return 0; //dump && destload(create temporary index table) have not complete.
	}
	try {
		sprintf(sqlbf,"update dp.dp_datapart set oldstatus=status,status=%d where tabid=%d and datapartid=%d and (status =4 or status=40 )",20,tabid,datapartid);
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
		sprintf(sqlbf,"update dp.dp_datapart set status=%d,blevel=if(ifnull(blevel,0)>=100,blevel,ifnull(blevel,0)+100) where tabid=%d and datapartid=%d", preponl?3:30,tabid,datapartid);
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
	mdt.FetchAll("select distinct tabid,datapartid,status,compflag from dp.dp_datapart where (status=6 or status=9) and begintime<now() %s order by blevel,tabid,datapartid",psa->GetNormalTaskDesc());
	int rn1=mdt.Wait();
	int rn;
	int i=0;
	bool deepcmp;
	if(rn1<1) {
		return 0;
	}
	for(i=0;i<rn1;i++) {
		tabid=mdt.GetInt("tabid",i);
	  psa->SetTrace("recompress",tabid);
		datapartid=mdt.GetInt("datapartid",i);
		deepcmp=mdt.GetInt("status",i)==6;
		mdf.FetchAll("select * from dp.dp_datafilemap where tabid=%d and procstatus =0 and (fileflag=0 or fileflag is null)  order by blevel,datapartid,indexgid,fileid",tabid);
		rn=mdf.Wait();
		if(rn<1) {
			mdf.FetchAll("select * from dp.dp_datafilemap where tabid=%d and procstatus <>2 and (fileflag=0 or fileflag is null)  order by datapartid,indexgid,fileid",tabid);
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
				lgprintf("��%d--����ѹ����������ɣ�����״̬���޸�Ϊ%d,�����ļ�����״̬�޸�Ϊ����(0)",tabid,deepcmp?7:10);
				return 1;
			}
			else lgprintf("��%d(%d)---����ѹ������δ���,����û�еȴ�ѹ��������",tabid,datapartid);
		}
		else break;
	}
	if(i==rn1) return 0;
	
	//��ֹ��һ��mdt�е����ݱ���������������Ĵ���Ĺ�dp_datapart status
	mdt.FetchAll("select tabid,datapartid,status,compflag from dp.dp_datapart where (status=6 or status=9) and begintime<now() and tabid=%d and datapartid=%d",
	   mdf.GetInt("tabid",0),mdf.GetInt("datapartid",0));
	if(mdt.Wait()<1) {
		lgprintf("Ҫ����ѹ������������ļ�,��Ӧ����״̬�Ѹı�,ȡ������.\n"
		         " tabid:%d,datapartid:%d,fileid:%d.",mdf.GetInt("tabid",0),mdf.GetInt("datapartid",0),mdf.GetInt("fileid",0));
		return 0;
	}
	psa->OutTaskDesc("��������ѹ������(tabid %d datapartid %d)",mdf.GetInt("tabid",0),mdf.GetInt("datapartid",0));
	int compflag=mdt.GetInt("compflag",0);
	lgprintf("ԭѹ������:%d, �µ�ѹ������:%d .",mdf.GetInt("compflag",0),compflag);
	int fid=mdf.GetInt("fileid",0);
	psa->log(tabid,0,121,"����ѹ�������ͣ� %d-->%d ,�ļ���%d,��־�ļ� '%s' .",mdf.GetInt("compflag",0),compflag,fid,wociGetLogFile());

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
		//��ֹ���룬�޸������ļ�״̬��
		AutoStmt st(psa->GetDTS());
		st.Prepare("update dp.dp_datafilemap set procstatus=1 where tabid=%d and fileid=%d and procstatus=0 and fileflag!=2",
			tabid,fid);
		st.Execute(1);
		st.Wait();
		if(wociGetFetchedRows(st)!=1) {
			lgprintf("�����ļ�ѹ��ʱ״̬�쳣,tabid:%d,fid:%d,�������������̳�ͻ��"
				,tabid,fid);
			return 0;
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
		lgprintf("�����߳���:%d.",threadnum);
#define BFNUM 32
		char *srcbf=new char[SRCBUFLEN];//?�һ�δ����������ݿ飨��ѹ���󣩡
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
							pbh->origlen<1024?1024:pbh->origlen); //Unlock internal
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
					if(i==BFNUM) ThrowWith("���ش���ѹ���������������޷�����!.");
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
								else break;
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
							printf("�Ѵ���%d�����ݿ�(%d%%),%.2f(MB/s) ��ʱ%.0f��--Ԥ�ƻ���Ҫ%.0f��.\r",nextid,slastrow*100/idxrn,lastoffset/tm1/1024/1024,tm1,tm1/slastrow*(idxrn-slastrow));
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
					mSleep(10);
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
		st.DirectExecute("update dp.dp_datafilemap set procstatus=0, blevel=ifnull(blevel,0)+1 where tabid=%d and fileid=%d and procstatus=1 and fileflag=0",
			tabid,fid);
		st.DirectExecute("update dp.dp_datapart set blevel=ifnull(blevel,0)+100 where tabid=%d and datapartid=%d",
		   tabid,datapartid);
		errprintf("ɾ�������ļ��������ļ�");
		unlink(dstfn);
		unlink(idxdstfn);
		throw;
	}
	
	psa->log(tabid,0,122,"����ѹ������,�ļ�%d����С%d->%.0f",fid,origsize,dstfilelen);
	lgprintf("�ļ�ת������,Ŀ���ļ�:'%s',�ļ�����(�ֽ�):%.0f.",dstfn,dstfilelen);
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
		psa->SetTrace("transblock",tabid);
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
	char sqlbuf[MAX_STMT_LEN];
	char choose[200];
	wociSetEcho(FALSE); 
	lgprintf("remove table '%s.%s ' ...",dbn,tabname);
	AutoMt mt(psa->GetDTS(),MAX_DST_DATAFILENUM);
	AutoStmt st(psa->GetDTS());
	mt.FetchAll("select * from dp.dp_table where tabname=lower('%s') and databasename=lower('%s')",tabname,dbn);
	int rn=mt.Wait();
	if(rn<1) {
		//����dp_table��û�м�¼�ı�,�п�����dpio���ٻָ��ı�.
		char streamPath[300];
		sprintf(streamPath,"%s%s/%s.DTP",psa->GetMySQLPathName(0,"msys"),dbn,tabname);
        	FILE *fp=fopen(streamPath,"rb");
        	if(fp!=NULL) {
        		fclose(fp);
        		dtioStreamFile *pdtio=new dtioStreamFile("./");
			pdtio->SetStreamName(streamPath);
			pdtio->SetWrite(false);
			pdtio->StreamReadInit();
			DTIO_STREAM_TYPE stp=pdtio->GetStreamType();
			if(stp!=/*DTP_BIND) && stp!=*/DTP_DETACH)
				ThrowWith("ָ�����ļ�'%s'����%d���ǲ����ļ�!",streamPath,stp);
			dtioDTTable dtt(dbn,tabname,pdtio,false);
			dtt.DeserializeParam();
			dtparams_mt &dtmts=*dtt.GetParamMt();
			if(prompt) {
				sprintf(choose,"���ٻָ���DP��'%s.%s'����ɾ������¼��:%lld ?(Y/N)",dbn,tabname,dtmts.GetRecordNum());
				if(!GetYesNo(choose,false)) {
					lgprintf("ȡ��ɾ���� ");
					return 0;
				}			
			}
			sprintf(sqlbuf,"%s.%s",dbn,tabname);
			psa->FlushTables(sqlbuf);
			sprintf(sqlbuf,"drop table %s.%s",dbn,tabname);
			psa->DoQuery(sqlbuf);
			for(int i=0;i<dtmts.GetSoledIndexNum();i++) {
				char dbn1[200],tbn1[200];
				dtmts.GetIndexTable(i,dbn1,tbn1);
				sprintf(sqlbuf,"drop table %s.%s",dbn1,tbn1);
				psa->DoQuery(sqlbuf);
				char fn[PATH_LEN];
				//�������������Ƿ����(.frm).
				sprintf(fn,"%s%s/%s.frm",psa->GetMySQLPathName(0,"msys"),dbn1,dtmts.PartIndexTbn(i,0));
				fp=fopen(fn,"rb");
				if(fp!=NULL)
				{
					fclose(fp);
					for(int datapart=0;datapart<dtmts.GetTotDataPartNum();datapart++) {
						sprintf(sqlbuf,"drop table %s.%s",dbn1,dtmts.PartIndexTbn(i,datapart));
						psa->DoQuery(sqlbuf);
					}
				}
			}
			delete pdtio;
			unlink(streamPath);
			lgprintf("��'%s.%s'��ɾ��.",dbn,tabname);
			return 1;
		}
		else {
			//JIRA DM-8: �쳣�ָ��ı�������С���ϣ� Ŀ���+��������
			//  ������������������������ļ���û���������������Ҫ�ֹ�����
			sprintf(sqlbuf,"%s.%s",dbn,tabname);
			psa->FlushTables(sqlbuf);
			sprintf(sqlbuf,"drop table %s.%s",dbn,tabname);
			psa->DoQuery(sqlbuf);
			sprintf(sqlbuf,"%s.%sidx1",dbn,tabname);
			psa->FlushTables(sqlbuf);
			sprintf(sqlbuf,"drop table %s.%sidx1",dbn,tabname);
			psa->DoQuery(sqlbuf);
			sprintf(sqlbuf,"%s.%sidx1_p_1",dbn,tabname);
			psa->FlushTables(sqlbuf);
			sprintf(sqlbuf,"drop table %s.%sidx1_p_1",dbn,tabname);
			psa->DoQuery(sqlbuf);
		  ThrowWith("��%s.%s��dp_table���Ҳ�����Ŀ��������������ɾ�����������ݿ�����Ҫ�ֹ�����",dbn,tabname);
		}
	}
	tabid=mt.GetInt("tabid",0);
	double recordnum=mt.GetDouble("recordnum",0);
	int firstdatafileid=mt.GetInt("firstdatafileid",0);
	psa->CloseTable(tabid,NULL,true);
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
	
	
	lgprintf("ɾ�������ļ�.");
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

    //>> Begin: fix dm-254
    // ɾ��������м������������
	mt.FetchAll("select tabid,datafilename,indexfilename from dp.dp_middledatafile where tabid = %d",tabid);
    rn=mt.Wait();
	for(i=0;i<rn;i++)
	{
        lgprintf("ɾ���ɼ���ɺ�������ļ�'%s'",mt.PtrStr("datafilename",i));
	    unlink(mt.PtrStr("datafilename",i));

        lgprintf("ɾ���ɼ���ɺ�������ļ�'%s'",mt.PtrStr("indexfilename",i));
		unlink(mt.PtrStr("indexfilename",i));
	}
	//<< End:fix dm-254
	
	st.Prepare(" delete from dp.dp_middledatafile where tabid=%d",tabid);
	st.Execute(1);
	st.Wait();
	
	bool forcedel=false;
	//if(prompt) {
		sprintf(choose,"��'%s.%s'�����ò���ҲҪɾ����?(Y/N)",dbn,tabname);
		forcedel=GetYesNo(choose,false);
	//}
	mt.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>0",tabid);
	rn=mt.Wait();
	for(i=0;i<rn;i++) {
			char tmp[300];
			psa->GetTableName(tabid,mt.GetInt("indexgid",i),NULL,tmp,TBNAME_DEST);
			sprintf(sqlbuf,"drop table %s",tmp);
			lgprintf(sqlbuf);
	 	 	psa->FlushTables(tmp);
			if(psa->TouchTable(tmp)) psa->DoQuery(sqlbuf);
			int ilp=0;
			psa->GetTableName(tabid,mt.GetInt("indexgid",i),NULL,tmp,TBNAME_DEST,ilp);
			if(psa->TouchTable(tmp)) {
			 //3.10���¸�ʽ����.
			 while(psa->GetTableName(tabid,mt.GetInt("indexgid",i),NULL,tmp,TBNAME_DEST,ilp++)) {
	 	 		psa->FlushTables(tmp);
				if(psa->TouchTable(tmp)) {
			          lgprintf("ɾ�������� '%s'",tmp);
				  sprintf(sqlbuf,"drop table %s",tmp);
				  lgprintf(sqlbuf);
				  psa->DoQuery(sqlbuf);
				}
			 }
			}
	}
	if(forcedel) {
		st.Prepare(" delete from dp.dp_index where tabid=%d",tabid);
		st.Execute(1);
		st.Wait();
		st.Prepare(" delete from dp.dp_datapart where tabid=%d",tabid);
		st.Execute(1);
		st.Wait();
		st.Prepare(" delete from dp.dp_table where tabid=%d",tabid);
		st.Execute(1);
		st.Wait();
		st.Prepare(" delete from dp.dp_log where tabid=%d",tabid);
		st.Execute(1);
		st.Wait();
	}
	else {
		st.Prepare(" update dp.dp_table set recordnum=0,lstfid=0,cdfileid=0,firstdatafileid=0,datafilenum=0,totalbytes=0 where tabid=%d",tabid);
		st.Execute(1);
		st.Wait();
	}//psa->EmptyIndex(tabid);
	
	lgprintf("��'%s.%s'��ɾ��%s.",dbn,tabname,forcedel?"":",����������");
	return 1;
}

