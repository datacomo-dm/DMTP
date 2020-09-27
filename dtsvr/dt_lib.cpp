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
// 2005/08/27修改，partid等效于sub	

// 为保持wdbi库的连续运行(dtadmin单独升级),TestColumn在此实现。理想位置是到wdbi库实现
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
  Description : 删除字符串中连续重复的空格，create by liujs
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
        sp.log(dp.tabid,partid,DUMP_DST_TABLE_DATA_BLOCK_SIZE_ERROR,"表%d的目标表的数据块大小(%d)不合适，至少为%d.",dp.tabid,maxblockrn,MIN_BLOCKRN);
	    ThrowWith("目标表的数据块大小(%d)不合适，至少为%d.",maxblockrn,MIN_BLOCKRN);  
	}
	if(maxblockrn>MAX_BLOCKRN){
        sp.log(dp.tabid,partid,DUMP_DST_TABLE_DATA_BLOCK_SIZE_ERROR,"表%d的目标表的数据块大小(%d)不合适，不能超过%d.",dp.tabid,maxblockrn,MAX_BLOCKRN);
        ThrowWith("目标表的数据块大小(%d)不合适，不能超过%d.",maxblockrn,MAX_BLOCKRN);
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
	//屏蔽PK模式，全部按普通模式处理
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
		//子块分割
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
					     sp.log(dp.tabid,partid,DUMP_INDEX_BLOCK_SIZE_ERROR,"表%d,数据预处理错误,分区号%d,索引字段'%s',超过允许索引块长度%d.",dp.tabid,partid,idxcolsname,MAX_BLOCKRN);
						 ThrowWith("数据预处理错误,分区号%d,索引字段'%s',超过允许索引块长度%d.",partid,idxcolsname,MAX_BLOCKRN);
					}
				}
				strow=thisrow;
				idx_startrow=blockrn;
			}
			//blockmt.QuickCopyFrom(pcur,blockrn,thisrow);
			wociCopyRowsTo(cur,blockmt,-1,thisrow,1);
			blockrn++;//=wociGetMemtableRows(blockmt);
			//块和子块的分割
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
						sp.log(dp.tabid,partid,DUMP_INDEX_BLOCK_SIZE_ERROR,"表%d,数据预处理错误,分区号%d,索引字段'%s',超过允许索引块长度%d.",dp.tabid,partid,idxcolsname,MAX_BLOCKRN);					        	 
						ThrowWith("数据预处理错误,分区号%d,索引字段'%s',超过允许索引块长度%d.",partid,idxcolsname,MAX_BLOCKRN);
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
	//保存最后的块数据
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
	
	//保存索引数据
	{
		di.WriteMt(idxdt,COMPRESSLEVEL,0,false);
		di.SetFileHeader(totidxrn,NULL);
	}
	//保存文件索引
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
		// 防止并发访问？
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
	  //CMNET:增加文件导出是允许重入的条件，一个分区的导出可以多个进程并行
	//  mdf.FetchAll("select * from dp.dp_datapart where (status=0 or (status=72 and lower(extsql) like 'load %')) and begintime<now() %s order by blevel,touchtime,tabid,datapartid limit 2",sp.GetNormalTaskDesc());
        if(systype==NULL)
          mdf.FetchAll("select * from dp.dp_datapart where (status=0 or (status=72 and lower(extsql) like 'load %%')) and begintime<now() %s order by blevel,touchtime,tabid,datapartid limit 2",sp.GetNormalTaskDesc());
        else {
           printf("限制类型：%s\n",systype);
           mdf.FetchAll("select * from dp.dp_datapart where (status=0 or (status=72 and lower(extsql) like 'load %%')) and exists(select 1 from dp.dp_datasrc a where a.sysid=dp_datapart.srcsysid and a.systype in ( %s) ) and begintime<now() %s order by blevel,touchtime,tabid,datapartid limit 2",systype,sp.GetNormalTaskDesc ());
        }
        if(mdf.Wait()<1) return 0;
	//sp.GetFirstTaskID(NEWTASK,tabid,datapartid);
	sp.Reload();
	//CMNET:增加重入标记
	bool keepfiles=mdf.GetInt("status",0)==72;
	tabid=mdf.GetInt("tabid",0);
	datapartid=mdf.GetInt("datapartid",0);
	if(tabid<1) return 0;
	sp.SetTrace("dump",tabid);
	sorttm.Clear();
	fiotm.Clear();
	adjtm.Clear();
	sp.GetSoledIndexParam(datapartid,&dp,tabid);
	sp.OutTaskDesc("执行数据导出任务: ",tabid,datapartid);
	if(xmkdir(dp.tmppath[0])) 
	{
	    sp.log(tabid,datapartid,DUMP_CREATE_PATH_ERROR,"临时主路径无法建立,表:%d,数据组:%d,路径:%s.",tabid,datapartid,dp.tmppath[0]);
		ThrowWith("临时主路径无法建立,表:%d,数据组:%d,路径:%s.",tabid,datapartid,dp.tmppath[0]);
	}
	AutoHandle srcdbc;
	AutoHandle fmtdbc;
	// datapartid对应的源系统，不一定是格式表对应的源系统
	// Jira:DM-48
	try {
	  srcdbc.SetHandle(sp.BuildSrcDBC(tabid,datapartid));
		fmtdbc.SetHandle(sp.BuildSrcDBC(tabid,-1));
	}catch(...) {
		sp.log(tabid,datapartid,DUMP_CREATE_DBC_ERROR,"源(目标)数据源连接失败,表:%d,分区:%d",tabid,datapartid);
		AutoStmt st(dtdbc);
		st.DirectExecute("update dp.dp_datapart set blevel=ifnull(blevel,0)+100 where tabid=%d and datapartid=%d",
		  tabid,datapartid);
		throw;
	}

	AutoMt srctstmt(0,10);
	int partoff=0;
	try {	
		//如果格式表与目标表的结构不一致
		sp.BuildMtFromSrcTable(fmtdbc,tabid,&srctstmt);
		srctstmt.AddrFresh();
		int srl=sp.GetMySQLLen(srctstmt);//wociGetRowLen(srctstmt);
		char tabname[150];
		sp.GetTableName(tabid,-1,tabname,NULL,TBNAME_DEST);
		AutoMt dsttstmt(dtdbc,10);
		dsttstmt.FetchFirst("select * from dp.dp_datapart where tabid=%d and status=5 ",tabid);
		int ndmprn=dsttstmt.Wait();
                //天津移动发现的问题：两个分区并行导出，第二个分区在第一个开始整理后启动导出，dp_table.lstfid被复位，整理过程中文件序号错乱
                // 增加下面的5行代码
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
			sp.log(tabid,datapartid,DUMP_DST_TABLE_ERROR,"目标表%d不存在或结构错误,需要重新构造,已经设置了重构标记.",tabid);   
			AutoStmt st(dtdbc);
			st.DirectExecute("update dp.dp_table set cdfileid=0 where tabid=%d",tabid);
			lgprintf("目标表%d不存在或结构错误,需要重新构造,已经设置了重构标记.",tabid);
			throw;
		}
		if(srctstmt.CompareMt(dsttstmt)!=0 ) {
			if(tstrn>0 && ndmprn>0) 
			{
			    sp.log(tabid,datapartid,DUMP_DST_TABLE_FORMAT_MODIFIED_ERROR,"表%s中已经有数据，但源表(格式表)格式被修改，不能导入数据，请导入新的(空的)目标表中。",tabname);   
				ThrowWith("表%s中已经有数据，但源表(格式表)格式被修改，不能导入数据，请导入新的(空的)目标表中。",tabname);
			}
			lgprintf("源表数据格式发生变化，重新解析源表... ");
			if(tstrn==0) {
				sp.CreateDT(tabid);
				sp.Reload();
				sp.GetSoledIndexParam(datapartid,&dp,tabid);
			}
			else {
				//全部数据分组重新导入数据,可以允许结构暂时不一致
				//由于目标表有数据，暂时不修改dt_table.recordlen
				dp.rowlen=srl;
			}
		}
		else if(srl!=dp.rowlen) {
			lgprintf("目标表中的记录长度错误，%d修改为%d",dp.rowlen,srl);
			wociClear(dsttstmt);
			AutoStmt &st=dsttstmt.GetStmt();
			st.Prepare("update dp.dp_table set recordlen=%d where tabid=%d",srl,tabid);
			st.Execute(1);
			st.Wait();
			dp.rowlen=srl;
		}
		if(ndmprn==0 && tstrn==0) {
			//复位文件编号,如果目标表非空,则文件编号不置位
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
	//>> Begin:DMA-458,内存记录条数最大值限制,20130129
	if(realrn > MAX_ROWS_LIMIT-8)
	{
	    realrn = MAX_ROWS_LIMIT - 8;	
	    lgprintf("导出记录条数已经超过2G条，现将其从%d条修改为%d条,导出过程继续执行",memlimit/dp.rowlen,realrn);
	}
	//<< End:
	lgprintf("开始导出数据,数据抽取内存%ld字节(折合记录数:%d)",realrn*dp.rowlen,realrn);
	sp.log(tabid,datapartid,DUMP_BEGIN_DUMPING_NOTIFY,"开始数据导出:数据块%d字节(记录数%d),日志文件 '%s'.",realrn*dp.rowlen,realrn,wociGetLogFile());
	sp.log(tabid,datapartid,DUMP_BEGIN_DUMPING_NOTIFY,"开始导出表%d,分区%d数据",tabid,datapartid);
	//if(realrn>dp.maxrecnum) realrn=dp.maxrecnm;
	//CMNET: 重入时不删除数据
	 {
		lgprintf("清除上次导出的数据...");
		
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
			if(keepfiles) { //检查数据一致性 
				 clsmt.FetchAll("select sum(recordnum) srn from dp.dp_middledatafile where tabid=%d and datapartid=%d and procstate<=3",
				   tabid,datapartid);
				 clsmt.Wait();
				 double srn=clsmt.GetDouble("srn",0);
				 clsmt.FetchAll("select sum(recordnum) srn from dp.dp_filelog where tabid=%d and datapartid=%d and status=2",
				   tabid,datapartid);
				 clsmt.Wait();
				 if(srn!=clsmt.GetDouble("srn",0)) {
				 	sp.log(tabid,datapartid,DUMP_FILE_LINES_ERROR,"中间数据%.0f行,数据文件%.0f行，不一致，不能继续上一次数据抽取，建议重新导入。",srn,clsmt.GetDouble("srn",0));
					ThrowWith("中间数据%.0f行,数据文件%.0f行，不一致，不能继续上一次数据抽取，建议重新导入。",
					   srn,clsmt.GetDouble("srn",0));
				 }

				//add by ljs:删除上一次采集后，没有形成临时文件的源数据文件
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
		 		 sp.log(tabid,datapartid,DUMP_RECORD_NUM_ERROR,"表%d,分区%d 记录长度太大,无法迁移(记录长度%d)。",tabid,datapartid,dp.rowlen);
		 return 0;
		}
		lgprintf("由于记录长度太大,块大小调整为%d,目标表的缓存机制将失效。(记录数:%d,记录长度%d)",blocksize,maxblockrn,dp.rowlen);
	}
	else if(maxblockrn>MAX_BLOCKRN) {
		blocksize=(MAX_BLOCKRN-1)*dp.rowlen;
		maxblockrn=blocksize/dp.rowlen;
		lgprintf("由于记录长度太小，块大小调整为%d。 (记录数:%d,记录长度%d)",blocksize,maxblockrn,dp.rowlen);
	}

	//CMNET:增加重入处理
	if(!keepfiles){
		//在作数据导出时设置块记录数,以后的处理和查询以此为依据
		//字段maxrecinblock的使用方法变更为:程序根据后台设置的参数自动计算,管理控制台只读
		lgprintf("设置目标数据块%d字节(记录数:%d)",maxblockrn*dp.rowlen,maxblockrn);
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
		//CMNET:增加重入处理,
		//if(!keepfiles)
			 sp.UpdateTaskStatus(DUMPING,tabid,datapartid);
		// 更新新增字段 touchtime,pid,hostname,否则，任务只会重复执行第一个表，后面的表被搁置
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
			sp.log(tabid,datapartid,99,"数据抽取sql:%s.",dumpsql);
			TradeOffMt dtmt(0,realrn);
			blockmt.Clear();
			sp.BuildMtFromSrcTable(fmtdbc,tabid,&blockmt);
			//blockmt.Build(stmt);
			blockmt.AddrFresh();
			sp.BuildMtFromSrcTable(fmtdbc,tabid,dtmt.Cur());
			sp.BuildMtFromSrcTable(fmtdbc,tabid,dtmt.Next());
			//CMNET :文件模式判断，依据dumpsql的内容
			//  Load data from files using cmnet.ctl [exclude backup]
			// 动态库文件名中不能包含空格
			char libname[300];
			bool withbackup=true;//default to true
			if(strcasestr(dumpsql,"load data from files")!=NULL) {
				filesmode=true;
				char *plib=strcasestr(dumpsql,"using ");
				if(!plib) 
				{
				    sp.log(tabid,datapartid,DUMP_FILE_ERROR,"表%d,分区%d,文件导入数据缺少using关键字.",tabid,datapartid);
					ThrowWith("文件导入数据缺少using关键字.");
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
			//CMNET :文件模式
			if(!filesmode) {

				//>> Begin: fix dm-230 , 重新构造源表mt
                sp.IgnoreBigSizeColumn(srcdbc,dumpsql);
				//<< End: fix dm-230
				
				stmt.Prepare(dumpsql);


				/* // try to fix dm-356
			 	AutoMt tstmt(0,1);
			 	tstmt.Build(stmt);
			 	if(blockmt.CompatibleMt(tstmt)!=0 ) {
			 		sp.log(tabid,datapartid,DUMP_SQL_ERROR,"表%d,分区%d,数据抽取语句 %s 得到的格式与源表定义的格式不一致.\n",tabid,datapartid,dumpsql);
			 		ThrowWith("以下数据抽取语句 %s 得到的格式与源表定义的格式不一致.\n",dumpsql);
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
			//准备数据索引表插入变量数组
			//CMNET :文件模式
			int rn;
			bool filecp=false;
			bool needcp=false;
			int uncheckfct=0,uncheckbct=0;//检查点后的文件数和数据库数
			
			//>> Begin: DM-201 , modify by liujs
			IDumpFileWrapper *dfw;
			IFileParser *fparser;
			//<< End: modify by liujs

			bool filehold=false;//文件处理中（内存表满)
			if(filesmode) {
				dfw=new DumpFileWrapper(libname);
				fparser=dfw->getParser();
				fparser->SetTable(dtdbc,tabid,datapartid,dfw);
				//Get file and parser
				//如果上次的文件未处理完(内存表满),或者有新文件，则进入分解过程
				while(true) {
					if(!filehold) {
						 int gfstate=fparser->GetFile(withbackup,sp,tabid,datapartid);
						 if(gfstate==0) {
						 	//没有数据需要处理
						 	if(fparser->ExtractEnd()) break;
						 	//if(dtmt.Cur()->GetRows()>0) break;
							mySleep(fparser->GetIdleSeconds());
						 	continue;
						}
						else if(gfstate==2) //文件错误，但应该忽略
							continue;
						uncheckfct++;
					}
					int frt=fparser->DoParse(*dtmt.Cur(),sp,tabid,datapartid);
					filehold=false;
					if(frt==-1) {
						  sp.log(tabid,datapartid,DUMP_FILE_ERROR,"表%d,分区%d,文件处理错误.",tabid,datapartid); 
							ThrowWith("文件处理错误.");
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
		//文件检查点是否已到达
		while(rn>0) {
				if(!filesmode)
					dtmt.FetchNext();
				lgprintf("开始数据处理");
				int retryct=0;
				while(true) {
				 try{
				  freeinfo1("before call prcblk");
				  for(int i=0;i<dp.soledindexnum;i++) {
						ProcBlock(sp,datapartid,dtmt.Cur(),i/*dp.psoledindex*/,blockmt,sp.NextTmpFileID());
				  }
				  lgprintf("数据处理结束");
				  if(fnmt.GetRows()>0){
				  			wociSetEcho(false);
		 						wociAppendToDbTable(fnmt,"dp.dp_middledatafile",dtdbc,true);
		 						wociReset(fnmt);
		 						wociSetEcho(true);
					}
				  //文件模式目前只能在一个内存表中处理，将来再考虑两个同时用。
				  if(filesmode) {
				  	double fillrate=(double)dtmt.Cur()->GetRows()/dtmt.Cur()->GetMaxRows();
				  	wociReset(*dtmt.Cur());
				  	uncheckbct++;
					
					//检查点的提交条件
					//必须是一个文件处理结束，不允许数据文件的处理中间过程提交
					//达到5个数据块或者分区数据已经处理结束，则提交
					//或者达到或超过2个数据库未提交，但最后一个数据块已经超过80%--或者超过10个文件未提交							
					//后面的文件处理部分还需要做一致的调整
					if(!filehold && (uncheckbct>=5 || (uncheckbct>1 && fillrate>0.6) || fparser->ExtractEnd()))
					{			
				  	 	lgprintf("提交检查点：数据块%d,文件数%d,填充率:%.1f%%.",uncheckfct,uncheckbct,fillrate*100);
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
	    				 sp.log(tabid,datapartid,DUMP_WRITE_FILE_ERROR,"表%d,分区%d,写数据失败.",tabid,datapartid);
						throw;
					}
					lgprintf("写数据失败，重试...");
				 }
				}
				freeinfo1("after call prcblk");
			  if(filesmode) {
				//Get file and parser
				//如果上次的文件未处理完(内存表满),或者有新文件，则进入分解过程
				while(true) {
					if(!filehold) {
						 int gfstate=fparser->GetFile(withbackup,sp,tabid,datapartid);
						 if(gfstate==0) {
						 	//没有数据需要处理
						 	if(fparser->ExtractEnd()) break;
						 	//if(dtmt.Cur()->GetRows()>0) break;
							mySleep(fparser->GetIdleSeconds());
						 	continue;
						}
						else if(gfstate==2) //文件错误，但应该忽略
							continue;
						
						uncheckfct++;
					}
					int frt=fparser->DoParse(*dtmt.Cur(),sp,tabid,datapartid);
					filehold=false;
					if(frt==-1) {
						    sp.log(tabid,datapartid,DUMP_FILE_ERROR,"表%d,分区%d,文件处理错误.",tabid,datapartid); 						    
							ThrowWith("文件处理错误.");
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
		errprintf("数据导出异常终止，表%d(%d),中间文件数:%d.",tabid,datapartid,frn);
		AutoStmt st(dtdbc);
		st.DirectExecute("unlock tables");
		sp.log(tabid,datapartid,DUMP_EXCEPTION_ERROR,"表%d,分区%d 数据导出异常终止，中间文件数:%d.",tabid,datapartid,frn);
		bool restored=false;
		if(dumpcomplete) {
			//当前任务的导出已完成，但修改DP参数失败.重试10次,如果仍然失败,则放弃.
			int retrytimes=0;
			while(retrytimes<10 &&!restored) {
				restored=true;
				try {
					wociAppendToDbTable(fnmt,"dp.dp_middledatafile",dtdbc,true);
					sp.UpdateTaskStatus(DUMPED,tabid,datapartid);
				}
				catch(...) {
					sp.log(tabid,datapartid,DUMP_FINISHED_NOTIFY,"表%d(%d)导出已完成,但写入dp参数表(dp_middledatafile)失败,一分钟后重试(%d)...",tabid,datapartid,++retrytimes);
					lgprintf("表%d(%d)导出已完成,但写入dp参数表(dp_middledatafile)失败,一分钟后重试(%d)...",tabid,datapartid,++retrytimes);
					restored=false;
					mSleep(60000);
				}
			}
		}
		if(!restored) {
			int i;
			wdbi_kill_in_progress=false;
			wociMTPrint(fnmt,0,NULL);
			//先做恢复任务状态的操作,因为任务状态的人工调整最为容易.如果数据库连接一直没有恢复,
			//则任务状态恢复会引起异常,后续的删除文件和记录的操作不会被执行,可以由人工来确定是否可恢复,如何恢复
			errprintf("恢复任务状态.");
		    sp.log(tabid,datapartid,DUMP_RECOVER_TAST_STATUS_NOTIFY,"恢复任务状态.");
		  // 2010-12-01 增加任务状态 72，用于持续文件装入
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
			errprintf("删除中间文件...");
			for(i=0;i<frn;i++) {
				errprintf("\t %s \t %s",fnmt.PtrStr("datafilename",i),
					fnmt.PtrStr("indexfilename",i));
			}
			for(i=0;i<frn;i++) {
				unlink(fnmt.PtrStr("datafilename",i));
				unlink(fnmt.PtrStr("indexfilename",i));
			}
			errprintf("删除中间文件记录...");
			st.Prepare("delete from dp.dp_middledatafile where tabid=%d and datapartid=%d",tabid,datapartid);
			st.Execute(1);
			st.Wait();
			st.Prepare("delete from dp.dp_filelog where tabid=%d and datapartid=%d",tabid,datapartid);
			st.Execute(1);
			st.Wait();
		/* on a busy statement,wociSetTerminate will failed and throw a exception,so the last chance to
		    restore enviriement is lost. so move to here from begining or this catch block.hope this can process more stable.
		  LOGS:
		  [2007:11:02 10:40:31] 开始数据处理
		  [2007:11:02 10:40:42] Write file failed! filename:/dbsturbo/dttemp/cas/mddt_340652.dat,blocklen:218816,offset:3371426
		  [2007:11:02 10:40:42]  ErrorCode: -9.  Exception : Execute(Query) or Delete on a busy statement.
		*/
			wociSetTerminate(dtdbc,false);
			wociSetTerminate(sp.GetDTS(),false);
			throw;
		}
	}
	if(dumpcomplete) {
	 lgprintf("数据抽取结束,任务状态1-->2,tabid %d(%d)",tabid,datapartid);
	 lgprintf("sort time :%11.6f file io time :%11.6f adjust data time:%11.6f",
		sorttm.GetTime(),fiotm.GetTime(),adjtm.GetTime());
		lgprintf("结束");
		sp.log(tabid,datapartid,DUMP_FINISHED_NOTIFY,"表%d,分区%d,数据抽取结束 ,记录数%lld.",tabid,datapartid,srn);
  }	
	//lgprintf("按任意键继续...");
	//getchar();
	//MYSQL中的MY_ISAM不支持事务处理，对MYSQL表的修改不需要提交.
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
  // 判断指定列是否在索引列文件列表IndexListFile中，如果指定列columnName在则返回：true,
  // 否则返回：false
  bool GetColumnIndex(const char* IndexListFile,const char* columnName)
  {
	  FILE *fp = NULL;
	  fp = fopen(IndexListFile,"rt");
	  if(fp==NULL) 
	  {
			ThrowWith("打开索引列表文件失败.");
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
  
  // 创建数据任务表，通过源表命令参数设置
  int MiddleDataLoader::CreateSolidIndexTable(const char* orasvcname,const char * orausrname,const char* orapswd,
          const char* srcdbname,const char* srctabname,const char* dstdbname,const char* dsttabname,
		  const int  cmptype,const char* tmppath,const char* backpath,const char *taskdate,
		  const char* solid_index_list_file,char* ext_sql)
  {
      int ret = 0;
	  
	  // 1-- 判断目标表是否已经存在
	  AutoMt dst_table_mt(sp->GetDTS(),10);
	  dst_table_mt.FetchAll("select * from dp.dp_table where databasename=lower('%s') and tabname=lower('%s')",dstdbname,dsttabname);
	  if(dst_table_mt.Wait()>0) 
		  ThrowWith("表 %s.%s 已经存在。",dstdbname,dsttabname);

      // 2-- 连接源表数据库,判断ORACLE源表是否存在
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
		  ThrowWith("源表:%s.%s 不存在无法创建表:%s.%s。",srcdbname,srctabname,dstdbname,dsttabname);
	  }

      // 3-- 判断dp数据中的数据源是否存在,存在取出数据源，不存在添加数据源
      AutoMt data_src_mt(sp->GetDTS(),10);
	  int  data_src_id = 0;
	  #define ORACLE_TYPE 1
	  data_src_mt.FetchFirst("select sysid from dp.dp_datasrc where systype = %d and jdbcdbn = '%s' and username = '%s' and pswd = '%s'",
	  	ORACLE_TYPE,orasvcname,orausrname,orapswd);
	  int trn=data_src_mt.Wait();
	  if(trn<1) 
	  {
	      // dp数据库中不存在数据源，需要添加
          AutoStmt tmp_data_src_mt(sp->GetDTS());
		  ret = tmp_data_src_mt.DirectExecute("INSERT INTO dp.dp_datasrc(sysid,sysname,svcname,username,pswd,systype,jdbcdbn) "
		  	" select max(sysid)+1,'%s','CreateSolidIndexTable add','%s','%s',%d,'%s' from dp.dp_datasrc",
            orasvcname,orausrname,orapswd,ORACLE_TYPE,orasvcname);

		  if(ret != 1)
		  {             
		      ThrowWith("dp 数据库插入Oracle数据源[%s],插入失败!",orasvcname);
		  }

		  // 获取新插入的sysid
		  data_src_mt.FetchFirst("select max(sysid) as sysid from dp.dp_datasrc");
		  trn = data_src_mt.Wait();
          data_src_id = data_src_mt.GetInt("sysid",0);
	  }
	  else
	  {
          data_src_id = data_src_mt.GetInt("sysid",0);
	  }

	  // 4-- 判断dp.dp_path中路径ID是否存在
      // 4.1--查询临时路径
	  AutoMt path_mt(sp->GetDTS(),10);
	  int  data_tmp_pathid = 0,data_backup_pathid=0;
	  path_mt.FetchFirst("SELECT pathid FROM dp.dp_path where pathval = '%s' and pathtype = 'temp'",tmppath);
	  if(1 > path_mt.Wait()){
          AutoStmt tmp_path_mt(sp->GetDTS());
          ret = tmp_path_mt.DirectExecute("INSERT INTO dp.dp_path(pathid,pathtype,pathdesc,pathval) values(%d,'temp','CreateSolidIndexTable add','%s')",
		  sp->NextTableID(),tmppath);
		  if(-1 == ret) ThrowWith("dp数据库插入临时路径[%s]失败!",tmppath);
		  path_mt.FetchFirst("select pathid from dp.dp_path where pathval = '%s' and pathtype= 'temp'", tmppath);
		  path_mt.Wait();
		  data_tmp_pathid = path_mt.GetInt("pathid",0);
	  }
	  else
	  {
	      data_tmp_pathid =  path_mt.GetInt("pathid",0);
	  }

	  // 4.2--查询数据路径
	  path_mt.FetchFirst("SELECT pathid FROM dp.dp_path where pathval = '%s' and pathtype = 'data'",backpath);
	  if(1 > path_mt.Wait()){
	      AutoStmt tmp_path_mt(sp->GetDTS());
		  ret = tmp_path_mt.DirectExecute("INSERT INTO dp.dp_path(pathid,pathtype,pathdesc,pathval) values(%d,'data','CreateSolidIndexTable add','%s')",
		  sp->NextTableID(),backpath);
		  if(-1 == ret) ThrowWith("dp数据库插入备份路径[%s]失败!",backpath);
		  path_mt.FetchFirst("select pathid from dp.dp_path where pathval = '%s' and pathtype= 'data'", backpath);
		  path_mt.Wait();
		  data_backup_pathid = path_mt.GetInt("pathid",0);
	  }
	  else
	  {
	  	  data_backup_pathid =  path_mt.GetInt("pathid",0);
	  }
	  
	  // 5-- 判断时间格式是否正确
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
			  ThrowWith("日期格式错误:'%s'",taskdate);
		  }
		  memcpy(tdt,mt.PtrDate("tskdate",0),7);
		  if(wociGetYear(tdt)==0){
			  ThrowWith("日期格式错误:'%s'",taskdate);
		  }
	  }
	  
      // 6-- 插入dp_table表中数据信息
      int table_id = 0;
      AutoMt table_mt(sp->GetDTS(),100);
	  table_mt.FetchFirst("select * from dp.dp_table limit 1");
	  if(table_mt.Wait() != 1)
	  {
          ThrowWith("数据库表dp_table中没有记录，请先通过web管理平台创建一个任务后再进行该操作.");
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

	  
	  // 获取tabid
	  table_id = sp->NextTableID();
	  *table_mt.PtrInt("tabid",0)=table_id;

	  // 7-- 检查表id是否存在记录
	  AutoMt check_mt(sp->GetDTS(),10);
	  // 对应的源表稍后修改
	  check_mt.FetchAll("select * from dp.dp_table where tabid=%d",table_id);
	  if(check_mt.Wait()>0)
		ThrowWith("编号重复: 编号%d的目标表'%s.%s'已经存在!",check_mt.GetInt("tabid",0),check_mt.PtrStr("databasename",0),
		    check_mt.PtrStr("tabname",0));
	  check_mt.FetchAll("select * from dp.dp_index where tabid=%d",table_id);
	  if(check_mt.Wait()>0)
		ThrowWith("发现不正确的索引参数(表%d)！",check_mt.GetInt("tabid",0));
	  check_mt.FetchAll("select * from dp.dp_datafilemap where tabid=%d and fileflag!=2",table_id);
	  if(check_mt.Wait()>0) 
		ThrowWith("发现不正确的数据文件记录(表%d)!",check_mt.GetInt("tabid",0));
	  check_mt.FetchAll("select * from dp.dp_datapart where tabid=%d",table_id);
	  if(check_mt.Wait()>0) 
		ThrowWith("发现不正确的数据分组参数(表%d)!",check_mt.GetInt("tabid",0));
	  	

	  // 8-- 插入dp_index表中数据信息
	  AutoMt  src_table_mt(ora_dts,5);
	  // 索引列名
      char idx_col_name[256] = {0}; 
	  try
	  {
	      // 8.1 -- 查询oracle源数据库表，获取列信息,获取sql语句
          sp->IgnoreBigSizeColumn(ora_dts,srcdbname,srctabname,ext_sql);

          // 8.2 -- 获取源表结构
		  src_table_mt.FetchFirst(ext_sql);
	      src_table_mt.Wait();

	      // 8.3 -- 如果源表结构中存在mysql关键字，则将其替换掉
	      char cfilename[256];
          strcpy(cfilename,getenv("DATAMERGER_HOME"));
	      strcat(cfilename,"/ctl/");
	      strcat(cfilename,MYSQL_KEYWORDS_REPLACE_LIST_FILE);
	      sp->ChangeMtSqlKeyWord(src_table_mt,cfilename);

		  // 8.4 -- 逐个索引列判断
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
	      if(!get_column_index_flag)  // 列表文件中没有找到索引，就用第一个列作为索引
	      {  
	          wociGetColumnName(src_table_mt,0,idx_col_name);
	      }
	  }
	  catch(...)
	  {
         ThrowWith("源表:%s.%s 不存在无法创建表:%s.%s。",srcdbname,srctabname,dstdbname,dsttabname);
	  }
	  
	 
      // 8.3 -- 插入dp_index表中记录
      AutoMt index_mt(sp->GetDTS(),10);
	  index_mt.FetchFirst("select * from dp.dp_index limit 1");
	  if(index_mt.Wait() != 1)
	  {
          ThrowWith("数据库表dp.dp_index中没有记录，请先通过web管理平台创建一个任务后再进行该操作.");
	  }
	  *index_mt.PtrInt("indexgid",0)=1;  
	  *index_mt.PtrInt("tabid",0)=table_id;
	  strcpy(index_mt.PtrStr("indextabname",0),"");//dsttabname);	 
	  *index_mt.PtrInt("seqindattab",0)=1;
	  *index_mt.PtrInt("seqinidxtab",0)=1;
	  *index_mt.PtrInt("issoledindex",0)=1;
	  strcpy(index_mt.PtrStr("columnsname",0),idx_col_name);	
	  *index_mt.PtrInt("idxfieldnum",0)=1;
	  
	  // 9-- 插入dp_datapart表中数据信息
      AutoMt datapart_mt(sp->GetDTS(),10);
	  datapart_mt.FetchFirst("select * from dp.dp_datapart limit 1");
	  if(datapart_mt.Wait() != 1)
	  {
          ThrowWith("数据库表dp.dp_datapart中没有记录，请先通过web管理平台创建一个任务后再进行该操作.");
	  }

      // 9.1 -- 判断源表中是否存在大字段，如果存在大字段就将其过滤掉
      sp->IgnoreBigSizeColumn(ora_dts,srcdbname,srctabname,ext_sql);

	  // 9.2 -- 保存任务信息到dp_datapart表中
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
		  //恢复数据，回退所有操作
		  AutoStmt st(sp->GetDTS());
		  st.DirectExecute("delete from dp.dp_table where tabid=%d",table_id);
		  st.DirectExecute("delete from dp.dp_index where tabid=%d",table_id);
		  st.DirectExecute("delete from dp.dp_datapart where tabid=%d",table_id);
		  errprintf("作类似创建时数据提交失败，已删除数据。");
		  throw;
	  }
	  {
		  char dtstr[100];
		  wociDateTimeToStr(tdt,dtstr);
		  lgprintf("创建成功,目标表'%s.%s:%d',开始时间'%s'.",dstdbname,dsttabname,table_id,dtstr);
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
			  ThrowWith("日期格式错误:'%s'",taskdate);
		  memcpy(tdt,mt.PtrDate("tskdate",0),7);
		  if(wociGetYear(tdt)==0)
			  ThrowWith("日期格式错误:'%s'",taskdate);
	  }
	  mt.FetchAll("select * from dp.dp_table where databasename=lower('%s') and tabname=lower('%s')",ndbn,ndsttbn);
	  if(mt.Wait()>0) 
		  ThrowWith("表 %s.%s 已经存在。",dbn,ndsttbn);
	  
	  AutoMt tabmt(sp->GetDTS(),100);
	  tabmt.FetchAll("select * from dp.dp_table where databasename=lower('%s') and lower(tabname)='%s'",dbn,tbn);
	  if(tabmt.Wait()!=1) 
		  ThrowWith("参考表 %s.%s 不存在。",dbn,tbn);
	  int reftabid=tabmt.GetInt("tabid",0);
	  //填充目标表信息
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
	  //参考源表后面还要引用,暂时不替换
	  //strcpy(tabmt.PtrStr("srcowner",0),srcowner);
	  //strcpy(tabmt.PtrStr("srctabname",0),nsrctbn);
	  tabid=sp->NextTableID();
	  *tabmt.PtrInt("tabid",0)=tabid;
	  // 对应的源表稍后修改
	  mt.FetchAll("select * from dp.dp_table where tabid=%d",tabid);
	  if(mt.Wait()>0)
		ThrowWith("编号重复: 编号%d的目标表'%s.%s'已经存在!",mt.GetInt("tabid",0),mt.PtrStr("databasename",0),mt.PtrStr("tabname",0));
	  mt.FetchAll("select * from dp.dp_index where tabid=%d",tabid);
	  if(mt.Wait()>0)
		ThrowWith("发现不正确的索引参数(表%d)！",mt.GetInt("tabid",0));
	  mt.FetchAll("select * from dp.dp_datafilemap where tabid=%d and fileflag!=2",tabid);
	  if(mt.Wait()>0) 
		ThrowWith("发现不正确的数据文件记录(表%d)!",mt.GetInt("tabid",0));
	  mt.FetchAll("select * from dp.dp_datapart where tabid=%d",tabid);
	  if(mt.Wait()>0) 
		ThrowWith("发现不正确的数据分组参数(表%d)!",mt.GetInt("tabid",0));

	  AutoMt indexmt(sp->GetDTS(),200);
	  indexmt.FetchAll("select * from dp.dp_index where tabid=%d",reftabid);
	  int irn=indexmt.Wait();
	  if(irn<1)
		  ThrowWith("参考表 %s.%s 没有建立索引。",dbn,tbn);
	  int soledct=1;
	  //填充索引信息，重建复用关系
	  for(int ip=0;ip<irn;ip++) {
	  	*indexmt.PtrInt("tabid",ip)=tabid;
	  	//set indextabname to null 
	  	*indexmt.PtrStr("indextabname",ip)=0;
	  }
	  
	  AutoMt taskmt(sp->GetDTS(),500);
	  taskmt.FetchAll("select * from dp.dp_datapart where tabid=%d",reftabid);
	  int trn=taskmt.Wait();
	  if(trn<1) 
		  ThrowWith("参考表 %s.%s 没有数据分组信息。",dbn,tbn);
	  
	  //对数据抽取语句作大小写敏感的源表名称替换,可能有以下问题:
	  // 1. 如果字段名或常量名中与源表名称相同,会造成替换失败
	  // 2. 如果源表名称大小写不一致,会造成替换失败
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
		   //复制 from 之前的语句(含 from );
		   strncpy(extsql,psql,sch-tmp);
		   extsql[sch-tmp]=0;
		   bool fullsrc=true; 
		   int tablen=strlen(treffull);
		   char *sch2=strstr(sch,treffull);
		   if(sch2==NULL) {
		        //参考表的sql语句中不含格式表的完整形式
		        //检查是否含部分（只有表名，没有库名)
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
		   //至少要含完整或部分形式的格式表，才替换
		   if(sch2) {
		   	//只把from ... where 之间符合替换条件的部分替换，其它的保留
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
	   	lgprintf("数据分组%d的数据抽取语句未作修改，请根据需要手工调整.",taskmt.GetInt("datapartid",tp));
	    else
	     lgprintf("数据分组%d的数据抽取语句已经修改：\n%s\n--->\n%s\n请根据需要手工调整.",taskmt.GetInt("datapartid",tp),sqlbk,psql);
	    
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
		  //恢复数据，回退所有操作
		  AutoStmt st(sp->GetDTS());
		  st.DirectExecute("delete from dp.dp_table where tabid=%d",tabid);
		  st.DirectExecute("delete from dp.dp_index where tabid=%d",tabid);
		  st.DirectExecute("delete from dp.dp_datapart where tabid=%d",tabid);
		  errprintf("作类似创建时数据提交失败，已删除数据。");
		  throw;
	  }
	  {
		  char dtstr[100];
		  wociDateTimeToStr(tdt,dtstr);
		  lgprintf("创建成功,源表'%s',目标表'%s',开始时间'%s'.",nsrctbn,ndsttbn,dtstr);
	  }
	  return 0;
}

//DT data&index File Check
int MiddleDataLoader::dtfile_chk(const char *dbname,const char *tname) {
	//Check deserved temporary(middle) fileset
	//检查状态为1的任务
	mdf.FetchAll("select * from dp.dp_table where databasename=lower('%s') and tabname=lower('%s')",dbname,tname);
	int rn=mdf.Wait();
	if(rn<1) ThrowWith("DP文件检查:在dp_table中'%s.%s'目标表无记录。",dbname,tname);
	int firstfid=mdf.GetInt("firstdatafileid",0);
	int tabid=*mdf.PtrInt("tabid",0);
	int blockmaxrn=*mdf.PtrInt("maxrecinblock",0);
	double totrc=mdf.GetDouble("recordnum",0);
	sp->OutTaskDesc("目标表检查 :",0,tabid);
	char *srcbf=new char[SRCBUFLEN];//每一次处理的最大数据块（解压缩后）。
	int errct=0;
	mdf.FetchAll("select * from dp.dp_datafilemap where tabid=%d and fileflag!=2 order by indexgid,fileid",tabid);
	int irn=mdf.Wait();
	if(irn<1) {
		ThrowWith("DP文件检查:在dp_datafilemap中%d目标表无数据文件的记录。",tabid);
	}
	
	{
		AutoMt datmt(sp->GetDTS(),100);
		datmt.FetchAll("select * from dp.dp_datafilemap where tabid=%d and fileid=%d",tabid,firstfid);
		if(datmt.Wait()!=1) 
			ThrowWith("开始数据文件(编号%d)在系统中没有记录.",firstfid);
		char linkfn[300];
		strcpy(linkfn,datmt.PtrStr("filename",0));
		datmt.FetchAll("select * from dp.dp_datafilemap where tabid=%d and fileflag!=2 and isfirstindex=1 order by datapartid,fileid",tabid);
		int rn1=datmt.Wait();
		if(firstfid!=datmt.GetInt("fileid",0)) 
			ThrowWith("开始数据文件(编号%d)在设置错误，应该是%d..",firstfid,datmt.GetInt("fileid",0));
		int lfn=0;
		while(true) {
			file_mt dtf;
			lfn++;
			dtf.Open(linkfn,0);
			const char *fn=dtf.GetNextFileName();
			if(fn==NULL) {
				printf("%s==>结束.\n",linkfn);
				break;
			}
			printf("%s==>%s\n",linkfn,fn);
			strcpy(linkfn,fn);
		}
		if(lfn!=rn1) 
			ThrowWith("文件链接错误，缺失%d个文件.",rn1-lfn);
	}
	
	mytimer chktm;
	chktm.Start();
	try {
		int oldidxid=-1;
		for(int iid=0;iid<irn;iid++) {
			//取基本参数
			int indexid=mdf.GetInt("indexgid",iid);
			
			AutoMt idxsubmt(sp->GetDTS(),100);
			idxsubmt.FetchAll("select * from dp.dp_index where tabid=%d and indexgid=%d ",tabid,indexid);
			rn=idxsubmt.Wait();
			if(rn<1) {
				ThrowWith("DP文件检查:在dp.dp_index中%d目标表无%d索引的记录。",tabid,indexid);
			}
			
			printf("检查文件%s--\n",mdf.PtrStr("filename",iid));
			fflush(stdout);
			
			//遍历全部索引数据
			char dtfn[300];
			dt_file idxf;
			idxf.Open(mdf.PtrStr("idxfname",iid),0);
			printf("索引文件：%s.\n",mdf.PtrStr("idxfname",iid));
			AutoMt idxmt(0);
			idxmt.SetHandle(idxf.CreateMt(1));
			idxmt.SetHandle(idxf.CreateMt(FIX_MAXINDEXRN/wociGetRowLen(idxmt)));
			idxf.Open(mdf.PtrStr("idxfname",iid),0);
			int brn=0;//idxf.ReadMt(-1,0,idxmt,false);
			int sbrn=0;
			while( (sbrn=idxf.ReadMt(-1,0,idxmt,true,1))>0) brn+=sbrn;
			printf("索引%d行。\n",brn);
			int thiserrct=errct;
			//把索引数据文件的后缀由idx替换为dat就是数据文件.
			AutoMt destmt(0);
			strcpy(dtfn,mdf.PtrStr("idxfname",iid));
			strcpy(dtfn+strlen(dtfn)-3,"dat");
			//从数据文件取字段结构，内存表大小为目标表的每数据块最大行数。
			//destmt.SetHandle(dtf.CreateMt(blockmaxrn));
			int myrowlen=0;
			{
				dt_file datf;
				datf.Open(dtfn,0);
				myrowlen=datf.GetMySQLRowLen();
			}
			FILE *fp=fopen(dtfn,"rb");
			printf("检查数据文件：%s.\n",dtfn);
			if(fp==NULL)
				ThrowWith("DP文件检查:文件'%s'错误.",dtfn);
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
				//直接使用字段名称会造成idx_rownum字段的名称不匹配，早期的idx数据文件中的字段名为rownum.
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
							errprintf("读数据错误，位置:%d,长度:%d,索引序号:%d.\n",blockstart,blocksize,i);
							throw -1;
						}
						pbhdr->ReverseByteOrder();
						if(!dt_file::CheckBlockFlag(pbhdr->blockflag))
						{
							errprintf("错误的块标识，位置:%d,长度:%d,索引序号:%d.\n",blockstart,blocksize,i);
							throw -1;
						}
						if(pbhdr->blockflag!=bkf) {
							bkf=pbhdr->blockflag;
							//if(bkf==BLOCKFLAG) printf("数据块类型:WOCI.\n");
							//else if(bkf==MYSQLBLOCKFLAG)
							//  printf("数据块类型:MYISAM.\n");
							printf("数据块类型: %s .\n",dt_file::GetBlockTypeName(pbhdr->blockflag));
							printf("压缩类型:%d.\n",pbhdr->compressflag);
						}
						if(pbhdr->storelen!=blocksize-sizeof(block_hdr))
						{
							errprintf("错误的块长度，位置:%d,长度:%d,索引序号:%d.\n",blockstart,blocksize,i);
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
							errprintf("太多的错误，已放弃检查！");
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
				errprintf("文件长度不匹配，数据文件长度:%d,索引文件指示的结束位置:%d\n",flen,ftell(fp));
				errct++;
			}
			printf("文件检查完毕，错误数 ：%d.    \n",errct-thiserrct);
			fclose(fp);
		}// end of for
	} // end of try
	catch (...) {
		errprintf("DP文件检查出现异常，tabid:%d.",tabid);
	}
	delete []srcbf;
	if(errct>0)
		errprintf("DP文件检查出现错误，请重新装入或者重新抽取数据！");
	printf("\n");
	chktm.Stop();
	lgprintf("DP文件检查结束,共处理%d个文件,发现%d个错误(%.2fs).",irn,errct,chktm.GetTime());
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
				  lgprintf("表%d(%d)的数据为空，修改为已整理。",mdf.GetInt("tabid",rn),mdf.GetInt("datapartid",rn));
			  }
		  }
	  }
  }
  // MAXINDEXRN 缺省值500MB,LOADIDXSIZE缺省值1600MB
  // MAXINDEXRN 为最终索引文件,记录长度对应索引表.
  // LOADIDXSIZE 存储临时索引,记录长度从数据导出时的索引文件中内存表计算.
  
  //2005/08/24修改： MAXINDEXRN不再使用
  //2005/11/01修改： 装入状态分为1(待装入），2（正装入),3（已装入);取消10状态(原用于区分第一次和第二次装入)
  //2005/11/01修改： 索引数据装入内存时的策略改为按索引分段。
  //2006/02/15修改： 一次装入所有索引数据，改为分段装入
  //			一次只装入每个文件中的一个内存表(解压缩后1M,索引记录设50字节长,则有约2万条记录,1G内存可以容纳1000/(1*1.2)=800个数据文件)
  //procstate状态说明：
  //  >1 :正在导出数据
  //  <=1:已完成数据导出
  int MiddleDataLoader::Load(int MAXINDEXRN,long LOADTIDXSIZE,bool useOldBlock) {
	  //Check deserved temporary(middle) fileset
	  //检查状态为1的任务，1为抽取结束等待装入.
	  CheckEmptyDump();
	  //2010-12-01 增加分区任务状态必须是2的选项
	  mdf.FetchAll("select mf.* from dp.dp_middledatafile mf,dp.dp_datapart dp "
	     " where mf.procstate<=1 and mf.tabid=dp.tabid and mf.datapartid=dp.datapartid  "
	     " and ifnull(dp.blevel,0)%s and dp.status=2 "
	     " order by mf.blevel,mf.tabid,mf.datapartid,mf.indexgid limit 100",
	     sp->GetNormalTask()?"<100":">=100");
	  int rn=mdf.Wait();
	  //如果索引超过LOADIDXSIZE,需要分批处理,则只在处理第一批数据时清空DT_DATAFILEMAP/DT_INDEXFILEMAP表
	  //区分第一次和第二次装入的意义：如果一份数据子集（指定数据集-〉分区(datapartid)->索引)被导出后，在开始装入以前，要
	  //  先删除上次正在装入的数据（注意：已上线的数据不在这里删除，而在装入结束后删除).
	  bool firstbatch=true;
	  if(rn<1) return 0;
	  
	  //检查该数据子集是否第一次装入
	  int tabid=mdf.GetInt("tabid",0);
	  int indexid=mdf.GetInt("indexgid",0);
	  int datapartid=mdf.GetInt("datapartid",0);
	  mdf.FetchAll("select procstate from dp.dp_middledatafile "
	    " where tabid=%d and datapartid=%d and indexgid=%d and procstate>1 limit 10",
		  tabid,datapartid,indexid);
	  firstbatch=mdf.Wait()<1;//数据子集没有正整理或已整理的记录。
	  
	  //取出中间文件记录
	  mdf.FetchAll("select * from dp.dp_middledatafile "
	    " where  tabid=%d and datapartid=%d and indexgid=%d and procstate<=1 order by mdfid limit %d",
		  tabid,datapartid,indexid,MAX_MIDDLE_FILE_NUM);
	  rn=mdf.Wait();
	  if(rn<1) 
	  {
		  sp->log(tabid,datapartid,MLOAD_CAN_NOT_FIND_MIDDLEDATA_ERROR,"表%d,分区%d,确定数据子集后找不到中间数据记录(未处理)。",tabid,datapartid);
		  ThrowWith("MiddleDataLoader::Load : 确定数据子集后找不到中间数据记录(未处理)。");
	  }
	  
	  //取基本参数
	  long idxtlimit=0,idxdlimit=0;//临时区(多个抽取数据文件对应)和目标区(单个目标数据文件对应)的索引最大记录数.
	  tabid=mdf.GetInt("tabid",0);
	  sp->SetTrace("datatrim",tabid);
	  indexid=mdf.GetInt("indexgid",0);
	  datapartid=mdf.GetInt("datapartid",0);
	  int compflag=5;
	  bool col_object=false; // useOldBlock=true时，忽略col_object
	  //取压缩类型和数据块类型
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
	  
	  //从dt_datafilemap(存blockmt文件表)和dt_indexfilemap(存indexmt文件表)
	  //建立内存表结构
	  char fn[300];
	  AutoMt fnmt(sp->GetDTS(),MAX_DST_DATAFILENUM);
	  
	  fnmt.FetchAll("select * from dp.dp_datafilemap limit 2");
	  fnmt.Wait();
	  wociReset(fnmt);
	  LONG64 dispct=0,lstdispct=0,llt=0;
	  sp->OutTaskDesc("数据重组 :",tabid,datapartid,indexid);
	  sp->log(tabid,datapartid,MLOAD_DATA_RECOMBINATION_NOTIFY,"表%d,分区%d,数据重组,索引组%d,日志文件 '%s' .",tabid,datapartid,indexid,wociGetLogFile());
	  int start_mdfid=0,end_mdfid=0;
	  char sqlbf[200];
	  LONG64 extrn=0,lmtextrn=-1;
	  LONG64 adjrn=0;
	  try {	
		  tmpfilenum=rn;
		  //索引数据文件遍历，累加索引总行数
		  LONG64 idxrn=0;
		  long i;
		  long mdrowlen=0;
		  //取索引记录的长度(临时索引数据记录)
		  {
			  dt_file df;
			  df.Open(mdf.PtrStr("indexfilename",0),0);
			  mdrowlen=df.GetRowLen();
		  }
		  
		  lgprintf("临时索引数据记录长度:%d",mdrowlen);  
		  long lmtrn=-1,lmtfn=-1;
		  //检查临时索引的内存用量,判断当前的参数设置是否可以一次装入全部索引记录
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
			  if(llt>LOADTIDXSIZE && lmtrn==-1) { //使用的临时索引超过内存允许参数的上限，需要拆分
				  if(i==0) 
				  {
				  	  sp->log(tabid,datapartid,MLOAD_DP_LOADTIDXSIZE_TOO_LOW_ERROR,"内存参数DP_LOADTIDXSIZE设置太低:%dMB，不足以装载至少一个临时抽取块:%dMB。\n",LOADTIDXSIZE,(int)llt);				  
					  ThrowWith("MLoader:内存参数DP_LOADTIDXSIZE设置太低:%dMB，\n"
					  "不足以装载至少一个临时抽取块:%dMB。\n",LOADTIDXSIZE,(int)llt);
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
		  if(lmtrn!=-1) { //使用的临时索引超过内存允许参数的上限，需要拆分
			  lgprintf("MLoader:数据整理超过内存限制%dMB,需要处理文件数%d,数据组%d,起始点:%d,截至点:%d,文件数:%d .",LOADTIDXSIZE,rn,datapartid,mdf.GetInt("mdfid",0),mdf.GetInt("mdfid",lmtfn-1),lmtfn);
			  lgprintf("索引需要内存%dM ",idxrn*mdrowlen/(1024*1024));
			  sp->log(tabid,datapartid,MLOAD_DATA_NOTIFY,"MLoader:数据整理超过内存限制%dMB,需要处理文件数%d,起始点:%d,截至点:%d,文件数:%d ,需要内存%dMB.",LOADTIDXSIZE,rn,mdf.GetInt("mdfid",0),mdf.GetInt("mdfid",lmtfn-1),lmtfn,idxrn*mdrowlen/(1024*1024));
			  idxrn=lmtrn;
			  //fix a bug
			  rn=lmtfn;
		  }
		  if(lmtextrn==-1) lmtextrn=extrn;
		  lgprintf("数据整理实际使用内存%dM,索引列宽%d.",idxrn*mdrowlen/(1024*1024),mdrowlen);
		  start_mdfid=mdf.GetInt("mdfid",0);
		  end_mdfid=mdf.GetInt("mdfid",rn-1);
		  lgprintf("索引记录数:%d",idxrn);
		  //为防止功能重入,中间文件状态修改.
		  lgprintf("修改中间文件的处理状态(tabid:%d,datapartid:%d,indexid:%d,%d个文件)：1-->2",tabid,datapartid,indexid,rn);
		  sprintf(sqlbf,"update dp.dp_middledatafile set procstate=2 where tabid=%d  and datapartid=%d and indexgid=%d and procstate<=1 and mdfid>=%d and mdfid<=%d ",
			  tabid,datapartid,indexid,start_mdfid,end_mdfid); 
		  int ern=sp->DoQuery(sqlbf);
		  if(ern!=rn) {
			  if(ern>0) {  //上面的UPdate语句修改了一部分记录状态,且不可恢复,需要对数据子集作重新装入.
			  	  sp->log(tabid,datapartid,MLOAD_UPDATE_MIDDLE_FILE_STATUS_ERROR,"表%d,分区%d,修改中间文件的处理状态异常，可能是与其它进程冲突.部分文件的处理状态不一致，请立即停止所有的数据整理任务，重新作数据整理操作。",
			  	  tabid,datapartid);
			  				  	
				  ThrowWith("MLoader修改中间文件的处理状态异常，可能是与其它进程冲突。\n"
					  " 部分文件的处理状态不一致，请立即停止所有的数据整理任务，重新作数据整理操作。\n"
					  "  tabid:%d,datapartid:%d,indexid:%d.\n",
					  tabid,datapartid,indexid);
			  }
			  else //上面的update语句未产生实际修改操作,其它进程可以继续处理.
			  {
				 //不要用ThrowWith,否则会引起catch中恢复dp_middledatafile表.
				  errprintf("MLoader修改中间文件的处理状态异常，可能是与其它进程冲突。\n"
					  "  tabid:%d,datapartid:%d,indexid:%d.\n",
					  tabid,datapartid,indexid);
				  return 0;	  
			  }
		  }
		  
		  	//ThrowWith("调试终止---%d组数据.",dispct);
	if(firstbatch) {
		lgprintf("删除数据分组%d,索引%d 的数据和索引记录(表:%d)...",datapartid,indexid,tabid);
		sp->log(tabid,datapartid,107,"删除索引组%d过去生成的数据和索引记录...",indexid);
		AutoMt dfnmt(sp->GetDTS(),MAX_DST_DATAFILENUM);
		dfnmt.FetchAll("select * from dp.dp_datafilemap where tabid=%d and datapartid=%d and indexgid=%d and fileflag=1",tabid,datapartid,indexid);
		int dfn=dfnmt.Wait();
		if(dfn>0) {
			AutoStmt st(sp->GetDTS());
			for(int di=0;di<dfn;di++)
			{
				lgprintf("删除'%s'/'%s'和附加的depcp,dep5文件",dfnmt.PtrStr("filename",di),dfnmt.PtrStr("idxfname",di));
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

		  //建立中间索引(中间文件数据块索引)内存表mdindexmt和目标数据块内存表blockmt
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
		  // pdtfid为一个字符数组，偏移为x的值表示中间索引内存表第x行的文件序号(Base0);
		  if(pdtfid)
			  delete [] pdtfid;
		  pdtfid=new unsigned short [idxrn];
		  //dtfidlen=idxrn;
		  //pdtf为file_mt对象的数组。存放数据文件对象。
		  if(pdtf) delete [] pdtf;
		  pdtf=new file_mt[rn];
		  //mdindexmt.SetMaxRows(idxrn);
		  //读入全部索引数据到mdindexmt(中间索引内存表),并打开全部数据文件
		  //pdtfid指向对应的文件序号。
		  lgprintf("读索引数据...");
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
		  lgprintf("索引数据:%d.",crn);
		  if(crn!=idxrn) {
		  	sp->log(tabid,0,MLOAD_INDEX_DATA_FILE_RECORD_NUM_ERROR,"表%d,索引数据文件的总记录数%lld,与指示信息不一致:%lld",tabid,crn,idxrn);
		  	ThrowWith("索引数据文件的总记录数%lld,与指示信息不一致:%lld",crn,idxrn);
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
		  //对mdindexmt(中间索引内存表)做排序。
		  //由于排序不涉及内存表的数据排列，而是新建记录顺序表，因此，
		  // pdtfid作为内存表外的等效内存表字段，不需做处理。
		  lgprintf("排序('%s')...",mdf.PtrStr("soledindexcols",0));
		  {
			  char sort[300];
			  sprintf(sort,"%s,idx_fid,idx_blockoffset",mdf.PtrStr("soledindexcols",0));
			  wociSetSortColumn(mdindexmt,sort);
			  wociSortHeap(mdindexmt);
		  }
		  lgprintf("排序完成.");
		  //取得全部独立索引结构
		  sp->GetSoledIndexParam(datapartid,&dp,tabid);
		  //检查需要处理的中间数据是否使用主独立索引，如果是，isfirstidx=1.
		  int isfirstidx=0;
		  indexparam *ip;
		  {
			  int idxp=dp.GetOffset(indexid);
			  ip=&dp.idxp[idxp];
			  if(idxp==dp.psoledindex) isfirstidx=1;
		  }
		  //从结构描述文件建立indexmt,indexmt是目标索引内存表。是建立目标索引表的数据源。
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
		  //取独立索引在mdindexmt(中间索引存表)结构中的位置。
		  //设置对indexmt插入记录需要的结构和变量。
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
		  //indexmt中的blocksize,blockstart?,blockrownum需要滞后写入，
		  //因而需要取出这些字段的首地址。
		  int *pblocksize;
		  int *pblockstart;
		  int *pblockrn;
		  wociGetIntAddrByName(indexmt,"blocksize",0,&pblocksize);
		  wociGetIntAddrByName(indexmt,"blockstart",0,&pblockstart);
		  wociGetIntAddrByName(indexmt,"blockrownum",0,&pblockrn);
		  //mdindexmt中下列字段是读中间数据文件的关键项。
		  int *poffset,*pstartrow,*prownum;
		  wociGetIntAddrByName(mdindexmt,"idx_blockoffset",0,&poffset);
		  wociGetIntAddrByName(mdindexmt,"idx_startrow",0,&pstartrow);
		  wociGetIntAddrByName(mdindexmt,"idx_rownum",0,&prownum);
		  
		  //indexmt 记录行数计数复位
		  int indexmtrn=0;
		  
		  //建立目标数据文件和目标索引文件对象(dt_file).
		  // 目标数据文件和目标索引文件一一对应，目标数据文件中按子块方式存储内存表
		  //  目标索引文件中为一个单一的内存表，文件头结构在写入内存表时建立
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
		  		sp->log(tabid,datapartid,MLOAD_CAN_NOT_MLOAD_DATA_ERROR,"表%d,分区%d,文件'%s'已经存在，不能继续整理数据。",tabid,datapartid,fn);
		  		ThrowWith("文件'%s'已经存在，不能继续整理数据。",fn);
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
		  lgprintf("开始数据处理(MiddleDataLoading)....");
		  
		  /*******按照Sort顺序遍历mdindexmt(中间索引内存表)***********/
		  //
		  //
		  lgprintf("创建文件,编号:%d...",dtfid);
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
				  //要处理的数据块和上次的数据块不是一个关键字，
				  // 所以，先保存上次的关键字索引，关键字的值从blockmt中提取。
				  // startrow变量始终保存第一个未存储关键字索引的数据块开始行号。
				  int c;
				  for(c=0;c<bcn;c++) {
					  indexptr[c]=blockmt.PtrVoid(pblockc[c],startrow);
				  }
				  if(rownum>0) {
					  wociInsertRows(indexmt,indexptr,NULL,1);
					  idxtotrn++;
				  }
				  firstrn=thisrn;
				  //这里，出现startrow暂时指向无效行的情况(数据未填充).
				  startrow=blockrn;
				  rownum=0;
			  }
			  //从数据文件中读入数据块
			  int mt=pdtf[rfid].ReadBlock(poffset[thisrn],0,true);
			  //2005/08/24 修改： 索引数据文件为多块数据顺序存储
			  
			  //由于结束当前文件有可能还要增加一条索引,因此判断内存表满的条件为(idxdlimit-1)
			  int irn=wociGetMemtableRows(indexmt);
			  if(irn>=(idxdlimit-2))
			  {
				  int pos=irn-1;
				  //如果blocksize，blockrn等字段还未设置，则不清除
				  while(pos>=0 && pblockstart[pos]==blockstart) pos--;
				  if(pos>0) {
					  //保存已经设置完整的索引数据,false参数表示不需要删除位图区.
					  idxf.WriteMt(indexmt,COMPRESSLEVEL,pos+1,false);
					  if(pos+1<irn) 
						  wociCopyRowsTo(indexmt,indexmt,0,pos+1,irn-pos-1);
					  else wociReset(indexmt);
				  }
				  else 
				  {
				  	  sp->log(tabid,datapartid,MLOAD_INDEX_NUM_OVER_ERROR,"目标表%d,索引号%d,装入时索引超过最大单一块允许记录数%d",tabid,indexid,idxdlimit);
				      ThrowWith("目标表%d,索引号%d,装入时索引超过最大单一块允许记录数%d",tabid,indexid,idxdlimit);
				  }
			  }
			  //数据块拆分 
			  //检查数据块是否需要拆分
			  // 增加一个循环，用于处理mt中的sbrn本身大于maxblockrn的异常情况：
			  //	由于异常原因导致临时导出块大于最终数据块，在索引聚合良好时，会出现不能一次容纳所有临时记录
			  while(true) {
				  if(blockrn+sbrn>maxblockrn ) {
					  //每个数据块至少需要达到最大值的80%。
					  if(blockrn<maxblockrn*.8 ) {
						  //如果不足80%，把当前处理的数据块拆分
						  int rmrn=maxblockrn-blockrn;
						  wociCopyRowsTo(mt,blockmt,-1,sbstart,rmrn);
						  rownum+=rmrn;
						  sbrn-=rmrn;
						  sbstart+=rmrn;
						  blockrn+=rmrn;
					  }
					  
					  //保存块数据
					  if(useOldBlock) 
						  blocksize=dstfile.WriteMt(blockmt,compflag,0,false)-blockstart;
					  else if(col_object)
						  blocksize=dstfile.WriteMySQLMt(blockmt,compflag)-blockstart;
					  else 
						  blocksize=dstfile.WriteMySQLMt(blockmt,compflag,false)-blockstart;
					  adjrn+=wociGetMemtableRows(blockmt);
					  //保存子快索引
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
					  //数据文件长度超过2G时拆分
					  if(blockstart>2000000000 ) {
						  //增加文件对照表记录(dt_datafilemap)
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
		  						sp->log(tabid,datapartid,MLOAD_FILE_EXISTS_ERROR,"表%d,分区%d,文件'%s'已经存在，不能继续整理数据。",tabid,datapartid,fn);
		  						ThrowWith("文件'%s'已经存在，不能继续整理数据。",fn);
		  					}
		  				  }
						  dstfile.SetFileHeader(subtotrn,fn);
						  dstfile.Open(fn,1);
						  blockstart=dstfile.WriteHeader(blockmt,0,dtfid);
						  printf("\r                                                                            \r");
						  lgprintf("创建文件,编号:%d...",dtfid);
		  				  sprintf(idxfn,"%s%s_%d_%d_%d.idx",dp.dstpath[0],tbname,datapartid,indexid,dtfid);
						  
						  idxf.SetFileHeader(idxtotrn,idxfn);
						  // create another file
						  idxf.Open(idxfn,1);
						  idxf.WriteHeader(indexmt,0,dtfid);
						  indexmt.Reset();
						  subtotrn=0;
						  blockrn=0;
						  idxtotrn=0;
						  //lgprintf("创建文件,编号:%d...",dtfid);
						  
					  } // end of IF blockstart>2000000000)
					  blockmt.Reset();
					  blockrn=0;
					  firstrn=thisrn;
					  startrow=blockrn;
					  rownum=0;
					  dispct++;
				    if(wdbi_kill_in_progress) {
				    	wdbi_kill_in_progress=false;
					  	ThrowWith("用户取消操作!");
					  }
					  if(dispct-lstdispct>=200) {
						  lstdispct=dispct;
						  arrtm.Stop();
						  double tm1=arrtm.GetTime();
						  arrtm.Start();
						  printf("  已生成%lld数据块,用时%.0f秒,预计还需要%.0f秒          .\r",dispct,tm1,(tm1*(idxrn-i))/i);
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
		
		//保存子快索引
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
		//保存块数据
		//保存块数据
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
		//增加文件对照表记录(dt_datafilemap)
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
	
	//记录数校验。这里的校验仅是本次数据重组，有可能是一个数据组的某个索引组的一部分。后面的校验是挑一个索引组，校验整个数据组
	if(adjrn!=lmtextrn) {
		sp->log(tabid,datapartid,MLOAD_CHECK_RESULT_ERROR,"表%d,分区%d,数据重组要求处理导出数据%lld行，但实际生成%lld行! 索引组%d.",tabid,datapartid,lmtextrn,adjrn,indexid);
		ThrowWith("数据重组要求处理导出数据%lld行，但实际生成%lld行! 表%d(%d),索引组%d.",lmtextrn,adjrn,tabid,datapartid,indexid);
	}
	wociAppendToDbTable(fnmt,"dp.dp_datafilemap",sp->GetDTS(),true);
	lgprintf("修改中间文件的处理状态(表%d,索引%d,数据组:%d,%d个文件)：2-->3",tabid,indexid,datapartid,rn);
	sprintf(sqlbf,"update dp.dp_middledatafile set procstate=3 where tabid=%d and datapartid=%d and indexgid=%d and procstate=2 and mdfid>=%d and mdfid<=%d ",
	  tabid,datapartid,indexid,start_mdfid,end_mdfid);
	sp->DoQuery(sqlbf);
	sp->log(tabid,datapartid,MLOAD_DATA_NOTIFY,"表%d,分区%d,数据重组,索引组:%d,记录数%lld.",tabid,datapartid,indexid,lmtextrn);
	}
	catch (...) {
		int frn=wociGetMemtableRows(fnmt);
		errprintf("数据整理出现异常，表:%d,数据组:%d.",tabid,datapartid);
		sp->log(tabid,datapartid,111,"数据整理出现异常");
		errprintf("恢复中间文件的处理状态(数据组:%d,%d个文件)：2-->1",datapartid,rn);
		sprintf(sqlbf,"update dp.dp_middledatafile set procstate=1,blevel=ifnull(blevel,0)+1 where tabid=%d and datapartid=%d and indexgid=%d and mdfid>=%d and mdfid<=%d",tabid,datapartid,indexid,start_mdfid,end_mdfid);
		sp->DoQuery(sqlbf);
		sprintf(sqlbf,"update dp.dp_datapart set blevel=ifnull(blevel,0)+100 where tabid=%d and datapartid=%d ",tabid,datapartid);
		sp->DoQuery(sqlbf);
		errprintf("删除已整理的数据和索引文件.");
		errprintf("删除数据文件...");
		int i;
		for(i=0;i<frn;i++) {
			errprintf("\t %s ",fnmt.PtrStr("filename",i));
			errprintf("\t %s ",fnmt.PtrStr("idxfname",i));
		}
		for(i=0;i<frn;i++) {
			unlink(fnmt.PtrStr("filename",i));
			unlink(fnmt.PtrStr("idxfname",i));
		}
		errprintf("删除已处理数据文件和索引文件记录...");
		AutoStmt st(sp->GetDTS());
		for(i=0;i<frn;i++) {
			st.Prepare("delete from dp.dp_datafilemap where tabid=%d and datapartid=%d and indexgid=%d and fileflag=1 and fileid=%d",tabid,datapartid,indexid,fnmt.PtrInt("fileid",i));
			st.Execute(1);
			st.Wait();
		}
		wociCommit(sp->GetDTS());
		sp->log(tabid,datapartid,MLOAD_DATA_NOTIFY,"表%d,分区%d,整理过程中错误，状态已恢复.",tabid,datapartid);
		throw ;
	}
	
	lgprintf("数据处理(MiddleDataLoading)结束,共处理数据包%lld个.",dispct);
	lgprintf("生成%d个数据文件,已插入dp.dp_datafilemap表.",wociGetMemtableRows(fnmt));
	sp->log(tabid,datapartid,MLOAD_DATA_NOTIFY,"表%d,分区%d,数据重组结束,共处理数据包%lld个,生成文件%d个.",tabid,datapartid,dispct,wociGetMemtableRows(fnmt));
	//wociMTPrint(fnmt,0,NULL);
	//检查是否该数据分组的最后一批数据
	// 如果是最后一批数据，则说明：
	//  1. 拆分处理的最后一个批次数据已处理完。
	//  2. 所有该分组对应的一个或多个独立索引都已整理完成。
	//在这个情况下才删除导出的临时数据。
	try
	{
		//检查其他独立索引或同一个分组中其它批次的数据（其他数据子集）是否整理完毕。
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
			//数据校验
			if(trn!=mdf.GetLong("rn",0)) {
				//未知原因引起的错误，不能直接恢复状态. 暂停任务的执行
				sp->log(tabid,datapartid,MLOAD_CHECK_RESULT_ERROR,"表%d,分区%d,数据校验错误,导出%lld行，整理生成%lld行(验证索引组%d),数据迁移过程已被暂停。",tabid,datapartid,trn,mdf.GetLong("rn",0),indexid);
				sprintf(sqlbf,"update dp.dp_datapart set oldstatus=status,status=70,blevel=ifnull(blevel,0)+100 where tabid=%d and datapartid=%d",tabid,datapartid);
				sp->DoQuery(sqlbf);
				ThrowWith("数据校验错误,表%d(%d),导出%lld行，整理生成%lld行(验证索引组%d),数据迁移过程已被暂停",tabid,datapartid,trn,mdf.GetLong("rn",0),indexid);
			}
			if(mdf.GetLong("rn",0))
			lgprintf("最后一批数据已处理完,任务状态2-->3,表%d(%d)",tabid,datapartid);
			//如果是单分区处理任务，必须是所有相同数据集的任务状态为3，才能启动下一步的操作（数据装入）。
			sprintf(sqlbf,"update dp.dp_datapart set status=3 where tabid=%d and datapartid=%d",
				tabid,datapartid);
			sp->DoQuery(sqlbf);
			//重新创建表结构。
			//sp->CreateDT(tabid);
			//}
			lgprintf("删除中间临时文件...");
			mdf.FetchAll("select * from dp.dp_middledatafile where tabid=%d and datapartid=%d and procstate=3",tabid,datapartid);
			int dfn=mdf.Wait();
			{
				for(int di=0;di<dfn;di++) {
					lgprintf("删除文件'%s'",mdf.PtrStr("datafilename",di));
					unlink(mdf.PtrStr("datafilename",di));
					lgprintf("删除文件'%s'",mdf.PtrStr("indexfilename",di));
					unlink(mdf.PtrStr("indexfilename",di));
				} 
				lgprintf("删除记录...");
				AutoStmt st(sp->GetDTS());
				st.Prepare("delete from dp.dp_middledatafile where tabid=%d and datapartid=%d and procstate=3",tabid,datapartid);
				st.Execute(1);
				st.Wait();
			}
		}
		
	}
	catch(...) {
		errprintf("数据整理任务已完成，但任务状态调整或临时中间文件删除时出现错误，需要人工调整。\n表%d(%d)。",
			tabid,datapartid);
		sp->log(tabid,datapartid,MLOAD_EXCEPTION_ERROR,"数据整理任务已完成，但任务状态调整或临时中间文件删除时出现错误，需要人工调整。\n表%d(%d)。",tabid,datapartid);
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

  // 用于多个表的并发上线装入
  #define DATALOAD_NUM 100

  int DestLoader::Load(bool directIOSkip) {
	  //Check deserved temporary(middle) fileset
	  AutoMt mdf_task(psa->GetDTS(),DATALOAD_NUM);
	  mdf_task.FetchFirst("select * from dp.dp_datapart where (status=3 or status=30) and begintime<now() %s order by blevel,tabid,datapartid limit %d",psa->GetNormalTaskDesc(),DATALOAD_NUM);
	  int rn_task=mdf_task.Wait();
	  if(rn_task<1) {
		  printf("没有发现处理完成等待装入的数据(任务状态=3).\n");
		  return 0;
	  }

	  // 记录正在装入的表的id
	  std::vector<int> loading_tabid_arry;
	  int load_part_index = 0;
	  
	  char sqlbf[1000];
start_dataload:	  
	  // 判断id是否存在
	  if(load_part_index<rn_task-1)
	  {
		  for(int i=0;i<loading_tabid_arry.size();i++)
		  {
		  	  // 如果该表的任务已经在装入，处理下一个任务的数据装入
	          if(mdf_task.GetInt("tabid",load_part_index) == loading_tabid_arry[i]){
			  	  if(load_part_index < rn_task-1){
				  	load_part_index++;
					goto start_dataload;
			  	  }
				  else{ // 已经是最后一个记录
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
	  lgprintf("装入类型:%s.",preponl?"新装入":"重装入");
	  psa->OutTaskDesc("数据装载 :",tabid);

	  AutoMt mdf(psa->GetDTS(),MAX_DST_DATAFILENUM);
	  mdf.FetchAll("select * from dp.dp_datafilemap where tabid=%d and procstatus=0 and fileflag=%d and datapartid=%d limit 2",
		  tabid,preponl?1:0,datapartid);
	  if(mdf.Wait()<1) {
		  //errprintf("数据组%d(%d)指示已完成数据整理，但找不到对应的数据记录。\n可能是数据文件记录不存在或状态非空闲(0).\n",
		  //  tabid,datapartid);
	 	  mdf.FetchAll("select * from dp.dp_datafilemap where tabid=%d and procstatus=1 and fileflag=%d and datapartid=%d limit 2",
		   tabid,preponl?1:0,datapartid);
		  if(mdf.Wait()>0) {
		    lgprintf("数据组%d(%d)的数据装载任务已由其它进程处理.\n",tabid,datapartid);

			// 没有到最后一条装入任务记录，继续下一条装入任务
			if(load_part_index < rn_task-1){
			  	load_part_index++;
				if(load_part_index < rn_task-1) // 防止越界rn_task
				{
					loading_tabid_arry.push_back(tabid);
					goto start_dataload;
				}
		  	}

		    return 0;
		  }
		  lgprintf("数据组%d(%d)指示已完成数据整理，但找不到对应的数据记录。\n如果是数据文件记录状态异常，请调整后重新装入.\n按空记录处理．",
		    tabid,datapartid);
		  
		  //JIRA DM-61 空表任务在创建或删除索引过程中后台死掉问题
		  /* 去掉空表的重建操作,使用新表建立的结构
		     恢复，注释后不能继续后面的上线过程
		  */
		  sprintf(sqlbf," update dp.dp_datapart set status=21 ,istimelimit=0,oldstatus=%d where tabid=%d and datapartid=%d and status<>21",
		    preponl?4:40,tabid,datapartid);
		  if(psa->DoQuery(sqlbf)!=1) {
		    				  lgprintf("修改空表装入处理状态异常，可能是与其它进程冲突。\n"
			  "   tabid:%d.\n",tabid);
			  return 0;
		  }
		  AutoMt srctstmt(0,10);
		  //先从数据文件建目标表对象,如果失败,从源系统建
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
	  psa->log(tabid,0,DLOAD_DATA_NOTIFY,"表%d,分区%d,数据装入,独立索引数%d,日志文件 '%s' .",tabid,datapartid,idxrn,wociGetLogFile());
		  //为防止功能重入,数据文件状态修改.
		  mdf.FetchAll("select * from dp.dp_datafilemap where tabid=%d and fileflag=%d and datapartid=%d order by indexgid,fileid",
			  tabid,preponl?1:0,datapartid);
		  totdatrn=rn=mdf.Wait();
		  lgprintf("修改数据文件的处理状态(tabid:%d,%d个文件)：0-->1",tabid,rn);
		  sprintf(sqlbf,"update dp.dp_datafilemap set procstatus=1 where tabid=%d and "
			  "procstatus=0 and fileflag=%d and datapartid=%d",tabid,preponl?1:0,datapartid);
		  if(psa->DoQuery(sqlbf)!=rn) {
			  lgprintf("修改数据文件的处理状态异常，可能是与其它进程冲突.tabid:%d.\n",tabid);
			   psa->log(tabid,datapartid,DLOAD_DATA_NOTIFY,"修改数据文件的处理状态异常，可能是与其它进程冲突.tabid:%d.\n",tabid);
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
		      psa->log(tabid,datapartid,DLOAD_CAN_NOT_FIND_DATA_FILE_ERROR,"找不到数据文件(indexgid:%d,tabid:%d,datapartid:%d,fileflag:%d).",indexid,tabid,datapartid,preponl?1:0);
		      ThrowWith("找不到数据文件(indexgid:%d,tabid:%d,datapartid:%d,fileflag:%d).",indexid,tabid,datapartid,preponl?1:0);
		   }
		  //索引数据表的结构从文件提取，比从template中提取更准确。
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
		  //建立目标标及其表结构的描述文件
		  wociGetCreateTableSQL(idxmt,sqlbf,dp.idxp[off].idxtbname,true);
		  conn.DoQuery(sqlbf);
		  */
		  //}
		 
		  AutoMt datmt(psa->GetDTS(),MAX_DST_DATAFILENUM);
		  datmt.FetchAll("select * from dp.dp_datafilemap where tabid=%d "
			  " and indexgid=%d and procstatus=1 and fileflag=%d and datapartid=%d order by fileid",
			  tabid,indexid,preponl?1:0,datapartid);
		  int datrn=datmt.Wait();
		  //防止功能重入或并行运行时的重复执行
		  if(datrn<1) continue;
		  char fn[300];
		  bool isfixed=false;
		  int k=0;
		  const char *pathval=psa->GetMySQLPathName(0,"msys");		
			  lgprintf("开始数据装入(DestLoading),文件数:%d,tabid:%d,indexid:%d ...",
				  rn,tabid,indexid);
			  //原来的方式为不建立索引(FALSE参数)
			  //2005/12/01修改为建立索引（在空表之上),在索引重建过程中(::RecreateIndex,taskstaus 4->5),
			  //  使用repair table ... quick来快速建立索引结构。
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
					  psa->log(tabid,datapartid,DLOAD_OPEN_WRITE_FILE_ERROR,"表%d,分区%d,打开文件%s写入失败.",tabid,datapartid,fn);
					  ThrowWith("Open file %s for writing failed!",fn);
			      }
			  }
			  LONG64 totidxrn=0;
			  //lgprintf("索引文件：%s",mdf.PtrStr("filename",0));
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
					  	ThrowWith("用户取消操作!");
					  }
					  lgprintf("生成索引数据...");
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
					  lgprintf("索引数据:%lld行.",totidxrn);
				  }
			  }
			  if(!directIOSkip) {
				  fclose(fp);
				  fp=fopen(fn,"rb");
				  fseeko(fp,0,SEEK_END);
				  LONG64 fsz=ftello(fp);//_filelength(_fileno(fp));
				  fclose(fp);
				  //索引数据表的结构从文件提取，比从template中提取更准确。
				  psa->GetPathName(fn,idxname,"MYI");
				  char tmp[20];
				  memset(tmp,0,20);
				  
				  revlint(&totidxrn);
				  revlint(&fsz);
				  fp=fopen(fn,"r+b");
				  if(fp==NULL) 
				  {
				  	  psa->log(tabid,datapartid,DLOAD_OPEN_READ_FILE_ERROR,"表%d,分区%d,打开文件%s读取失败.",tabid,datapartid,fn);
					  ThrowWith("无法打开文件'%s'，请检查目录参数设置(dt_path)是否正确。",fn);
				  }
				  fseek(fp,28,SEEK_SET);
				  dp_fwrite(&totidxrn,1,8,fp);
				  // reset deleted records count.
				  dp_fwrite(tmp,1,8,fp);
				  fseek(fp,68,SEEK_SET);
				  dp_fwrite(&fsz,1,8,fp);
				  fseek(fp,0,SEEK_END);
				  fclose(fp); 
				  lgprintf("索引表刷新...");
				  psa->FlushTables(idxname);
				  revlint(&fsz);
				  // BUG FIXING
				  //**ThrowWith("调试中断");
				  
				  if(fsz>1024*1024) {
					  lgprintf("压缩索引表:%s....",idxname);
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
						  psa->log(tabid,datapartid,DLOAD_COMPRESS_INDEX_TABLE_ERROR,"表%d,分区%d,索引表%s压缩失败.",tabid,datapartid,idxname);
						  ThrowWith("索引表%s压缩失败.",idxname);
					   }
					  lgprintf("压缩成功。");
				  }
			  }
		  }
	}
	catch(...) {
		  lgprintf("恢复数据文件的处理状态(tabid %d(%d),%d个文件)：1-->0",tabid,datapartid,totdatrn);
		  sprintf(sqlbf,"update dp.dp_datafilemap set procstatus=0 where tabid=%d  and fileflag=%d and datapartid=%d",
			  tabid,preponl?1:0,datapartid);
		  psa->DoQuery(sqlbf);
		  char tbname[150],idxname[150];
		  psa->log(tabid,datapartid,DLOAD_UPDATE_FILE_STATUS_NOTIFY,"表%d,分区%d,装入文件异常，已恢复处理状态.",tabid,datapartid);
	
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
	lgprintf("数据装入(DestLoading)结束 ...");
	AutoStmt updst(psa->GetDTS());
	updst.DirectExecute("update dp.dp_datapart set status=%d,istimelimit=0 where tabid=%d and datapartid=%d",
		preponl?4:40,tabid,datapartid);
	updst.DirectExecute("update dp.dp_datafilemap set procstatus=0 where tabid=%d and datapartid=%d",
		tabid,datapartid);
	lgprintf("任务状态更新,3(MLoaded)--->4(DLoaded),表:%d,分区:%d.",tabid,datapartid);
	psa->log(tabid,0,DLOAD_DATA_NOTIFY,"数据装入结束，等待建立索引.");
	//ThrowWith("DEBUG_BREAK_HEAR.");
	return 1;
}

// 源表为DBPLUS管理的目标表，且记录数非空。
int DestLoader::MoveTable(const char *srcdbn,const char *srctabname,const char * dstdbn,const char *dsttabname)
{
	char dtpath[300];
	lgprintf("目标表改名(转移) '%s.%s -> '%s.%s'.",srcdbn,srctabname,dstdbn,dsttabname);
	sprintf(dtpath,"%s.%s",srcdbn,srctabname);
	if(!psa->TouchTable(dtpath))
	  ThrowWith("源表没找到");
	sprintf(dtpath,"%s.%s",dstdbn,dsttabname);
	if(psa->TouchTable(dtpath)) {
		  lgprintf(dtpath,"表%s.%s已存在,不能执行更名操作!",dstdbn,dsttabname);
//		if(!GetYesNo(dtpath,false)) {
			lgprintf("取消操作。 ");
			return 0;
//		}			
	}
	AutoMt mt(psa->GetDTS(),MAX_DST_DATAFILENUM);
	int rn;
	//mt.FetchAll("select pathval from dp.dp_path where pathtype='msys'");
	//int rn=mt.Wait();
	//i/f(rn<1) 
	//	ThrowWith("找不到MySQL数据目录(dt_path.pathtype='msys'),数据转移异常中止.");
	strcpy(dtpath,psa->GetMySQLPathName(0,"msys"));
	if(STRICMP(srcdbn,dstdbn)==0 && STRICMP(srctabname,dsttabname)==0) 
		ThrowWith("源表和目标表名称不能相同.");
	mt.FetchAll("select * from dp.dp_table where tabname=lower('%s') and databasename=lower('%s')",dsttabname,dstdbn);
	rn=mt.Wait();
	if(rn>0) {
		ThrowWith("表'%s.%s'已存在(记录数:%d)，操作失败!",dstdbn,dsttabname,mt.GetInt("recordnum",0));
	}
	mt.FetchAll("select * from dp.dp_table where tabname=lower('%s') and databasename=lower('%s')",srctabname,srcdbn);
	rn=mt.Wait();
	if(rn<1) {
		ThrowWith("源表'%s.%s'不存在.",srcdbn,srctabname);
	}
	mt.FetchAll("select * from dp.dp_datapart where tabid=%d and status not in(0,5)",mt.GetInt("tabid",0));
	rn=mt.Wait();
	if(rn>0) {
		ThrowWith("源表'%s.%s'迁移过程未完成，不能更名.",srcdbn,srctabname);
	}
	mt.FetchAll("select * from dp.dp_table where tabname=lower('%s') and databasename=lower('%s')",srctabname,srcdbn);
	rn=mt.Wait();
	lgprintf("配置参数转移.");
	int dsttabid=psa->NextTableID();
	tabid=mt.GetInt("tabid",0);
	long recordnum=mt.GetLong("recordnum",0);
	int firstdatafileid=mt.GetInt("firstdatafileid",0);
	double totalbytes=mt.GetDouble("totalbytes",0);
	int datafilenum=mt.GetInt("datafilenum",0);
	if(recordnum<1) {
		lgprintf("源表'%s.%s'数据为空，表更名失败。",srcdbn,srctabname);
		return 0;
	}
	lgprintf("源表'%s.%s' id:%d,记录数:%d,起始数据文件号 :%d",
		srcdbn,srctabname,tabid,recordnum,firstdatafileid);
	
	//新表不存在，在dt_table中新建一条记录
	*mt.PtrInt("tabid",0)=dsttabid;
	strcat(mt.PtrStr("tabdesc",0),"_r");
	strcpy(mt.PtrStr("tabname",0),dsttabname);
	strcpy(mt.PtrStr("databasename",0),dstdbn);
	wociAppendToDbTable(mt,"dp.dp_table",psa->GetDTS(),true);
	psa->CloseTable(tabid,NULL,false);
	CopyMySQLTable(dtpath,srcdbn,srctabname,dstdbn,dsttabname);
	//暂时关闭源表的数据访问，记录数已存在本地变量recordnum。
	//目标表的.DTP文件已经存在,暂时屏蔽访问
	psa->CloseTable(dsttabid,NULL,false);
	char sqlbuf[MAX_STMT_LEN];
	mt.FetchAll("select * from dp.dp_datapart where tabid=%d order by datapartid ",tabid);
	rn=mt.Wait();
	//创建索引记录和索引表，修改索引文件和数据文件的tabid 指向
	int i=0;
	for(i=0;i<rn;i++) 
		*mt.PtrInt("tabid",i)=dsttabid;
	wociAppendToDbTable(mt,"dp.dp_datapart",psa->GetDTS(),true);

	mt.FetchAll("select * from dp.dp_index where tabid=%d order by seqindattab ",tabid);
	rn=mt.Wait();
	//创建索引记录和索引表，修改索引文件和数据文件的tabid 指向
	for(i=0;i<rn;i++) {
		*mt.PtrInt("tabid",i)=dsttabid;
		strcpy(mt.PtrStr("indextabname",i),"");
	}
	wociAppendToDbTable(mt,"dp.dp_index",psa->GetDTS(),true);
	
	lgprintf("索引转移.");
	for(i=0;i<rn;i++) {
		if(mt.GetInt("issoledindex",i)>0) {
			char tbn1[300],tbn2[300];
			//如果有老的索引表表存在,则不再处理分区索引表.
			psa->GetTableName(tabid,mt.GetInt("indexgid",i),NULL,tbn1,TBNAME_DEST);
			if(psa->TouchTable(tbn1)) {
			 psa->GetTableName(dsttabid,mt.GetInt("indexgid",i),NULL,tbn2,TBNAME_DEST);
			 lgprintf("索引表 '%s'-->'%s...'",tbn1,tbn2);
		 	 psa->FlushTables(tbn1);
			 MoveMySQLTable(dtpath,srcdbn,strstr(tbn1,".")+1,dstdbn,strstr(tbn2,".")+1);
			}
			else
				ThrowWith("找不到索引表'%s',程序异常终止,需要手工检查并修复参数!",tbn1);
			int ilp=0;
			psa->GetTableName(tabid,mt.GetInt("indexgid",i),NULL,tbn1,TBNAME_DEST,ilp);
			if(psa->TouchTable(tbn1)) {
			 //3.10版新格式索引.
			char srcf[300];
			//修正MERGE文件
			//
			sprintf(srcf,"%s%s/%s.MRG",dtpath,dstdbn,strstr(tbn2,".")+1);
			FILE *fp=fopen(srcf,"w+t");
			 while(psa->GetTableName(tabid,mt.GetInt("indexgid",i),NULL,tbn1,TBNAME_DEST,ilp)) {
			         psa->GetTableName(dsttabid,mt.GetInt("indexgid",i),NULL,tbn2,TBNAME_DEST,ilp++);
			         lgprintf("索引表 '%s'-->'%s...'",tbn1,tbn2);
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
	lgprintf("数据文件转移.");
	for(i=0;i<rn;i++) {
		  char fn[300];
		  psa->GetMySQLPathName(mt.GetInt("pathid",i));
		  sprintf(fn,"%s%s.%s_%d_%d_%d.dat",psa->GetMySQLPathName(mt.GetInt("pathid",i)),
		    dstdbn,dsttabname,mt.GetInt("datapartid",i),mt.GetInt("indexgid",i),mt.GetInt("fileid",i));
		  
		  FILE *fp;
		  fp=fopen(mt.PtrStr("filename",i),"rb");
		  if(fp==NULL) ThrowWith("找不到文件'%s'.",mt.PtrStr("filename",i));
		  fclose(fp);
		  fp=fopen(fn,"rb");
		  if(fp!=NULL) ThrowWith("文件'%s'已经存在.",fn);
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
	psa->log(dsttabid,0,118,"数据从%s.%s表转移而来.",srcdbn,srctabname);
	//建立文件链接
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

	//Move操作结束,打开目标表
	lgprintf("MySQL刷新...");
	char tbn[300];
	sprintf(tbn,"%s.%s",dstdbn,dsttabname);
	psa->BuildDTP(tbn);
	psa->FlushTables(tbn);
	lgprintf("删除源表..");
	RemoveTable(srcdbn,srctabname,false);
	lgprintf("数据已从表'%s'转移到'%s'。",srctabname,dsttabname);
	return 1;
}


// 7,10的任务状态处理:
//   1. 从文件系统获取二次压缩后的文件大小.
//   2. 任务状态修改为(8,11) (?? 可以省略)
//   3. 关闭目标表(unlink DTP file,flush table).
//   4. 修改数据/索引映射表中的文件大小和压缩类型.
//   5. 数据/索引 文件替换.
//   6. 任务状态修改为30(等待重新装入).
int DestLoader::ReLoad() {
	
	AutoMt mdf(psa->GetDTS(),MAX_DST_DATAFILENUM);
	mdf.FetchAll("select * from dp.dp_datapart where status in (7,10) and begintime<now() %s order by blevel,tabid,datapartid limit 2",psa->GetNormalTaskDesc());
	int rn=mdf.Wait();
	if(rn<1) {
		printf("没有发现重新压缩完成等待装入的数据.\n");
		return 0;
	}
	bool dpcp=mdf.GetInt("status",0)==7;
	int compflag=mdf.GetInt("compflag",0);
	tabid=mdf.GetInt("tabid",0); 
	psa->SetTrace("reload",tabid);
	mdf.FetchAll("select * from dp.dp_datafilemap where tabid=%d and (fileflag=0 or fileflag is null) order by indexgid",tabid);
	rn=mdf.Wait();
	if(rn<1) {
		lgprintf("装入二次压缩数据时找不到数据文件记录,按空表处理。");
		char sqlbf[MAX_STMT_LEN];
		sprintf(sqlbf,"update dp.dp_datapart set status=30 where tabid=%d", tabid);
		if(psa->DoQuery(sqlbf)<1) 
			ThrowWith("二次压缩数据重新装入过程修改任务状态异常，可能是与其它进程冲突(tabid:%d)。\n",tabid);
		return 0;
	}
	mdf.FetchAll("select * from dp.dp_table where tabid=%d",tabid);
	rn=mdf.Wait();
	if(rn<1) {
		lgprintf("装入二次压缩数据时找不到dp.dp_table记录(tabid:%d).",tabid);
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
	//先检查
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
		ThrowWith("二次压缩数据重新装入过程修改任务状态异常，可能是与其它进程冲突(tabid:%d)。\n",tabid);
	
	//防止功能重入，修改任务状态
	//后续修改将涉及数据文件的替换,操作数据前先关闭表
	// 这个关闭操作将锁住表的访问，直至上线成功。
	//TODO  文件保留到上线时刻最为理想
	psa->CloseTable(tabid,NULL,false,true);
	lgprintf("数据已关闭.");
	//用新的数据文件替换原来的文件：先删除原文件，新文件名称更改为原文件并修改文件记录中的文件大小字段。
	lgprintf("开始数据和索引文件替换...");
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
	lgprintf("数据和索引文件已成功替换...");
	sprintf(sqlbf,"update dp.dp_datapart set status=30,istimelimit=0 where tabid=%d", tabid);
	psa->DoQuery(sqlbf);
	sprintf(sqlbf,"update dp.dp_datafilemap set procstatus=0 where tabid=%d and fileflag=0",tabid);
	psa->DoQuery(sqlbf);
	lgprintf("任务状态修改为数据整理结束(3),数据文件处理状态改为未处理(0).");
	Load();
	return 1;
}


int DestLoader::RecreateIndex(SysAdmin *_Psa) 
{
	AutoMt mdf(psa->GetDTS(),MAX_MIDDLE_FILE_NUM);
	AutoMt mdf1(psa->GetDTS(),MAX_MIDDLE_FILE_NUM);
	char sqlbf[MAX_STMT_LEN];
	//检查需要上线的数据
	// 使用子查询，可以使逻辑更简单
	//   select * from dp.dp_datapart a where status=21 and istimelimit!=22 and begintime<now() %s and tabid not in (
   	//select tabid from dp_datapart b where b.tabid=a.tabid and b.status!=21 and b.status!=5 and b.begintime<now())
	//order by blevel,tabid,datapartid
	
	mdf1.FetchAll("select * from dp.dp_datapart where status=21 and begintime<now() %s order by blevel,tabid,datapartid",psa->GetNormalTaskDesc());
	if(mdf1.Wait()>0) {
   	   int mrn=mdf1.GetRows();
	   for(int i=0;i<mrn;i++) {
	   //做上线处理
	   tabid=mdf1.GetInt("tabid",i);
   	   psa->SetTrace("dataload",tabid);
	   datapartid=mdf1.GetInt("datapartid",i);
   	   int newld=mdf1.GetInt("oldstatus",i)==4?1:0;
   	   int oldstatus=mdf1.GetInt("oldstatus",i);
   	   if(mdf1.GetInt("istimelimit",i)==22)
   	   //其它进程在处理上线
   	   {
	   	mdf.FetchAll("select * from dp.dp_table where tabid=%d ",
		  tabid);
		if(mdf.Wait()>0)
		 lgprintf("表 %s.%s 上线过程在其它进程处理，如果其它进程异常退出，请重新装入",
		 mdf.PtrStr("databasename",0),mdf.PtrStr("tabname",0));
		else
		 lgprintf("tabid为%d的表上线过程在其它进程处理，如果其它进程异常退出，请重新装入",tabid);
		continue;
	   }
	   mdf.FetchAll("select * from dp.dp_datapart where tabid=%d and status!=21 and status!=5 and begintime<now()",
		  tabid);
	   if(mdf.Wait()<1) 
	   {
	    //从源表(格式表)构建目标表结构,避免分区数据抽取为空的情况
	    char tbname[150],idxname[150];
	    psa->GetTableName(tabid,-1,tbname,idxname,TBNAME_PREPONL);
	    sprintf(sqlbf,"update dp.dp_datapart set istimelimit=22 where tabid=%d and status =21 and begintime<now()",tabid);
	    if(psa->DoQuery(sqlbf)<1)  {
		lgprintf("表%d上线时修改任务状态异常，可能是与其它进程冲突。\n",
	        tabid);
	        continue;
	    }
	    try {
	    AutoMt destmt(0,10);
	    //如果找不到数据文件,则从源表创建表结构
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
	
	    lgprintf("删除中间临时文件...");
	    mdf.FetchAll("select * from dp.dp_middledatafile where tabid=%d",tabid);
	    
	    int dfn=mdf.Wait();
	    for(int di=0;di<dfn;di++) {
		lgprintf("删除文件'%s'",mdf.PtrStr("datafilename",di));
		unlink(mdf.PtrStr("datafilename",di));
		lgprintf("删除文件'%s'",mdf.PtrStr("indexfilename",di));
		unlink(mdf.PtrStr("indexfilename",di));
	    }
	    lgprintf("删除记录...");
	    st.Prepare("delete from dp.dp_middledatafile where tabid=%d",tabid);
	    st.Execute(1);
	    st.Wait();
	    psa->CleanData(false,tabid);
	    return 1;
	    }
	    catch(...) {
			errprintf("表%d 上线时出现异常错误,恢复处理状态...",tabid);
			//
			//sprintf(sqlbf,"update dp.dp_datapart set status=21,istimelimit=0,blevel=if(ifnull(blevel,0)>=100,blevel,ifnull(blevel,0)+100),oldstatus=%d where tabid=%d and istimelimit=22", oldstatus,tabid);
			sprintf(sqlbf,"update dp.dp_datapart set istimelimit=0,blevel=if(ifnull(blevel,0)>=100,blevel,ifnull(blevel,0)+100),oldstatus=%d where tabid=%d and istimelimit=22", oldstatus,tabid);
			//sprintf(sqlbf,"update dp.dp_datapart set status=21 where tabid=%d and istimelimit=22", oldstatus,tabid);
			psa->DoQuery(sqlbf);
  	        	psa->log(tabid,0,124,"上线时出现异常错误,已恢复处理状态.");
			throw;
	    }		
	  }
	  //else {
	  //  AutoStmt st(psa->GetDTS());
	  //  st.DirectExecute("update dp.dp_datapart set blevel=if(ifnull(blevel,0)>=100,blevel,ifnull(blevel,0)+100) where tabid=%d and datapartid=%d and ifnull(blevel,0)<100",tabid,datapartid);
	  //}
	  }
	}
	
	// 在装载完数据的索引表上，修复索引，并修改任务状态为21
	mdf.FetchAll("select * from dp.dp_datapart where (status =4 or status=40 ) %s order by blevel,tabid,datapartid limit 2",psa->GetNormalTaskDesc());
	int rn=mdf.Wait();
	if(rn<1) {
		return 0;
	}
	datapartid=mdf.GetInt("datapartid",0);
	tabid=mdf.GetInt("tabid",0);
  	psa->SetTrace("dataload",tabid);
	bool preponl=mdf.GetInt("status",0)==4;
	//if(tabid<1) ThrowWith("找不到任务号:%d中Tabid",taskid);
	
	//check intergrity.
	mdf.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>0",tabid);
	rn=mdf.Wait();
	if(rn<1) 
		ThrowWith("目标表%d缺少主索引记录.",tabid);
	
	mdf.FetchAll("select distinct indexgid from dp.dp_datafilemap where tabid=%d and fileflag=%d",tabid,preponl?1:0);
	rn=mdf.Wait();
	mdf.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>0",tabid);
	int rni=mdf.Wait();
	if(rni!=rn) 
	{
		lgprintf("出现错误: 重建索引时，数据文件中的独立索引数(%d)和索引参数表中的值(%d)不符,tabid:%d,datapartid:%d.",
			rn,rni,tabid,datapartid);
		return 0; //dump && destload(create temporary index table) have not complete.
	}
	try {
		sprintf(sqlbf,"update dp.dp_datapart set oldstatus=status,status=%d where tabid=%d and datapartid=%d and (status =4 or status=40 )",20,tabid,datapartid);
		if(psa->DoQuery(sqlbf)<1) 
			ThrowWith("数据装入重建索引过程修改任务状态异常，可能是与其它进程冲突。\n"
			"  tabid:%d.\n",
			tabid);
		
		lgprintf("开始索引重建,tabid:%d,总索引数 :%d",
			tabid,rn);
		psa->log(tabid,0,119,"开始建立索引.");
		//2005/12/01 索引改为数据新增后重建(修复)。
		lgprintf("建立索引的过程可能需要较长的时间，请耐心等待...");
		psa->RepairAllIndex(tabid,TBNAME_PREPONL,datapartid);
		lgprintf("索引建立完成.");
		psa->log(tabid,0,120,"索引建立完成.");
		AutoStmt st(psa->GetDTS());
		st.DirectExecute("update dp.dp_datapart set status=21 where tabid=%d and datapartid=%d",
		     tabid,datapartid);
		mdf.FetchAll("select * from dp.dp_datafilemap where tabid=%d and isfirstindex=1 and fileflag!=2 order by datapartid,indexgid,fileid",
			tabid);
		rn=mdf.Wait();
	}
	catch (...) {
		errprintf("建立索引结构时出现异常错误,恢复处理状态...");
		sprintf(sqlbf,"update dp.dp_datapart set status=%d,blevel=if(ifnull(blevel,0)>=100,blevel,ifnull(blevel,0)+100) where tabid=%d and datapartid=%d", preponl?3:30,tabid,datapartid);
		psa->DoQuery(sqlbf);
		sprintf(sqlbf,"update dp.dp_datafilemap set procstatus=0 where tabid=%d and "
			"procstatus=1 and fileflag=%d and datapartid=%d",tabid,preponl?1:0,datapartid);
		psa->DoQuery(sqlbf);
  	        psa->log(tabid,0,124,"建立索引结构时出现异常错误,已恢复处理状态.");
		throw;
	}
	return 1;
}

thread_rt LaunchWork(void *ptr) 
{
	((worker *) ptr)->work();
	thread_end;
}

//以下代码有错误
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
				lgprintf("表%d--二次压缩任务已完成，任务状态已修改为%d,数据文件处理状态修改为空闲(0)",tabid,deepcmp?7:10);
				return 1;
			}
			else lgprintf("表%d(%d)---二次压缩任务未完成,但已没有等待压缩的数据",tabid,datapartid);
		}
		else break;
	}
	if(i==rn1) return 0;
	
	//防止上一次mdt中的数据被其它进程中上面的代码改过dp_datapart status
	mdt.FetchAll("select tabid,datapartid,status,compflag from dp.dp_datapart where (status=6 or status=9) and begintime<now() and tabid=%d and datapartid=%d",
	   mdf.GetInt("tabid",0),mdf.GetInt("datapartid",0));
	if(mdt.Wait()<1) {
		lgprintf("要二次压缩处理的数据文件,对应任务状态已改变,取消处理.\n"
		         " tabid:%d,datapartid:%d,fileid:%d.",mdf.GetInt("tabid",0),mdf.GetInt("datapartid",0),mdf.GetInt("fileid",0));
		return 0;
	}
	psa->OutTaskDesc("数据重新压缩任务(tabid %d datapartid %d)",mdf.GetInt("tabid",0),mdf.GetInt("datapartid",0));
	int compflag=mdt.GetInt("compflag",0);
	lgprintf("原压缩类型:%d, 新的压缩类型:%d .",mdf.GetInt("compflag",0),compflag);
	int fid=mdf.GetInt("fileid",0);
	psa->log(tabid,0,121,"二次压缩，类型： %d-->%d ,文件号%d,日志文件 '%s' .",mdf.GetInt("compflag",0),compflag,fid,wociGetLogFile());

	char srcfn[300];
	strcpy(srcfn,mdf.PtrStr("filename",0));
	int origsize=mdf.GetInt("filesize",0);
	char dstfn[300];
	sprintf(dstfn,"%s.%s",srcfn,deepcmp?"depcp":"dep5");
	tabid=mdf.GetInt("tabid",0);
	mdf.FetchAll("select filename,idxfname from dp.dp_datafilemap where tabid=%d and fileid=%d and fileflag!=2",
		tabid,fid);
	if(mdf.Wait()<1)
		ThrowWith(" 找不到数据文件记录,dp_datafilemap中的记录已损坏,请检查.\n"
		" 对应的数据文件为:'%s',文件编号: '%d'",srcfn,fid);
	char idxdstfn[300];
	sprintf(idxdstfn,"%s.%s",mdf.PtrStr("idxfname",0),deepcmp?"depcp":"dep5");
	double dstfilelen=0;
	try {
		//防止重入，修改数据文件状态。
		AutoStmt st(psa->GetDTS());
		st.Prepare("update dp.dp_datafilemap set procstatus=1 where tabid=%d and fileid=%d and procstatus=0 and fileflag!=2",
			tabid,fid);
		st.Execute(1);
		st.Wait();
		if(wociGetFetchedRows(st)!=1) {
			lgprintf("处理文件压缩时状态异常,tabid:%d,fid:%d,可能与其它进程冲突！"
				,tabid,fid);
			return 0;
		}
		file_mt idxf;
		lgprintf("数据处理，数据文件:'%s',字节数:%d,索引文件:'%s'.",srcfn,origsize,mdf.PtrStr("idxfname",0));
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
			ThrowWith("索引文件读取错误: '%s'",mdf.PtrStr("filename",0));
		AutoMt *pidxmt=(AutoMt *)idxf;
		//lgprintf("从索引文件读入%d条记录.",wociGetMemtableRows(*pidxmt));
		int *pblockstart=pidxmt->PtrInt("blockstart",0);
		int *pblocksize=pidxmt->PtrInt("blocksize",0);
		blockcompress bc(compflag);
		for(i=1;i<threadnum;i++) {
			bc.AddWorker(new blockcompress(compflag));
		}
		lgprintf("启用线程数:%d.",threadnum);
#define BFNUM 32
		char *srcbf=new char[SRCBUFLEN];//?恳淮未淼淖畲笫菘椋ń庋顾鹾螅
		char *dstbf=new char[DSTBUFLEN*BFNUM];//可累积的最多数据(压缩后).
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
		while(!isalldone) {//文件处理完退出
			if(srcf.ReadMt(-1,0,mdf,1,1,srcbf,false,true)<0) {
				iseof=true;
			}
				    if(wdbi_kill_in_progress) {
				    	wdbi_kill_in_progress=false;
					  	ThrowWith("用户取消操作!");
					  }
			block_hdr *pbh=(block_hdr *)srcbf;
			int doff=srcf.GetDataOffset(pbh);
			if(pbh->origlen+doff>SRCBUFLEN) 
				ThrowWith("Decompress data exceed buffer length. dec:%d,bufl:%d",
				pbh->origlen+sizeof(block_hdr),SRCBUFLEN);
			bool deliverd=false;
			while(!deliverd) { //任务交付后退出
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
						ThrowWith("要压缩的数据:%d,超过缓存上限:%d.",dstlen,dstseplen);
					//get empty buf:
					for(i=0;i<BFNUM;i++) if(!isfilled[i]) break;
					if(i==BFNUM) ThrowWith("严重错误：压缩缓冲区已满，无法继续!.");
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
									//ThrowWith("索引文件读取错误: '%s'",mdf.PtrStr("filename",0));
									pidxmt=(AutoMt *)idxf;
									//lgprintf("从索引文件读入%d条记录.",wociGetMemtableRows(*pidxmt));
									pblockstart=pidxmt->PtrInt("blockstart",0);
									pblocksize=pidxmt->PtrInt("blocksize",0);
								}
								else break;
							}
							else if(lastrow>idxrn1) 
								ThrowWith("索引文件读取错误: '%s'",mdf.PtrStr("filename",0));
							
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
							printf("已处理%d个数据块(%d%%),%.2f(MB/s) 用时%.0f秒--预计还需要%.0f秒.\r",nextid,slastrow*100/idxrn,lastoffset/tm1/1024/1024,tm1,tm1/slastrow*(idxrn-slastrow));
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
			ThrowWith("异常错误：并非所有数据都被处理，已处理%d,应处理%d.",lastrow,wociGetMemtableRows(*pidxmt));
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
		errprintf("数据二次压缩出现异常，文件处理状态恢复...");
		AutoStmt st(psa->GetDTS());
		st.DirectExecute("update dp.dp_datafilemap set procstatus=0, blevel=ifnull(blevel,0)+1 where tabid=%d and fileid=%d and procstatus=1 and fileflag=0",
			tabid,fid);
		st.DirectExecute("update dp.dp_datapart set blevel=ifnull(blevel,0)+100 where tabid=%d and datapartid=%d",
		   tabid,datapartid);
		errprintf("删除数据文件和索引文件");
		unlink(dstfn);
		unlink(idxdstfn);
		throw;
	}
	
	psa->log(tabid,0,122,"二次压缩结束,文件%d，大小%d->%.0f",fid,origsize,dstfilelen);
	lgprintf("文件转换结束,目标文件:'%s',文件长度(字节):%.0f.",dstfn,dstfilelen);
	return 1;
}


int DestLoader::ToMySQLBlock(const char *dbn, const char *tabname)
{
	lgprintf("格式转换 '%s.%s' ...",dbn,tabname);
	AutoMt mt(psa->GetDTS(),100);
	mt.FetchAll("select * from dp.dp_table where tabname=lower('%s') and databasename=lower('%s')",tabname,dbn);
	int rn=mt.Wait();
	if(rn<1) {
		printf("表'%s'不存在!",tabname);
		return 0;
	}
	tabid=mt.GetInt("tabid",0);
	int recordnum=mt.GetInt("recordnum",0);
	int firstdatafileid=mt.GetInt("firstdatafileid",0);
	if(recordnum<1) {
		lgprintf("源表'%s'数据为空.",tabname);
		return 0;
	}
	AutoMt mdt(psa->GetDTS(),MAX_DST_DATAFILENUM);
	AutoMt mdf(psa->GetDTS(),MAX_DST_DATAFILENUM);
		psa->SetTrace("transblock",tabid);
	mdf.FetchAll("select * from dp.dp_datafilemap where tabid=%d and procstatus =0 and (fileflag=0 or fileflag is null) order by datapartid,indexgid,fileid",tabid);
	rn=mdf.Wait();
	//防止重入，修改数据文件状态。
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
			lgprintf("处理文件转换时状态异常,tabid:%d,fid:%d,可能与其它进程冲突！"
				,tabid,fid);
			return 1;
		}
		file_mt idxf;
		lgprintf("数据处理，数据文件:'%s',字节数:%d,索引文件:'%s'.",srcfn,origsize,mdf.PtrStr("filename",0));
		idxf.Open(mdf.PtrStr("filename",0),0);
		if(idxf.ReadBlock(-1,0)<0)
			ThrowWith("索引文件读取错误: '%s'",mdf.PtrStr("filename",0));
		
		file_mt srcf;
		srcf.Open(srcfn,0,fid);
		dt_file dstf;
		dstf.Open(dstfn,1,fid);
		mdf.SetHandle(srcf.CreateMt());
		int lastoffset=dstf.WriteHeader(mdf,0,fid,srcf.GetNextFileName());
		
		AutoMt *pidxmt=(AutoMt *)idxf;
		int idxrn=wociGetMemtableRows(*pidxmt);
		lgprintf("从索引文件读入%d条记录.",idxrn);
		int *pblockstart=pidxmt->PtrInt("blockstart",0);
		int *pblocksize=pidxmt->PtrInt("blocksize",0);
		int lastrow=0;
		int oldblockstart=pblockstart[0];
		int dspct=0;
		while(true) {//文件处理完退出
				    if(wdbi_kill_in_progress) {
				    	wdbi_kill_in_progress=false;
					  	ThrowWith("用户取消操作!");
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
		errprintf("数据转换出现异常，文件处理状态恢复...");
		AutoStmt st(psa->GetDTS());
		st.Prepare("update dp.dp_datafilemap set procstatus=0 where tabid=%d and fileid=%d and procstatus=1 and fileflag=0",
			tabid,fid);
		st.Execute(1);
		st.Wait();
		errprintf("删除数据文件和索引文件");
		unlink(dstfn);
		unlink(idxdstfn);
		throw;
	}
	
	lgprintf("文件转换结束,目标文件:'%s',文件长度(字节):%f.",dstfn,dstfilelen);
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
		//对于dp_table中没有记录的表,有可能是dpio快速恢复的表.
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
				ThrowWith("指定的文件'%s'类型%d不是参数文件!",streamPath,stp);
			dtioDTTable dtt(dbn,tabname,pdtio,false);
			dtt.DeserializeParam();
			dtparams_mt &dtmts=*dtt.GetParamMt();
			if(prompt) {
				sprintf(choose,"快速恢复的DP表'%s.%s'将被删除，记录数:%lld ?(Y/N)",dbn,tabname,dtmts.GetRecordNum());
				if(!GetYesNo(choose,false)) {
					lgprintf("取消删除。 ");
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
				//检查分区索引表是否存在(.frm).
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
			lgprintf("表'%s.%s'已删除.",dbn,tabname);
			return 1;
		}
		else {
			//JIRA DM-8: 异常恢复的表，清理最小集合： 目标表+主索引表
			//  对于其它独立索引表和数据文件，没有清理的线索，需要手工处理
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
		  ThrowWith("表%s.%s在dp_table中找不到，目标表和主索引表已删除，其它数据可能需要手工清理！",dbn,tabname);
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
			lgprintf("删除DP参数文件.");
			char streamPath[300];
			sprintf(streamPath,"%s%s/%s.DTP",psa->GetMySQLPathName(0,"msys"),dbn,tabname);
			unlink(streamPath);
		}
	}
	if(rn<1) {
		lgprintf("表'%s.%s'已删除.",dbn,tabname);
		return 0;
	}
	
	if(prompt) {
		if(recordnum<1)
			sprintf(choose,"DP表'%s.%s'将被删除，数据为空，继续？(Y/N)",dbn,tabname);
		else
			sprintf(choose,"DP表'%s.%s'将被删除，记录数:%.0f？(Y/N)",dbn,tabname,recordnum);
		if(!GetYesNo(choose,false)) {
			lgprintf("取消删除。 ");
			return 0;
		}			
	}
	
	
	lgprintf("删除数据文件.");
	//下面的查询不需要加fileflag!=2的限制条件，可以全部删除。
	mt.FetchAll("select * from dp.dp_datafilemap where tabid=%d ",
		tabid);
	rn=mt.Wait();
	int i=0;
	for(i=0;i<rn;i++) {
		char tmp[300];
		lgprintf("删除'%s'和附加的depcp,dep5文件",mt.PtrStr("filename",i));
		unlink(mt.PtrStr("filename",i));
		sprintf(tmp,"%s.depcp",mt.PtrStr("filename",i));
		unlink(tmp);
		sprintf(tmp,"%s.dep5",mt.PtrStr("filename",i));
		unlink(tmp);
		lgprintf("删除'%s'和附加的depcp,dep5文件",mt.PtrStr("idxfname",i));
		unlink(mt.PtrStr("idxfname",i));
		sprintf(tmp,"%s.depcp",mt.PtrStr("idxfname",i));
		unlink(tmp);
		sprintf(tmp,"%s.dep5",mt.PtrStr("idxfname",i));
		unlink(tmp);
	}
	//下面的语句不需要加fileflag!=2的限制条件，可以全部删除。
	st.Prepare(" delete from dp.dp_datafilemap where tabid=%d ",tabid);
	st.Execute(1);
	st.Wait();

    //>> Begin: fix dm-254
    // 删除整理过中及待整理的问题
	mt.FetchAll("select tabid,datafilename,indexfilename from dp.dp_middledatafile where tabid = %d",tabid);
    rn=mt.Wait();
	for(i=0;i<rn;i++)
	{
        lgprintf("删除采集完成后的数据文件'%s'",mt.PtrStr("datafilename",i));
	    unlink(mt.PtrStr("datafilename",i));

        lgprintf("删除采集完成后的索引文件'%s'",mt.PtrStr("indexfilename",i));
		unlink(mt.PtrStr("indexfilename",i));
	}
	//<< End:fix dm-254
	
	st.Prepare(" delete from dp.dp_middledatafile where tabid=%d",tabid);
	st.Execute(1);
	st.Wait();
	
	bool forcedel=false;
	//if(prompt) {
		sprintf(choose,"表'%s.%s'的配置参数也要删除吗?(Y/N)",dbn,tabname);
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
			 //3.10版新格式索引.
			 while(psa->GetTableName(tabid,mt.GetInt("indexgid",i),NULL,tmp,TBNAME_DEST,ilp++)) {
	 	 		psa->FlushTables(tmp);
				if(psa->TouchTable(tmp)) {
			          lgprintf("删除索引表 '%s'",tmp);
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
	
	lgprintf("表'%s.%s'已删除%s.",dbn,tabname,forcedel?"":",但参数表保留");
	return 1;
}

