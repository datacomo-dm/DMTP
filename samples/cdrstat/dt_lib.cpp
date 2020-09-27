#ifdef WIN32
#include <direct.h>
#include <io.h>
#else
#include <unistd.h>
#include <ctype.h>
#endif
#include <stdio.h>
#include <stdlib.h>
#include "dt_lib.h"
#include "zlib.h"
#define COMPRESSLEVEL 5
#ifdef WIN32
#define mCopyFile(a,b) CopyFile(a,b,FALSE)
#else
#define mCopyFile(a,b) uCopyFile(a,b)
#endif

#ifdef __unix
int uCopyFile(const char * src,const char *dest) {
 FILE *fsrc=fopen(src,"rb");
 if(fsrc==NULL) 
   ThrowWith("Open source file error while copy file '%s' to '%s'",
 	  	   src,dest);
 FILE *fdest=fopen(dest,"w+b");
 if(fdest==NULL) {
 	ThrowWith("Create dest file error while copy file '%s' to '%s'",
 	  	   src,dest);
 	fclose(fsrc);
 	return -2;
 }
 char buf[1024];
 for(;;) {
 	int l=fread(buf,1,1024,fsrc);
 	if(l>0) {
 	  l=fwrite(buf,1,l,fdest);
 	  if(l<1) {
 	  	fclose(fsrc);
 	  	fclose(fdest);
 	  	ThrowWith("Wirte file error while copy file '%s' to '%s'",
 	  	   src,dest);
 	  }
 	}
 	else break;
 	  
 }
 fclose(fsrc);
 fclose(fdest);
 return 1;		
}
#endif

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
	return 1;
}


int CreateMtFromFile(int maxrows,char *filename)
{
	FILE *fp=fopen(filename,"rb");
	if(!fp)
		ThrowWith("CreateMt from file '%s' which could not open.",filename);
	int cdlen=0,cdnum=0;
	fread(&cdlen,sizeof(int),1,fp);
	fread(&cdnum,sizeof(int),1,fp);
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

	MySQLConn::MySQLConn() {
		memset(host,0,sizeof(host));
		memset(username,0,sizeof(username));
		memset(password,0,sizeof(password));
		memset(dbname,0,sizeof(dbname));
		myData=NULL;
		result=NULL;
	}

	 void MySQLConn::FlushTables()
	{
		DoQuery("flush tables");
#ifdef WIN32
			Sleep(2000);
#else
			sleep(2);
#endif

	}

         void MySQLConn::SelectDB(char *db) {
		if(!myData) ThrowWith("Connection has not build while select db:%s\n",db);
		if ( mysql_select_db( myData, db ) < 0 ) {
			mysql_close( myData ) ;
			ThrowWith( "Can't select the %s database !\n", dbname ) ;
		}
	}

	void MySQLConn::Connect(char *host,char *username,char *password,char *dbname,unsigned int portnum) {
		if(host) strcpy(this->host,host);
		else this->host[0]=0;
		if(username) strcpy(this->username,username);
		else strcpy(this->username,"root");
		if(password) strcpy(this->password,password);
		else strcpy(this->password,"root");
		if(dbname) strcpy(this->dbname,dbname);
		else strcpy(this->dbname,"mysql");
		if(portnum!=0) this->portnum=portnum;
		else portnum=3306;
		if ( (myData = mysql_init((MYSQL*) 0)) && 
         mysql_real_connect( myData, host,username,password,NULL,portnum,
			   NULL, 0 ) )
		{
			if ( mysql_select_db( myData, this->dbname ) < 0 ) {
				mysql_close( myData ) ;
				myData=NULL;
				ThrowWith( "Can't select the '%s' database !\n", this->dbname ) ;
			}
		}
		else {
			myData=NULL;
			ThrowWith( "Can't connect to the mysql server on port %d !\n",
					portnum ) ;
		}
	}

	int MySQLConn::DoQuery(const char *szSQL)
	{
		if(!myData) ThrowWith("Connection has not build while query:%s\n",szSQL);
		if(result) {
			mysql_free_result(result);
			result=NULL;
		}
		//char dstsql[3000];
		//mysql_real_escape_string(myData,dstsql,szSQL,strlen(szSQL));
		if ( mysql_query( myData, szSQL)) {//dstsql,strlen(dstsql) ) ) 
			//  ./include/mysqld_error.h :
			//   #define ER_CANT_DROP_FIELD_OR_KEY 1091
			//   #define ER_NO_SUCH_TABLE 1146
			if(mysql_errno(myData)==1091 || mysql_errno(myData)==1146) return 0;
  		 ThrowWith( "Couldn't execute %s on the server !\n errcode:%d,info:%s\n", szSQL ,
			 mysql_errno(myData),mysql_error(myData)) ;
		}
		result=mysql_store_result(myData);
		if(!result) {
			//ERROR 1091: Can't DROP 'tab_gsmvoicdr2_2'. Check that column/key exists
			//if(mysql_errno(myData)==1091) return ;
			if (mysql_errno(myData))
			{
				ThrowWith("Error: %s\n", mysql_error(myData));
			}
			else if (mysql_field_count(myData) == 0)
			{
				// query does not return data
				// (it was not a SELECT)
				num_rows = mysql_affected_rows(myData);
				//const char *info=mysql_info(myData) ;
				//if(info) lgprintf(info);
			}
		}
		return num_rows;
	}

	bool MySQLConn::TouchTable(const char *tabname) {
		if(!myData) ThrowWith("Connection has not build while touchtable:%s\n",tabname);
		char sql[300];
		sprintf(sql,"desc %s",tabname);
		if(result) {
			mysql_free_result(result);
			result=NULL;
		}
		if ( mysql_query( myData, sql ) ) {
		 if(mysql_errno(myData)==1146) return false;
  		 ThrowWith( "Couldn't execute %s on the server !\n errcode:%d,info:%s\n", sql ,
			 mysql_errno(myData),mysql_error(myData)) ;
		}
		result=mysql_store_result(myData);
		return true;
	}


	
	void SysAdmin::CreateDT(int tabid) 
	{
	    MySQLConn conn;
	    wociSetTraceFile("CreateDT");
	    conn.Connect(NULL,NULL,NULL,"dt");
		int tabp=wociSearchIK(dt_table,tabid);
		int srctabid=dt_table.GetInt("srctabid",tabp);
		int srctabp=wociSearchIK(dt_srctable,srctabid);

		int srcid=dt_srctable.GetInt("srcsysid",srctabp);
		int srcidp=wociSearchIK(dt_srcsys,srcid);
		//int tabid=dt_table.GetInt("tabid",tabp);
		
		//Reset recordnum before create data table and index
		char sqlbf[200];
		sprintf(sqlbf,"update dt.dt_table set recordnum=0 where tabid=%d",tabid);
		lgprintf("��¼������...");
		conn.DoQuery(sqlbf);

		lgprintf("Flush MySQL...");
		conn.FlushTables();
		//����������Դ������
		AutoHandle srcsys;
		char tabname[300];
		const char *pathval=GetPathName(0,"cdes");
		srcsys.SetHandle(BuildSrcDBC(srcidp));
	    //����Դ���ݵ��ڴ��ṹ
		AutoMt srcmt(srcsys,0);
		srcmt.SetHandle(GetSrcTableStructMt(srctabp,tabp,srcsys));

		//����Ŀ���
		char *dbsn=dt_table.PtrStr("databasename",tabp);
		conn.SelectDB(dbsn);
		strcpy(tabname,dt_table.PtrStr("tabname",tabp));
		bool newdttab=CreateTableOnMysql(pathval,conn,srcmt,tabname,tabid,0,0,true);
		
		//�����������Ŀ��������������
		AutoMt idxtarget(dts,100);
		AutoMt idxmt(dts,100);
		//ÿһ��������������Ҫ����������
		idxmt.FetchAll("select * from dt_index where tabid=%d and issoledindex=1",
			tabid);
		int rn=idxmt.Wait();
		if(rn<1) 
			ThrowWith("no soled index defined in table '%s'",
			  dt_srctable.PtrStr("srctabname",srctabp));
		for(int i=0;i<rn;i++) {
			CreateIndexTable(conn,srcmt,tabid,idxmt.GetInt("indexid",i),
				idxmt.PtrStr("indextabname",i),idxmt.PtrStr("columnsname",i),pathval,true);
			CreateIndex(tabname,idxmt.PtrStr("indextabname",i),0,conn,tabid,
				idxmt.GetInt("indexid",i),true);
		}
	}


DataDump::DataDump(int dtdbc,int maxmem,int maxrows):fnmt(dtdbc,300)
{
	this->dtdbc=dtdbc;
	//Create fnmt and build column structrues.
	fnmt.FetchAll("select * from dt_middledatafile where rownum<1");
	fnmt.Wait();
	fnmt.Reset();
	taskid=srctabid=indexid=0;
	memlimit=maxmem;
	idxmaxrows=maxrows;
	maxblockrn=0;
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
			lgprintf("Available space on hard disk less then 1G : %dM,waitting 5 minutes for free...",freem);
#ifdef WIN32
			Sleep(300000);
#else
			sleep(300);
#endif
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
	
	AutoMt idxdt(0,idxmaxrows);
	wociCopyColumnDefine(idxdt,cur,idxcolsname);
	wociAddColumn(idxdt,"idx_blockoffset","",COLUMN_TYPE_INT,0,0);
//	wociAddColumn(idxdt,"idx_storesize","",COLUMN_TYPE_INT,0,0);
	wociAddColumn(idxdt,"idx_startrow","",COLUMN_TYPE_INT,0,0);
	wociAddColumn(idxdt,"idx_rownum","",COLUMN_TYPE_INT,0,0);
	wociAddColumn(idxdt,"idx_fid","",COLUMN_TYPE_INT,0,0);
	idxdt.Build();
	freeinfo1("After Build indxdt mt");
	
	void *idxptr[20];
	int pidxc1[10];
	bool pkmode=false;
	sorttm.Start();
	int cn1=wociConvertColStrToInt(cur,idxcolsname,pidxc1);
	if(cn1==1 && wociGetColumnType(cur,pidxc1[0])==COLUMN_TYPE_INT)
		pkmode=true;
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
	idxdt.Reset();
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
			idx_blockoffset=df.WriteMt(blockmt,COMPRESSLEVEL);
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
		idx_blockoffset=df.WriteMt(blockmt,COMPRESSLEVEL);
		idx_startrow=0;
		strow=-1;blockrn=0;
		blockmt.Reset();
	}
	//������������
	{
		dt_file di;
		di.Open(tmpidxfn,1);
		di.WriteHeader(idxdt,wociGetMemtableRows(idxdt));
		di.WriteMt(idxdt,COMPRESSLEVEL);
	}
	//�����ļ�����
	{
		void *ptr[20];
		int subdatasetid=((partid%100)*100+(idx%100))*10000+(datasetid%10000);
		ptr[0]=&fid;ptr[1]=&taskid;ptr[2]=&partid;
		ptr[3]=&subdatasetid;ptr[4]=&srctabid;ptr[5]=&dp.tabid;
		int rn=df.GetRowNum();
		int fl=df.GetLastOffset();
		ptr[6]=&rn;ptr[7]=&fl;
		char now[10];
		wociGetCurDateTime(now);
		ptr[8]=tmpfn;ptr[9]=tmpidxfn;ptr[10]=now;
		int state=1;
		ptr[11]=&state;
		char nuldt[10];
		memset(nuldt,0,10);
		ptr[12]=now;//nuldt;
		ptr[13]=&dp.idxp[idx].idxid;
		ptr[14]=dp.idxp[idx].idxcolsname;
		ptr[15]=dp.idxp[idx].idxreusecols;
		ptr[16]=&datasetid;
		ptr[17]=NULL;
		wociInsertRows(fnmt,ptr,NULL,1);
	}
	freeinfo1("End ProcBlock");
}

int DataDump::DoDump(SysParam &sp) {
	wociSetTraceFile("dtdump");
	taskid=sp.GetFirstTaskID(NEWTASK,srctabid,datasetid);
	if(taskid<1) return 0;
	sorttm.Clear();
	fiotm.Clear();
	adjtm.Clear();
	sp.GetSoledInexParam(srctabid,&dp);
	if(xmkdir(dp.tmppath[0])) 
		ThrowWith("��ʱ��·���޷�����,Դ��:%d,�����:%d,·��:%s.",
		       srctabid,taskid,dp.tmppath[0]);
	if(xmkdir(dp.tmppath[1]))
		ThrowWith("��ʱ����·���޷�����,Դ��:%d,�����:%d,·��:%s.",
		       srctabid,taskid,dp.tmppath[1]);
	int partoff=0;
	int realrn=memlimit/dp.rowlen;
	if(realrn>dp.maxrecnum) realrn=dp.maxrecnum;
	{
	AutoStmt st(dtdbc);
	st.Prepare("delete from dt_middledatafile where datasetid=%d and taskid=%d",datasetid,taskid);
	st.Execute(1);
	st.Wait();
	}
	//realrn=50000;
	//indexparam *ip=&dp.idxp[dp.psoledindex];
	maxblockrn=sp.GetMaxBlockRnFromSrcTab(srctabid);
	AutoMt blockmt(0,maxblockrn);
	fnmt.Reset();
	int partid=0;
	AutoHandle srcdbc;
	fnorder=0;
	srcdbc.SetHandle(sp.BuildSrcDBCFromSrcTableID(srctabid));
	sp.UpdateTaskStatus(DUMPING,taskid);
	/*
	try {
	*/
	wociSetEcho(TRUE);
	
	while(partid=sp.GetDumpSQL(taskid,partoff++,dumpsql),partid!=-1) {
		//idxdt.Reset();
		
		TradeOffMt dtmt(0,realrn);
		AutoStmt stmt(srcdbc);
		stmt.Prepare(dumpsql);
		dtmt.Cur()->Build(stmt);
		dtmt.Next()->Build(stmt);
		blockmt.Clear();
		blockmt.Build(stmt);
		//׼����������������������
		dtmt.FetchFirst();
		
		int rn=dtmt.Wait();
		while(rn>0) {
			dtmt.FetchNext();
			lgprintf("��ʼ���ݴ���");
			freeinfo1("before call prcblk");
			for(int i=0;i<dp.soledindexnum;i++) {
				ProcBlock(partid,dtmt.Cur(),i/*dp.psoledindex*/,blockmt,sp.NextFileID());
			}
			lgprintf("���ݴ������");
			freeinfo1("after call prcblk");
			rn=dtmt.Wait();
		}
		//delete dtmt;
		//delete stmt;
	}
	wociSetEcho(FALSE);
	wociAppendToDbTable(fnmt,"dt_middledatafile",dtdbc);
	sp.UpdateTaskStatus(DUMPED,taskid);
	/*
	}
	catch(...) {
		int frn=wociGetMemtableRows(fnmt);
		errprintf("���ݵ����쳣��ֹ�������:%d,������:%d,�м��ļ���:%d.",taskid,partid,frn);
		int i;
		errprintf("ɾ���м��ļ�...");
		for(i=0;i<frn;i++) {
			errprintf("\t %s \t %s",fnmt.PtrStr("datafilename",i),
			   fnmt.PtrStr("indexfilename",i));
		}
		for(i=0;i<frn;i++) {
			unlink(fnmt.PtrStr("datafilename",i));
			unlink(fnmt.PtrStr("indexfilename",i));
		}
		errprintf("�ָ�����״̬.");
		sp.UpdateTaskStatus(NEWTASK,taskid);
		errprintf("ɾ���м��ļ���¼...");
		AutoStmt st(dtdbc);
		st.Prepare("delete from dt_middledatafile where datasetid=%d and taskid=%d",datasetid,taskid);
		st.Execute(1);
		st.Wait();
		throw;
	}
	*/
	lgprintf("���ݳ�ȡ����,����״̬1-->2,taskid:%d",taskid);
	lgprintf("sort time :%11.6f file io time :%11.6f adjust data time:%11.6f",
		sorttm.GetTime(),fiotm.GetTime(),adjtm.GetTime());
	lgprintf("����");
	//lgprintf("�����������...");
	//getchar();
	//MYSQL�е�MY_ISAM��֧����������MYSQL����޸Ĳ���Ҫ�ύ.
	return 0;
}

MiddleDataLoader::MiddleDataLoader(SysAdmin *_sp):
	   indexmt(0,0),mdindexmt(0,0),blockmt(0,0),mdf(_sp->GetDTS(),300)
	  {
		  sp=_sp;
		  tmpfilenum=0;
		  pdtf=NULL;
		  pdtfid=NULL;
		  dtfidlen=0;
	  }

int MiddleDataLoader::Load(MySQLConn &conn,int MAXINDEXRN) {
	//Check deserved temporary(middle) fileset
	//���״̬Ϊ1������
	wociSetTraceFile("MLoad");

	mdf.FetchAll("select * from dt_middledatafile where procstate<=1  order by subdatasetid");
	int rn=mdf.Wait();
	if(rn>0) {
		mdf.FetchAll("select * from dt_middledatafile where subdatasetid=%d",
			mdf.GetInt("subdatasetid",0));
		rn=mdf.Wait();
		if(rn<1) 
			ThrowWith("MiddleDataLoader::Load : ȷ�������Ӽ����Ҳ����м����ݼ�¼��");
	}
	else return 0;
	//ȡ��������
	int subdatasetid=mdf.GetInt("subdatasetid",0);
	int srctabid=mdf.GetInt("srctabid",0);
	int indexid=mdf.GetInt("indexid",0);
	int datasetid=mdf.GetInt("datasetid",0);
	int tabid=mdf.GetInt("tabid",0);
	int taskid=mdf.GetInt("taskid",0);
	//��dt_datafilemap(��blockmt�ļ���)��dt_indexfilemap(��indexmt�ļ���)
	//�����ڴ��ṹ
	char fn[300];
	AutoMt fnmt(sp->GetDTS(),300);
	fnmt.FetchAll("select * from dt_datafilemap where rownum<1");
	fnmt.Wait();
	AutoMt fnidxmt(sp->GetDTS(),300);
	fnidxmt.FetchAll("select * from dt_indexfilemap where rownum<1");
	fnidxmt.Wait();
#ifdef __unix
	long dispct=0,lstdispct=0;
#else
	__int64 dispct=0,lstdispct=0;
#endif
	
	char sqlbf[200];
	try {	
	//Ϊ��ֹ��������,�м��ļ�״̬�޸�.
	lgprintf("�޸��м��ļ��Ĵ���״̬(subdatasetid:%d,%d���ļ�)��1-->2",subdatasetid,rn);
	sprintf(sqlbf,"update dt.dt_middledatafile set procstate=2 where subdatasetid=%d and procstate<=1 and taskid=%d",subdatasetid,taskid);
	if(conn.DoQuery(sqlbf)!=rn) 
		ThrowWith("MLoader�޸��м��ļ��Ĵ���״̬�쳣�����������������̳�ͻ��\n"
			"   taskid:%d,tabid:%d,datasetid:%d,subdatasetid:%d,srctabid:%d,indexid:%d.\n",
			taskid,tabid,datasetid,subdatasetid,srctabid,indexid);

	tmpfilenum=rn;
	//���������ļ��������ۼ�����������
	int idxrn=0;
	int i;
	for( i=0;i<rn;i++) {
		dt_file df;
		df.Open(mdf.PtrStr("indexfilename",i),0);
		idxrn+=df.GetRowNum();
	}
	lgprintf("������¼��:%d",idxrn);
	//�����м�����(�м��ļ����ݿ�����)�ڴ��mdindexmt��Ŀ�����ݿ��ڴ��blockmt
	int maxblockrn=sp->GetMaxBlockRnFromSrcTab(srctabid);
	{
	dt_file idf;
	idf.Open(mdf.PtrStr("indexfilename",0),0);
	mdindexmt.SetHandle(idf.CreateMt(idxrn));
	//wociAddColumn(idxmt,"dtfileid",NULL,COLUMN_TYPE_INT,4,0);
	//idxmt.SetMaxRows(idxrn);
	mdindexmt.Build();
	idf.Open(mdf.PtrStr("datafilename",0),0);
	blockmt.SetHandle(idf.CreateMt(maxblockrn));
	//mdblockmt.SetHandle(idf.CreateMt(maxblockrn));
	}
	int crn=0;
//	wociGetIntAddrByName(idxmt,"dtfileid",0,&pdtfid);
	// pdtfidΪһ���ַ����飬ƫ��Ϊx��ֵ��ʾ�м������ڴ���x�е��ļ����(Base0);
	if(dtfidlen<idxrn) 
	{
		if(pdtfid)
			delete [] pdtfid;
		pdtfid=new unsigned char [idxrn];
		dtfidlen=idxrn;
	}
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
		int brn=df.ReadMt(0,0,mdindexmt,false);
		for(int j=crn;j<crn+brn;j++)
			pdtfid[j]=(unsigned char )i;
		crn+=brn;
		//pdtf[i].SetParalMode(true);
		pdtf[i].Open(mdf.PtrStr("datafilename",i),0);
	}
	lgprintf("��������:%d.",crn);
	//��mdindexmt(�м������ڴ��)������
	//���������漰�ڴ����������У������½���¼˳�����ˣ�
	// pdtfid��Ϊ�ڴ����ĵ�Ч�ڴ���ֶΣ�����������
	lgprintf("����...");
	{
		char sort[300];
		sprintf(sort,"%s,idx_fid,idx_blockoffset",mdf.PtrStr("soledindexcols",0));
		wociSetSortColumn(mdindexmt,sort);
		wociSortHeap(mdindexmt);
	}
	lgprintf("�������.");
	//ȡ��ȫ�����������ṹ
	sp->GetSoledInexParam(srctabid,&dp);
	//�����Ҫ������м������Ƿ�ʹ������������������ǣ�isfirstidx=1.
	int isfirstidx=0;
	indexparam *ip;
	{
	int idxp=dp.GetOffset(indexid);
	ip=&dp.idxp[idxp];
	if(idxp==dp.psoledindex) isfirstidx=1;
	}
	//�ӽṹ�����ļ�����indexmt,indexmt��Ŀ�������ڴ���ǽ���Ŀ�������������Դ��
	indexmt.SetHandle(CreateMtFromFile(MAXINDEXRN,ip->cdsfilename));

	//ȡ���������͸���������blockmt(Ŀ�����ݿ��ڴ��)�ṹ�е�λ�ã�
	// ���ṹ�����ļ������������Ƿ��ϵͳ��������ָ�����ֶ�����ͬ��
	void *indexptr[40];
	int pblockc1[20];
	int pblockc2[20];
	bool pkmode=false;
	int bcn1=wociConvertColStrToInt(blockmt,ip->idxcolsname,pblockc1);
	if(bcn1==1 && wociGetColumnType(blockmt,pblockc1[0])==COLUMN_TYPE_INT)
		pkmode=true;
	int bcn2=wociConvertColStrToInt(blockmt,ip->idxreusecols,pblockc2);
	if(wociGetColumnNumber(indexmt)!=bcn1+bcn2+6) {
		ThrowWith("Column number error,subdatasetid:%d,colnum:%d,deserved:%d",
			subdatasetid,wociGetColumnNumber(indexmt),bcn1+bcn2+6);
	}
	//����dt_index�е�idxfieldnum
	sprintf(sqlbf,"update dt.dt_index set idxfieldnum=%d where indexid=%d or reuseindexid=%d",
		bcn1+bcn2,indexid,indexid);
	lgprintf("����dt_index�е������ֶ���(idxfieldnum).");
	conn.DoQuery(sqlbf);

	//ȡ����������mdindexmt(�м��������)�ṹ�е�λ�á�
	//���ö�indexmt�����¼��Ҫ�Ľṹ�ͱ�����
	int pidxc1[20];
	int cn1=wociConvertColStrToInt(mdindexmt,ip->idxcolsname,pidxc1);
	int dtfid,blockstart,blocksize,blockrn=0,startrow,rownum;
	int stcn=bcn1+bcn2;
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
	dtfid=sp->NextFileID();
	dp.usingpathid=0;
	sprintf(fn,"%s%d_%d_%d.dat",dp.dstpath[0],srctabid,subdatasetid,dtfid);
	dt_file dstfile;
	dstfile.Open(fn,1);
	blockstart=dstfile.WriteHeader(blockmt,0,dtfid);
	char idxfn[300];
	sprintf(idxfn,"%s%d_%d_%d.idx",dp.dstpath[0],srctabid,subdatasetid,dtfid);
	dt_file idxf;
	idxf.Open(idxfn,1);

	startrow=0;
	rownum=0;
	blockrn=0;
	int subtotrn=0;
	lgprintf("��ʼ���ݴ���(MiddleDataLoading)....");

	/*******����Sort˳�����mdindexmt(�м������ڴ��)***********/
	//
	//
	lgprintf("�����ļ�,���:%d...",dtfid);
	int firstrn=wociGetRawrnBySort(mdindexmt,0);
	//mytimer instm,
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
			for(c=0;c<bcn1;c++) {
				indexptr[c]=blockmt.PtrVoid(pblockc1[c],startrow);
			}
			for(c=0;c<bcn2;c++) {
				indexptr[bcn1+c]=blockmt.PtrVoid(pblockc2[c],startrow);
			}
			if(rownum>0) {
				wociInsertRows(indexmt,indexptr,NULL,1);
				dispct++;
				if(dispct-lstdispct>5000) {
					lstdispct=dispct;
					lgprintf("�Ѵ���%d������",dispct);
				}
			}
			firstrn=thisrn;
			//�������startrow��ʱָ����Ч�е����(����δ���).
			startrow=blockrn;
			rownum=0;
		}
		//�������ļ��ж������ݿ�
		int mt=pdtf[rfid].ReadBlock(poffset[thisrn],0,true);
		//���ݿ��� 
		//������ݿ��Ƿ���Ҫ���
		if(blockrn+sbrn>maxblockrn) {
			//ÿ�����ݿ�������Ҫ�ﵽ���ֵ��80%��
			if(blockrn<maxblockrn*.8) {
				//�������80%���ѵ�ǰ��������ݿ���
				int rmrn=maxblockrn-blockrn;
				wociCopyRowsTo(mt,blockmt,-1,sbstart,rmrn);
				rownum+=rmrn;
				sbrn-=rmrn;
				sbstart+=rmrn;
				blockrn+=rmrn;
			}
			//�����ӿ�����
			if(startrow<blockrn) {
			int c;
			for(c=0;c<bcn1;c++) {
				indexptr[c]=blockmt.PtrVoid(pblockc1[c],startrow);
			}
			for(c=0;c<bcn2;c++) {
				indexptr[bcn1+c]=blockmt.PtrVoid(pblockc2[c],startrow);
			}
			if(rownum>0) {
				wociInsertRows(indexmt,indexptr,NULL,1);
				dispct++;
			}
			}

			//���������
			blocksize=dstfile.WriteMt(blockmt,COMPRESSLEVEL)-blockstart;
			int irn=wociGetMemtableRows(indexmt);
			while(--irn>=0) {
				if(pblockstart[irn]==blockstart) {
					pblocksize[irn]=blocksize;
					pblockrn[irn]=blockrn;
				}
				else break;
			}
			blockstart+=blocksize;
			subtotrn+=blockrn;
			if(blockstart>2000000000) {
			//�����ļ����ձ��¼(dt_datafilemap)
				{
					void *fnmtptr[20];
					fnmtptr[0]=&dtfid;
					fnmtptr[1]=fn;
					fnmtptr[2]=&subdatasetid;
					fnmtptr[3]=&dp.dstpathid[dp.usingpathid];
					fnmtptr[4]=&dp.tabid;
					fnmtptr[5]=&ip->idxid;
					fnmtptr[6]=&isfirstidx;
					fnmtptr[7]=&blockstart;
					fnmtptr[8]=&subtotrn;
					fnmtptr[9]=&datasetid;
					int procstatus=0;
					fnmtptr[10]=&procstatus;
					int compflag=COMPRESSLEVEL;
					fnmtptr[11]=&compflag;
					wociInsertRows(fnmt,fnmtptr,NULL,1);
				}
			//
			//�����������ݶ��ձ��¼(dt_indexfilemap)
				{
					int irn1=wociGetMemtableRows(indexmt);
					idxf.WriteHeader(indexmt,dtfid);
					int idxfsize=idxf.WriteMt(indexmt,COMPRESSLEVEL);
					void *fnmtptr[20];
					fnmtptr[0]=&dtfid;
					fnmtptr[1]=idxfn;
					fnmtptr[2]=&subdatasetid;
					fnmtptr[3]=&dp.dstpathid[dp.usingpathid];
					fnmtptr[4]=&dp.tabid;
					fnmtptr[5]=&ip->idxid;
					fnmtptr[6]=&isfirstidx;
					fnmtptr[7]=&idxfsize;
					fnmtptr[8]=&irn1;
					fnmtptr[9]=&datasetid;
					int compflag=COMPRESSLEVEL;
					fnmtptr[10]=&compflag;
					wociInsertRows(fnidxmt,fnmtptr,NULL,1);
				}
			//
				dtfid=sp->NextFileID();
				sprintf(fn,"%s%d_%d_%d.dat",dp.dstpath[0],srctabid,subdatasetid,dtfid);
				dstfile.SetFileHeader(subtotrn,fn);
				dstfile.Open(fn,1);
				blockstart=dstfile.WriteHeader(blockmt,0,dtfid);
				lgprintf("�����ļ�,���:%d...",dtfid);
				sprintf(idxfn,"%s%d_%d_%d.idx",dp.dstpath[0],srctabid,subdatasetid,dtfid);
				idxf.SetFileHeader(wociGetMemtableRows(indexmt),idxfn);
				idxf.Open(idxfn,1);
				idxf.WriteHeader(indexmt,0,dtfid);
				indexmt.Reset();
				subtotrn=0;
				blockrn=0;
				lgprintf("�����ļ�,���:%d...",dtfid);
				
			} // end of IF blockstart>2000000000)
			blockmt.Reset();
			blockrn=0;
			firstrn=thisrn;
			startrow=blockrn;
			rownum=0;
		} //end of blockrn+sbrn>maxblockrn
		wociCopyRowsTo(mt,blockmt,-1,sbstart,sbrn);
		rownum+=sbrn;
		blockrn+=sbrn;
	} // end of for(...)

	if(blockrn>0) {
			
			//�����ӿ�����
			int c;
			for( c=0;c<bcn1;c++) {
				indexptr[c]=blockmt.PtrVoid(pblockc1[c],startrow);
			}
			for(c=0;c<bcn2;c++) {
				indexptr[bcn1+c]=blockmt.PtrVoid(pblockc2[c],startrow);
			}
			if(rownum>0) {
			 wociInsertRows(indexmt,indexptr,NULL,1);
			 dispct++;
			}
			//���������
			//���������
			blocksize=dstfile.WriteMt(blockmt,COMPRESSLEVEL)-blockstart;
			int irn=wociGetMemtableRows(indexmt);
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
					fnmtptr[0]=&dtfid;
					fnmtptr[1]=fn;
					fnmtptr[2]=&subdatasetid;
					fnmtptr[3]=&dp.dstpathid[dp.usingpathid];
					fnmtptr[4]=&dp.tabid;
					fnmtptr[5]=&ip->idxid;
					fnmtptr[6]=&isfirstidx;
					fnmtptr[7]=&blockstart;
					fnmtptr[8]=&subtotrn;
					fnmtptr[9]=&datasetid;
					int procstatus=0;
					fnmtptr[10]=&procstatus;
					int compflag=COMPRESSLEVEL;
					fnmtptr[11]=&compflag;
					wociInsertRows(fnmt,fnmtptr,NULL,1);
				}
			//�����������ݶ��ձ��¼(dt_indexfilemap)
				{
					int irn1=wociGetMemtableRows(indexmt);
					idxf.WriteHeader(indexmt,dtfid);
					int idxfsize=idxf.WriteMt(indexmt,COMPRESSLEVEL);
					void *fnmtptr[20];
					fnmtptr[0]=&dtfid;
					fnmtptr[1]=idxfn;
					fnmtptr[2]=&subdatasetid;
					fnmtptr[3]=&dp.dstpathid[dp.usingpathid];
					fnmtptr[4]=&dp.tabid;
					fnmtptr[5]=&ip->idxid;
					fnmtptr[6]=&isfirstidx;
					fnmtptr[7]=&idxfsize;
					fnmtptr[8]=&irn1;
					fnmtptr[9]=&datasetid;
					int compflag=COMPRESSLEVEL;
					fnmtptr[10]=&compflag;
					wociInsertRows(fnidxmt,fnmtptr,NULL,1);
				}
			//
				
			//
			dstfile.SetFileHeader(subtotrn,NULL);
			idxf.SetFileHeader(wociGetMemtableRows(indexmt),NULL);
			indexmt.Reset();
			blockmt.Reset();
			blockrn=0;
			startrow=blockrn;
			rownum=0;
	}

	lgprintf("ɾ�������Ӽ����Ϊ%d�����ݺ�������¼...",subdatasetid);
	{
	AutoMt dfnmt(sp->GetDTS(),100);
	dfnmt.FetchAll("select * from dt_datafilemap where subdatasetid=%d",subdatasetid);
	int dfn=dfnmt.Wait();
	if(dfn>0) {
	  AutoStmt st(sp->GetDTS());
	  for(int di=0;di<dfn;di++)
	  {
		  lgprintf("ɾ��'%s'�͸��ӵ�depcp,dep5�ļ�",dfnmt.PtrStr("filename",di));
		  unlink(dfnmt.PtrStr("filename",di));
		  char tmp[300];
		  sprintf(tmp,"%s.depcp",dfnmt.PtrStr("filename",di));
		  unlink(tmp);
		  sprintf(tmp,"%s.dep5",dfnmt.PtrStr("filename",di));
		  unlink(tmp);
	  }
	  st.Prepare(" delete from dt_datafilemap where subdatasetid=%d",subdatasetid);
	  st.Execute(1);
	  st.Wait();
	}
	
	dfnmt.FetchAll("select * from dt_indexfilemap where subdatasetid=%d",subdatasetid);
	dfn=dfnmt.Wait();
	if(dfn>0) {
	  AutoStmt st(sp->GetDTS());
	  for(int di=0;di<dfn;di++)
	  {
		  lgprintf("ɾ��'%s'�͸��ӵ�depcp,dep5�ļ�",dfnmt.PtrStr("filename",di));
		  unlink(dfnmt.PtrStr("filename",di));
		  char tmp[300];
		  sprintf(tmp,"%s.depcp",dfnmt.PtrStr("filename",di));
		  unlink(tmp);
		  sprintf(tmp,"%s.dep5",dfnmt.PtrStr("filename",di));
		  unlink(tmp);
	  }
	  st.Prepare(" delete from dt_indexfilemap where subdatasetid=%d",subdatasetid);
	  st.Execute(1);
	  st.Wait();
	}
	}
	//Ŀ������������ļ����,�����Ӽ������ͬ�ļ�¼ɾ��,�������ļ�����(???),��Ҫ�ֹ�ɾ��.
	while(true){
	 AutoMt ckmt(sp->GetDTS(),300);
	 ckmt.FetchAll("select * from dt_table where tabid=%d",tabid);
	 if(ckmt.Wait()<1) 
	 	ThrowWith("dt_table��û��tabid=%d�ļ�¼��",tabid);
	 if(ckmt.GetInt("recordnum",0)>0 ) {
	 	errprintf("���Ϊ%d�ı�:%s���ǿձ��������ݴ�����ǰ��Ҫ��գ�",
	 	    tabid,ckmt.PtrStr("tabname",0));
	 	printf("\n���ھ���ձ�?ɾ��(Y)/����(A)/����(����):");
		char ans[100];
		gets(ans);
		if(tolower(ans[0])=='y') {
			DestLoader dl(sp);
			dl.RemoveTable(ckmt.PtrStr("databasename",0),
			               ckmt.PtrStr("tabname",0),conn,false);
		}
		else if(tolower(ans[0])=='a')
			ThrowWith("��������ʱ�û�ѡ����ֹ������Ŀ���%d�ǿ�,����%d.",tabid,taskid);
	 }
	 else break;
	 //sprintf(sqlbf,"update dt.dt_table set recordnum=0 where tabid=%d",tabid);
	 //lgprintf("��¼������...");
	 //conn.DoQuery(sqlbf);
	 //lgprintf("Դ������...");
	 //lgprintf("Flush MySQL...");
	 //conn.FlushTables();//.DoQuery("flush tables");
	}

	wociAppendToDbTable(fnmt,"dt_datafilemap",sp->GetDTS());
	wociAppendToDbTable(fnidxmt,"dt_indexfilemap",sp->GetDTS());
	lgprintf("�޸��м��ļ��Ĵ���״̬(subdatasetid:%d,%d���ļ�)��2-->3",subdatasetid,rn);
	sprintf(sqlbf,"update dt.dt_middledatafile set procstate=3 where subdatasetid=%d",subdatasetid);
	conn.DoQuery(sqlbf);
	}	
	catch (...) {
		int frn=wociGetMemtableRows(fnmt);
		int fidxrn=wociGetMemtableRows(fnidxmt);
		errprintf("������������쳣��subdatasetid:%d,taskid:%d.",subdatasetid ,taskid);
		errprintf("�ָ��м��ļ��Ĵ���״̬(subdatasetid:%d,%d���ļ�)��2-->1",subdatasetid,rn);
		sprintf(sqlbf,"update dt.dt_middledatafile set procstate=1 where subdatasetid=%d ",subdatasetid);
		conn.DoQuery(sqlbf);
		errprintf("ɾ������������ݺ������ļ�.");
		int i;
		errprintf("ɾ�������ļ�...");
		for(i=0;i<frn;i++) {
			errprintf("\t %s \t %s",fnmt.PtrStr("filename",i),
			   fnmt.PtrStr("indexfilename",i));
		}
		for(i=0;i<frn;i++) {
			unlink(fnmt.PtrStr("filename",i));
		}
		errprintf("ɾ�������ļ�...");
		for(i=0;i<fidxrn;i++) {
			errprintf("\t %s \t %s",fnidxmt.PtrStr("filename",i),
			   fnidxmt.PtrStr("indexfilename",i));
		}
		for(i=0;i<fidxrn;i++) {
			unlink(fnidxmt.PtrStr("filename",i));
		}
		errprintf("ɾ���Ѵ��������ļ��������ļ���¼...");
		AutoStmt st(sp->GetDTS());
		st.Prepare("delete from dt_datafilemap where subdatasetid=%d",subdatasetid);
		st.Execute(1);
		st.Wait();
		st.Prepare("delete from dt_indexfilemap where subdatasetid=%d ",subdatasetid);
		st.Execute(1);
		st.Wait();
		throw;
	}

	lgprintf("���ݴ���(MiddleDataLoading)����,���������ݰ�%d��.",dispct);
	lgprintf("����%d�������ļ�,�Ѳ���dt_datafilemap��.",wociGetMemtableRows(fnmt));
	//wociMTPrint(fnmt,0,NULL);
	lgprintf("����%d�������ļ�,�Ѳ���dt_indexfilemap��.",wociGetMemtableRows(fnidxmt));
	//wociMTPrint(fnidxmt,0,NULL);
	//����Ƿ����һ������
	try
	{
	//mdf.FetchAll("select * from dt_taskschedule where taskid=%d and taskstatus>=2",
	//	taskid);
	//int rn=mdf.Wait();
	//if(rn>=1) {
		//��Զ������������񣬼���������������ݣ����������Ӽ����Ƿ�������ϡ�
		//  ����ǵ������������������²�ѯ���Ƿ��ؿռ�¼����
		mdf.FetchAll("select * from dt_middledatafile where procstate!=3 and taskid=%d",
				taskid);
		int rn=mdf.Wait();
		if(rn==0) {
			lgprintf("���һ�������Ѵ�����,����״̬2-->3,taskid:%d",taskid);
			//����ǵ������������񣬱�����������ͬ���ݼ�������״̬Ϊ3������������һ���Ĳ���������װ�룩��
			sprintf(sqlbf,"update dt.dt_taskschedule set taskstatus=3 where taskid=%d",
				taskid);
			conn.DoQuery(sqlbf);
		}
		//���´�����ṹ��
		sp->CreateDT(tabid);
	//}
	lgprintf("ɾ���м���ʱ�ļ�...");
	mdf.FetchAll("select * from dt_middledatafile where subdatasetid=%d and taskid=%d",subdatasetid,taskid);
	{
	  int dfn=mdf.Wait();
	  for(int di=0;di<dfn;di++) {
		lgprintf("ɾ���ļ�'%s'",mdf.PtrStr("datafilename",di));
		unlink(mdf.PtrStr("datafilename",di));
		lgprintf("ɾ���ļ�'%s'",mdf.PtrStr("indexfilename",di));
		unlink(mdf.PtrStr("indexfilename",di));
	 } 
	 lgprintf("ɾ����¼...");
  	 AutoStmt st(sp->GetDTS());
	 st.Prepare("delete from dt_middledatafile where subdatasetid=%d and taskid=%d",subdatasetid,taskid);
	 st.Execute(1);
	 st.Wait();
	}
	
	}
	catch(...) {
		errprintf("����������������ɣ�������״̬��������ʱ�м��ļ�ɾ��ʱ���ִ�����Ҫ�˹�������\ntaskid:%d,subdatasetid:%d��",
		taskid,subdatasetid);
		throw;
	}
	return 1;
	//Load index data into memory table (indexmt)
  }

bool SysAdmin::EmptyIndex(const char *databasename,int tabid,MySQLConn &conn)
{
	AutoMt indexmt(dts,100);
	indexmt.FetchAll("select * from dt_index where tabid=%d and issoledindex=1",tabid);
	int rn=indexmt.Wait();
	if(rn<1) return false;
	char sqlbf[300];
	for(int i=0;i<rn;i++) {
		lgprintf("���������%s.%s...",databasename,indexmt.PtrStr("indextabname",i));
		sprintf(sqlbf,"truncate table %s.%s",databasename,indexmt.PtrStr("indextabname",i));
		conn.DoQuery(sqlbf);
	}
	return true;
}

int SysAdmin::GetSrcTableStructMt(int srctabp, int tabp, int srcsys)
{
		AutoStmt srcst(srcsys);
		srcst.Prepare("select * from %s.%s",dt_srctable.PtrStr("srcschedulename",srctabp),
			dt_srctable.PtrStr("srctabname",srctabp));
		int mt=wociCreateMemTable();
		wociBuildStmt(mt,srcst,10);
		return mt;
}

bool SysAdmin::CreateTableOnMysql(const char *cdpathval,MySQLConn &conn, int srcmt, const char *tabname, int tabid, int indexid,int targettype,bool forcecreate)
{
		//���Ŀ����Ѵ��ڣ���ɾ��
	    char sqlbf[3000];
		bool exist=conn.TouchTable(tabname);
		if(exist && !forcecreate) return false;
		if(exist) {
			printf("table %s has exist,dropped.\n",tabname);
			sprintf(sqlbf,"drop table %s",tabname);
			conn.DoQuery(sqlbf);
		}
		//else 
		{
			//����Ŀ��꼰���ṹ�������ļ�
			wociGetCreateTableSQL(srcmt,sqlbf,tabname,true);
			conn.DoQuery(sqlbf);
			int fid=NextFileID();
			char fn[300];
			sprintf(fn,"%sdt_%s_%d_%d.cdsc",cdpathval,tabname,tabid,fid);
			FILE *fp=fopen(fn,"w+b");
			if(fp==NULL) ThrowWith("Create file '%s' failed\n",fn);
			void *pcd;
			int cdlen,cdnum;
			wociGetColumnDesc(srcmt,&pcd,cdlen,cdnum);
			fwrite(&cdlen,1,sizeof(int),fp);
			fwrite(&cdnum,1,sizeof(int),fp);
			fwrite(pcd,1,cdlen,fp);
			fclose(fp);
			//�޸ı�ṹ�����ļ�������
			if(targettype==0)
			sprintf(sqlbf,"update dt.dt_table set cdfileid=%d,recordlen=%d where tabid=%d",
				fid,wociGetRowLen(srcmt),tabid);
			else 
			sprintf(sqlbf,"update dt.dt_index set cdfileid=%d where indexid=%d",
				fid,indexid);
			conn.DoQuery(sqlbf);
			char tmp[300];
			mysql_escape_string(tmp,fn,strlen(fn));
			AutoMt oldval(dts,100);
			oldval.FetchAll("select * from dt_cdfilemap where tabid=%d and targettype=%d and indexid=%d",
				tabid,targettype,indexid);
			int rn=oldval.Wait();
			if(rn>0) {
				AutoMt chkmt(dts,100);
				for(int i=0;i<rn;i++) {
					int oldfid=oldval.GetInt("fileid",i);
					int ckrn=0;
					if(targettype==0) {
					    chkmt.FetchAll("select * from dt_table where cdfileid=%d",
							oldfid);
						ckrn=chkmt.Wait();
					}
					else
					{
					    chkmt.FetchAll("select * from dt_index where cdfileid=%d",
							oldfid);
						ckrn=chkmt.Wait();
					}
					if(ckrn==0) {
						lgprintf("ɾ��ԭ���ֶθ�ʽ��¼���ļ�'%s'",oldval.PtrStr("filename",i));
						unlink(oldval.PtrStr("filename",i));
					 AutoStmt st(dts);
					 st.Prepare("delete from dt_cdfilemap where fileid=%d",oldfid);
					 st.Execute(1);
					 st.Wait();
					}
				}
			}
			sprintf(sqlbf,"insert into dt.dt_cdfilemap values (%d,'%s',0,%d,%d,%d,%d,%d)",
				fid,tmp,targettype,tabid,indexid,sizeof(int)*2+cdlen,cdnum);
			conn.DoQuery(sqlbf);
		}
		return true;
}

//���������¸����ֶ�ֵ(dt_index.reusecols)
bool SysAdmin::CreateIndexTable(MySQLConn &conn,int srcmt,int tabid,int indexid, const char *tabname, const char *solecolsname,const char *pathval, bool forcecreate)
{
		//�����������Ŀ��������������
		AutoMt idxtarget(dts,100);
		//ÿһ��������������Ҫ����������
		wociClear(idxtarget);
		wociCopyColumnDefine(idxtarget,srcmt,solecolsname);
		//���Ҹö������������ķǶ������������Դ�Ϊ���ݽ���������
		AutoMt idxsubmt(dts,100);
		idxsubmt.FetchAll("select * from dt_index where reuseindexid=%d order by indexidinidxtab",
					indexid);
		int srn=idxsubmt.Wait();
		char colsname[300];
		for(int j=0;j<srn;j++) {
			strcpy(colsname,idxsubmt.PtrStr("columnsname",j));
			//�ظ����ֶ��Զ��޳�
			wociCopyColumnDefine(idxtarget,srcmt,colsname);
		}
		//�ع��������ö�
		char reusedcols[300];
		int pcol[20];
		int cn1;
		cn1=wociConvertColStrToInt(srcmt,solecolsname,pcol);
		reusedcols[0]=0;
		int tcn=wociGetMtColumnsNum(idxtarget);
		if(tcn>cn1) {
			for(int i=cn1;i<tcn;i++) {
				if(i!=cn1) strcat(reusedcols,",");
				wociGetColumnName(idxtarget,i,reusedcols+strlen(reusedcols));
			}
		}
		{
			AutoStmt st(dts);
			if(strlen(reusedcols)>0)
			 st.Prepare("update dt_index set reusecols='%s' where indexid=%d",
				reusedcols,indexid);
			else
			 st.Prepare("update dt_index set reusecols=null where indexid=%d",
				indexid);
			st.Execute(1);
			st.Wait();
		}
		//���������ֶ�
		wociAddColumn(idxtarget,"dtfid",NULL,COLUMN_TYPE_INT,10,0);
		wociAddColumn(idxtarget,"blockstart",NULL,COLUMN_TYPE_INT,10,0);
		wociAddColumn(idxtarget,"blocksize",NULL,COLUMN_TYPE_INT,10,0);
		wociAddColumn(idxtarget,"blockrownum",NULL,COLUMN_TYPE_INT,10,0);
		wociAddColumn(idxtarget,"startrow",NULL,COLUMN_TYPE_INT,10,0);
		wociAddColumn(idxtarget,"rownum",NULL,COLUMN_TYPE_INT,10,0);
		//����������Ѵ��ڣ���ɾ��
		char sqlbf[3000];
		bool exist=conn.TouchTable(tabname);
		if(exist && !forcecreate) return false;
		if( exist ) {
			printf("Index table '%s' has exist,dropped!\n",tabname);
			sprintf(sqlbf,"drop table %s",tabname);
			conn.DoQuery(sqlbf);
		}
		CreateTableOnMysql(pathval,conn,idxtarget,tabname,tabid,indexid,1,true);
		{
				char emptytabname[300];
				sprintf(emptytabname,"template.pri_%s",tabname);
				if(conn.TouchTable(emptytabname)) {
					printf("Empty Temp table '%s' has exist,skip create.\n",emptytabname);
					sprintf(sqlbf,"drop table %s",emptytabname);
					conn.DoQuery(sqlbf);
				}
				//else
				{
					wociGetCreateTableSQL(idxtarget,sqlbf,emptytabname,true);
					conn.DoQuery(sqlbf);
					printf(sqlbf);
					printf("\n");
				}
		}
		return true;
}

//createtype: 0--- datatable and indextable 1--datatable 2--indextable
bool SysAdmin::CreateIndex(char *dttabname, char *idxtabname, int createtype, MySQLConn &conn, int tabid, int indexid, bool forcecreate)
{
			AutoMt mt(dts,10);
			mt.FetchAll("select * from dt_index where indexid=%d and issoledindex=1",indexid);
			int rn=mt.Wait();
			if(rn<1)
				ThrowWith("Indexid is invalid or not a soled :%d",indexid);

			char colsname[1000];
			//char idxname[300];
			strcpy(colsname,mt.PtrStr("columnsname",0));
			if(createtype==0 || createtype==2) {
			  CreateIndex(idxtabname,conn,mt.GetInt("indexidinidxtab",0),colsname,forcecreate);
			}
			if(createtype==0 || createtype==1) {
				CreateIndex(dttabname,conn,mt.GetInt("indexidindattab",0),colsname,forcecreate);
			}
			
			//���Ҹö������������ķǶ������������Դ�Ϊ���ݽ���������
			AutoMt idxsubmt(dts,100);
			idxsubmt.FetchAll("select * from dt_index where reuseindexid=%d order by indexidinidxtab",
						indexid);
			int srn=idxsubmt.Wait();
			for(int j=0;j<srn;j++) {
				strcpy(colsname,idxsubmt.PtrStr("columnsname",j));
				CreateIndex(idxtabname,conn,idxsubmt.GetInt("indexidinidxtab",j),colsname,forcecreate);
				CreateIndex(dttabname,conn,idxsubmt.GetInt("indexidindattab",j),colsname,forcecreate);
			}
			return true;
}

void SysAdmin::CreateIndex(char *dtname, MySQLConn &conn, int id, char *colsname, bool forcecreate)
{
	char sqlbf[3000];	
	if(forcecreate) {
		sprintf(sqlbf,"drop index %s_%d on %s",dtname,
			id,dtname);
		conn.DoQuery(sqlbf);
	}
	sprintf(sqlbf,"create index %s_%d on %s(%s)",
		dtname,id,
		dtname,colsname);
	conn.DoQuery(sqlbf);
}

#define revint(v) ((v>>24)|(v<<24)|(v>>8&0xff00)|(v<<8&0xff0000))

int DestLoader::Load(MySQLConn &conn) {
	//Check deserved temporary(middle) fileset
	wociSetTraceFile("DLoad");

	AutoMt mdf(psa->GetDTS(),10);
	mdf.FetchAll("select * from dt_taskschedule where taskstatus=3 and rownum<2");
	int rn=mdf.Wait();
	if(rn<1) {
		printf("û�з��ִ�����ɵȴ�װ�������(����״̬=3).\n");
		return 0;
	}
	char sqlbf[1000];
	srctabid=mdf.GetInt("srctabid",0);
	taskid=mdf.GetInt("taskid",0);
	datasetid=mdf.GetInt("datasetid",0);
	mdf.FetchAll("select * from dt_taskschedule where taskstatus!=3 and datasetid=%d and rownum<2",datasetid);
	rn=mdf.Wait();
	if(rn>0) {
		printf("����װ��ʱ�����ݼ�%d�����������У�һЩ����״̬����'������'(3),���������%d(״̬%d).\n",datasetid,
		  mdf.GetInt("taskid",0),mdf.GetInt("taskstatus",0));
		return 0;
	}
	mdf.FetchAll("select * from dt_datafilemap where datasetid=%d and procstatus=0 and rownum<2",datasetid);
	if(mdf.Wait()<1) 
		ThrowWith("������������%dָʾ����������������Ҳ�����Ӧ���ݼ�%d�����ݼ�¼��\n�����������ļ���¼�����ڻ�״̬�ǿ���(0).\n",taskid,datasetid);
	indexid=mdf.GetInt("indexid",0);
	//mdf.FetchAll("select * from dt_datafilemap where datasetid=%d and indexid=%d ",datasetid,indexid);
	subdatasetid=mdf.GetInt("subdatasetid",0);
	tabid=mdf.GetInt("tabid",0);
	//partid=mdf.GetInt("partid",0);
	dumpparam dpsrc;
	psa->GetSoledInexParam(srctabid,&dpsrc);
	psa->GetSoledInexParam(srctabid,&dp,tabid);
	
	AutoMt idxmt(psa->GetDTS(),10);
	idxmt.FetchAll("select * from dt_index where tabid=%d and issoledindex=1",tabid);
	int idxrn=idxmt.Wait();
	for(int i=0;i<idxrn;i++) {
		indexid=idxmt.GetInt("indexid",i);
		mdf.FetchAll("select * from dt_indexfilemap where datasetid=%d and indexid=%d order by subdatasetid,fileid",
		 datasetid,indexid);
		rn=mdf.Wait();
		AutoMt datmt(psa->GetDTS(),100);
		datmt.FetchAll("select * from dt_datafilemap where datasetid=%d and tabid=%d and indexid=%d and procstatus=0 order by subdatasetid,fileid",datasetid,tabid,
			indexid);
		int datrn=datmt.Wait();
		//��ֹ���������������ʱ���ظ�ִ��
		if(datrn<1) continue;
		int off=dp.GetOffset(indexid);
		char fn[300];
		const char *pathval=psa->GetPathName(0,"msys");		
		try {
			//Ϊ��ֹ��������,�����ļ�״̬�޸�.
		  lgprintf("�޸������ļ��Ĵ���״̬(datasetid:%d,indexid:%d,%d���ļ�)��0-->1",datasetid,indexid,datrn);
		  sprintf(sqlbf,"update dt.dt_datafilemap set procstatus=1 where datasetid=%d and procstatus=0 and indexid=%d",
			        datasetid,indexid);
		  if(conn.DoQuery(sqlbf)!=rn) 
				ThrowWith("�޸������ļ��Ĵ���״̬�쳣�����������������̳�ͻ��\n"
					"   taskid:%d,tabid:%d,datasetid:%d,srctabid:%d,indexid:%d.\n",
					taskid,tabid,datasetid,srctabid,indexid);
		  for(int k=0;k<datrn;k++) {
			//Build table data file link information.
			if(k+1==datrn) {
				dt_file df;
				df.Open(datmt.PtrStr("filename",k),2,datmt.GetInt("fileid",k));
				df.SetFileHeader(0,NULL);
			}
			else {
				dt_file df;
				df.Open(datmt.PtrStr("filename",k),2,datmt.GetInt("fileid",k));
				df.SetFileHeader(0,datmt.PtrStr("filename",k+1));
			}
		  }
	    	  lgprintf("��ʼ����װ��(DestLoading),�ļ���:%d,tabid:%d,datasetid:%d,taskid:%d ,indexid:%d ...",
		  	rn,tabid,datasetid,taskid,indexid);
#ifdef WIN32
		  sprintf(fn,"%stmpidx\\pri_%s_%d.MYD",pathval,dp.idxp[off].idxtbname,datasetid);
#else
		  sprintf(fn,"%stmpidx/pri_%s_%d.MYD",pathval,dp.idxp[off].idxtbname,datasetid);
#endif		
		  //struct _finddata_t ft;
		  FILE *fp =NULL;
		  fp=fopen(fn,"a+b");
		  if(fp==NULL) 
			ThrowWith("Open file %s for writing failed!",fn);
		  int totidxrn=0;
		  //lgprintf("�����ļ���%s",mdf.PtrStr("filename",0));
		  for(int i=0;i<rn;i++) {
			//Build index data file link information.
			if(i+1==rn) {
				dt_file df;
				df.Open(mdf.PtrStr("filename",i),2,mdf.GetInt("fileid",i));
				df.SetFileHeader(0,NULL);
				//lgprintf(
			}
			else {
				dt_file df;
				df.Open(mdf.PtrStr("filename",i),2,mdf.GetInt("fileid",i));
				df.SetFileHeader(0,mdf.PtrStr("filename",i+1));
			}
			file_mt idxf;
			idxf.Open(mdf.PtrStr("filename",i),0);
			int mt=idxf.ReadBlock(0,0);
			while(mt) {
				wociCopyToMySQL(mt,0,0,fp);
				totidxrn+=wociGetMemtableRows(mt);
				mt=idxf.ReadBlock(-1,0);
			}
		  }
		  fclose(fp);
		  fp=fopen(fn,"rb");
		  fseek(fp,0,SEEK_END);
		  long fsz=ftell(fp);//_filelength(_fileno(fp));
		  fclose(fp);
		  char srcfn[300];
#ifdef WIN32
		  sprintf(srcfn,"%stemplate\\pri_%s.frm",pathval,dpsrc.idxp[off].idxtbname);
		  sprintf(fn,"%stmpidx\\pri_%s_%d.frm",pathval,dp.idxp[off].idxtbname,datasetid);
		  if(!CopyFile(srcfn,fn,FALSE))
#else
		  char cmdl[300];
		  sprintf(srcfn,"%stemplate/pri_%s.frm",pathval,dpsrc.idxp[off].idxtbname);
		  sprintf(fn,"%stmpidx/pri_%s_%d.frm",pathval,dp.idxp[off].idxtbname,datasetid);
		  if(uCopyFile(srcfn,fn)!=1)
#endif
		  ThrowWith("Copy file '%s' to '%s' failed!\n",srcfn,fn);

#ifdef WIN32
		  sprintf(srcfn,"%stemplate\\pri_%s.MYI",pathval,dpsrc.idxp[off].idxtbname);
		  sprintf(fn,"%stmpidx\\pri_%s_%d.MYI",pathval,dp.idxp[off].idxtbname,datasetid);
		  if(!CopyFile(srcfn,fn,FALSE))
#else
		  sprintf(srcfn,"%stemplate/pri_%s.MYI",pathval,dpsrc.idxp[off].idxtbname);
		  sprintf(fn,"%stmpidx/pri_%s_%d.MYI",pathval,dp.idxp[off].idxtbname,datasetid);
		  if(uCopyFile(srcfn,fn)!=1)
#endif
			 ThrowWith("Copy file '%s' to '%s' failed!\n",srcfn,fn);
		  totidxrn=revint(totidxrn);
		  fsz=revint(fsz);
		  int bk=0;
		  fp=fopen(fn,"r+b");
		  fseek(fp,28,SEEK_SET);
		  fwrite(&bk,1,4,fp);
		  fwrite(&totidxrn,1,4,fp);
		  fseek(fp,68,SEEK_SET);
		  fwrite(&bk,1,4,fp);
		  fwrite(&fsz,1,4,fp);
		  fseek(fp,0,SEEK_END);
		  fclose(fp); 
		}
		catch(...) {
		  lgprintf("�ָ������ļ��Ĵ���״̬(datasetid:%d,indexid:%d,%d���ļ�)��1-->0",datasetid,indexid,datrn);
		  sprintf(sqlbf,"update dt.dt_datafilemap set procstatus=0 where datasetid=%d and indexid=%d",
			        datasetid,indexid);
		  conn.DoQuery(sqlbf);
#ifdef WIN32
		  sprintf(fn,"%stmpidx\\pri_%s_%d.MYI",pathval,dp.idxp[off].idxtbname,datasetid);
		  unlink(fn);
		  sprintf(fn,"%stmpidx\\pri_%s_%d.MYD",pathval,dp.idxp[off].idxtbname,datasetid);
		  unlink(fn);
		  sprintf(fn,"%stmpidx\\pri_%s_%d.frm",pathval,dp.idxp[off].idxtbname,datasetid);
		  unlink(fn);
#else
		  sprintf(fn,"%stmpidx/pri_%s_%d.MYI",pathval,dp.idxp[off].idxtbname,datasetid);
		  unlink(fn);
		  sprintf(fn,"%stmpidx/pri_%s_%d.MYD",pathval,dp.idxp[off].idxtbname,datasetid);
		  unlink(fn);
		  sprintf(fn,"%stmpidx/pri_%s_%d.frm",pathval,dp.idxp[off].idxtbname,datasetid);
		  unlink(fn);
#endif
                  throw;
		}
	}
	lgprintf("����װ��(DestLoading)���� ...");
	AutoStmt updst(psa->GetDTS());
	updst.Prepare("update dt_taskschedule set taskstatus=4 where datasetid=%d",
		datasetid);
	updst.Execute(1);
	updst.Wait();
	lgprintf("����״̬����,3(MLoaded)--->4(DLoaded),�����:%d,���ݼ����:%d",taskid,datasetid);
	return 1;
}


int DestLoader::MoveTable(const char *srcdbn,const char *srctabname,const char * dstdbn,const char *dsttabname,MySQLConn &conn)
{
	lgprintf("Move table '%s -> '%s'.",srctabname,dsttabname);
	AutoMt mt(psa->GetDTS(),100);
	mt.FetchAll("select pathval from dt_path where pathtype='msys'");
	int rn=mt.Wait();
	if(rn<1) 
		ThrowWith("�Ҳ���MySQL����Ŀ¼(dt_path.pathtype='msys'),����ת���쳣��ֹ.");
	char dtpath[300];
	strcpy(dtpath,mt.PtrStr(0,0));

	mt.FetchAll("select * from dt_table where tabname=lower('%s') and databasename=lower('%s')",dsttabname,dstdbn);
	int ntabid=-1;
	rn=mt.Wait();
	if(rn>0) {
		//if(mt.GetInt("srctabid",0)>0) {
		//	lgprintf("�������ѱ�ռ�ã����Ҳ��ܱ����ǣ�Ǩ��Ŀ���srctabid>0����");
		//	return 0;
		//}
		printf("��'%s'�Ѵ���(��¼��:%d)��ɾ��(Y/N)?",dsttabname,mt.GetInt("recordnum",0));
		char ans[100];
		gets(ans);
		if(tolower(ans[0])!='y') {
			lgprintf("ȡ������ת�ơ� ");
			return 0;
		}
		RemoveTable(dstdbn,dsttabname,conn,false);
		if(mt.GetInt("srctabid",0)>0) 
			ntabid=mt.GetInt("tabid",0);
	}
	int dsttabid=ntabid;
	if(ntabid==-1) {
		mt.FetchAll("select max(tabid) from dt_table");
		mt.Wait();
		dsttabid=*mt.PtrInt(0,0)+1;
	}
	mt.FetchAll("select * from dt_table where tabname=lower('%s') and databasename=lower('%s')",srctabname,srcdbn);
	rn=mt.Wait();
	if(rn<1) {
		lgprintf("Դ��'%s'������.",srctabname);
		return 0;
	}
	tabid=mt.GetInt("tabid",0);
	datasetid=mt.GetInt("datasetid",0);
	int recordnum=mt.GetInt("recordnum",0);
	int firstdatafileid=mt.GetInt("firstdatafileid",0);
	double totalbytes=mt.GetDouble("totalbytes",0);
	int datafilenum=mt.GetInt("datafilenum",0);
	//if(recordnum<1) {
	//	lgprintf("Դ��'%s'����Ϊ�գ�����ת��ʧ�ܡ�",srctabname);
	//	return 0;
	//}
	lgprintf("Դ��'%s.%s' id:%d,datasetid:%d,recordnum:%d,first data file id :%d",
		srcdbn,srctabname,tabid,datasetid,recordnum,firstdatafileid);
	int idxstart=0;
	//�±����ڣ�����dt_table���½�һ����¼
	if(ntabid==-1) {
		*mt.PtrInt(0,0)=dsttabid;
		strcat(mt.PtrStr("tabdesc",0),"_r");
		strcpy(mt.PtrStr("tabname",0),dsttabname);
		strcpy(mt.PtrStr("databasename",0),dstdbn);
		if(mt.GetInt("srctabid",0)>0) 
			*mt.PtrInt("srctabid",0)=-*mt.PtrInt("srctabid",0);
		wociAppendToDbTable(mt,"dt_table",psa->GetDTS());
		mt.FetchAll("select max(indexid) from dt_index");
		mt.Wait();
		idxstart=*mt.PtrInt(0,0)+1;
		CopyMySQLTable(dtpath,srcdbn,srctabname,dstdbn,dsttabname);
	}
	//��ʱ�ر�Դ���Ŀ�������ݷ��ʣ���¼���Ѵ��ڱ��ر���recordnum��
	char sqlbuf[1000];
	lgprintf("�ر�Դ���Ŀ�����¼����%d",recordnum);
	sprintf(sqlbuf,"update dt.dt_table set recordnum=0 where tabid in (%d,%d)",
		tabid,dsttabid);
	conn.DoQuery(sqlbuf);
	lgprintf("MySQLˢ��...");
	conn.FlushTables();//.DoQuery("flush tables ");
	//�����Ŀ�����ģʽ��Ŀ�������Դ��Ľṹ���������ݺ�����������һ�£�����
	//  ���������Ԥ�ϡ�
	mt.FetchAll("select * from dt_index where tabid=%d order by indexid ",tabid);
	rn=mt.Wait();
	AutoMt idxdmt(psa->GetDTS(),100);
	if(ntabid!=-1) {
		idxdmt.FetchAll("select * from dt_index where tabid=%d order by indexid",ntabid);
		if(idxdmt.Wait()!=rn) 
			ThrowWith("Ŀ������������Դ��ͬ���޷�ִ������ת�Ʋ���!");
	}
	//����������¼���������޸������ļ��������ļ���tabid,indexid ָ��
	for(int i=0;i<rn;i++) {
		int oldidxid=*mt.PtrInt(0,i);
		if(ntabid==-1) {
			*mt.PtrInt(0,i)=idxstart;// indexid
			*mt.PtrInt(1,i)=dsttabid; // tabid
			lgprintf("������������� :%d",idxstart);
		}
		else
			idxstart=*idxdmt.PtrInt(0,i);
		if(mt.GetInt("issoledindex",i)==1) {
			char tmpfn[300];
			strcpy(tmpfn,mt.PtrStr("indextabname",i));
			const char *pdfn;
			if(ntabid==-1) {
			 sprintf(mt.PtrStr("indextabname",i),"%sidx%d",dsttabname,idxstart);
			 pdfn=mt.PtrStr("indextabname",i);
			}
			else pdfn=idxdmt.PtrStr("indextabname",i);
			lgprintf("������ '%s.%s'-->'%s.%s...'",srcdbn,tmpfn,dstdbn,pdfn);
			CopyMySQLTable(dtpath,srcdbn,tmpfn,dstdbn,pdfn);
			sprintf(sqlbuf,"update dt.dt_datafilemap set tabid=%d,indexid=%d where tabid=%d and indexid=%d and datasetid=%d",
				dsttabid,idxstart,tabid,oldidxid,datasetid);
			int un=conn.DoQuery(sqlbuf);
			if(un<1 && recordnum>0) 
				ThrowWith("�Ҳ��������ļ���¼������%d,������%d,���ݼ���:%d,�����ƶ��쳣��ֹ��",
					tabid,oldidxid,datasetid);
			sprintf(sqlbuf,"update dt.dt_indexfilemap set tabid=%d,indexid=%d where tabid=%d and indexid=%d and datasetid=%d",
				dsttabid,idxstart,tabid,oldidxid,datasetid);
			if(un<1 && recordnum>0) 
				ThrowWith("�Ҳ��������ļ���¼������%d,������%d,���ݼ���:%d,�����ƶ��쳣��ֹ��",
					tabid,oldidxid,datasetid);
			un=conn.DoQuery(sqlbuf);
			if(ntabid==-1) {
				for(int j=0;j<rn;j++) {
					if(mt.GetInt("reuseindexid",j)==oldidxid) {
						*mt.PtrInt("reuseindexid",j)=idxstart;
						strcpy(mt.PtrStr("indextabname",j),mt.PtrStr("indextabname",i));
						lgprintf("�������%d���ñ��%d.",mt.GetInt("indexid",j),idxstart);
					}
				}
			}
		}
		idxstart++;
	}
	if(ntabid==-1) 
		wociAppendToDbTable(mt,"dt_index",psa->GetDTS());// waitting not needed.
		
	sprintf(sqlbuf,"update dt.dt_table set datasetid=%d,recordnum=%d,firstdatafileid=%d,totalbytes=%15.0f,datafilenum=%d where tabid=%d",
		datasetid,recordnum,firstdatafileid,totalbytes,datafilenum,dsttabid);
	conn.DoQuery(sqlbuf);
	// reset firstdatafileid of srctable?
	//sprintf(sqlbuf,"update dt.dt_table set datasetid=0,firstdatafileid=0 where tabid=%d",
	//	tabid);
	//conn.DoQuery(sqlbuf);
	lgprintf("MySQLˢ��...");
	conn.FlushTables();//.DoQuery("flush tables ");
	lgprintf("ɾ��Դ��..");
	RemoveTable(srcdbn,srctabname,conn,false);
	lgprintf("�����Ѵӱ�'%s'ת�Ƶ�'%s'��",srctabname,dsttabname);
	return 1;
}

int DestLoader::ReLoad(MySQLConn &conn) {
	wociSetTraceFile("ReLoad");

	AutoMt mdf(psa->GetDTS(),10);
	mdf.FetchAll("select * from dt_taskschedule where taskstatus in (7,10) and rownum<2");
	int rn=mdf.Wait();
	if(rn<1) {
		printf("û�з�������ѹ����ɵȴ�װ�������(Recompressed,taskstatus in(7,10).\n");
		return 0;
	}
	bool dpcp=mdf.GetInt("taskstatus",0)==7;
	datasetid=mdf.GetInt("datasetid",0);
	mdf.FetchAll("select * from dt_datafilemap where datasetid=%d order by indexid",datasetid);
	rn=mdf.Wait();
	if(rn<1) {
		lgprintf("װ�����ѹ������ʱ�Ҳ��������ļ���¼��");
		return 1;
	}
	tabid=mdf.GetInt("tabid",0);
	AutoMt idxmt(psa->GetDTS(),300);
	mdf.FetchAll("select * from dt_indexfilemap where datasetid=%d and tabid=%d order by subdatasetid,fileid",
	  datasetid,tabid);
	rn=mdf.Wait();
	AutoMt datmt(psa->GetDTS(),300);
	datmt.FetchAll("select * from dt_datafilemap where datasetid=%d and tabid=%d order by subdatasetid,fileid",datasetid,tabid);
	int datrn=datmt.Wait();
	AutoStmt updst(psa->GetDTS());
	char tmpfn[300];
	int k;
	unsigned long dtflen[1000];
	unsigned long idxflen[1000];
	//�ȼ��
	for(k=0;k<datrn;k++) {
		sprintf(tmpfn,"%s.%s",datmt.PtrStr("filename",k),dpcp?"depcp":"dep5");
		dt_file df;
		df.Open(tmpfn,0);
		dtflen[k]=df.GetFileSize();
		if(dtflen[k]<1) 
			ThrowWith("file '%s' empty!",tmpfn);
	}
	for(k=0;k<rn;k++) {
		sprintf(tmpfn,"%s.%s",mdf.PtrStr("filename",k),dpcp?"depcp":"dep5");
		dt_file df;
		df.Open(tmpfn,0);
		idxflen[k]=df.GetFileSize();
		if(idxflen[k]<1) 
			ThrowWith("file '%s' empty!",tmpfn);
	}
	char sqlbf[200];
	sprintf(sqlbf,"update dt.dt_taskschedule set taskstatus=%d where datasetid=%d", dpcp?8:11,datasetid);
	if(conn.DoQuery(sqlbf)<1) 
		ThrowWith("����ѹ����������װ������޸�����״̬�쳣�����������������̳�ͻ��\n"
			"  tabid:%d,datasetid:%d.\n",
			tabid,datasetid);
		
	//��ֹ�������룬�޸�����״̬
	//��������ǰ�ȹرձ�
	sprintf(sqlbf,"update dt.dt_table set recordnum=0 where tabid=%d",tabid);
	conn.DoQuery(sqlbf);
	lgprintf("MySQLˢ��...");
	conn.FlushTables();//.DoQuery("flush tables");
	lgprintf("�����ѹر�.");
	//���µ������ļ��滻ԭ�����ļ�����ɾ��ԭ�ļ������ļ����Ƹ���Ϊԭ�ļ����޸��ļ���¼�е��ļ���С�ֶΡ�
	lgprintf("��ʼ���ݺ������ļ��滻...");
	for(k=0;k<datrn;k++) {
		updst.Prepare("update dt_datafilemap set filesize=%d,compflag=%d where fileid=%d",
			dtflen[k],dpcp?10:5,datmt.GetInt("fileid",k));
		updst.Execute(1);
		updst.Wait();
		const char *filename=datmt.PtrStr("filename",k);
		unlink(filename);
		//sprintf(tmpfn,"%s.%s",filename,dpcp?"dep5":"depcp");
		//rename(filename,tmpfn);
		//lgprintf("rename file '%s' as '%s'",filename,tmpfn);
		sprintf(tmpfn,"%s.%s",filename,dpcp?"depcp":"dep5");
		rename(tmpfn,filename);
		lgprintf("rename file '%s' as '%s'",tmpfn,filename);
	}
	for(int i=0;i<rn;i++) {
		updst.Prepare("update dt_indexfilemap set filesize=%d ,compflag=%d where fileid=%d",
			idxflen[i],dpcp?10:5,mdf.GetInt("fileid",i));
		updst.Execute(1);
		updst.Wait();
		const char *filename=mdf.PtrStr("filename",i);
		unlink(filename);
		//sprintf(tmpfn,"%s.%s",filename,dpcp?"dep5":"depcp");
		//rename(filename,tmpfn);
		//lgprintf("rename file '%s' as '%s'",filename,tmpfn);
		sprintf(tmpfn,"%s.%s",filename,dpcp?"depcp":"dep5");
		rename(tmpfn,filename);
		lgprintf("rename file '%s' as '%s'",tmpfn,filename);
	}
	lgprintf("���ݺ������ļ��ѳɹ��滻...");
	sprintf(sqlbf,"update dt.dt_taskschedule set taskstatus=3 where datasetid=%d", datasetid);
	conn.DoQuery(sqlbf);
	sprintf(sqlbf,"update dt.dt_datafilemap set procstatus=0 where datasetid=%d",datasetid);
	lgprintf("����״̬�޸�Ϊ�����������(3),�����ļ�����״̬��Ϊδ����(0).");
	Load(conn);
	return 1;
}


int DestLoader::RecreateIndex(SysAdmin *_Psa,MySQLConn &conn) 
{
	AutoMt mdf(psa->GetDTS(),200);
	mdf.FetchAll("select * from dt_taskschedule where taskstatus in(4,8,11) and rownum<2");
	int rn=mdf.Wait();
	if(rn<1) {
		return 0;
	}
	taskid=mdf.GetInt("taskid",0);
	srctabid=mdf.GetInt("srctabid",0);
	datasetid=mdf.GetInt("datasetid",0);
	//���Ŀ�����ת�Ʋ������������ŵ�Ŀ����ŵĶ�Ӧֻ�ܴ����ݼ�ӳ�䡣
	mdf.FetchAll("select distinct tabid from dt_table where datasetid=%d",datasetid);
	rn=mdf.Wait();
	if(rn<1) {
		mdf.FetchAll("select distinct tabid from dt_srctable where srctabid=%d",srctabid);
		rn=mdf.Wait();
		if(rn<1) {
			lgprintf("�Ҳ��������:%d��Ӧ��Tabid",taskid);
			return 0;
		}
	}
	tabid=mdf.GetInt("tabid",0);
	
	//check intergrity.
	int newdsid=datasetid;
	mdf.FetchAll("select * from dt_index where tabid=%d and issoledpindex=1",tabid);
	rn=mdf.Wait();
	if(rn<1) 
		ThrowWith("Ŀ���%dȱ����������¼.",tabid);
	int pindexid=mdf.GetInt("indexid",0);
	mdf.FetchAll("select sum(filesize) tsize,count(*) fnum from dt_datafilemap where datasetid=%d and indexid=%d",
		datasetid,pindexid);
	mdf.Wait();
	double totalbytes=mdf.GetDouble("tsize",0);
	int datafilenum=mdf.GetInt("fnum",0);
	
	mdf.FetchAll("select count(*) rn from dt_index where tabid=%d and issoledindex=1",tabid);
	rn=mdf.Wait();
	mdf.FetchAll("select distinct indexid from dt_datafilemap where datasetid=%d",datasetid);
	if(mdf.Wait()!=rn) 
	{
		lgprintf("���ִ���: װ�����������(%d)�������������е�ֵ(%d)����.",
			rn,mdf.GetInt("rn",0));
		return 0; //dump && destload(create temporary index table) have not complete.
	}
	
	// Get new & old datasetid
	mdf.FetchAll("select datasetid,databasename from dt_table where tabid=%d",tabid);
	mdf.Wait();
	char dbname[100];
	strcpy(dbname,mdf.PtrStr("databasename",0));
	conn.SelectDB(dbname);
	int olddsid=mdf.GetInt("datasetid",0);
	char sqlbf[3000];
	mdf.FetchAll("select * from dt_index where tabid=%d order by indexidindattab",tabid);
	rn=mdf.Wait();
	//int psoledid=-1;
	//�ȹر�Ŀ���
	sprintf(sqlbf,"update dt.dt_table set recordnum=0 where tabid=%d",tabid);
	conn.DoQuery(sqlbf);
	lgprintf("MySQLˢ��...");
	conn.FlushTables();//.DoQuery("flush tables");
	lgprintf("�����ѹر�.");
	
	lgprintf("��ʼ�����ؽ�,tabid:%d,���������� :%d,datasetid=%d",
		tabid,rn,datasetid);
	int i;
	for(i=0;i<rn;i++) {
		int soled=mdf.GetInt("issoledindex",i);
		if(soled) {
		// Ŀ�����--��Դ��ת�Ƶ�Ŀ���
			int indexidinidxtab=mdf.GetInt("indexidinidxtab",i);
			char *tbname=mdf.PtrStr("indextabname",i);
			
			char *colsname=mdf.PtrStr("columnsname",i);
			indexid=mdf.GetInt("indexid",i);
			
			//if(mdf.GetInt("issoledpindex",i)==1) psoledid=indexid;
			
			//sprintf(sqlbf,"bakidx.%s_%d",tbname,olddsid);
			
			//bool bakex=conn.TouchTable(sqlbf);
			sprintf(sqlbf,"tmpidx.pri_%s_%d",tbname,datasetid);
			if(!conn.TouchTable(sqlbf)) 
				ThrowWith("û���ҵ�����װ��ʱ���ɵ�������:%s,��Ҫ�ֹ�������װ�루��������״̬Ϊ3�������).",
				 sqlbf);
			
			sprintf(sqlbf,"drop table %s.%s",dbname,tbname);
			conn.DoQuery(sqlbf);
			
			//bool dstex=conn.TouchTable(sqlbf);
			
			
			//if(bakex && dstex &&srcex) {
			//	sprintf(sqlbf,"drop table bakidx.%s_%d",tbname,olddsid);
			//	conn.DoQuery(sqlbf);
			//	lgprintf("������������ɾ��.");
			//}
			//if(dstex && srcex) {
			//	sprintf(sqlbf,"alter table %s.%s rename bakidx.%s_%d",
			//		dbname,tbname,tbname,olddsid);
			//	conn.DoQuery(sqlbf);
			//	lgprintf("�����������ѽ���.");
			//}
			//Դ�����ڣ��п������ϴη��������У����в���������ɹ�������
			//if(srcex) {
				sprintf(sqlbf,"alter table tmpidx.pri_%s_%d rename %s.%s",
					tbname,datasetid,dbname,tbname);
				conn.DoQuery(sqlbf);
				lgprintf("�����������ѽ���.");
			//}
			//if(dstex) {
				_Psa->CreateIndex(tbname,conn,indexidinidxtab,colsname,true);
				lgprintf("��������:%s.%s",tbname,colsname);
				for(int j=0;j<rn;j++) {
					if(mdf.GetInt("reuseindexid",j)==indexid) {
						_Psa->CreateIndex(tbname,conn,mdf.GetInt("indexidinidxtab",j),
							mdf.PtrStr("columnsname",j),true);
						lgprintf("��������:%s.%s",tbname,mdf.PtrStr("columnsname",j));
					}
	                                //else {					
					//	printf("reuseindexid:%d!=%d",mdf.GetInt("reuseindexid",j),indexid) ;
					//}

				}
			//}
		}
	}
	//if(psoledid==-1) 
	//	ThrowWith("No primary soled index define in dt_index on tableid: %d",tabid);
	lgprintf("�����������.");
	mdf.FetchAll("select * from dt_datafilemap where datasetid=%d and isfirstindex=1 order by subdatasetid,fileid",
		datasetid);
	rn=mdf.Wait();
		
	int sumrn=0;
	for(i=0;i<rn;i++) sumrn+=mdf.GetInt("recordnum",i);
	lgprintf("����Դ׼�����,��¼��:%d.",sumrn);
	
	sprintf(sqlbf,"update dt.dt_table set datasetid=%d,recordnum=%d,firstdatafileid=%d ,totalbytes=%15.0f ,datafilenum=%d where tabid=%d",
		datasetid,sumrn,mdf.GetInt("fileid",0),totalbytes,datafilenum,tabid);
	conn.DoQuery(sqlbf);
	lgprintf("MySQLˢ��...");
	conn.FlushTables();//.DoQuery("flush tables");
	lgprintf("�����ѳɹ�����.");
	AutoStmt st(psa->GetDTS());
	st.Prepare("update dt_taskschedule set taskstatus=5 where datasetid=%d",
		datasetid);
	st.Execute(1);
	st.Wait();
	st.Prepare("update dt_datafilemap set procstatus=0 where datasetid=%d",
		datasetid);
	st.Execute(1);
	st.Wait();
	
	lgprintf("ɾ���м���ʱ�ļ�...");
	mdf.FetchAll("select * from dt_middledatafile where datasetid=%d",datasetid);
	{
		int dfn=mdf.Wait();
		for(int di=0;di<dfn;di++) {
			lgprintf("ɾ���ļ�'%s'",mdf.PtrStr("datafilename",di));
			unlink(mdf.PtrStr("datafilename",di));
			lgprintf("ɾ���ļ�'%s'",mdf.PtrStr("indexfilename",di));
			unlink(mdf.PtrStr("indexfilename",di));
		}
		lgprintf("ɾ����¼...");
		st.Prepare("delete from dt_middledatafile where datasetid=%d",datasetid);
		st.Execute(1);
		st.Wait();
	} 
	
	lgprintf("����״̬4(DestLoaded)-->5(Complete),taskid:%d.",taskid);
	return 1;
}

thread_rt LaunchWork(void *ptr) 
{
	((worker *) ptr)->work();
	thread_end;
}


int DestLoader::ReCompress(int threadnum)
{
	wociSetTraceFile("DeepCompress");
	AutoMt mdt(psa->GetDTS(),200);
	AutoMt mdf(psa->GetDTS(),200);
	mdt.FetchAll("select * from dt_taskschedule where (taskstatus=6 or taskstatus=9) and rownum<2");
	int rn1=mdt.Wait();
	int rn;
	int i=0;
	bool deepcmp;
	if(rn1<1) {
		return 0;
	}
	for(i=0;i<rn1;i++) {
		taskid=mdt.GetInt("taskid",i);
		srctabid=mdt.GetInt("srctabid",i);
		datasetid=mdt.GetInt("datasetid",i);
		deepcmp=mdt.GetInt("taskstatus",i)==6;
		mdf.FetchAll("select * from dt_datafilemap where datasetid=%d and procstatus =0 order by subdatasetid,fileid",datasetid);
		rn=mdf.Wait();
		if(rn<1) {
	        	mdf.FetchAll("select * from dt_datafilemap where datasetid=%d and procstatus <>2 order by subdatasetid,fileid",datasetid);
	        	rn=mdf.Wait();
	        	if(rn<1) {
				AutoStmt st1(psa->GetDTS());
				st1.Prepare("update dt_taskschedule set taskstatus=%d where datasetid=%d",
					deepcmp?7:10,datasetid);
				st1.Execute(1);
				st1.Wait();
				st1.Prepare("update dt_datafilemap set procstatus =0 where datasetid=%d",datasetid);
				st1.Execute(1);
				st1.Wait();
				lgprintf("����ѹ����������ɣ�����״̬���޸�Ϊ%d,�����ļ�����״̬�޸�Ϊ����(0)",deepcmp?7:10);
			}
			else lgprintf("����ѹ������δ���,����û�еȴ�ѹ��������");
		}
		else break;
	}
	if(i==rn1) return 0;
	//��ֹ���룬�޸������ļ�״̬��
	int fid=mdf.GetInt("fileid",0);
	char srcfn[300];
	strcpy(srcfn,mdf.PtrStr("filename",0));
	char dstfn[300];
	sprintf(dstfn,"%s.%s",srcfn,deepcmp?"depcp":"dep5");
	tabid=mdf.GetInt("tabid",0);

	//dstf.SetFileHeader(0,srcf.GetNextFileName());
	//mdf.SetMaxRows(10);
	mdf.FetchAll("select filename from dt_indexfilemap where fileid=%d",
		fid);
	mdf.Wait();
	char idxdstfn[300];
	sprintf(idxdstfn,"%s.%s",mdf.PtrStr("filename",0),deepcmp?"depcp":"dep5");
	double dstfilelen=0;
	try {
	AutoStmt st(psa->GetDTS());
	st.Prepare("update dt_datafilemap set procstatus=1 where fileid=%d and procstatus=0",
		fid);
	st.Execute(1);
	st.Wait();
	if(wociGetFetchedRows(st)!=1) {
		lgprintf("�����ļ�ѹ��ʱ״̬�쳣,taskid:%d,fid:%d,�������������̳�ͻ��"
				,taskid,fid);
		return 1;
	}
	file_mt idxf;
	lgprintf("���ݴ��������ļ�:'%s',�����ļ�:'%s'.",srcfn,mdf.PtrStr("filename",0));
	idxf.Open(mdf.PtrStr("filename",0),0);
	if(idxf.ReadBlock(-1,0)<0)
		ThrowWith("�����ļ���ȡ����: '%s'",mdf.PtrStr("filename",0));

	dt_file srcf;
	srcf.Open(srcfn,0,fid);
	dt_file dstf;
	dstf.Open(dstfn,1,fid);
	mdf.SetHandle(srcf.CreateMt());
	int lastoffset=dstf.WriteHeader(mdf,0,fid,srcf.GetNextFileName());

	AutoMt *pidxmt=(AutoMt *)idxf;
	lgprintf("�������ļ�����%d����¼.",wociGetMemtableRows(*pidxmt));
	int *pblockstart=pidxmt->PtrInt("blockstart",0);
	int *pblocksize=pidxmt->PtrInt("blocksize",0);
	dt_file idxdstf;
	idxdstf.Open(idxdstfn,1,fid);
	mdf.SetHandle(idxf.CreateMt());
	idxdstf.WriteHeader(mdf,wociGetMemtableRows(*pidxmt),fid,idxf.GetNextFileName());
	//idxdstf.SetFileHeader(wociGetMemtableRows(*pidxmt),idxf.GetNextFileName());
	blockcompress bc(deepcmp?10:5);
	for(i=1;i<threadnum;i++) {
		bc.AddWorker(new blockcompress(deepcmp?10:5));
	}
	char *srcbf=new char[10000000];//ÿһ�δ����������ݿ飨��ѹ���󣩡�
	char *dstbf=new char[10000000];//���ۻ����������(ѹ����).
	int dstseplen=1000000;
	bool isfilled[10];
	int filledlen[10];
	int filledworkid[10];
	char *outcache[10];
	for(i=0;i<10;i++) {
		isfilled[i]=false;
		filledworkid[i]=0;
		outcache[i]=dstbf+i*1000000;
		filledlen[i]=0;
	}
	int workid=0;
	int nextid=0;
	int oldblockstart=pblockstart[0];
	int lastrow=0;
	bool iseof=false;
	bool isalldone=false;
	int lastdsp=0;
	while(!isalldone) {//�ļ��������˳�
		if(srcf.ReadMt(-1,0,mdf,1,1,srcbf)<0) {
			iseof=true;
		}
		block_hdr *pbh=(block_hdr *)srcbf;
		while(true) { //���񽻸����˳�
			worker *pbc=NULL;
			if(!iseof) {
				pbc=bc.GetIdleWorker();
				if(pbc) {
					pbc->Do(workid++,srcbf,pbh->origlen+sizeof(block_hdr),
						pbh->origlen/2); //Unlock internal
			 		break;
				}
			}
			pbc=bc.GetDoneWorker();
			if(pbc) {
				char *pout;
				int dstlen=pbc->GetOutput(&pout);//Unlock internal;
				int doneid=pbc->GetWorkID();
				if(dstlen>dstseplen) 
					ThrowWith("Compress destionation buffer exceed %d",1000000);
				//get empty buf:
				for(i=0;i<10;i++) if(!isfilled[i]) break;
				if(i==10) ThrowWith("Write cache buffer fulled!.");
				memcpy(outcache[i],pout,dstlen);
				filledworkid[i]=doneid;
				filledlen[i]=dstlen;
				isfilled[i]=true;
				//lgprintf("Fill to cache %d,doneid:%d,len:%d",i,doneid,dstlen);
			}
			
			for(i=0;i<10;i++) {
				if(isfilled[i] && filledworkid[i]==nextid) 
					break;
			}
			if(i<10) {
				for(;pblockstart[lastrow]==oldblockstart;) {
					pblockstart[lastrow]=lastoffset;
					pblocksize[lastrow++]=filledlen[i];
				}
				lastoffset=dstf.WriteBlock(outcache[i],filledlen[i],0,true);
				oldblockstart=pblockstart[lastrow];
				dstfilelen+=filledlen[i];
				filledworkid[i]=0;
				filledlen[i]=0;
				isfilled[i]=false;
				nextid++;
				if(nextid-lastdsp>=200) {
				 printf("write from cache %d,nextid %d\n",i,nextid);
				 lastdsp=nextid;
				}
			}
			else if(iseof) {
				if(bc.isidleall()) {
					isalldone=true;
					break;
				}
			}
			if(!pbc) {
#ifdef WIN32
				Sleep(200);
#else
				usleep(200);
#endif
			}
		}
	}
	idxdstf.WriteMt(*pidxmt,COMPRESSLEVEL);
	dstf.Close();
	idxdstf.Close();
	st.Prepare("update dt_datafilemap set procstatus=2 where fileid=%d and procstatus=1",
		fid);
	st.Execute(1);
	st.Wait();
	}
	catch(...) {
	errprintf("���ݶ���ѹ�������쳣���ļ�����״̬�ָ�...");
	AutoStmt st(psa->GetDTS());
	st.Prepare("update dt_datafilemap set procstatus=0 where fileid=%d and procstatus=1",
		fid);
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

int DestLoader::RemoveTable(const char *dbn, const char *tabname, MySQLConn &conn,bool prompt)
{
	lgprintf("remove table '%s ...",tabname);
	AutoMt mt(psa->GetDTS(),100);
	AutoStmt st(psa->GetDTS());
	mt.FetchAll("select * from dt_table where tabname=lower('%s') and databasename=lower('%s')",tabname,dbn);
	int rn=mt.Wait();
	if(rn<1) {
		printf("��'%s'������!",tabname);
		return 0;
	}
	tabid=mt.GetInt("tabid",0);
	datasetid=mt.GetInt("datasetid",0);
	int recordnum=mt.GetInt("recordnum",0);
	int firstdatafileid=mt.GetInt("firstdatafileid",0);
	int srctabid=mt.GetInt("srctabid",0);
	if(recordnum<1 && prompt) {
		lgprintf("Դ��'%s'����Ϊ�գ�������(Y/N)",tabname);
		char ans[100];
		gets(ans);
		if(tolower(ans[0])!='y') {
			lgprintf("ȡ��ɾ���� ");
			return 0;
		}
	}
	if(prompt)
	{
		printf("�� '%s.%s': %s����ɾ������¼��:%d,����?(Y/N)?",
			dbn,tabname,mt.PtrStr("tabdesc",0),recordnum);
		char ans[100];
		gets(ans);
		if(tolower(ans[0])!='y') {
			lgprintf("ȡ��ɾ���� ");
			return 0;
		}
	}
	char sqlbuf[1000];
	lgprintf("�ر�Դ����¼����%d",recordnum);
	sprintf(sqlbuf,"update dt.dt_table set recordnum=0,firstdatafileid=0,datasetid=0,totalbytes=0 where tabid=%d",
		tabid);
	conn.DoQuery(sqlbuf);
	lgprintf("MySQLˢ��...");
	conn.FlushTables();//.DoQuery("flush tables ");
	mt.FetchAll("select * from dt_datafilemap where tabid=%d and datasetid=%d",
		tabid,datasetid);
	rn=mt.Wait();
	int i=0;
	for(i=0;i<rn;i++) {
		  lgprintf("ɾ��'%s'�͸��ӵ�depcp,dep5�ļ�",mt.PtrStr("filename",i));
		  unlink(mt.PtrStr("filename",i));
		  char tmp[300];
		  sprintf(tmp,"%s.depcp",mt.PtrStr("filename",i));
		  unlink(tmp);
		  sprintf(tmp,"%s.dep5",mt.PtrStr("filename",i));
		  unlink(tmp);
	  }
	st.Prepare(" delete from dt_datafilemap where tabid=%d and datasetid=%d",tabid,datasetid);
	st.Execute(1);
	st.Wait();
	
	mt.FetchAll("select * from dt_indexfilemap where tabid=%d and datasetid=%d",
		tabid,datasetid);
	rn=mt.Wait();
	for(i=0;i<rn;i++) {
		  lgprintf("ɾ��'%s'�͸��ӵ�depcp,dep5�ļ�",mt.PtrStr("filename",i));
		  unlink(mt.PtrStr("filename",i));
		  char tmp[300];
		  sprintf(tmp,"%s.depcp",mt.PtrStr("filename",i));
		  unlink(tmp);
		  sprintf(tmp,"%s.dep5",mt.PtrStr("filename",i));
		  unlink(tmp);
	}
	st.Prepare(" delete from dt_indexfilemap where tabid=%d and datasetid=%d",tabid,datasetid);
	st.Execute(1);
	st.Wait();
	
	if(srctabid<0) {
		st.Prepare(" delete from dt_table where tabid=%d",tabid);
		st.Execute(1);
		st.Wait();
		sprintf(sqlbuf,"drop table %s.%s",dbn,tabname);
		lgprintf(sqlbuf);
		conn.DoQuery(sqlbuf);
		mt.FetchAll("select * from dt_index where tabid=%d and issoledindex=1",tabid);
		rn=mt.Wait();
		for(i=0;i<rn;i++) {
			sprintf(sqlbuf,"drop table %s.%s",dbn,mt.PtrStr("indextabname",i));
			lgprintf(sqlbuf);
			conn.DoQuery(sqlbuf);
		}
		st.Prepare(" delete from dt_index where tabid=%d",tabid);
		st.Execute(1);
		st.Wait();
	}
	else psa->EmptyIndex(dbn,tabid,conn);
		
	lgprintf("��'%s.%s'��ɾ��.",dbn,tabname);
	return 1;
}
