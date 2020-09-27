#ifndef DBS_TURBO_SVR_INTERFACE_LIB_HEADER
#define DBS_TURBO_SVR_INTERFACE_LIB_HEADER

#ifdef __unix
#include <stdio.h>
#include <time.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/wait.h>
#else
#include <windows.h>
#include <conio.h>
#endif
#include <string.h>
#include "zlib.h"
#include "AutoHandle.h"
#include "mysqlconn.h"
#include "dt_common.h"
#define TBNAME_DEST 		0
#define TBNAME_PREPONL 		1
#define TBNAME_FORDELETE 	2
#ifdef __unix 
#define INFINITE -1
#define WAIT_OBJECT_0 0
#define WAIT_TIMEOUT -1
#define thread_rt void *
#define thread_end return NULL
#else
#define thread_rt void
#define thread_end return
#endif

#define CI_DAT_ONLY	0
#define CI_IDX_ONLY	1
#define CI_ALL		2
//�̶�Ϊ1MB
#define FIX_MAXINDEXRN 1*1024*1024

#define MAX_COLS_IN_DT 190
#define COMPRESSLEVEL 5

#define MAX_DUMPIDXBYTES	1024*1024
#define	SRCBUFLEN 2500000
#define DSTBUFLEN 2500000
#define PREPPARE_ONLINE_DBNAME "preponl"
#define FORDELETE_DBNAME "fordelete"

typedef enum {
	NEWTASK=0,DUMPING,DUMPED,LOADING,LOADED,BUILDIDX,IDXBUILDED,TASKEND,HETERO_NEW=15,HETERO_DUMPING
} TASKSTATUS;

struct indexparam {
	char idxcolsname[300];
	char idxreusecols[300];
	int idxid;
	char idxtbname[300];
	//char cdsfilename[300];
	//int cdsfs,colnum;
	int colnum;
	int idinidx,idindat;
};

#define MAXSOLEDINDEX 10

struct dumpparam {
	char tmppath[2][300],dstpath[2][300];
	int tmppathid[2],dstpathid[2];
	indexparam idxp[MAXSOLEDINDEX];
	int soledindexnum;
	int psoledindex;
	int usingpathid;
	int tabid;
	long maxrecnum;
	int rowlen;
	dumpparam() {
		memset(this,0,sizeof(dumpparam));
	}
	int GetOffset(int indexid) {
		for(int i=0;i<soledindexnum;i++) 
			if(idxp[i].idxid==indexid) return i;

		ThrowWith("Index id:%d could not found in tabid:%d",
			indexid,tabid);
		return 0;
	}
};


#define BLOCKFLAG 0x5a
#define MYSQLBLOCKFLAG 0x5c
#define BLOCKFLAGEDIT 0x5e
#define MYSQLBLOCKFLAGEDIT 0x5b
#define MYCOLBLOCKFLAGEDIT 0x5d

#define MAX_APPEND_BLOCKRN 1024
#define MAX_DATAFILELEN 1024*1024*1000*2
struct block_hdr {
  char blockflag;
  char compressflag;
  int origlen;
  int storelen;
  void ReverseByteOrder() 
  {
  	revInt(&origlen);
  	revInt(&storelen);
  }
};
#define FILE_VER40       0x5100
#ifdef MYSQL_VER_51
#define FILEFLAG     0x52aa // old 4.0: 0x51aa
#define FILEFLAGEDIT 0x52ac // old 4.0: 0x51ac
#define FILE_VER51	 0x5200
#define FILE_DEF_VER FILE_VER51
#else
#define FILEFLAG     0x51aa // old 4.0: 0x51aa
#define FILEFLAGEDIT 0x51ac // old 4.0: 0x51ac
#define FILE_DEF_VER 0x5100
#endif
struct file_hdr {
 int fileflag;
 int fnid;
 int islastfile;
 unsigned int rownum;
  int cdlen;
  int cdnum;
 unsigned int rowlen;
 char nextfile[300];
 void ReverseByteOrder() {
 	revInt(&fileflag);
 	revInt(&fnid);
 	revInt(&islastfile);
 	revInt(&rownum);
 	revInt(&cdlen);
 	revInt(&cdnum);
 	revInt(&rowlen);
 }
};

struct file_hdr_ext {
 unsigned int insertrn;
 unsigned int deletern;
 unsigned int dtp_sync;
 unsigned int lst_blk_off;
 void ReverseByteOrder() {
 	revInt(&insertrn);
 	revInt(&deletern);
 	revInt(&dtp_sync);
 	revInt(&lst_blk_off);
 }
};

struct delmask_hdr {
 unsigned int rownum;
 unsigned int deletedrn;
 void ReverseByteOrder() {
 	revInt(&rownum);
 	revInt(&deletedrn);
 }
};

class dtioStream;
class dtiofile;
class DTIOExport dt_file {
#ifdef __unix
#define INFINITE -1
#define WAIT_OBJECT_0 0
#define WAIT_TIMEOUT -1
  pthread_t hd_pthread_t;
  pthread_mutex_t buf_empty;
  pthread_mutex_t buf_ready;
#else
 HANDLE buf_ready,buf_empty;
#endif
 bool terminate;
 int dbgct;
 int file_version;
	char filename[300];
	char errstr[1000];
	FILE *fp;
	dtioStream *pdtio;
	dtiofile *pdtfile;
	int openmode;
	int contct;//�����ݿ�δ������ת,Ϊ�����Ĵ���,���ڿ����첽���ж�����.
	int fnid;
	// blocklen ��ʾһ�����ݿ����ļ��еĴ洢�ֽ���(����blockhdr).
	int readedrn,bufrn;
	unsigned int readedoffset,bufoffset,curoffset,curblocklen;
	unsigned int readedblocklen,bufblocklen,buforiglen;
	int readedfnid,buffnid;
	bool isreading;
	int mycollen[MAX_COLS_IN_DT+1];//mysql���ݿ鰴����֯�����м�¼ʱ��ƫ����
	//int mycoloff[MAX_COLS_IN_DT+1];

protected:
	file_hdr fh;
	int blockflag;
	int colct;
	file_hdr_ext fhe;
	delmask_hdr dmh;
	unsigned int filesize;	
	char *blockbf,*cmprsbf;
	char *pblock;//��������ʵ������(����blockhdr).
	char *offlineblock;
	char *delmaskbf; //ɾ����־ӳ����
	int offlinelen;
	unsigned int bflen,cmprslen,delmasklen;
	bool paral;
	int mysqlrowlen;
	char *pwrkmem;
	bool OpenNext();
public:
	int SetLastWritePos(unsigned int off);
	void SetReadError(const char *es) {
		strcpy(errstr,es);
		bufoffset=(unsigned int)-2;
	}

	bool rebuildrow( char *src, char *row,  int rn, int rp) {
		if(blockflag!=MYCOLBLOCKFLAGEDIT) return false;
		int *plen=mycollen;
		int cp=colct+1;
		while(--cp>=0) 
		{ 
			register int cl=*plen;
			char *p1=src+rp*cl;
			while(--cl>=0) *row++=*p1++;
			src+=*plen++*rn;
		}
		return true;
	}
	
	inline bool rebuildblock( char *src, char *dst, int rn) {
		if(blockflag!=MYCOLBLOCKFLAGEDIT) return false;
  		char *mycolptr[MAX_COLS_IN_DT+1];//����mask��������Ϊʵ���ֶ�����1
		int cp;
		int rl=0;
		register char *p=src;
		for(cp=0;cp<=colct;cp++) {
			mycolptr[cp]=p;
			p+=rn*mycollen[cp];
		}
		int rp;
		p=dst;
		rp=rn;
		while(--rp>=0) {
		 cp=0;
		 do
		 { 
			register int cl=mycollen[cp];
			register char *tp=mycolptr[cp];
			while(--cl>=0) *p++=*tp++;
			 mycolptr[cp]=tp;
		 }while(++cp<=colct);
		}
		return true;
	}
	
	int GetDataOffset(block_hdr *pbh) {
		switch(pbh->blockflag) {
		case BLOCKFLAG :
		case MYSQLBLOCKFLAG :
			return sizeof(block_hdr);
		case BLOCKFLAGEDIT :
		case MYSQLBLOCKFLAGEDIT :
		case MYCOLBLOCKFLAGEDIT :
			return sizeof(block_hdr)+sizeof(delmask_hdr)+(pbh->origlen/GetBlockRowLen(pbh->blockflag)+7)/8;
		}
        ThrowWith("block header has a uncognized type :%d",pbh->blockflag);
	}
	
	static const char *GetBlockTypeName(int blocktp) {
		switch(blocktp) {
		case BLOCKFLAG :
			return "������֯��δת����(v1.2)";
		case MYSQLBLOCKFLAG :
			return "������֯����ת����(v1.2)";
		case BLOCKFLAGEDIT :
			return "������֯��δת����(v2.1)";
		case MYSQLBLOCKFLAGEDIT :
			return "������֯����ת����(v2.1)";
		case MYCOLBLOCKFLAGEDIT :
			return "������֯����ת����(v2.1)";
		}
		return "δ֪�Ŀ�����";
	}
	int AppendRecord(const char *rec,bool deleteable=false);
	bool CanEdit() { return pdtio==NULL && fh.fileflag==FILEFLAGEDIT;}
	int deleterow(int rn)
	{
		if(!CanEdit()) return 0;
		if(IsDeleted(rn)) return 0;
		int off=readedoffset;
		Open(filename,2,fnid);
		// modify file header extend area
		fseek(fp,sizeof(file_hdr),SEEK_SET);
		fhe.deletern++;
		fhe.dtp_sync=0;
		fhe.ReverseByteOrder();
		dp_fwrite(&fhe,sizeof(fhe),1,fp);
		fhe.ReverseByteOrder();

		// modify block's delete bit map.
		fseek(fp,off+sizeof(block_hdr),SEEK_SET);
		dmh.deletedrn++;
		delmaskbf[rn/8]|=(1<<(rn%8));
		dmh.ReverseByteOrder();
		dp_fwrite(&dmh,sizeof(dmh),1,fp);
		dp_fwrite(delmaskbf,(dmh.rownum+7)/8,1,fp);
		dmh.ReverseByteOrder();
		
		Open(filename,0,fnid);
		fseek(fp,off,SEEK_SET);
		readedoffset=off;
		return 1;
	}
	// st: 0 based
	int deleterows(int st,int rn)
	{
		int rnct=0;
		for(int i=0;i<rn;i++) rnct+=deleterow(st+i);
		return rnct;
	}
	bool getdelmaskbuf(char **pbuffer) {
		if(dmh.deletedrn<1) return false;
		*pbuffer=delmaskbf;
		return true;
	}
	// rn start from 0.
	inline bool IsDeleted(int rn) { 
		return dmh.deletedrn>0 && delmaskbf[rn/8]&(1<<(rn%8));
	}
	static bool CheckBlockFlag(int flag) {return flag==BLOCKFLAG || flag==MYSQLBLOCKFLAGEDIT || flag==MYCOLBLOCKFLAGEDIT|| flag==MYSQLBLOCKFLAG || flag==BLOCKFLAGEDIT ;}
	static bool EditEnabled(int flag)  {return flag==MYSQLBLOCKFLAGEDIT || flag==MYCOLBLOCKFLAGEDIT || flag==BLOCKFLAGEDIT ;}
	int dtfseek(long offset) ;
	size_t dtfread(void *ptr,size_t size) ;
//	void Reset() {
//		
	void SetStreamName(const char *sfn);
	bool Terminated() {return terminate;}
	void ResetTerminated() { terminate=false;}
	const char *GetFileName() {return filename;};
 	//void End();
	//void Start();
	int GetFirstBlockOffset();
	const char *GetNextFileName() {
		if(fh.islastfile) return NULL;
		return fh.nextfile;
	}
	int WaitBufReady(int tm=INFINITE);
	int WaitBufEmpty(int tm=INFINITE);
	void SetBufReady();
	void SetBufEmpty(char *cacheBuf=NULL,int cachelen=0);
	int ReadMtThread(char *cacheBuf=NULL,int cachelen=0);
	int GetFileSize() { return filesize;}
	int ReadMt(int offset, int storesize,AutoMt & mt,int clearfirst=1,int singlefile=1,char *poutbf=NULL,BOOL forcparal=false,BOOL dm=false);
	int ReadBlock(int offset, int storesize,bool &contread,int _singlefile=1,char *cacheBuf=NULL,int cachelen=0);
	int GeneralRead(int offset,int storesize,AutoMt &mt,char **ptr,int clearfirst=1,int singlefile=1,char *cacheBuf=NULL,int cachelen=0);
	int ReadHeader();
	int CreateMt(int maxrows=0);
	dt_file(bool _paral=false) ;
	void SetFileHeader(int rn=0,const char *nextfile=NULL) ;
	virtual void Open(const char *filename,int openmode,int fnid=-1) ; //openmode 0:rb read 1:w+b 2:wb
	int WriteHeader(int mt=0,int rn=0,int fid=0,const char *nextfilename=NULL) ;
	int WriteMySQLMt(int mt,int compress,bool corder=true) ;
	int ReadMySQLBlock(int offset, int storesize,char **poutbf,int singlefile=1);
	int WriteMt(int mt,int compress,int rn=0,bool deleteable=false) ;
	int SetMySQLRowLen(); 
	int GetRowLen() {
		return fh.rowlen;
	} 
	int GetBlockRowLen(int blocktype) {
		return (blocktype==BLOCKFLAGEDIT || blocktype==BLOCKFLAG)?fh.rowlen:mysqlrowlen;
	}
	int GetMySQLRowLen() {
		return mysqlrowlen;
	} 
	void SetDbgCt(int ct) {dbgct=ct;}
	inline int GetFnid() {return fnid;}
	int GetRowNum() {
		return fh.rownum;
	}
	inline int GetBlockRowNum() {
		return readedrn;
	}
	int GetLastOffset() {
		return curoffset; 
	}
	int GetOldOffset() {
		return readedoffset;
	}
	int WriteBlock(char *bf,unsigned int len,int compress,bool packed=false,char bflag=BLOCKFLAGEDIT) ;
	void Close() {
		if(fp) fclose(fp);fp=NULL;fnid=0;openmode=-1;
	}
	void SetParalMode(bool var);
	virtual ~dt_file() ;
};

class DTIOExport file_mt :public dt_file {
	int rowrn,mtlastoffset;
	AutoMt mt;
public:
	file_mt(bool _para=false):mt(0,0),dt_file(_para) {rowrn=0;mtlastoffset=0;}
	void Open(const char *filename,int openmode,int fnid=-1)  //openmode 0:rb read 1:w+b 2:wb
	{
		dt_file::Open(filename,openmode,fnid);
		if(openmode!=1) {
		int hd=CreateMt();
		if(mt.CompareMt(hd))
			mt.SetHandle(hd);
		else wocidestroy(hd);
		}
	}
	int ReadBlock(int offset,int storesize,int _singlefile=1,bool forceparl=false) {
		//Acceleration especially for middle load of data.(MiddleDataLoader::Load)
		if(_singlefile && mtlastoffset==offset && offset>0) return mt;
		mtlastoffset=offset;
		if(ReadMt( offset,storesize,mt,1,_singlefile,NULL,forceparl)==-1) 
			return 0;
		return mt;
	}
	
	int ReadMtOrBlock(int offset,int storesize,int _singlefile,char **ptr,char *cacheBuf=NULL,int cachelen=0) {
		char *mptr=NULL;
		int rn=GeneralRead(offset,storesize,mt,&mptr,1,_singlefile,cacheBuf,cachelen);//Clear first:Yes
		if(rn==-1) return -1;
		if(mptr==NULL) return mt;
		*ptr=mptr;
		return 0;
	}
	int AppendMt(int mt,int compress,int rn);
	operator int () {return mt;}
	operator AutoMt * () { return &mt;}
	virtual ~file_mt() {};
};


class DTIOExport SysParam {
protected :
	AutoMt dt_path,dt_srcsys,dt_table,dt_index;
	int dts;
public :
	const char * internalGetPathName(int pathid,char *pathtype);
	int GetMaxBlockRn(int tabid);
	int GetMiddleFileSet(int procstate);
	int BuildDBC(int srcidp);
	SysParam(int dts) :dt_path(dts),dt_srcsys(dts),dt_table(dts),dt_index(dts,500)	{
		this->dts=dts;
		//Reload();
	};
	int log(int tabid,int datapartid,int evt_type,const char *format,...) {
		char msg[LOG_MSG_LEN+MAX_STMT_LEN];
		#ifdef __unix
		va_list vlist;
		#else
		va_list vlist;
		#endif
		va_start(vlist,format);
		int rt=vsprintf(msg,format,vlist);
		va_end(vlist);
		if(strlen(msg)>LOG_MSG_LEN) // truncate log to maximum length
		  msg[LOG_MSG_LEN]=0;
		AutoStmt st(dts);
		// add to bypass mysql5.1 report error string in sql 
		//strcpy(msg,"test");
		st.Prepare("insert into dp.dp_log values (%d,%d,now(),%d,?)",tabid,datapartid,evt_type);
		st.BindStr(1,msg,strlen(msg));
		st.Execute(1);
		st.Wait();
		return rt;
	}
	int BuildSrcDBC(int tabid,int datapartid=-1) ;
	int GetSoledIndexParam(int datapartid,dumpparam *dp,int tabid=-1) ;
	int GetFirstTaskID(TASKSTATUS ts,int &tabid,int &datapartid) ;
	int UpdateTaskStatus(TASKSTATUS ts,int tabid,int datapartid) ;
	
	int GetDumpSQL(int taskid,int partoffset,char *sqlresult) ;
	//�ͷ��ڲ���Ա���󣬲���ʡ�ԡ�
	virtual ~SysParam() {};
	//pathtype first.
	const char *GetMySQLPathName(int pathid,char *pathtype=NULL);

	//�м���ʱ�����ļ���id��
	int NextTmpFileID() ;
	int NextTableID() ;
	int NextDstFileID(int tabid);
	int GetSeqID(const char *seqfield);
	virtual void Reload();
	int GetDTS() {return dts;}
	int GetMySQLLen(int mt);
	void GetSrcSys(int sysid,char *sysname,char *svcname,char *username,char *pwd);
};
 class DTIOExport SvrAdmin :public SysParam {
	static SvrAdmin *psa;
	static AutoHandle *pdts;
	static int svrstarted;
	static int shutdown;
#ifdef __unix
    static pthread_t hd_pthread_t;
#endif
	AutoMt filemap,dsttable;
	SvrAdmin(int dts):filemap(dts,1000),dsttable(0,10),SysParam(dts)
	{
		filenamep=1;
	}
	virtual ~SvrAdmin() {};
	int filenamep;
public :
	static void SetShutdown() {shutdown=1;}
	const char * GetDbName(int p);
	static SvrAdmin *GetInstance() ;
	static SvrAdmin *RecreateInstance(const char *svcname,const char *usrnm,const char *pswd);
	static void ReleaseInstance() ;
	static int CreateDTS(const char *svcname,const char *usrnm,const char *pswd) ;
	static void ReleaseDTS() ;
	static void SetSvrStarted() {svrstarted=1;}
	static void CreateInstance();
	int GetIndexStruct(int p) ;
	int Search(const char *pathval) ;
	int GetTotRows(int p) {
		return *dsttable.PtrInt(3,p);
	}
	int GetSoleIndexNum(int p) {
		return *dsttable.PtrInt(1,p);
	}
	const char *GetFirstFileName(int p)
	{
		return dsttable.PtrStr(5,p);
	}
	int GetTotIndexNum(int p) {
		return *dsttable.PtrInt(2,p);
	}
	const char *GetFileName(int tabid,int fileid) ;
	virtual void Reload() ;
};

#ifndef MYSQL_SERVER

class DTIOExport SysAdmin :public SysParam {
	MySQLConn conn,connlock;
	bool normal;
	char lastlocktable[300];
public :
	void SetNormalTask(bool val) {normal=val;}
	const char * GetNormalTaskDesc() {
		if(normal) return " and ifnull(blevel,0)<100 ";
		return " and ifnull(blevel,0)>=100 ";
	}
	bool GetBlankTable(int &tabid);
	
	bool GetNormalTask() {return normal;}
	void OutTaskDesc(const char *prompt,int tabid=0,int datapartid=0,int indexid=0);
	bool GetTableName(int tabid,int indexid,char *tbname,char *idxname,int type,int datapartoff=-1,int datapartid=-1);
	void GetPathName(char *path,const char *tbname,const char *surf);
	bool CreateTableOnMysql(int srcmt,const char *tabname,bool forcecreate);
	int GetSrcTableStructMt(int tabp,int srcsys);
	int BuildMtFromSrcTable(int srcsys,int tabid,AutoMt *mt);
	bool EmptyIndex(int tabid);
	SysAdmin(int dts,const char *host=NULL,const char *username=NULL,const char *password=NULL,const char *dbname=NULL,unsigned int portnum=0):SysParam(dts) {
		conn.Connect(host,username,password,dbname,portnum);
		connlock.Connect(host,username,password,dbname,portnum);
		normal=true;memset(lastlocktable,0,sizeof(lastlocktable));
	};
	
	bool TouchDatabase(const char *dbn,bool create=false)
	{
		return conn.TouchDatabase(dbn,create);
	}
	virtual void Reload()
	{
		SysParam::Reload();
		conn.SetDataBasePath(GetMySQLPathName(0,(char *)"msys"));
	}
	void ReleaseTable() ;
	void SelectDB(const char *db) {
		conn.SelectDB(db);
	}
	void FlushTables(const char *tab){
		conn.FlushTables(tab);
	}
	virtual ~SysAdmin() {ReleaseTable();};
	int DoQuery(const char *szSQL) {
		return conn.DoQuery(szSQL);
	}
	bool TouchTable(const char *tbn) {
		return conn.TouchTable(tbn);
	}
	bool CreateDT(int tabid);
	void SetTrace(const char *type,int tabid);
	void DropDTTable(int tabid,int nametype);
	int CleanData(bool prompt,int tabid);
	int CreateDataMtFromFile(AutoMt &destmt,int rownum,int tabid,int fileflag);
	int CreateIndexMtFromFile(AutoMt &indexmt,int rownum,int tabid,int indexid);
	int CreateIndexMT(AutoMt &idxtarget,int destmt,int tabid,int indexid,int *colidx,char *colsname,bool update_idxtb);
	void CreateIndexTable(int tabid,int indexid,int indexmt,int destmt,int nametype,bool createidx,int ci_type=CI_ALL,int datapartid=-1);
	void CreateAllIndexTable(int tabid,int destmt,int nametype,bool createidx,int ci_type=CI_ALL,int datapartid=-1);
	void CreateAllIndex(int tabid,int nametype,bool forcecreate,int ci_type=CI_ALL,int datapartid=-1);
	void CreateIndex(int tabid,int indexid,int nametype,bool forcecreate,int ci_type=CI_ALL,int datapartoff=-1,int datapartid=-1);
	void CreateIndex(const char *dtname, int id, const char *colsname, bool forcecreate);
	void CreateMergeIndexTable(int tabid);
	void RepairAllIndex(int tabid,int nametype,int datapartid=-1);
	void DataOnLine(int tabid);
	void CloseTable(int tabid,char *tbname,bool cleandt,bool withlock=false);
	void BuildDTP(const char *tbname);
};
#endif


#endif
