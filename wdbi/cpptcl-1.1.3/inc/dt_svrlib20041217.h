#ifndef DBS_TURBO_SVR_INTERFACE_LIB_HEADER
#define DBS_TURBO_SVR_INTERFACE_LIB_HEADER

#ifdef __unix
#include <stdio.h>
#include <time.h>
#include <pthread.h>
#include <unistd.h>
#else
#include <windows.h>
#include <conio.h>
#endif
#include <string.h>
#include "woci8intface.h"
#include "zlib.h"
#include "woci8err.h"
#include "AutoHandle.h"

#ifndef BOOL
#define BOOL int
#endif
#ifndef FALSE
#define FALSE 0
#endif
#ifndef TRUE
#define TRUE 1
#endif

#ifdef WIN32
#define mSleep(msec) Sleep(msec)
#else
//毫秒转换到纳秒
#define mSleep(msec) \
{ \
   struct timespec req,rem;\
   req.tv_sec=msec/1000;\
   req.tv_nsec=msec%1000*1000*1000;\
   nanosleep(&req, &rem);\
}
#endif 
	
#define mySleep(a) {if(a>10) printf("\n%d秒后运行。\n",a); mSleep(a*1000);}
#define freeinfo1(hdinfo) 

void ThrowWith(const char *format,...) ;
int xmkdir(const char *dir) ;
int SplitStr(const char *src,char **dst,char **pbf) ;
void BuildWhere(const char *cols,const char *bvals,const char *evals,char *sqlbf);
void decode(char *str) ;
void encode(char *str) ;
int GetFreeM(const char *path) ;

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
	int srctabid,tabid;
	long maxrecnum;
	int rowlen;
	dumpparam() {
		memset(this,0,sizeof(dumpparam));
	}
	int GetOffset(int indexid) {
		for(int i=0;i<soledindexnum;i++) 
			if(idxp[i].idxid==indexid) return i;

		ThrowWith("Index id:%d could not found in srctabid:%d,tabid:%d",
			indexid,srctabid,tabid);
		return 0;
	}
};


#define BLOCKFLAG 0x5a
struct block_hdr {
  char blockflag;
  char compressflag;
  int origlen;
  int storelen;
};

#define FILEFLAG 0x51aa
struct file_hdr {
 int fileflag;
 int fnid;
 int islastfile;
 int rownum;
 int cdlen;
 int cdnum;
 int rowlen;
 char nextfile[300];
};

class dt_file {
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
	char filename[300];
	char errstr[1000];
	FILE *fp;
	int openmode;
	int contct;//读数据快未出现跳转,为连续的次数,用于控制异步并行读数据.
	int fnid;
	// blocklen 表示一个数据块在文件中的存储字节数(包含blockhdr).
	int readedrn,bufrn;
	int readedoffset,bufoffset,curoffset,curblocklen;
	int readedblocklen,bufblocklen,buforiglen;
	int readedfnid,buffnid;
	bool isreading;

	file_hdr fh;
	int filesize;	
	char *blockbf,*cmprsbf;
	char *pblock;//解包后的真实块数据(不含blockhdr).
	int bflen,cmprslen;
	bool paral;
	char *pwrkmem;
	bool OpenNext();
public:
	void SetReadError(const char *es) {
		strcpy(errstr,es);
		bufoffset=-2;
	}
//	void Reset() {
//		
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
	void SetBufEmpty();
	int ReadMtThread();
	int GetFileSize() { return filesize;}
	int ReadMt(int offset, int storesize,AutoMt & mt,int clearfirst=1,int singlefile=1,char *poutbf=NULL,BOOL forcparal=false);

	int ReadHeader();
	int CreateMt(int maxrows=0);
	dt_file(BOOL _paral=false) ;
	void SetFileHeader(int rn=0,const char *nextfile=NULL) ;
	virtual void Open(const char *filename,int openmode,int fnid=-1) ; //openmode 0:rb read 1:w+b 2:wb
	int WriteHeader(int mt=0,int rn=0,int fid=0,const char *nextfilename=NULL) ;
	int WriteMt(int mt,int compress) ;
	int GetRowLen() {
		return fh.rowlen;
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
	int WriteBlock(char *bf,unsigned int len,int compress,bool packed=false) ;
	void Close() {
		if(fp) fclose(fp);fp=NULL;fnid=0;openmode=-1;
	}
	void SetParalMode(bool var);
	virtual ~dt_file() {
		SetParalMode(false);
		if(blockbf) delete[]blockbf;
		if(cmprsbf) delete[]cmprsbf;
		if(pwrkmem) delete[]pwrkmem;
		Close();
	}
};

class file_mt :public dt_file {
	int rowrn;
	AutoMt mt;
public:
	file_mt(bool _para=false):mt(0,0),dt_file(_para) {rowrn=0;}
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
		if(ReadMt( offset,storesize,mt,1,_singlefile,NULL,forceparl)==-1) 
			return 0;
		return mt;
	}
	operator int () {return mt;}
	operator AutoMt * () { return &mt;}
	virtual ~file_mt() {};
};


class SysParam {
protected :
	AutoMt dt_path,dt_srcsys,dt_srctable,dt_table,dt_index;
	int dts;
public :
	const char * internalGetPathName(int pathid,char *pathtype);
	int GetMaxBlockRnFromSrcTab(int srctabid);
	int GetMaxBlockRn(int tabid);
	int GetMiddleFileSet(int procstate);
	int BuildSrcDBC(int srcidp);
	SysParam(int dts) :dt_path(dts),dt_srcsys(dts),dt_srctable(dts),dt_table(dts),dt_index(dts,500)	{
		this->dts=dts;
		//Reload();
	};
	int BuildSrcDBCFromSrcTableID(int srctabid) ;

	int GetSoledInexParam(int srctabid,dumpparam *dp,int tabid=-1) ;
	int GetFirstTaskID(TASKSTATUS ts,int &srctabid,int &datasetid) ;
	int UpdateTaskStatus(TASKSTATUS ts,int taskid) ;
	
	int GetDumpSQL(int taskid,int partoffset,char *sqlresult) ;
	//释放内部成员对象，不能省略。
	virtual ~SysParam() {};
	//pathtype first.
	const char *GetMySQLPathName(int pathid,char *pathtype=NULL);
	int NextFileID() ;
	int NextTableID() ;
	int NextIndexID() ;	
	int NextDatasetID() ;
	virtual void Reload();
	int GetDTS() {return dts;}
	int GetSrcTabid(int tabid,int &p) ;
	bool GetBlankTable(int &tabid);
	void GetSrcSys(int sysid,char *sysname,char *svcname,char *username,char *pwd);
};

class SvrAdmin :public SysParam {
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
	const char *GetFileName(int fileid) ;
	virtual void Reload() ;
};
#ifndef MYTIMER_CLASS
#define MYTIMER_CLASS
class mytimer {
#ifdef WIN32 
	LARGE_INTEGER fcpu,st,ct;
#else
	struct timespec st,ct;
#endif
public:
	mytimer() {
#ifdef WIN32
	 QueryPerformanceFrequency(&fcpu);
	 QueryPerformanceCounter(&st);
	 ct.QuadPart=0;
#else
	 memset(&st,0,sizeof(timespec));
	 memset(&ct,0,sizeof(timespec));
#endif
	}
	void Clear() {
#ifdef WIN32
		ct.QuadPart=0;
#else
		memset(&ct,0,sizeof(timespec));
#endif
	}
	void Start() {
#ifdef WIN32
	 QueryPerformanceCounter(&st);
#else
	 clock_gettime(TIMEOFDAY,&st);
#endif
	}
	void Stop() {
#ifdef WIN32
	 LARGE_INTEGER ed;
	 QueryPerformanceCounter(&ed);
	 ct.QuadPart+=(ed.QuadPart-st.QuadPart);
	 st.QuadPart=ed.QuadPart;
#else
	timespec ed;
	clock_gettime(TIMEOFDAY,&ed);
	ct.tv_sec+=(ed.tv_sec-st.tv_sec);
	ct.tv_nsec+=(ed.tv_nsec-st.tv_nsec);
	//printf("Stop at %d s,%dns\n",ct.tv_sec,ct.tv_nsec);
	st=ed;
#endif
	}
	void Restart() {
	 Clear();Start();
	}
	double GetTime() {
#ifdef WIN32
		return (double)ct.QuadPart/fcpu.QuadPart;
#else
		return (double)ct.tv_sec+ct.tv_nsec/1e9;
#endif
	}
};

class autotimer {
	mytimer *ptm;
public :
	autotimer(mytimer *p) {
		ptm=p;
		p->Start();
	}
	~autotimer() {
		ptm->Stop();
	}
};
#endif
#endif
