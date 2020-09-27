#ifndef DBS_TURBO_LIB_HEADER
#define DBS_TURBO_LIB_HEADER
#ifdef __unix
#include <pthread.h>
#include <malloc.h>
#include <stdlib.h>
#ifndef BOOL
#define BOOL int
#endif 
#ifndef FALSE
#define FALSE 0
#endif
#ifndef TRUE
#define TRUE 1
#endif
#else
#include <windows.h>
#include <process.h>
#include <conio.h>
#endif
#include <stdio.h>
#include <string.h>

#include "AutoHandle.h"
#include "dt_svrlib.h"
#include "zlib.h"
#include "ucl.h"
#include <lzo1x.h>
#include <bzlib.h>


#define MAX_ROWS_LIMIT  2*1024*1024*1024            // 最大记录条数2G的限制
#ifdef PLATFORM_64
#define MAX_LOADIDXMEM	200000
#else
#define MAX_LOADIDXMEM  200000
#endif


class DataDump {
	AutoMt fnmt;
	int taskid,indexid,datapartid;
	char tmpfn[300];
	char tmpidxfn[300];
	char dumpsql[MAX_STMT_LEN];
	dumpparam dp;
	long memlimit;
	int dtdbc;
	int fnorder;
	int maxblockrn;
	int blocksize;
	mytimer fiotm,sorttm,adjtm;
public:
	DataDump(int dtdbc,int maxmem,int _blocksize);
	int BuildMiddleFilePath(int _fid) ;
	void ProcBlock(SysAdmin &sp,int partid,AutoMt *pCur,int idx,AutoMt &blockmt,int _fid);
	int DoDump(SysAdmin &sp,const char *systype=NULL) ; 
	int heteroRebuild(SysAdmin &sp);
	~DataDump() {};
};
                      
class MiddleDataLoader {
	//int blockmaxrows;
	//int maxindexrows;
	SysAdmin *sp;
	AutoMt mdindexmt;
	AutoMt indexmt;
	AutoMt blockmt;
	///AutoMt mdblockmt;
	AutoMt mdf;
	dumpparam dp;
	int tmpfilenum;
	file_mt *pdtf;
	unsigned short *pdtfid;
	//int dtfidlen;
public:
		void CheckEmptyDump();
	  int Load(int MAXINDEXRN,long LOADTIDXSIZE,bool useOldBlock=true) ;
	  int homo_reindex(const char *dbname,const char *tname);
	  int dtfile_chk(const char *dbname,const char *tname) ;
	  int CreateLike(const char *dbn,const char *tbn,const char *nsrcowner,const char *nsrctbn,const char * ndstdbn,const char *ndsttbn,const char *taskdate,bool presv_fmt);

      //>> Begin:dm-228,创建数据任务表，通过源表命令参数设置
      int CreateSolidIndexTable(const char* orasvcname,const char * orausrname,const char* orapswd,
          const char* srcdbname,const char* srctabname,const char* dstdbname,const char* dsttabname,
		  const int  cmptype,const char* tmppath,const char* backpath,const char *taskdate,
		  const char* solid_index_list_file,char* ext_sql);

      // 忽略大字段列，并更新采集数据sql语句
      int IgnoreBigSizeColumn(int dts,const char* dbname,const char* tabname,char* dp_datapart_extsql);
	  //<< End:dm-228
      
	  MiddleDataLoader(SysAdmin *_sp);
	  ~MiddleDataLoader() {
		  if(pdtf) delete [] pdtf;
		  if(pdtfid) delete [] pdtfid;
		  pdtf=NULL;
		  pdtfid=NULL;
		  //dtfidlen=0;
	  }
};

class DestLoader {
	int indexid;
	int datapartid;
	int tabid;
	SysAdmin *psa;
	dumpparam dp;
public :
	int RemoveTable(const char *dbn,const char *tabname,bool prompt=true);
	int ReCompress(int threadnum);
	DestLoader(SysAdmin *_psa) {
		psa=_psa;
	}
	int MoveTable(const char *srcdbn,const char *srctabname,const char * dstdbn,const char *dsttabname);
	int Load (bool directIO=false) ;
	int ToMySQLBlock(const char *dbn, const char *tabname);
	int ReLoad ();
	~DestLoader() {};
	int RecreateIndex(SysAdmin *_Psa) ;
};

thread_rt LaunchWork(void *ptr) ;

class worker {
protected :
	char *inbuf;
	int inbuflen;
	char *outbuf;
	int outbuflen;
	int workid;
	bool isidle;
	bool isdone;
	int blockoffset;
	int islocked;
	unsigned long dstlen;
	worker *pNext;
#ifdef __unix
	pthread_t hd_pthread_t;
	pthread_mutex_t status_mutex;
#else
	CRITICAL_SECTION status_mutex;
#endif

public :
	worker() {
		inbuf=outbuf=NULL;
		inbuflen=outbuflen=0;
		isidle=true;dstlen=0;isdone=false;
		pNext=NULL;islocked=0;
#ifdef __unix
	if(pthread_mutex_init(&status_mutex,NULL)!=0)
		ThrowWith("Create mutex for worker failed.");
#else
	InitializeCriticalSection(&status_mutex);
//(NULL,false,NULL);
//	if(status_mutex==NULL) ThrowWith("Create mutex for worker failed.");
#endif

	}

	virtual int work() =0;
	bool isidleall() {
		if(!isidle) return false;
		if(pNext) return pNext->isidleall();
		return true;
	}
	void AddWorker(worker *pwk) {
		if(pNext) pNext->AddWorker(pwk);
		else pNext=pwk;pwk->pNext=NULL;
	}
	
	int LockStatus() {
	 #ifdef __unix
	 int rt=pthread_mutex_lock(&status_mutex);
	 #else
	 DWORD rt=WAIT_OBJECT_0;
	 EnterCriticalSection(&status_mutex);
	 #endif
	 islocked++;
	 return rt;
	}
	int GetWorkID() { return workid;}
	unsigned long GetOutput(char **pout) {
		*pout=outbuf;
		isdone=false;
		Unlock();
		return dstlen;
	}
	bool LockIdle() {
		LockStatus();
		if(isdone || !isidle) {
			Unlock();
			return false;
		}
		
		return true;
	}

	bool LockDone() {
		LockStatus();
		if(!isdone) {
			Unlock();
			return false;
		}
		isidle=true;
		return true;
	}

	void Unlock()
	{
		#ifdef __unix
		pthread_mutex_unlock(&status_mutex);
		#else
		LeaveCriticalSection(&status_mutex);
		#endif
		islocked--;
	}

	worker *GetIdleWorker() {
		if(LockIdle()) return this;
		else if(pNext) return pNext->GetIdleWorker();
		return NULL;
	}
	
	worker *GetDoneWorker() {
		if(LockDone()) return this;
		else if(pNext) return pNext->GetDoneWorker();
		return NULL;
	}

	char *getinbuf(int len) {
		if(inbuflen<len) {
			if(inbuf) delete []inbuf;
			inbuf=new char [(int)(len *1.3)];
			inbuflen=(int)(len*1.3);
		}
		return inbuf;
	}
	char *getoutbuf(int len) {
		if(outbuflen<len) {
			if(outbuf) delete []outbuf;
			outbuf=new char [(int)(len *1.3)];
			outbuflen=(int)(len*1.3);
		}
		return inbuf;
	}
	void Do(int _workid,const char *in,int inlen,int _blockoff,int estDstlen) {
		isidle=false;
		isdone=false;
		workid=_workid;
		dstlen=0;
		blockoffset=_blockoff;
		memcpy(getinbuf(inlen),in,inlen);
		getoutbuf(estDstlen);
		Unlock();
#ifndef __unix
		_beginthread(LaunchWork,81920,(void *)this);
#else
		pthread_create(&hd_pthread_t,NULL,LaunchWork,(void *)this);
		pthread_detach(hd_pthread_t);
#endif
	}
	virtual ~worker() {
		if(pNext) delete pNext;
		//Wait task idle;
		LockIdle();
		Unlock();
		if(inbuf) delete [] inbuf;
		if(outbuf) delete [] outbuf;
		inbuf=outbuf=NULL;
		pNext=NULL;
		inbuflen=outbuflen=0;
#ifdef __unix
		pthread_mutex_unlock(&status_mutex);
		pthread_mutex_destroy(&status_mutex);
#else
		DeleteCriticalSection(&status_mutex);
		//ReleaseMutex(status_mutex);
		//CloseHandle(status_mutex);
#endif
	}
};

class blockcompress: public worker
{
	int compress;
	char *pwrkmem;
public :
	blockcompress(int cmpflag) {compress=cmpflag;pwrkmem=NULL;};
	virtual ~blockcompress() {if(pwrkmem) delete[] pwrkmem;};
	int work() {
		//memcpy(outbuf,inbuf,sizeof(block_hdr));
		//char *cmprsbf=outbuf+sizeof(block_hdr);
		//char *bf=inbuf+sizeof(block_hdr);
		memcpy(outbuf,inbuf,blockoffset);
		char *cmprsbf=outbuf+blockoffset;
		char *bf=inbuf+blockoffset;
		block_hdr * pbh=(block_hdr * )(inbuf);
		unsigned int len=pbh->origlen;

		int rt=0;
		//dstlen=outbuflen-sizeof(block_hdr);
		dstlen=outbuflen-blockoffset;
		/******bz2 compress *********/
		if(compress==10) {
			unsigned int dst2=dstlen;
			rt=BZ2_bzBuffToBuffCompress(cmprsbf,&dst2,bf,len,1,0,0);
			if(dstlen>outbuflen-blockoffset) 
			  ThrowWith("Compress error,inlen:%d,outlen:%d.",len,dstlen);
			dstlen=dst2;
		//	printf("com %d->%d.\n",len,dst2);
		}			
		/****   UCL compress **********/
		else if(compress==8) {
			unsigned int dst2=dstlen;
			rt = ucl_nrv2d_99_compress((Bytef *)bf,len,(Bytef *)cmprsbf, &dst2,NULL,5,NULL,NULL);
			if(dstlen>outbuflen-blockoffset) 
			  ThrowWith("Compress error,inlen:%d,outlen:%d.",len,dstlen);
			dstlen=dst2;
		}			
		/******* lzo compress ****/
		else if(compress==5) {
			if(!pwrkmem) {
				pwrkmem = //new char[LZO1X_999_MEM_COMPRESS];
				new char[LZO1X_MEM_COMPRESS+2048];
				memset(pwrkmem,0,LZO1X_MEM_COMPRESS+2048);
			}
			lzo_uint dst2=dstlen;
			rt=lzo1x_1_compress((const unsigned char*)bf,len,(unsigned char *)cmprsbf,&dst2,pwrkmem);
			dstlen=dst2;
			if(dstlen>outbuflen-blockoffset) 
			  ThrowWith("Compress error,inlen:%d,outlen:%d.",len,dstlen);
		}
	        /*** zlib compress ***/
		else if(compress==1) {
			 rt=compress2((Bytef *)cmprsbf,&dstlen,(Bytef *)bf,len,1);
			if(dstlen>outbuflen-blockoffset) 
			  ThrowWith("Compress error,inlen:%d,outlen:%d.",len,dstlen);
		}
		else 
				ThrowWith("Invalid compress flag %d",compress);
	    	if(rt!=Z_OK) {
				ThrowWith("Compress failed on compressworker,datalen:%d,compress flag%d,errcode:%d",
					len,compress,rt);
		}
		//lgprintf("workid %d,data len 1:%d===> cmp as %d",workid,len,dstlen);
	    	pbh=(block_hdr *)outbuf;
		pbh->storelen=dstlen+blockoffset-sizeof(block_hdr);
		pbh->compressflag=compress;
		dstlen+=blockoffset;
		//printf("storelen:%d,dstlen:%d\n",pbh->storelen,dstlen);
		LockStatus();
		//isidle=true;
		isdone=true;
		Unlock();
		return 1;
	}
};

#endif
