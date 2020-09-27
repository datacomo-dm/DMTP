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
#include "ocidfn.h"
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
//固定为1MB
// jira DM-232 ,装入时索引超过最大单一块允许记录数32768
// 对于512KB的块长度，最大索引数: 512K/2字节=256K记录*26,6.6M
#define FIX_MAXINDEXRN 7*1024*1024

#define MAX_COLS_IN_DT 500
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

//不可编辑的MT映射数据块
#define BLOCKFLAG 0x5a
//不可编辑的MySQL映射数据块
#define MYSQLBLOCKFLAG 0x5c
//可编辑的MT映射数据块
#define BLOCKFLAGEDIT 0x5e
//可编辑的MySQL映射数据块
#define MYSQLBLOCKFLAGEDIT 0x5b
//可编辑的MySQL按列映射数据块
#define MYCOLBLOCKFLAGEDIT 0x5d
//可编辑的MT映射块，带空值标记
#define BLOCKNULLEDIT 0x5f


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
	int contct;//读数据块未出现跳转,为连续的次数,用于控制异步并行读数据.
	int fnid;
	// blocklen 表示一个数据块在文件中的存储字节数(包含blockhdr).
	int readedrn,bufrn;
	unsigned int readedoffset,bufoffset,curoffset,curblocklen;
	unsigned int readedblocklen,bufblocklen,buforiglen;
	int readedfnid,buffnid;
	bool isreading;
	int mycollen[MAX_COLS_IN_DT+1];//mysql数据块按列组织，单行记录时的偏移量
	//int mycoloff[MAX_COLS_IN_DT+1];

protected:
	file_hdr fh;
	int blockflag;
	int colct;
	file_hdr_ext fhe;
	delmask_hdr dmh;
	unsigned int filesize;	
	char *blockbf,*cmprsbf;
	char *pblock;//解包后的真实块数据(不含blockhdr).
	char *offlineblock;
	char *delmaskbf; //删除标志映射区
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
  		char *mycolptr[MAX_COLS_IN_DT+1];//加上mask区，数量为实际字段数加1
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
		return -1;
	}
	
	static const char *GetBlockTypeName(int blocktp) {
		switch(blocktp) {
		case BLOCKFLAG :
			return "按列组织的未转换块(v1.2)";
		case MYSQLBLOCKFLAG :
			return "按行组织的已转换块(v1.2)";
		case BLOCKFLAGEDIT :
			return "按列组织的未转换块(v2.1)";
		case MYSQLBLOCKFLAGEDIT :
			return "按行组织的已转换块(v2.1)";
		case MYCOLBLOCKFLAGEDIT :
			return "按列组织的已转换块(v2.1)";
		}
		return "未知的块类型";
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
		int rownum=dmh.rownum;
		dmh.ReverseByteOrder();
		dp_fwrite(&dmh,sizeof(dmh),1,fp);
		dp_fwrite(delmaskbf,(rownum+7)/8,1,fp);
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
	static bool CheckBlockFlag(int flag) {return flag==BLOCKFLAG || flag==MYSQLBLOCKFLAGEDIT || flag==MYCOLBLOCKFLAGEDIT|| flag==MYSQLBLOCKFLAG || flag==BLOCKFLAGEDIT || flag==BLOCKNULLEDIT;}
	static bool EditEnabled(int flag)  {return flag==MYSQLBLOCKFLAGEDIT || flag==MYCOLBLOCKFLAGEDIT || flag==BLOCKFLAGEDIT || flag==BLOCKNULLEDIT ;}
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
		if(blocktype==BLOCKNULLEDIT) return fh.rowlen+fh.cdnum*sizeof(int);
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
	int WriteBlock(char *bf,unsigned int len,int compress,bool packed=false,char bflag=BLOCKNULLEDIT) ;
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

//>>Begin:fix jira DMA-456, 20130118 ,add by liujs
// 事件通知类型,对应do.dp_log.notifystatus 字段
enum LogNotifyStatus    
{ 
    DO_NOT_NEED_TO_NOTIFY = 0, // 不需要通知
    WAIT_TO_NOTIFY = 1,        // 等待通知
    HAS_NOTIFYED = 2,          // 已经通知
};

// 告警事件级别，对应dp.dp_event.eventlevel字段
enum AlarmEventLevel
{
    DUMP_LOG = 10,             // 导出正常处理日志
    MLOAD_LOG = 11,            // 整理正常处理日志
    LOAD_LOG = 12,             // 装入正常处理日志
    DUMP_WARNING = 20,         // 导出警告
    MLOAD_WARNING = 21,        // 整理警告
    LOAD_WARNING=22,           // 装入警告
    DUMP_ERROR = 30,           // 导出错误
    MLOAD_ERROR = 31,          // 整理错误
    LOAD_ERROR=32,             // 装入错误
};

// 告警事件类型,对应dp.dp_eventtype.eventtypeid字段，将正常，错误，警告类型分割开来处理，不能与原系统值冲突
enum AlarmEventType
{
    //=== 数据导出过程日志整理，事件类型从200开始
    DUMP_CREATE_PATH_ERROR = 200,                // 导出错误:临时路径创建失败
    DUMP_CREATE_DBC_ERROR,                       // 导出错误:源(目标)数据源连接失败        
    DUMP_SOURCE_CONNECT_CFG_ERROR,               // 导出错误:源系统数据库连接配置错误
    DUMP_SOURCE_TABLE_PARSE_ERROR,               // 导出错误:源表解析错误
    DUMP_DST_TABLE_ERROR,                        // 导出错误:目标表不存在或结构错误
    DUMP_DST_TABLE_FORMAT_MODIFIED_ERROR,        // 导出错误:源表存在数据，源表(格式化)目标表结构变化无法导入数据
    DUMP_DST_TABLE_DATA_BLOCK_SIZE_ERROR,        // 导出错误:目标表数据块大小错误
    DUMP_INDEX_BLOCK_SIZE_ERROR,                 // 导出错误:索引块大小错误
    DUMP_FILE_LINES_ERROR,                       // 导出错误:文件行数错误
    DUMP_RECORD_NUM_ERROR,                       // 导出错误:记录长度太大,无法迁移
    DUMP_UPDATE_TASK_STATUS_ERROR,               // 导出错误:更改任务状态失败
    DUMP_FILE_ERROR,                             // 导出错误:文件采集错误
    DUMP_SQL_ERROR,                              // 导出错误:数据抽取语句格式错误
    DUMP_WRITE_FILE_ERROR,                       // 导出错误:写文件数据失败
    DUMP_EXCEPTION_ERROR,                        // 导出错误:数据导出异常终止
    // TODO : ADD
    
    //---通知
    DUMP_BEGIN_DUMPING_NOTIFY = 250,             // 导出通知:开始数据导出
    DUMP_RECOVER_TAST_STATUS_NOTIFY,             // 导出通知:恢复任务状态
    DUMP_RECORD_NUM_ADJUST_NOTIFY,               // 导出通知:记录行数调整
    DUMP_FINISHED_NOTIFY,                        // 导出通知:数据抽取过程结束
    DUMP_SOURCE_TABLE_PARSER_NOTIFY,             // 导出通知:源表解析过程通知
    DUMP_DST_TABLE_RECORD_LEN_MODIFY_NOTIFY,     // 导出通知:目标表中的记录长度错误修改通知
    // TODO : ADD

    
    //=== 数据整理过程日志整理，事件类型从300开始
    MLOAD_CAN_NOT_FIND_MIDDLEDATA_ERROR = 300,   // 整理错误:找不到中间数据记录处理
    MLOAD_DP_LOADTIDXSIZE_TOO_LOW_ERROR,         // 整理错误:内存参数DP_LOADTIDXSIZE设置太低
    MLOAD_UPDATE_MIDDLE_FILE_STATUS_ERROR,       // 整理错误:修改中间文件的处理状态异常
    MLOAD_STORAGE_FORMAT_ERROR,                  // 整理错误:存储格式错误
    MLOAD_INDEX_DATA_FILE_RECORD_NUM_ERROR,      // 整理错误:索引数据文件的总记录数,与指示信息不一致
    MLOAD_CAN_NOT_MLOAD_DATA_ERROR,              // 整理错误:文件已经存在不能继续整理数据
    MLOAD_INDEX_NUM_OVER_ERROR,                  // 整理错误:索引超过最大单一块允许记录数
    MLOAD_FILE_EXISTS_ERROR,                     // 整理错误:文件已经存在不能继续整理
    MLOAD_CHECK_RESULT_ERROR,                    // 整理错误:校验记录数错误
    MLOAD_EXCEPTION_ERROR,                       // 整理错误:数据整理异常终止
    // TODO : ADD

    //--- 通知    
    MLOAD_DATA_NOTIFY=350,                       // 整理通知:数据整理过程通知
    MLOAD_DATA_RECOMBINATION_NOTIFY,             // 整理通知:数据重组整理
    MLOAD_DELETE_DATA_NOTIFY,                    // 整理通知:删除数据
    // TODO : ADD
    
    //=== 数据装入过程日志整理，事件类型从400开始
    DLOAD_QUERY_TABLE_STORAGE_ERROR = 400,       // 装入错误:存储量查询失败
    DLOAD_CREATE_PIPE_FILE_ERROR,                // 装入错误:创建管道文件失败
    DLOAD_OPEN_PIPE_FILE_ERROR,                  // 装入错误:打开管道文件失败
    DLOAD_WRITE_PIPE_DATA_ERROR,                 // 装入错误:管道写入数据失败
    DLOAD_OPEN_DATA_SOURCE_FILE_ERROR,           // 装入错误:打开源数据文件失败
    DLOAD_UPDATE_TASK_STATUS_ERROR,              // 装入错误:更新任务状态错误
    DLOAD_UPDATE_FILE_STATUS_ERROR,              // 装入错误:更新文件状态错误
    DLOAD_EXCEPTION_ERROR,                       // 装入错误:数据装入异常终止
    DLOAD_ONLINE_EXCEPTION_ERROR,                // 装入错误:上线时出现异常错误
    DLOAD_DST_TABLE_MISS_INDEX,                  // 装入错误:目标表确认索引
    DLOAD_DST_TABLE_CREATE_INDEX_ERROR,          // 装入错误:目标表创建索引失败
    DLOAD_CHECK_FIEL_ERROR,                      // 装入错误:检查文件错误
    DLOAD_CAN_NOT_FIND_DATA_FILE_ERROR,          // 装入错误:找不到数据文件
    DLOAD_OPEN_WRITE_FILE_ERROR,                 // 装入错误:打开写入文件失败
    DLOAD_OPEN_READ_FILE_ERROR,                  // 装入错误:打开读取文件失败
    DLOAD_COMPRESS_INDEX_TABLE_ERROR,            // 装入错误:索引表压缩失败
    // TODO : ADD
 
    //--- 通知
    DLOAD_DATA_NOTIFY=450,                       // 装入通知:数据装入过程通知
    DLOAD_UPDATE_TASK_STATUS_NOTIFY,             // 装入通知:更改任务状态通知
    DLOAD_UPDATE_FILE_STATUS_NOTIFY,             // 装入通知:更改文件状态通知
    DLOAD_CREATE_INDEX_NOTIFY,                   // 装入通知:创建索引过程通知
    // TODO : ADD

 
};
//<<End,fix jira DMA-456
extern void  Trim(char * Text);
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
		    
        // 获取通知类型
        int notifystatus = 0;
        AutoMt st_et(dts);
        st_et.FetchFirst("select eventlevel from dp.dp_eventtype where eventtypeid = %d limit 5",evt_type);
        if(st_et.Wait()>0)
        {
           switch(st_et.GetInt("eventlevel",0))
           {
               case DUMP_LOG:             // 导出正常处理日志
               case MLOAD_LOG:            // 整理正常处理日志
               case LOAD_LOG:             // 装入正常处理日志   
                    notifystatus=DO_NOT_NEED_TO_NOTIFY;   // 不需要通知
                    break;
                             
               case DUMP_WARNING:         // 导出警告
               case MLOAD_WARNING:        // 整理警告
               case LOAD_WARNING:         // 装入警告
               case DUMP_ERROR:           // 导出错误
               case MLOAD_ERROR:          // 整理错误
               case LOAD_ERROR:           // 装入错误
               	    notifystatus=WAIT_TO_NOTIFY;         // 等待通知
                    break; 
                     	
               default:
               	   lgprintf("告警事件级别获取失败，错误的事件类型[%d].",evt_type);
               	   break;
           }  
        }
        //st_et.Clear();    

        AutoStmt st_log(dts);  
        // add to bypass mysql5.1 report error string in sql 
        st_log.Prepare("insert into dp.dp_log(`tabid`,`datapartid`,`evt_tm`,`evt_tp`,`event`,`notifystatus`) "
                   " values (%d,%d,now(),%d,?,%d)",tabid,datapartid,evt_type,notifystatus);
        st_log.BindStr(1,msg,strlen(msg));
        
        st_log.Execute(1);
        st_log.Wait();
        return rt;
	}
	int BuildSrcDBC(int tabid,int datapartid=-1) ;
	int GetSoledIndexParam(int datapartid,dumpparam *dp,int tabid=-1) ;
	int GetFirstTaskID(TASKSTATUS ts,int &tabid,int &datapartid) ;
	int UpdateTaskStatus(TASKSTATUS ts,int tabid,int datapartid) ;
	
	int GetDumpSQL(int taskid,int partoffset,char *sqlresult) ;
	//释放内部成员对象，不能省略。
	virtual ~SysParam() {};
	//pathtype first.
	const char *GetMySQLPathName(int pathid,char *pathtype=NULL);

	//中间临时导出文件的id号
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
// DM-230
#define  MYSQL_KEYWORDS_REPLACE_LIST_FILE	"MysqlKeyWordReplace.lst"		// mysql 关键字替换列表文件名称,DM-230

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

	int  ChangeColumns(char *columnName,char* MysqlKeyWordReplaceFile = MYSQL_KEYWORDS_REPLACE_LIST_FILE);
	bool ChangeMtSqlKeyWord(int mt,char* MysqlKeyWordReplaceFile = MYSQL_KEYWORDS_REPLACE_LIST_FILE);
	int IgnoreBigSizeColumn(int dts,const char* dbname,const char* tbname,char* dp_datapart_extsql);
	void IgnoreBigSizeColumn(int dts,char* dp_datapart_extsql);
};
#endif


#endif
