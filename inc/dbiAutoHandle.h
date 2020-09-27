// AutoHandle.h: interface for the AutoHandle class.
//
//////////////////////////////////////////////////////////////////////

#if !defined(AFX_DBIAUTOHANDLE_H__89D6B0D8_D437_40F8_9265_0474ED005698__INCLUDED_)
#define AFX_DBIAUTOHANDLE_H__89D6B0D8_D437_40F8_9265_0474ED005698__INCLUDED_
#include "wdbi_inc.h"
#if _MSC_VER > 1000
#pragma once
#endif // _MSC_VER > 1000
#define MAX_STMT_LEN 4048
#include <string.h>
#include <stdarg.h>
#ifndef TIMEOFDAY
#define TIMEOFDAY CLOCK_PROCESS_CPUTIME_ID
#endif
class AutoHandle  
{
public:
	int handle;
	int refct;
	AutoHandle() {handle=0;refct=0;}
	virtual void SetHandle(int hd,bool _ref=false) {
		if(handle &&refct==0) wdbidestroy(handle);
		handle=hd;
		if(_ref) refct=1;
		else refct=0;
	}
	void Cap() { refct++;}
	void Rel() { refct--;}
	inline operator int() {
		return handle;
	}
	//inline virtual operator =(AutoHandle &src) {
	//	SetHandle(src.handle);
	//	Cap();
	//}
	virtual int Wait() {
		return wdbiWaitLastReturn(handle);
	}
	virtual ~AutoHandle(){
		if(handle>0 && refct==0) 
			wdbidestroy(handle);
	}
};

class AutoStmt :public AutoHandle {
	char sqlstmt[MAX_STMT_LEN];
public :
	virtual ~AutoStmt() {};
	AutoStmt(int sess) {
		if(sess)
			handle=wdbiCreateStatment(sess);
	}
	void Create(int sess) {
		int hd=wdbiCreateStatment(sess);
		SetHandle(hd);
	}
	bool BindStr(int pos,char *str,int len) {
		return wdbiBindStrByPos(handle,pos,str,len);
	}
	bool BindDate(int pos,char *dt) {
		return wdbiBindDateByPos(handle,pos,dt);
	}
	bool BindInt(int pos,int *val) {
		return wdbiBindIntByPos(handle,pos,val);
	}
	bool BindDouble(int pos,double *val) {
		return wdbiBindDoubleByPos(handle,pos,val);
	}
	int Execute(int times) {
		return wdbiExecute(handle,times) ;
	}
	int ExecuteAt(int times,int offset) {
		return wdbiExecuteAt(handle,times,offset) ;
	}
#ifdef __unix
    bool vPrepare(const char *format, va_list argptr)
#else
    bool vPrepare(const char *format, va_list argptr)
#endif
	{
		vsprintf(sqlstmt,format,argptr);
		return wdbiPrepareStmt(handle,sqlstmt) ;
	}

	bool Prepare(const char *format,...) {
		#ifdef __unix
		va_list vlist;
		#else
		va_list vlist;
		#endif
		va_start(vlist,format);
		bool rt=vPrepare(format,vlist);
		va_end(vlist);
		return rt;
	};
		

};

class AutoMt :public AutoHandle {
	AutoStmt stmth;
	int dbc;
	int default_maxrows;
	int *collen;
	int *coltp;
	int colct;
	char **colptr;

public :
	AutoMt(int _dbc,int maxrows=100):AutoHandle(),stmth(_dbc){
		dbc=_dbc;default_maxrows=maxrows;collen=NULL;colptr=NULL;
		coltp=NULL;
		handle=wdbiCreateMemTable();
	}
	virtual ~AutoMt() {
		if(collen) delete []collen;
		if(colptr) delete []colptr;
		if(coltp) delete []coltp;
	};
	inline void SetMaxRows(int maxrn) {
		default_maxrows=maxrn;
	}
	inline int GetMaxRows() {return default_maxrows;}
	inline int GetRows() { return wdbiGetMemtableRows(handle);}
        inline void Reset() {
		wdbiReset(handle);
	}
	inline int GetStmt() {return stmth;}
	inline int CompareRows(int row1,int row2,int *colp,int coln) {
		
		int rt;
		for(int i=0;i<coln;i++) {
			int c=colp[i];
			int l=collen[c];
			if(coltp[i]==COLUMN_TYPE_CHAR) {
				if(rt=strcmp(colptr[c]+l*row1,
					colptr[c]+l*row2),rt!=0) return rt;
			}
			else {
				if(rt=memcmp(colptr[c]+l*row1,
					colptr[c]+l*row2,l),rt!=0) return rt;
			}
		}
		
		return 0;
	}
	inline void Clear() {
		wdbiClear(handle);
	}

	inline int CompareMt(int hd)  //0 for same ,1 for differ
	{
		if(handle<1 || colct==0) return 1;
		int colct1=wdbiGetColumnNumber(handle);
		if(colct1!=colct) return 1;
		int *collen1=new int[colct];
		char **colptr1=new char *[colct];
		int *coltp1=new int[colct];
		wdbiAddrFresh(hd,colptr1,collen1,coltp1);
		int i;
		for(i=0;i<colct;i++) {
			if(collen1[i]!=collen[i]) break;
			if(coltp1[i]!=coltp[i]) break;
		}
		delete []collen1;
		delete []colptr1;
		delete []coltp1;
		if(i==colct) return 0;
		return 1;
	}
	virtual void SetHandle(int hd,bool _ref=false) {
		AutoHandle::SetHandle(hd,_ref);
		default_maxrows=wdbiGetMaxRows(hd);
		AddrFresh();
	}

	inline BOOL CreateAndAppend(char *tablename,int _dbc=0,bool forcecreate=false,bool autocommit=true) {
		if(_dbc==0) _dbc=dbc;
		if(!wdbiTestTable(_dbc,tablename)) 
			wdbiGeneTable(handle,tablename,_dbc);
		else if(forcecreate) {
			AutoStmt st(_dbc);
			st.Prepare("drop table %s",tablename);
			st.Execute(1);
			
			wdbiGeneTable(handle,tablename,_dbc);
		}
		return wdbiAppendToDbTable(handle,tablename,_dbc,autocommit);
	}

	inline int FetchFirst() {
		return wdbiFetchFirst(handle,default_maxrows);
	}

	inline int FetchFirst(const char *format,...) {
		if(stmth==0) 
		  stmth.Create(dbc);
		  #ifdef __unix
		  va_list vlist;
		  #else
		va_list vlist;
		#endif
		va_start(vlist,format);
		bool rt=stmth.vPrepare(format,vlist);
		va_end(vlist);
		if(!rt) return -1;
	        Clear();
	//int hd=wdbiCreateMemTable();
		wdbiBuildStmt(handle,stmth,default_maxrows>10?default_maxrows:10);
		AddrFresh();
		//SetHandle(hd);
		return wdbiFetchFirst(handle,default_maxrows);
	}

	inline int FetchNext() {
		return wdbiFetchNext(handle,default_maxrows);
	}

	inline void SetIKGroupParam(int dtsrc,char *srcgrpcols,
		char *srcsumcols,char *srckey=NULL,int dtref=0,char *refgrpcols=NULL) {
		wdbiSetGroupSrc(handle,dtsrc);
		wdbiSetGroupSrcCol(handle,srcgrpcols);
		if(srckey) 
			wdbiSetIKGroupRef(handle,dtref,srckey);
		if(refgrpcols)
			wdbiSetGroupRefCol(handle,refgrpcols);
//		if(srcsumcols)
			wdbiSetSrcSumCol(handle,srcsumcols);
		Build();
	}

	inline void SetGroupParam(int dtsrc,char *srcgrpcols,
		char *srcsumcols,char *srckey=NULL,int dtref=0,char *refgrpcols=NULL) {
		wdbiSetGroupSrc(handle,dtsrc);
		wdbiSetGroupSrcCol(handle,srcgrpcols);
		if(srckey) 
			wdbiSetSortedGroupRef(handle,dtref,srckey);
		if(refgrpcols)
			wdbiSetGroupRefCol(handle,refgrpcols);
//		if(srcsumcols)
			wdbiSetSrcSumCol(handle,srcsumcols);
		Build();
	}

	inline void SetDBC(int _dbc)
	{
		stmth.Create(_dbc);
		dbc=_dbc;
	}

	inline void Build(int stmt) {
		wdbiBuildStmt(handle,stmt,default_maxrows>10?default_maxrows:10);
		AddrFresh();
	}
	
	inline void Build() {
		wdbiBuild(handle,default_maxrows);
		AddrFresh();
	}
	inline int GetColumnNum() {
		return wdbiGetColumnNumber(handle);
	}
	inline void AddrFresh() {
		colct=wdbiGetColumnNumber(handle);
		if(colct<1) return;
		if(collen) delete []collen;
		if(colptr) delete []colptr;
		if(coltp) delete []coltp;
		collen=new int[colct];
		colptr=new char *[colct];
		coltp=new int[colct];
		wdbiAddrFresh(handle,colptr,collen,coltp);
	}

	inline void QuickCopyFrom(AutoMt *from,int toStart,int fromrn) {
		for(int i=0;i<colct;i++) {
			int len=collen[i];
			memcpy(colptr[i]+len*toStart,from->colptr[i]+len*fromrn,
				len);
		}
	}

	inline int FetchAll() {
		return wdbiFetchAll(handle);
	}
	inline int FetchAll(const char *format,...) {
		if(stmth==0) 
			stmth.Create(dbc);
		#ifdef __unix
		va_list vlist;
		#else
		va_list vlist;
		#endif
		va_start(vlist,format);
		bool rt=stmth.vPrepare(format,vlist);
		va_end(vlist);
		if(!rt) return -1;
		//int hd=wdbiCreateMemTable();
		Clear();
		wdbiBuildStmt(handle,stmth,default_maxrows>10?default_maxrows:10);
		AddrFresh();
		//SetHandle(hd);
		return FetchAll();
	}
	
	inline int GetStrAddr(char *col,unsigned int rowst,char **pstr,int *celllen) {
		return wdbiGetStrAddrByName(handle,col,rowst,pstr,celllen) ;
	}
	inline int GetDateAddr(char *col,unsigned int rowst,char **pstr) {
		return wdbiGetDateAddrByName(handle,col,rowst,pstr) ;
	}
	inline int GetDoubleAddr(char *col,unsigned int rowst,double **pstr) {
		return wdbiGetDoubleAddrByName(handle,col,rowst,pstr);
	}
	inline int GetIntAddr(char *col,unsigned int rowst,int **pstr) {
		return wdbiGetIntAddrByName(handle,col,rowst,pstr) ;
	}
	
	inline int GetStrAddr(int col,unsigned int rowst,char **pstr,int *celllen) {
		return wdbiGetStrAddrByPos(handle,col,rowst,pstr,celllen) ;
	}
	inline int GetDateAddr(int col,unsigned int rowst,char **pstr) {
		return wdbiGetDateAddrByPos(handle,col,rowst,pstr) ;
	}
	inline int GetDoubleAddr(int col,unsigned int rowst,double **pstr) {
		return wdbiGetDoubleAddrByPos(handle,col,rowst,pstr);
	}
	inline int GetIntAddr(int col,unsigned int rowst,int **pstr) {
		return wdbiGetIntAddrByPos(handle,col,rowst,pstr) ;
	}

	inline void *PtrVoid(int col,unsigned int rowst) {
		return colptr[col]+rowst*collen[col];
	}

	inline char *PtrStr(int col,unsigned int rowst) {
		return colptr[col]+rowst*collen[col];
	}

	inline char *PtrStr(char *colname,unsigned int rowst) {
		int col=wdbiGetColumnPosByName(handle,colname);
		return colptr[col]+rowst*collen[col];
	}
	inline char *PtrDate(int col,unsigned int rowst) {
		return colptr[col]+rowst*7;
	}
	inline double *PtrDouble(int col,unsigned int rowst) {
		return (double *)(colptr[col]+rowst*sizeof(double));
	}
	inline double *PtrDouble(char *colname,unsigned int rowst) {
		int col=wdbiGetColumnPosByName(handle,colname);
		return (double *)(colptr[col]+rowst*sizeof(double));
	}
	inline int *PtrInt(int col,unsigned int rowst) {
		return (int *)(colptr[col]+rowst*sizeof(int)); 
	}
	inline int *PtrInt(char *colname,unsigned int rowst) {
		int col=wdbiGetColumnPosByName(handle,colname);
		return (int *)(colptr[col]+rowst*sizeof(int)); 
	}
	inline void SetStr(int col,unsigned int rowst,char *str) {
		memcpy(colptr[col]+rowst*collen[col],str,collen[col]);
	}
	inline void SetDate(int col,unsigned int rowst,char *str) {
		memcpy(colptr[col]+rowst*7,str,7);
	}
	inline void GetStr(int col,unsigned int rowst,char *str) {
		memcpy(str,colptr[col]+rowst*collen[col],collen[col]);
	}
	inline void GetDate(int col,unsigned int rowst,char *str) {
		memcpy(str,colptr[col]+rowst*7,7);
	}

	inline int GetStr(char *col,unsigned rowst,char *pstr) {
		pstr[0]=0;
		int  rt=wdbiGetStrValByName(handle,col,rowst,pstr) ;
		int l=strlen(pstr);
		for(int i=1;i<l;i++) 
			if(pstr[l-i]==' ') pstr[l-i]=0;
		return rt;
	}
	inline int GetDate(char *col,unsigned rowst,char *pstr) {
		return wdbiGetDateValByName(handle,col,rowst,pstr) ;
	}
	inline double GetDouble(char *col,unsigned rowst) {
		return wdbiGetDoubleValByName(handle,col,rowst);
	}
	inline int GetInt(char *col,unsigned rowst) {
		return wdbiGetIntValByName(handle,col,rowst) ;
	}

	inline int GetPos(char *colname,int coltype) {
		int pos=wdbiGetColumnPosByName(handle,colname);
		if(wdbiGetColumnType(handle,pos)!=coltype) {
			errprintf("Column %s have a different type",colname);
			return -1;
		}
		return pos;
	}
};

class TradeOffMt {
	bool first;
	AutoMt firstmt,secondmt;
	bool init;
public :
	TradeOffMt(int dbc,int maxrows):firstmt(dbc,maxrows),
	secondmt(dbc,maxrows) {
		Init();
	}
	void Init() {
		init=true;first=true;
	}

	AutoMt *Cur() {
		return first?&firstmt:&secondmt;
	}
	AutoMt *Next() {
		return first?&secondmt:&firstmt;
	}
	void flip() {
		first=!first;
	}
	int FetchNext() {
		return Next()->FetchNext();
	}
	int FetchFirst() {
		first=true;init=true;
		return Cur()->FetchFirst();
	}
	int Wait() {
		if(!init) {
			flip();
			return Cur()->Wait();
		}
		init=false;
		return Cur()->Wait();
	}
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
	ct.tv_sec+=ed.tv_sec-st.tv_sec;
	ct.tv_nsec+=ed.tv_nsec-st.tv_nsec;
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

#endif // !defined(AFX_AUTOHANDLE_H__89D6B0D8_D437_40F8_9265_0474ED005698__INCLUDED_)
