#include "wdbi.h"
#include "AVLIntTree.hpp"
#include <math.h>
#include <my_global.h>
#include <m_ctype.h>
extern "C" { 
#include <decimal.h>
}
#ifdef __unix
extern struct timespec interval ;
#endif

extern const char *DT_ERR_MSG[] ;
extern const char *DTL_ERR_MSG[];

extern OCIEnv *envhp;
extern OCIError *errhp;

extern char errfile[];
extern char lgfile[];
extern bool __output_to_console;
#define MAX_COLSNAMES	1000

void DataTable::adjustColumnType(bool initadj,bool importmode) {
	rowlen=0;
	for(ub4 i=0;i<colct;i++) {
		switch(cd[i].type) {
		case SQLT_CHR:
		case SQLT_STR:
		case SQLT_AFC:
		case SQLT_AVC:
			cd[i].type=SQLT_CHR;
			if(initadj)
				cd[i].dtsize+=1;
			cd[i].prec=cd[i].dtsize;
			cd[i].scale=0;
			rowlen+=cd[i].dtsize;
			cd[i].dspsize=max(cd[i].dspsize,cd[i].dtsize+1u);
			break;
		case SQLT_NUM:
			if(cd[i].scale==0 && cd[i].prec<10) {
				cd[i].type=SQLT_INT;
				cd[i].dspsize=max(cd[i].dspsize,(ub4)11);
				cd[i].dtsize=sizeof(int);
				rowlen+=sizeof(int);
				break;
			}
			else if(cd[i].scale==0 && !importmode && initadj) {
				cd[i].type=SQLT_LNG;
				cd[i].dspsize=max(cd[i].dspsize,(ub4)16);
				cd[i].dtsize=sizeof(LONG64);
				rowlen+=sizeof(LONG64);
				break;
			}
			cd[i].dspsize=max(cd[i].dspsize,cd[i].prec+1u);
			cd[i].dtsize =sizeof(double);
			rowlen+=sizeof(double);
			break;
		case SQLT_FLT:
			cd[i].dspsize=max(cd[i].dspsize,(ub4)11);
			cd[i].dtsize =sizeof(double);
			rowlen+=sizeof(double);
			break;
		case SQLT_INT:
			cd[i].dspsize=max(cd[i].dspsize,(ub4)11);
			cd[i].dtsize=sizeof(int);
			rowlen+=sizeof(int);
			break;
		case SQLT_LNG:
			cd[i].dspsize=max(cd[i].dspsize,(ub4)16);
			cd[i].dtsize=sizeof(LONG64);
			rowlen+=sizeof(LONG64);
			break;
		case SQLT_DAT:
			cd[i].dspsize=max(cd[i].dspsize,(ub4)20);
			cd[i].dtsize=7;
			rowlen+=7;
			break;
		default:
			//Not impletemented.
			ReturnVoidErr(DT_ERR_INVALIDCOLUMNTYPE,cd[i].type,cd[i].colname);
		}
		if(strlen(cd[i].dispname)==0) strcpy(cd[i].dispname,cd[i].colname);
	}
 }

#define MAX_ROWS_LIMIT  2*1024*1024*1024              // ����¼����2G������
bool DataTable::BuildStmt(WDBIStatement *pst,ub4 rows,bool noadj,bool importmode) {
	//if(pbf)
	//	Clear();
	
	if(pst) {
#ifndef NO_ORACLE	
		unsigned int colinstmt;
		colinstmt=pst->GetColumns();
		if(pst->GetStmtType()!=OCI_STMT_SELECT) ReturnErr(DT_ERR_NOTSELECT,0,"DataTable::BuildStmt");
		if(colinstmt<1) ReturnErr(DT_ERR_COLUMNEMPTY,0,"DataTable::BuildStmt");
		pstmt=pst;
		memmove(&cd[colinstmt],&cd[0],colct*sizeof(cd[0]));
		colct+=colinstmt;
		pst->CopyColumnDesc(&cd[0]);
		for(unsigned int i=colinstmt;i<colct;i++) {
			for (unsigned j=0;j<colinstmt;j++) {
				if(STRICMP(cd[i].colname,cd[j].colname)==0) ReturnErr(DT_ERR_COLNAMEDUPLICATE,i,cd[i].colname);
			}
		}
#endif
	} else pstmt=NULL;
	maxrows=rows;
	//>> begin: fix dma-458 20130128,1. wdbi�⣬datatable ����¼���޸�Ϊ2G���������������־��������¼����Ϊ2G��
	if(maxrows > MAX_ROWS_LIMIT-8 )
	{
		maxrows = MAX_ROWS_LIMIT-8;
	    lgprintf("��¼������2G���澯���Ѿ�������¼�����޸�Ϊ2G��.");	
	}
	//<< End: fix dma-458	
	maxrows+=(8-maxrows%8);
	adjustColumnType(pst!=NULL,importmode);
	try {
		if(pbf) delete [] pbf;	
		bflen=(long)rowlen*(maxrows+10); //Ԥ���ռ�������
		pbf=new char[bflen];
		if(pnullind) delete []pnullind;
		pnullind=new int[maxrows*colct];
		memset(pnullind,0,sizeof(int)*colct*maxrows);// reset buffer---assume all recorad is not null
		if(!pbf) ReturnErr(DT_ERR_OUTOFMEMORY,bflen,"Main data area: pbf");
		if(pQDelKey) {
			delete [] pQDelKey;
			pQDelKey=new int [maxrows];
			if(!pQDelKey) ReturnErr(DT_ERR_OUTOFMEMORY,maxrows*sizeof(int),"Quick delete buffer.");
			memset(pQDelKey,0,maxrows*sizeof(int));
		}
		if(pSortedPos) {
			delete [] pSortedPos;
			pSortedPos=new unsigned int [maxrows];
			if(!pSortedPos) ReturnErr(DT_ERR_OUTOFMEMORY,maxrows*sizeof(int),"Normal sort area.");
		}
		if(pPKSortedPos) {
			delete [] pPKSortedPos;
			pPKSortedPos=new unsigned int [maxrows];
			if(!pPKSortedPos) ReturnErr(DT_ERR_OUTOFMEMORY,maxrows*sizeof(int),"PK sort area.");
		}
		//qdelmax=3000;
		if(pstmt) {
			RestorePtr();
			memset(pbf,0,rowlen*maxrows);
		}
		rowct=0;
	}
	catch(...) {
		ReturnErr(DT_ERR_OUTOFMEMORY,bflen,"DataTable::BuildStmt");
	}
	
	//int mu=GetMemUsed();
	//if(mu>1000000) 
	//	lgprintf("Build MT Used :%d bytes",mu);
	return true;
}

unsigned int DataTable::FetchAll(int st) {
	if(!pstmt) ReturnErr(DT_ERR_EMPTYSTMT,st,"DataTable::FetchAll");
	//if(st==0) 
	rowct=st;
	FetchPrepare(st);
	bool rtval=true;
	while(true) {
		unsigned int rows=pstmt->GetRows();
		unsigned  fetchn=min((maxrows-rows-rowct),(ub4)pstmt->GetFetchSize());
		if(fetchn<1) {break;}
		if(pstmt->Fetch(fetchn,true)==-1) ReturnErr(DT_ERR_OCIERR,0,"DataTable::FetchAll");
		if(pnullind) pstmt->GetNullFlag(pnullind,pstmt->GetRows()-rows,rowct+rows,maxrows);
		SetNullValue(pstmt->GetRows()-rows,rowct+rows);
		if(pstmt->GetRows()<rows+fetchn) {
			break;}
		FetchPrepare(pstmt->GetRows()+rowct);
	}
	rowct+=pstmt->GetRows();
	if(echo) lgprintf("%d rows fetched.\n",rowct);
	if(rowct==maxrows && !pstmt->iseof()) {
		errprintf("***Warning : Memory table is full on FetchAll of DataTable!\n");
		//if(echo) {
		//errprintf("Press q to exit or any other key to continue ...");
		//fflush(stdout);
		//int ch=getch();
		//if(ch=='Q' || ch=='q') ReturnErr(DT_ERR_MEMFULL);
		//printf("\n");
		//}
		ReturnErr(DT_ERR_MEMFULL,st," <-start pos of 'DataTable::FetchAll' ");
	}
	RestorePtr();
	//ConvertFetchedLong(st);
	return rtval==false?0:rowct;
}

void DataTable::ConvertFetchedLong(int st) {
	if(pstmt==NULL) return;
	for(int i=0;i<colct;i++) {
		if(cd[i].type==SQLT_LNG) {
			for(int j=st;j<rowct;j++) {
			  if(!IsNull(i,j))
			   pstmt->ConvertFetchedLong(((LONG64 *)cd[i].pCol)+j,1);
			}
		}
	}
}

unsigned int DataTable::FetchFirst(ub4 rn,ub4 st) {
	if(!pstmt) ReturnErr(DT_ERR_EMPTYSTMT,rn,"DataTable::FetchFirst");
	bool rtval=true;
	rowct=st;
	long oldrn=rn;
	rn=min((ub4)rn,maxrows);
	FetchPrepare(st);
	bool first=true;
	while(true) {
		unsigned int rows=pstmt->GetRows();
		unsigned int fetchn=min((rn-rows),(ub4)pstmt->GetFetchSize());
		if(fetchn<1) {break;}
		if(pstmt->Fetch(fetchn,first)==-1) ReturnErr(DT_ERR_OCIERR,rn,"DataTable::FetchFirst");
		if(pnullind) pstmt->GetNullFlag(pnullind,pstmt->GetRows()-rows,rowct+rows,maxrows);
		SetNullValue(pstmt->GetRows()-rows,rowct+rows);
		if(pstmt->GetRows()<rows+fetchn) {break;}
		FetchPrepare(pstmt->GetRows());
		first=false;
	}
	rowct+=pstmt->GetRows();
	if(rowct!=rn && echo)
		lgprintf("%d rows fetched.\n",rowct);
	if(rowct==maxrows && oldrn!=maxrows && !pstmt->iseof()) {
		errprintf("***Warning : Memory table is full on FetchFirst of DataTable!\n");
		//if(echo) {
		//printf("Press q to exit or any other key to continue ...");
		//fflush(stdout);
		//int ch=getch();
		//if(ch=='Q' || ch=='q') ReturnErr(DT_ERR_MEMFULL);
		//printf("\n");
		//}
		ReturnErr(DT_ERR_MEMFULL,rn," <-row numbers required of 'DataTable::FetchFirst' ");
	}
	RestorePtr();
	//ConvertFetchedLong(st);
	return rtval==false?0:rowct;
}

unsigned int DataTable::FetchNext(ub4 rn,ub4 st) {
	if(!pstmt) ReturnErr(DT_ERR_EMPTYSTMT,rn,"DataTable::FetchNext");
	rowct=st;
	bool retv=true;
	int oldrn=rn;
	rn=min(rowct+rn,maxrows)-rowct;
	FetchPrepare(rowct);
	int oldrowct=pstmt->GetRows();
	while(true) {
		unsigned int rows=pstmt->GetRows();
		unsigned int fetchn=min((rn+oldrowct-rows),(ub4)pstmt->GetFetchSize());
		if(fetchn<1) {break;}
		int rt=pstmt->Fetch(fetchn,false);
		if(rt==-1) ReturnErr(DT_ERR_OCIERR,rn,"DataTable::FetchNext");
		if(rt==OCI_NO_DATA) {break;}
		if(pnullind) pstmt->GetNullFlag(pnullind,pstmt->GetRows()-rows,rowct+rows-oldrowct,maxrows);
		SetNullValue(pstmt->GetRows()-rows,rowct+rows-oldrowct);
		if(pstmt->GetRows()<rows+fetchn) {break;}
		FetchPrepare(rowct+pstmt->GetRows()-oldrowct);
	}
	int rct=pstmt->GetRows()-oldrowct;
	if(rct>0) rowct+=rct;
	if(rct!=rn && echo)  lgprintf("%d rows fetched.\n",pstmt->GetRows());
	RestorePtr();
	//ConvertFetchedLong(st);
	if(rowct==maxrows && oldrn!=maxrows && !pstmt->iseof()) {
		errprintf("***Warning : Memory table is full on FetchNext of DataTable!\n");
		//if(echo) {
		//printf("Press q to exit or any other key to continue ...");
		//fflush(stdout);
		//int ch=getch();
		//if(ch=='Q' || ch=='q') ReturnErr(DT_ERR_MEMFULL);
		//printf("\n");
		//}
		ReturnErr(DT_ERR_MEMFULL,rn," <-row numbers required of 'DataTable::FetchNext' ");
	}
	return retv==false?0:rct;
}

void DataTable::GetColumnType(int *colrf,int *ctype,int colnum) {
	int i;
	for(ub4 j=0;j<colnum;j++) {
			i=colrf[j];
			switch(cd[i].type) {
			case SQLT_CHR:
			case SQLT_STR:
			case SQLT_AFC:
			case SQLT_AVC:
				ctype[j]=SQLT_CHR;
				break;
			case SQLT_FLT:
			case SQLT_NUM:
				ctype[j]=SQLT_FLT;
				break;
			case SQLT_INT:
				ctype[j]=SQLT_INT;
				break;
			case SQLT_LNG:
				ctype[j]=SQLT_LNG;
				break;
			case SQLT_DAT:
				ctype[j]=SQLT_DAT;
				break;
			default:
				ReturnVoidErr(DT_ERR_INVALIDCOLUMNTYPE,cd[i].type,cd[i].colname);
				break;
			}
	}
}

int DataTable::GetSortColType(int *ctype) {
		if(pSortedPos==NULL) ReturnIntErr(DT_ERR_COLUNSORTED,0,"DataTable::GetSortColType");
		GetColumnType(sortcolumn,ctype,nSortColumn);
		return nSortColumn;
}
//�ֶ����ͷ���5��: SQLT_CHR SQLT_FLT  SQLT_INT SQLT_LNG SQLT_DAT
int DataTable::GetColumnType(const char *colsnm,int *ctype) {
	int colrf[MAX_COLUMN],i;
	if(colsnm!=NULL && strlen(colsnm)>0) 
		ConvertColStrToInt(colsnm,colrf);
	else {
		for(i=0;i<colct;i++) colrf[i]=i;
		colrf[i]=-1;
	}
	int colnum=0;
	while(colrf[colnum]!=-1) colnum++;
	GetColumnType(colrf,ctype,colnum);
	return colnum;
}

bool DataTable::GetCompactLen(int rowstart,int rownum,const char *colsnm,int *clen) {
	int colrf[MAX_COLUMN],i;
	char str[300];
	if(colsnm!=NULL) 
		ConvertColStrToInt(colsnm,colrf);
	else {
		for(i=0;i<colct;i++) colrf[i]=i;
		colrf[i]=-1;
	}
	if(pSortedPos) {
		if(!isinorder) ReInOrder();
	}
	int colnum=0;
	while(colrf[colnum]!=-1) colnum++;
	memset(clen,0,sizeof(int)*colnum);
	long lent=0;
	for(long r=rowstart;r<rownum+rowstart;r++)
	{
		long rn=GetRawrnBySort(r);
		for(ub4 j=0;j<colnum;j++) {
			i=colrf[j];
			lent=0;
			switch(cd[i].type) {
			case SQLT_CHR:
			case SQLT_STR:
			case SQLT_AFC:
			case SQLT_AVC:
				lent=strlen(cd[i].pCol+rn*cd[i].dtsize);
				break;
			case SQLT_NUM:
				if(IsNull(j,rn)) break;
				sprintf(str,"%-.*f",cd[i].scale,((double *)cd[i].pCol)[rn]);
				lent=strlen(str);
				break;
			case SQLT_FLT:
			if(IsNull(j,rn)) break;
			{
			 double v=((double *)cd[i].pCol)[rn];
			 LONG64 vl=(LONG64)v;
			 if(v==vl)
			  sprintf(str,"%-18lld",vl);
			 else sprintf(str,"%-18f",v);
		        }
			lent=strlen(str);
			break;
			case SQLT_INT:
			if(IsNull(j,rn)) break;
				sprintf(str,"%-d",((int*)cd[i].pCol)[rn]);
				lent=strlen(str);
				break;
			case SQLT_LNG:
			if(IsNull(j,rn)) break;
				sprintf(str,"%-lld",((LONG64 *)cd[i].pCol)[rn]);
				lent=strlen(str);
				break;
			case SQLT_DAT:
				lent=20;// exam: "2003/11/12 01:00:00"
				break;
			default:
				ReturnErr(DT_ERR_INVALIDCOLUMNTYPE,cd[i].type,cd[i].colname);
				break;
			}
			lent++;
			if(strlen(cd[i].dispname)+1>lent) lent=strlen(cd[i].dispname)+1;
			if(clen[j]<lent) clen[j]=lent;
		}
	}
	return true;
}

bool DataTable::GetLine(char *str,int _rownum,bool rawpos,const char *colsnm,int *clen) {
	int colrf[MAX_COLUMN],i;
        long rownum=_rownum;
	if(colsnm!=NULL) 
		ConvertColStrToInt(colsnm,colrf);
	else {
		for(i=0;i<colct;i++) colrf[i]=i;
		colrf[i]=-1;
	}
	long colnum=0;
	while(colrf[colnum]!=-1) colnum++;
	
	str[0]=0;
	
	if(!rawpos) {
		if(pSortedPos) {
			if(!isinorder) ReInOrder();
			rownum=pSortedPos[rownum];
		}
		else if(pPKSortedPos) {
			rownum=pPKSortedPos[rownum];
		}
	}
	
	if((ub4 )rownum>rowct-1) ReturnErr(DT_ERR_OUTOFROW,rownum,colsnm) ;
	for(ub4 j=0;j<colnum;j++) {
		i=colrf[j];
		switch(cd[i].type) {
		case SQLT_CHR:
		case SQLT_STR:
		case SQLT_AFC:
		case SQLT_AVC:
			sprintf(str+strlen(str),"%-*s",clen==NULL?cd[i].dspsize:clen[j],cd[i].pCol+rownum*cd[i].dtsize);
			break;
		case SQLT_NUM:
			if(IsNull(i,rownum)) sprintf(str+strlen(str),"%-*s",clen==NULL?cd[i].dspsize:clen[j]," ");
			else sprintf(str+strlen(str),"%-*.*f",clen==NULL?cd[i].dspsize:clen[j],cd[i].scale,((double *)cd[i].pCol)[rownum]);
			break;
		case SQLT_FLT: 
			if(IsNull(i,rownum)) sprintf(str+strlen(str),"%-*s",clen==NULL?cd[i].dspsize:clen[j]," ");
			else {
			 double v=((double *)cd[i].pCol)[rownum];
			 LONG64 vl=(LONG64)v;
			 if(v==vl)
			  sprintf(str+strlen(str),"%-*lld",clen==NULL?18:clen[j],vl);
			 else sprintf(str+strlen(str),"%-*f",clen==NULL?18:clen[j],v);
		                }
			break;
		case SQLT_INT:
			if(IsNull(i,rownum)) sprintf(str+strlen(str),"%-*s",clen==NULL?cd[i].dspsize:clen[j]," ");
			else sprintf(str+strlen(str),"%-*d",clen==NULL?cd[i].dspsize:clen[j],((int *)cd[i].pCol)[rownum]);
			break;
		case SQLT_LNG:
			if(IsNull(i,rownum)) sprintf(str+strlen(str),"%-*s",clen==NULL?cd[i].dspsize:clen[j]," ");
			else sprintf(str+strlen(str),"%-*lld",clen==NULL?cd[i].dspsize:clen[j],((LONG64 *)cd[i].pCol)[rownum]);
			break;
		case SQLT_DAT:
			{
				char fmt[30];
				WOCIDate dt(cd[i].pCol+rownum*7);
				dt.GetString(fmt);
				strcat(str,fmt);
			}
			break;
		default:
			ReturnErr(DT_ERR_INVALIDCOLUMNTYPE,cd[i].type,cd[i].colname);
			break;
		}
		//strcat(str,"|");
	}
	return true;
}



// ȥ���ַ�������Ŀո�add by liujs
#define	TRIM_RIGHT_STRING(str) \
do\
{\
    int l=strlen((str));\
    for(int i=1;i<l;i++)\
     if((str)[l-i]==' ')\
	     (str)[l-i]=0;\
	 else \
	     break;	\
}while(0);

// ȥ���ո�add by liujs
#define	TRIM_RIGHT_STRING_2(str,len) \
do\
{\
    int l=strlen((str));\
    l = (l > (len)) ? l:(len);\
    for(int i=1;i<l;i++)\
     if((str)[l-i]==' ')\
	     (str)[l-i]=0;\
	 else \
	     break;	\
}while(0);

// ��ȡ�������ݣ���ȡ�ַ������ɣ����ø������ݿ��п�Ȼ�ȡ����add by liujs
bool DataTable::GetLineStr(char *str,int _rownm,bool rawpos,const char *colsnm,int *clen){
    int colrf[MAX_COLUMN],i;
    long rownum=_rownm;
	if(colsnm!=NULL){
		ConvertColStrToInt(colsnm,colrf);
	}
	else{
		for(i=0;i<colct;i++) colrf[i]=i;
		colrf[i]=-1;
	}
	long colnum=0;
	while(colrf[colnum]!=-1) colnum++;
	char *pstr=str;
	*pstr=0;
	
	if(!rawpos) {
		if(pSortedPos) {
			if(!isinorder) ReInOrder();
			rownum=pSortedPos[rownum];
		}
		else if(pPKSortedPos) {
			rownum=pPKSortedPos[rownum];
		}
	}
	
	if((ub4 )rownum>rowct-1) ReturnErr(DT_ERR_OUTOFROW,rownum,colsnm) ;
	for(ub4 j=0;j<colnum;j++) {
		i=colrf[j];
        
    // column width 
		int buffLen = clen==NULL?cd[i].dspsize:clen[j];
	
		if(IsNull(i,rownum)) {
				*pstr++=',';*pstr=0;
				continue;
		}
		else switch(cd[i].type) {
		case SQLT_CHR:
		case SQLT_STR:
		case SQLT_AFC:
		case SQLT_AVC:
			*pstr++='\"';*pstr=0;
			strcpy(pstr,cd[i].pCol+rownum*cd[i].dtsize);
			pstr+=strlen(pstr);
			*pstr++='\"';*pstr=0;
			//sprintf(pBuffTemp,"%-*s",clen==NULL?cd[i].dspsize:clen[j],cd[i].pCol+rownum*cd[i].dtsize);
			break;
		case SQLT_NUM:
				//sprintf(pBuffTemp,"%-*.*f",clen==NULL?cd[i].dspsize:clen[j],cd[i].scale,((double *)cd[i].pCol)[rownum]);
				sprintf(pstr,"%-.*f",cd[i].scale,((double *)cd[i].pCol)[rownum]);
			break;
		case SQLT_FLT: 
			{
			 double v=((double *)cd[i].pCol)[rownum];
			 LONG64 vl=(LONG64)v;
			 if(v==vl)
			     sprintf(pstr,"%lld",vl);
			 else
			 	 sprintf(pstr,"%f",v);
		  }
			break;
		case SQLT_INT:
			sprintf(pstr,"%d",((int *)cd[i].pCol)[rownum]);
			break;
		case SQLT_LNG:
			sprintf(pstr,"%lld",((LONG64 *)cd[i].pCol)[rownum]);
			break;
		case SQLT_DAT:
			{
				WOCIDate dt(cd[i].pCol+rownum*7);
				dt.GetString_csv(pstr);
			}
			break;
		default:
			ReturnErr(DT_ERR_INVALIDCOLUMNTYPE,cd[i].type,cd[i].colname);
			break;
		}
		pstr+=strlen(pstr);
		//while(*pstr==' ') *pstr--=0;
		if(j != (colnum-1)) {
		   // strcat(str,",");
		   *pstr++=',';*pstr=0;
		}    

	}
	return true;	
	
}

void DataTable::GetTitle(char *str,int l,const char *colsnm,int *clen) {
	str[0]=0;
	int len=0;
	int colrf[MAX_COLUMN],i;
	if(colsnm!=NULL) 
		ConvertColStrToInt(colsnm,colrf);
	else {
		for(i=0;i<colct;i++) colrf[i]=i;
		colrf[i]=-1;
	}
	int colnum=0;
	while(colrf[colnum]!=-1) colnum++;
	
	for(ub4 j=0;j<colnum;j++) {
		int i=colrf[j];
		int len=0;
		switch(cd[i].type) {
		case SQLT_CHR:
		case SQLT_STR:
		case SQLT_AFC:
		case SQLT_AVC:
			len=clen==NULL?cd[i].dspsize:clen[j];
			break;
		case SQLT_NUM:
			len=clen==NULL?cd[i].dspsize:clen[j];
			break;
		case SQLT_INT:
		case SQLT_LNG:
			len=clen==NULL?cd[i].dspsize:clen[j];
			break;

		case SQLT_FLT: 
			len=clen==NULL?max(cd[i].dspsize,18):clen[j];
			break;
		case SQLT_DAT:
			{
				char fmt[30];
				WOCIDate dt;
				dt.SetToNow();
				dt.GetString(fmt);
				len=strlen(fmt);
			}
			break;
		default:
			ReturnVoidErr(DT_ERR_INVALIDCOLUMNTYPE,cd[i].type,cd[i].colname);
			break;
		}	
		//protect output buffer;
		if(strlen(str)+len>l) ReturnVoidErr(DTG_ERR_OUTOFMEMTEXTOUT,cd[i].type,cd[i].colname);
		sprintf(str+strlen(str),"%-*s",len,cd[i].dispname);
	}
}

bool DataTable::FetchPrepare(ub4 rn)
{
	if(!pstmt) ReturnErr(DT_ERR_EMPTYSTMT,rn,"DataTable::FetchPrepare()");
	if(rn==0)
		memset(pbf,0,rowlen*maxrows);
	rn=min(rn,maxrows-1);
	unsigned int colst=pstmt->GetColumns();
	RestorePtr();
	//	for(ub4 i=0;i<colst;i++)
	//		cd[i].pCol+=rn*cd[i].dtsize;
	int i;
	for( i=0;i<colst;i++) {
		switch(cd[i].type) {
		case SQLT_CHR:
		case SQLT_STR:
		case SQLT_AFC:
		case SQLT_AVC:
			pstmt->DefineByPos(i+1,cd[i].pCol+(long)rn*cd[i].dtsize,cd[i].dtsize);
			break;
		case SQLT_NUM:
		case SQLT_FLT:
			pstmt->DefineByPos(i+1,(double *)(cd[i].pCol+(long)rn*cd[i].dtsize));
			break;
		case SQLT_INT:
			pstmt->DefineByPos(i+1,(int *)(cd[i].pCol+(long)rn*cd[i].dtsize));
			break;
		case SQLT_LNG:
			pstmt->DefineByPos(i+1,(LONG64 *)(cd[i].pCol+(long)rn*cd[i].dtsize));
			break;
		case SQLT_DAT:
			pstmt->DefineByPos(i+1,cd[i].pCol+(long)rn*cd[i].dtsize);
			break;
		default:
			ReturnErr(DT_ERR_INVALIDCOLUMNTYPE,cd[i].type,cd[i].colname);
			break;
		}
	}
	return true;
}

void DataTable::RestorePtr()
{
	cd[0].pCol=pbf;
	for(ub4 i=1;i<colct;i++) 
		cd[i].pCol=cd[i-1].pCol+(long)maxrows*cd[i-1].dtsize;
}

char * DataTable::LocateCell(ub4 colid, ub4 rowst)
{
	if(maxrows<=rowst) ReturnNullErr(DT_ERR_OUTOFROW,rowst,"DataTable::LocateCell");
	if(colct<=colid) ReturnNullErr(DT_ERR_OUTOFCOLUMN,colid,"DataTable::LocateCell");
	return cd[colid].pCol+(long)rowst*cd[colid].dtsize;
}

bool DataTable::AddColumn(const char *name,const char *dspname,int ctype,int length,int scale) {
	// Clear before addcolumn
	if(pbf) ReturnErr(DT_ERR_CLEARBEFOREADD,ctype,name);
	if(colct>=MAX_COLUMN) ReturnErr(DT_ERR_OUTOFCOLUMN,colct,name);
	
	if(strlen(name)>=COLNAME_LEN-1 || (dspname && strlen(dspname)>=COLNAME_LEN-1) )
		ReturnErr(DT_ERR_OUTOFCOLNAME,0,name);
	for(unsigned int i=0;i<colct;i++) {
		if(STRICMP(cd[i].colname,name)==0) ReturnErr(DT_ERR_COLNAMEDUPLICATE,i,name);
	}
	
	switch (ctype) {
	case SQLT_CHR:
	case SQLT_STR:
	case SQLT_AFC:
	case SQLT_AVC:
		if(length==-1) ReturnErr(DT_ERR_INVALIDCOLLEN,length,name);
		cd[colct].type=SQLT_CHR;
		cd[colct].dtsize=length;
		//		cd[colct].dtsize=length-1; //Increase at buid,so minus 1 here.
		//		cd[colct].dspsize=strlen(name);
		strcpy(cd[colct].colname,name);
		cd[colct].prec=0;
		cd[colct].scale=0;
		break;
	case SQLT_FLT :
		cd[colct].type=SQLT_FLT;
		cd[colct].dtsize=sizeof(double);
		//		cd[colct].dspsize=strlen(name);
		break;
	case SQLT_NUM  :
		if(length==-1) ReturnErr(DT_ERR_INVALIDCOLLEN,length,name);
		if(scale==0 && length<10) {
			cd[colct].type=SQLT_INT;
			cd[colct].dtsize=sizeof(int);
			//			cd[colct].dspsize=strlen(name);
			cd[colct].prec=length;
			cd[colct].scale=scale;
			break;
		}
		if(scale==0) {
			cd[colct].type=SQLT_LNG;
			cd[colct].dtsize=sizeof(LONG64);
			//			cd[colct].dspsize=strlen(name);
			cd[colct].prec=length;
			cd[colct].scale=scale;
			break;
		}
		cd[colct].type=SQLT_NUM;
		cd[colct].dtsize=sizeof(double);
		//		cd[colct].dspsize=cd[colct].dspsize=strlen(name);
		cd[colct].prec=length;
		cd[colct].scale=scale;
		break;
	case SQLT_INT	:
		cd[colct].type=SQLT_INT;
		cd[colct].dtsize=sizeof(int);
		cd[colct].prec=0;
		cd[colct].scale=0;
		//		cd[colct].dspsize=strlen(name);
		break;
	case SQLT_LNG	:
		cd[colct].type=SQLT_LNG;
		cd[colct].dtsize=sizeof(LONG64);
		cd[colct].prec=0;
		cd[colct].scale=0;
		//		cd[colct].dspsize=strlen(name);
		break;
	case SQLT_DAT :
		cd[colct].type=SQLT_DAT;
		cd[colct].dtsize=7;
		cd[colct].prec=0;
		cd[colct].scale=0;
		break;
	default : //Invalid column type
		ReturnErr(DT_ERR_INVALIDCOLUMNTYPE,ctype,name);
	}
	
	strcpy(cd[colct].colname,name);
	if(dspname) 
		strcpy(cd[colct].dispname,dspname);
	else
		strcpy(cd[colct].dispname,name);
	cd[colct].dspsize=strlen(cd[colct].dispname)+1;
	rowlen+=cd[colct].dtsize;
	colct++;
	return true;
}

bool DataTable::Build(ub4 rows,bool noadj,bool importmode) {
	if(!BuildStmt(NULL,rows,noadj,importmode)) return false;
	RestorePtr();
	memset(pbf,0,rowlen*maxrows);
	return true;
}

void DataTable::SetColDspName(const char *colnm,const char *nm) {
	int ind=GetColumnIndex(colnm);
	if(strlen(nm)<COLNAME_LEN-1) {
		strcpy(cd[ind].dispname,nm);
		if(strlen(nm)>=cd[ind].dspsize)
			cd[ind].dspsize=strlen(nm)+1;
	}
}

int DataTable::GetValues(const char *col,ub4 rowst,ub4 rownm,int *bf) //integer type
{
	int id=GetColumnIndex(col);
	if(id<0) return -1;
	return GetValues(id,rowst,rownm,bf);
}

int DataTable::GetValues(const char *col,ub4 rowst,ub4 rownm,LONG64 *bf) //integer type
{
	int id=GetColumnIndex(col);
	if(id<0) return -1;
	return GetValues(id,rowst,rownm,bf);
}

int DataTable::GetValues(const char *col,ub4 rowst,ub4 rownm,double *bf) //NUM FLOAT type
{	
	int id=GetColumnIndex(col);
	if(id<0) return -1;
	return GetValues(id,rowst,rownm,bf);
}

int DataTable::GetValues(const char *col,ub4 rowst,ub4 rownm,char *bf) //Date type
{
	int id=GetColumnIndex(col);
	if(id<0) return -1;
	return GetValues(id,rowst,rownm,bf);
}

int DataTable::GetValues(const char *col,ub4 rowst,ub4 rownm,char *bf,int &cellLen)//char varchar type
{
	int id=GetColumnIndex(col);
	if(id<0) return -1;
	return GetValues(id,rowst,rownm,bf,cellLen);
}

int DataTable::GetValues(ub4 colid,ub4 rowst,ub4 rownm,int *bf) //integer type
{
	if(colid>=colct)
		ReturnIntErr(DT_ERR_OUTOFCOLUMN,colid,"DataTable::GetValues[int]");
	if( rowst>=rowct ) ReturnIntErr(DT_ERR_OUTOFROW,rowst,"DataTable::GetValues[int]");
	char *tp=LocateCell(colid,rowst);
	if(!tp) return -1;
	rownm=min(rownm,rowct-rowst);
	if( cd[colid].type==SQLT_INT) 
		memcpy(bf,tp,sizeof(int)*rownm);
	else if(cd[colid].type==SQLT_NUM || cd[colid].type==SQLT_FLT) {
		for(int i=0;i<rownm;i++) 
			bf[i]=(int)((double *)tp)[i];
	}
	else  ReturnIntErr(DT_ERR_MISMATCHCOLTYPE,colid,cd[colid].colname);
	
	return rownm;
}

int DataTable::GetValues(ub4 colid,ub4 rowst,ub4 rownm,LONG64 *bf) //integer type
{
	if(colid>=colct)
		ReturnIntErr(DT_ERR_OUTOFCOLUMN,colid,"DataTable::GetValues[int]");
	if( rowst>=rowct ) ReturnIntErr(DT_ERR_OUTOFROW,rowst,"DataTable::GetValues[int]");
	char *tp=LocateCell(colid,rowst);
	if(!tp) return -1;
	rownm=min(rownm,rowct-rowst);
	if( cd[colid].type==SQLT_LNG) 
		memcpy(bf,tp,sizeof(LONG64)*rownm);
	else if(cd[colid].type==SQLT_NUM || cd[colid].type==SQLT_FLT) {
		for(int i=0;i<rownm;i++) 
			bf[i]=(LONG64)((double *)tp)[i];
	}
	else  ReturnIntErr(DT_ERR_MISMATCHCOLTYPE,colid,cd[colid].colname);
	
	return rownm;
}

int DataTable::GetValues(ub4 colid,ub4 rowst,ub4 rownm,double *bf) //NUM FLOAT type
{
	if(colid>=colct)
		ReturnIntErr(DT_ERR_OUTOFCOLUMN,colid,"DataTable::GetValues[double]");
	if( rowst>=rowct ) ReturnIntErr(DT_ERR_OUTOFROW,rowst,"DataTable::GetValues[double]");
	if( cd[colid].type!=SQLT_FLT && cd[colid].type!=SQLT_NUM && cd[colid].type!=SQLT_INT && cd[colid].type!=SQLT_LNG) 
		ReturnIntErr(DT_ERR_MISMATCHCOLTYPE,colid,cd[colid].colname);
	char *tp=LocateCell(colid,rowst);
	if(!tp) return -1;
	rownm=min(rownm,rowct-rowst);
	if(cd[colid].type==SQLT_INT) {
		for(int i=0;i<rownm;i++) 
			bf[i]=(double)((int *)tp)[i];
	}
	else if(cd[colid].type==SQLT_LNG) {
		for(int i=0;i<rownm;i++) 
			bf[i]=(double)((LONG64 *)tp)[i];
	}
	else memcpy(bf,tp,sizeof(double)*rownm);
	return rownm;
}

int DataTable::GetValues(ub4 colid,ub4 rowst,ub4 rownm,char *bf) //Date type
{
	if(colid>=colct)
		ReturnIntErr(DT_ERR_OUTOFCOLUMN,colid,"DataTable::GetValues[date]");
	if( rowst>=rowct ) ReturnIntErr(DT_ERR_OUTOFROW,rowst,"DataTable::GetValues[date]");
	if( cd[colid].type!=SQLT_DAT) 
		ReturnIntErr(DT_ERR_MISMATCHCOLTYPE,colid,cd[colid].colname);
	char *tp=LocateCell(colid,rowst);
	if(!tp) return -1;
	rownm=min(rownm,rowct-rowst);
	memcpy(bf,tp,7*rownm);
	return rownm;
}

int DataTable::GetValues(ub4 colid,ub4 rowst,ub4 rownm,char *bf,int &cellLen)//char varchar type
{
	if(colid>=colct)
		ReturnIntErr(DT_ERR_OUTOFCOLUMN,colid,"DataTable::GetValues[string]");
	if( rowst>=rowct ) ReturnIntErr(DT_ERR_OUTOFROW,rowst,"DataTable::GetValues[string]");
	if( cd[colid].type!=SQLT_CHR && cd[colid].type!=SQLT_STR &&
		cd[colid].type!=SQLT_AFC && cd[colid].type!=SQLT_AVC) 
		ReturnIntErr(DT_ERR_MISMATCHCOLTYPE,colid,cd[colid].colname);
	char *tp=LocateCell(colid,rowst);
	cellLen=cd[colid].dtsize;
	if(!tp) return -1;
	rownm=min(rownm,rowct-rowst);
	memcpy(bf,tp,cd[colid].dtsize*(long)rownm);
	return rownm;
}

int DataTable::GetColumnIndex(const char *name) {
	int grp[MAX_COLUMN];
    if(ConvertColStrToInt(name,grp)<1)
		ReturnIntErr(DT_ERR_MISMATCHCOLNAME,0,name);
	return grp[0];
	/*
	unsigned int i;
	for( i=0;i<colct;i++) 
	if(STRICMP(cd[i].colname,name)==0) break;
	if(i==colct) 
	ReturnIntErr(DT_ERR_MISMATCHCOLNAME,0,name);
	return i;
	*/
}

int DataTable::GetColumnName(ub4 colid,char *nmbf) {
	if(colid<colct) 
		// strcpy(nmbf,cd[colid].dispname); // delete by liujs ,Ĭ�������dispname �� colname��һ����
		strcpy(nmbf,cd[colid].colname);
	else ReturnIntErr(DT_ERR_MISMATCHCOLID,colid,"DataTable::GetColumnName");
	return 0;
}

int DataTable::SetColumnName(ub4 colid,char *nmbf) {
	if(colid<colct){
        if(strlen(nmbf)<COLNAME_LEN - 1)
            strcpy(cd[colid].colname,nmbf);
		else 
			return -1;
	}
	else ReturnIntErr(DT_ERR_MISMATCHCOLID,colid,"DataTable::SetColumnName");
	return 0;
}

int DataTable::GetColumnLen(ub4 colid) {
	if(colid<colct) 
		return (cd[colid].type==SQLT_NUM || cd[colid].type==SQLT_INT || cd[colid].type==SQLT_LNG) ?cd[colid].prec:cd[colid].dtsize;
	return 0;
}


int DataTable::SetValues(ub4 colid,ub4 rowst,ub4 rownm,const int *bf) //integer type
{
	if(colid>=colct)
		ReturnIntErr(DT_ERR_OUTOFCOLUMN,colid,"DataTable::SetValues[int]");
	if( rowst+rownm>maxrows ) ReturnIntErr(DT_ERR_OUTOFROW,rownm,"DataTable::SetValues[int]");
	if( cd[colid].type!=SQLT_INT) 
		ReturnIntErr(DT_ERR_MISMATCHCOLTYPE,cd[colid].type,cd[colid].colname);
	char *tp=LocateCell(colid,rowst);
	if(!tp) return -1;
	rownm=min(rownm,maxrows-rowst);
	memcpy(tp,bf,sizeof(int)*rownm);
	if(rowct<rowst+rownm) rowct=rowst+rownm;
	return rownm;
}

int DataTable::SetValues(ub4 colid,ub4 rowst,ub4 rownm,const LONG64 *bf) //integer type
{
	if(colid>=colct)
		ReturnIntErr(DT_ERR_OUTOFCOLUMN,colid,"DataTable::SetValues[int]");
	if( rowst+rownm>maxrows ) ReturnIntErr(DT_ERR_OUTOFROW,rownm,"DataTable::SetValues[int]");
	if( cd[colid].type!=SQLT_LNG) 
		ReturnIntErr(DT_ERR_MISMATCHCOLTYPE,cd[colid].type,cd[colid].colname);
	char *tp=LocateCell(colid,rowst);
	if(!tp) return -1;
	rownm=min(rownm,maxrows-rowst);
	memcpy(tp,bf,sizeof(LONG64)*rownm);
	if(rowct<rowst+rownm) rowct=rowst+rownm;
	return rownm;
}

int DataTable::SetValues(ub4 colid,ub4 rowst,ub4 rownm,const double *bf) //NUM FLOAT type
{
	if(colid>=colct)
		ReturnIntErr(DT_ERR_OUTOFCOLUMN,colid,"DataTable::SetValues[double]");
	if( rowst+rownm>maxrows ) ReturnIntErr(DT_ERR_OUTOFROW,rownm,"DataTable::SetValues[double");
	if( cd[colid].type!=SQLT_FLT && cd[colid].type!=SQLT_NUM) 
		ReturnIntErr(DT_ERR_MISMATCHCOLTYPE,cd[colid].type,cd[colid].colname);
	
	char *tp=LocateCell(colid,rowst);
	if(!tp) return -1;
	rownm=min(rownm,maxrows-rowst);
	memcpy(tp,bf,sizeof(double)*rownm);
	if(rowct<rowst+rownm) rowct=rowst+rownm;
	return rownm;
}

int DataTable::SetValues(ub4 colid,ub4 rowst,ub4 rownm,const char *bf) //Date type
{
	if(colid>=colct)
		ReturnIntErr(DT_ERR_OUTOFCOLUMN,colid,"DataTable::SetValues[date]");
	if( rowst+rownm>maxrows ) ReturnIntErr(DT_ERR_OUTOFROW,rownm,"DataTable::SetValues[date]");
	if( cd[colid].type!=SQLT_DAT) 
		ReturnIntErr(DT_ERR_MISMATCHCOLTYPE,cd[colid].type,cd[colid].colname);
	char *tp=LocateCell(colid,rowst);
	if(!tp) return -1;
	rownm=min(rownm,maxrows-rowst);
	memcpy(tp,bf,7*rownm);
	if(rowct<rowst+rownm) rowct=rowst+rownm;
	return rownm;
}

int DataTable::SetValues(ub4 colid,ub4 rowst,ub4 rownm,const char *bf,int &cellLen)//char varchar type
{
	if(colid>=colct)
		ReturnIntErr(DT_ERR_OUTOFCOLUMN,colid,"DataTable::SetValues[string]");
	if( rowst+rownm>maxrows ) ReturnIntErr(DT_ERR_OUTOFROW,rownm,"DataTable::SetValues[string]");
	if( cd[colid].type!=SQLT_CHR && cd[colid].type!=SQLT_STR &&
		cd[colid].type!=SQLT_AFC && cd[colid].type!=SQLT_AVC) 
		ReturnIntErr(DT_ERR_MISMATCHCOLTYPE,cd[colid].type,cd[colid].colname);
	char *tp=LocateCell(colid,rowst);
	cellLen=cd[colid].dtsize;
	if(!tp) return -1;
	rownm=min(rownm,maxrows-rowst);
	memcpy(tp,bf,cd[colid].dtsize*(long)rownm);
	if(rowct<rowst+rownm) rowct=rowst+rownm;
	return rownm;
}

void DataTable::ErrorCheck(const char *fn, int ln, int ercd,int parami,const char *params)
{
	SetErrPos(ln,fn);
	if(ercd==DT_ERR_OCIERR) {
		char *msg;
		GetLastError(errcode,&msg);
		strcpy(errbuf,msg);
		retval=ercd;
	}
	else
	{
		retval=errcode=ercd;
		sprintf(errbuf,"Exception : %s on DataTable '%s'.\nColumns format:\n",DT_ERR_MSG[ercd],dtname);
		GetColumnInfo(errbuf+strlen(errbuf),false);
		strcat(errbuf,".\n");
		sprintf(errbuf+strlen(errbuf),"Call param1:%d,param2:%s.\n"
			"MT: columns num=%d,maxrows=%d,currows=%d,rowlen=%d,QDel num=%d,data buffer len=%ld, in order=%d.\n"
			"    DataArea=%p,normal sort area=%p,PK sort area=%p.\n"
			"STMT: stmt object =%p,sqltext=%s.",
			parami,(params==NULL||params[0]==0)?"NULL":params,
			colct,maxrows,rowct,rowlen,qdelct,bflen,(int)isinorder,
			pbf,pSortedPos,pPKSortedPos,
			pstmt,(pstmt==NULL||pstmt->GetSqlText()==NULL)?"NULL":pstmt->GetSqlText());
		//strcpy(errbuf,DT_ERR_MSG[ercd]);
	}
	if(autoThrow) Throw();
}

int DataTable::GetAddr(const char *col,ub4 rowst,ub4 rownm,int **bf) //integer type
{
	int id=GetColumnIndex(col);
	if(id<0) return -1;
	return GetAddr(id,rowst,rownm,bf);
}

int DataTable::GetAddr(const char *col,ub4 rowst,ub4 rownm,LONG64 **bf) //integer type
{
	int id=GetColumnIndex(col);
	if(id<0) return -1;
	return GetAddr(id,rowst,rownm,bf);
}

int DataTable::GetAddr(const char *col,ub4 rowst,ub4 rownm,double **bf) //NUM FLOAT type
{	
	int id=GetColumnIndex(col);
	if(id<0) return -1;
	return GetAddr(id,rowst,rownm,bf);
}

int DataTable::GetAddr(const char *col,ub4 rowst,ub4 rownm,char **bf) //Date type
{
	int id=GetColumnIndex(col);
	if(id<0) return -1;
	return GetAddr(id,rowst,rownm,bf);
}

int DataTable::GetAddr(const char *col,ub4 rowst,ub4 rownm,char **bf,int &cellLen)//char varchar type
{
	int id=GetColumnIndex(col);
	if(id<0) return -1;
	return GetAddr(id,rowst,rownm,bf,cellLen);
}

int DataTable::GetAddr(ub4 colid,ub4 rowst,ub4 rownm,int **bf) //integer type
{
	if(colid>=colct)
		ReturnIntErr(DT_ERR_OUTOFCOLUMN,colid,"DataTable::GetAddr[int]");
	if( rowst>rowct ) ReturnIntErr(DT_ERR_OUTOFROW,rowst,"DataTable::GetAddr[int]");
	if( cd[colid].type!=SQLT_INT ) ReturnIntErr(DT_ERR_MISMATCHCOLTYPE,cd[colid].type,cd[colid].colname);
	char *tp=LocateCell(colid,rowst);
	if(!tp) return -1;
	rownm=min(rownm,rowct-rowst);
	*bf=(int *)tp;
	return rownm;
}

int DataTable::GetAddr(ub4 colid,ub4 rowst,ub4 rownm,LONG64 **bf) //integer type
{
	if(colid>=colct)
		ReturnIntErr(DT_ERR_OUTOFCOLUMN,colid,"DataTable::GetAddr[int]");
	if( rowst>rowct ) ReturnIntErr(DT_ERR_OUTOFROW,rowst,"DataTable::GetAddr[int]");
	if( cd[colid].type!=SQLT_LNG ) ReturnIntErr(DT_ERR_MISMATCHCOLTYPE,cd[colid].type,cd[colid].colname);
	char *tp=LocateCell(colid,rowst);
	if(!tp) return -1;
	rownm=min(rownm,rowct-rowst);
	*bf=(LONG64 *)tp;
	return rownm;
}

int DataTable::GetAddr(ub4 colid,ub4 rowst,ub4 rownm,double **bf) //NUM FLOAT type
{
	if(colid>=colct)
		ReturnIntErr(DT_ERR_OUTOFCOLUMN,colid,"DataTable::GetAddr[double]");
	if( rowst>rowct ) ReturnIntErr(DT_ERR_OUTOFROW,rowst,"DataTable::GetAddr[double]");
	if( cd[colid].type!=SQLT_FLT && cd[colid].type!=SQLT_NUM) 
		ReturnIntErr(DT_ERR_MISMATCHCOLTYPE,cd[colid].type,cd[colid].colname);
	char *tp=LocateCell(colid,rowst);
	if(!tp) return -1;
	rownm=min(rownm,rowct-rowst);
	*bf=(double *)tp;
	return rownm;
}

int DataTable::GetAddr(ub4 colid,ub4 rowst,ub4 rownm,char **bf) //Date type
{
	if(colid>=colct)
		ReturnIntErr(DT_ERR_OUTOFCOLUMN,colid,"DataTable::GetAddr[date]");
	if( rowst>rowct ) ReturnIntErr(DT_ERR_OUTOFROW,rowst,"DataTable::GetAddr[date]");
	if( cd[colid].type!=SQLT_DAT) 
		ReturnIntErr(DT_ERR_MISMATCHCOLTYPE,cd[colid].type,cd[colid].colname);
	char *tp=LocateCell(colid,rowst);
	if(!tp) return -1;
	rownm=min(rownm,rowct-rowst);
	*bf=tp;
	return rownm;
}

int DataTable::GetAddr(ub4 colid,ub4 rowst,ub4 rownm,char **bf,int &cellLen)//char varchar type
{
	if(colid>=colct)
		ReturnIntErr(DT_ERR_OUTOFCOLUMN,colid,"DataTable::GetAddr[string]");
	if( rowst>rowct ) ReturnIntErr(DT_ERR_OUTOFROW,rowst,"DataTable::GetAddr[string]");
	if( cd[colid].type!=SQLT_CHR && cd[colid].type!=SQLT_STR &&
		cd[colid].type!=SQLT_AFC && cd[colid].type!=SQLT_AVC) 
		ReturnIntErr(DT_ERR_MISMATCHCOLTYPE,cd[colid].type,cd[colid].colname);
	char *tp=LocateCell(colid,rowst);
	cellLen=cd[colid].dtsize;
	if(!tp) return -1;
	rownm=min(rownm,rowct-rowst);
	*bf=tp;
	return rownm;
}

bool DataTable::SetPKID(int id)
{
	if(id<0 || (unsigned int)id>=colct) ReturnErr(DT_ERR_PKOUTOFCOL,id,"DataTable::SetPKID");
	if(cd[id].type!=SQLT_INT)
		ReturnErr(DT_ERR_NOTPKCOL,cd[id].type,cd[id].colname);
	pkid=id;
	return true;
}

int DataTable::SearchPK(int key,int schopt)
{
	if(pkid==-1) ReturnIntErr(DT_ERR_PKNOTDEFINED,0,"DataTable::SearchPK");
	if(rowct==0) return schopt==2?0:-1;
	int *bf=(int *)cd[pkid].pCol;
	unsigned int head=0;
	unsigned int tail=rowct-1;
	unsigned int mid;
    if(!pPKSortedPos) {
		for(;head<tail-1;) {
			mid=(head+tail)/2;
			if(bf[mid]>key) tail=mid;
			else if(bf[mid]<key) head=mid;
			else return mid;
			if(tail==0) break;
		}
		/*
		for(;head<tail-1;) {
		mid=(head+tail)/2;
		if(bf[mid]<key) tail=mid;
		else if(bf[mid]>key) head=mid;
		else break;
	}*/
		if(bf[tail]==key) return tail;
		if(bf[head]==key) return head;
		if(schopt==2) {
			if(bf[tail]<key)
				return tail+1;
			if(bf[mid]<key) 
				return mid+1;
			if(bf[head]<key) 
				return head+1;
			return head;
		}
		//if(bf[mid]==key) return mid;
	}
	else {
		for(;head<tail-1;) {
			mid=(head+tail)/2;
			if(bf[pPKSortedPos[mid]]>key) tail=mid;
			else if(bf[pPKSortedPos[mid]]<key) head=mid;
			else return (schopt!=0)?mid:pPKSortedPos[mid];
			if(tail==0) break;
		}
		/*
		for(;head<tail-1;) {
		mid=(head+tail)/2;
		if(bf[mid]<key) tail=mid;
		else if(bf[mid]>key) head=mid;
		else break;
	}*/
		if(bf[pPKSortedPos[tail]]==key) 
			return (schopt!=0)?tail:pPKSortedPos[tail];
		if(bf[pPKSortedPos[head]]==key) 
			return (schopt!=0)?head:pPKSortedPos[head];
		if(schopt==2) {
			if(bf[pPKSortedPos[tail]]<key)
				return tail+1;
			if(bf[pPKSortedPos[mid]]<key) 
				return mid+1;
			if(bf[pPKSortedPos[head]]<key) 
				return head+1;
			return head;
		}
		//if(bf[pPKSortedPos[mid]]==key) return pPKSortedPos[mid];
	}
	return -1;
}

bool DataTable::SetPKID(const char *nm)
{
	int gpr[MAX_COLUMN];
	int partlen[MAX_COLUMN];
	memset(partlen,0,sizeof(int)*MAX_COLUMN);
	if(!ConvertColStrToInt(nm,gpr,partlen)) return false;
	//int i=GetColumnIndex(nm);
	//if(i<0) return false;
	return SetPKID(gpr[0]);
}

int DataTable::GetColumnNum()
{	
	return colct;
}

short DataTable::GetColumnType(int id)
{
	if((unsigned int)id>=colct) ReturnIntErr(DT_ERR_OUTOFCOLUMN,id,"DataTable::GetColumnType");
	return cd[id].type;
}

bool DataTable::RefAllCol(ub4 rowst,char **bf)
{
	for(unsigned int i=0;i<colct;i++) {
		if( rowst>=rowct ) ReturnErr(DT_ERR_OUTOFROW,rowst,"DataTable::RefAllCol");
		char *tp=LocateCell(i,rowst);
		if(!tp) ReturnErr(DT_ERR_INVALIDPTR_ATREFALL,0,"DataTable::RefAllCol");
		bf[i]=tp;
	}
	return true;
}

bool DataTable::IsPKSet()
{
	return pkid!=-1;
}

bool DataTable::OrderByPK()
{
	if(pkid==-1) ReturnErr(DT_ERR_ORDERONNULLPK,0,"DataTable::OrderByPK");
	if(!pPKSortedPos) {
		pPKSortedPos=new unsigned int [maxrows];
		//		if(maxrows>500000) {
		//			lgprintf("PK used :%d bytes",maxrows*sizeof(int));
		//		}
	}
	if(rowct<1) return true;
	unsigned int n=rowct-1;
	unsigned int m=rowct/2-1;
	unsigned int i;
	for( i=0;i<rowct;i++) 
		pPKSortedPos[i]=i;
	int *pk;
	GetAddr(pkid,0,1,&pk);
	// Create heap;
	for(i=m;(int)i>=0;i--) 
		RebuildHeapPK(i,n,pk);
	// Order heap
	for(i=n;i>0;i--)
	{
		//int x=pk[i]; //exchange first one and last one.
		//pk[i]=pk[0];pk[0]=x;
		unsigned int x=pPKSortedPos[i]; //exchange first one and last one.
		pPKSortedPos[i]=pPKSortedPos[0];pPKSortedPos[0]=x;
		RebuildHeapPK(0,i-1,pk);
	}
	return true;
}

void DataTable::RebuildHeapPK(unsigned int m, unsigned int n, int *pk)
{
	unsigned int i=m,j=2*i+1;
	int x=pk[pPKSortedPos[i]];
	unsigned int xi=pPKSortedPos[i];
	while(j<=n) {
		if(j<n && pk[pPKSortedPos[j]]<pk[pPKSortedPos[j+1]]) j+=1; //ȡ���������еĽϴ�����
		if(x<pk[pPKSortedPos[j]]) {
			//pk[i]=pk[j];i=j;j=i*2; //��������
			pPKSortedPos[i]=pPKSortedPos[j];i=j;j=i*2+1; //��������
		}
		else break;
	}
	//pk[i]=x;
	pPKSortedPos[i]=xi;
}

bool DataTable::SetSortColumn(int *col)
{
	for(nSortColumn=0;*col!=-1;nSortColumn++,col++) 
		if(nSortColumn<colct)
			sortcolumn[nSortColumn]=*col;
		else ReturnErr(DT_ERR_OUTOFCOLUMN,nSortColumn,"DataTable::SetSortColumn");
		if(!pSortedPos) {
			pSortedPos=new unsigned int [maxrows];
		}
		//nSortColumn++;
		for(unsigned int i=0;i<rowct;i++) 
			pSortedPos[i]=i;
		return true;
}

bool DataTable::Sort()
{
	if(nSortColumn<1) ReturnErr(DT_ERR_NOCOLUMNTOSORT,0,"DataTable::Sort");
	//if(pkid==-1) ReturnErr(DT_ERR_ORDERONNULLPK);
	if(!pSortedPos) {
		pSortedPos=new unsigned int [maxrows];
	}
	if(ptree) delete ptree;
	ptree=new AVLtree(this);
	
	for( int i=0;i<rowct;i++) 
		ptree->add_item(i);
	ptree->inorder((int *)pSortedPos);
	isinorder=true;
	//	pSortedPos[i]=i;
	// Create heap;
	/*
	if(rowct<1) return true;
	int n=rowct-1;
	int m=rowct/2-1;
	
	  for(i=m;(int)i>=0;i--) 
	  RebuildHeap(i,n);
	  // Order heap
	  for(i=n;i>0;i--)
	  {
	  //int x=pk[i]; //exchange first one and last one.
	  //pk[i]=pk[0];pk[0]=x;
	  unsigned int x=pSortedPos[i]; //exchange first one and last one.
	  pSortedPos[i]=pSortedPos[0];pSortedPos[0]=x;
	  RebuildHeap(0,i-1);
	  }
	*/
	return true;
}

void DataTable::RebuildHeap(unsigned int m, unsigned int n)
{
	unsigned int i=m,j=2*i+1;
	
	//int x=pk[pSortedPos[i]];
	unsigned int xp=pSortedPos[i];
	//CopySortRow(i,maxrows);
	unsigned int xi=pSortedPos[i];
	while(j<=n) {
		//if(j<n && pk[pSortedPos[j]]<pk[pSortedPos[j+1]]) j+=1; //ȡ���������еĽϴ�����
		if(j<n && CompareSortRow(pSortedPos[j],pSortedPos[j+1])<0) j+=1; //ȡ���������еĽϴ�����
		//if(x<pk[pSortedPos[j]]) {
		if(CompareSortRow(xp,pSortedPos[j])<0) {
			//pk[i]=pk[j];i=j;j=i*2; //��������
			pSortedPos[i]=pSortedPos[j];i=j;j=i*2+1; //��������
		}
		else break;
	}
	//pk[i]=x;
	pSortedPos[i]=xi;
}

bool DataTable::CompactBuf()
{
	if(rowct<1 || maxrows<1 || colct<1) 
		ReturnErr(DT_ERR_OUTOFRANGE,0,"DataTable::CompactBuf");
	int off=cd[0].dtsize*(long)rowct;
	int indoff=rowct*sizeof(int);
	for(ub4 i=1;i<colct;i++) {
		memcpy(pbf+off,
			cd[i].pCol,cd[i].dtsize*(long)rowct);
		if(pnullind) {
		 memcpy(pnullind+indoff,
			pnullind+maxrows*i,sizeof(int)*(long)rowct);
		 indoff+=sizeof(int)*(long)rowct;
		}
		cd[i].pCol=pbf+off;
		off+=cd[i].dtsize*(long)rowct;
	}
	maxrows=rowct;
	return true;
}

bool DataTable::CopyRowsTo(DataTable *tbTo,int toStart,int start,int rowsnum,bool cut)
{
	if(toStart+rowsnum > tbTo->maxrows || start+rowsnum>rowct)
		ReturnErr(DT_ERR_MEMFULL,rowsnum,"DataTable::CopyRowsTo");
	if(toStart==-1) toStart=tbTo->rowct;
	for(ub4 i=0;i<colct;i++) {
		memcpy(tbTo->cd[i].pCol+cd[i].dtsize*(long)toStart,
			cd[i].pCol+cd[i].dtsize*(long)start,cd[i].dtsize*(long)rowsnum);
		if(tbTo->pnullind && pnullind) {
		   memcpy(tbTo->pnullind+(long)toStart+i*tbTo->maxrows,
			pnullind+(long)start+maxrows*i,sizeof(int)*(long)rowsnum);
		}
	}
	if(toStart==tbTo->rowct) 
		tbTo->BuildAppendIndex(rowsnum);
	else if(cut || toStart+rowsnum>tbTo->rowct)
	 tbTo->rowct=toStart+rowsnum;
	return true;
}

bool DataTable::GetColumnDesc(void **pColDesc,int &cdlen,int &_colnm) {
	if(colct<1) 
		ReturnErr(DT_ERR_COLUMNEMPTY,0,"DataTable::GetColumnDesc");
	*pColDesc=(void *)(&cd);
	cdlen=sizeof(Column_Desc)*colct;
	_colnm=colct;
	return true;
}

bool DataTable::AppendRowsWithNF(char *pData,int rnnum) 
{
	int rowtmp=rowct;
	AppendRows(pData,rnnum);
	char *pt=pData+rnnum*rowlen;
	for(ub4 i=0;i<colct;i++) {
		memcpy(pnullind+i*maxrows+rowtmp,pt+sizeof(int)*i*rnnum,sizeof(int)*(long)rnnum);
	}
	return true;
}

bool DataTable::AppendRows(char *pData,int rnnum) 
{
	//pData=(void *)pbf;
	if(rowct+rnnum>maxrows) 
		ReturnErr(DT_ERR_MEMFULL,rnnum,"DataTable::AppendRows");
	//_len=rowlen*rnnum;
	int rowtmp=rowct;
	int off=0;
	for(ub4 i=0;i<colct;i++) {
		memcpy(cd[i].pCol+cd[i].dtsize*(long)rowct,pData+off,cd[i].dtsize*(long)rnnum);
		off+=cd[i].dtsize*(long)rnnum;
	}
	rowct+=rnnum;
	ReverseByteOrder(rowtmp);
	return true;
}

bool DataTable::ExportSomeRowsWithNF(char *pData,int startrn,int rnnum) 
{
	ExportSomeRows(pData,startrn,rnnum);
	char *pNF=pData+rowlen*rnnum;
	int off=0;
	for(ub4 i=0;i<colct;i++) {
		memcpy(pNF+off,
			pnullind+i*maxrows+startrn,sizeof(int)*(long)rnnum);
		off+=sizeof(int)*(long)rnnum;
	}
	return true;
}

bool DataTable::ExportSomeRows(char *pData,int startrn,int rnnum) 
{
	//pData=(void *)pbf;
	if(startrn+rnnum>rowct) 
		ReturnErr(DT_ERR_OUTOFRANGE,rnnum,"DataTable::ExportSomeRows");
	//_len=rowlen*rnnum;
	int off=0;
	//Reverse for export
	ReverseByteOrder();
	for(ub4 i=0;i<colct;i++) {
		memcpy(pData+off,
			cd[i].pCol+cd[i].dtsize*(long)startrn,cd[i].dtsize*(long)rnnum);
		off+=cd[i].dtsize*(long)rnnum;
	}
	//Resore for later use
	ReverseByteOrder();
	return true;
}

void DataTable::ExportNullFlag(void *pData,int &_len) {
	pData=pnullind;
	_len=maxrows*sizeof(int);
}

void DataTable::ImportNullFlag(void *pData) {
	if(pnullind) memcpy(pnullind,pData,sizeof(int)*maxrows);
}

bool DataTable::Export(void *pData,int &_len,int &_maxrows,int &_rowct) 
{
	pData=(void *)pbf;
	_len=rowlen*maxrows;
	_maxrows=maxrows;
	_rowct=rowct;
	return true;
}

// Add _cdlen paramter for platform compatibility.
bool DataTable::Import(void *pData,int _len,void *pColDesc,int _colnm,int _cdlen,int _maxrows,int _rowct)
{
	Clear();
	
	//int cdl=_cdlen/_colnm;
	//if(fabs(cdl-sizeof(Column_desc))>2)
	//	cdl=sizeof(Column_desc);
	int cdl=sizeof(Column_Desc);
	// Last member 'char *pbf' of Column_Desc count not cross platform.
          // I make a mistake on aix platform which forget to define WDBI64bit   
         //  and so a more length(8bytes) pointer is export to DTFile.  
          // try to fix this error.  
        if(_cdlen==(cdl+8)*_colnm) cdl+=8;	
        char *pcd=(char *)&cd;
        for(int i=0;i<_colnm;i++) {
	  memcpy(pcd+sizeof(Column_Desc)*i,(char *)pColDesc+i*cdl,sizeof(Column_Desc)-sizeof(char *));	
	  cd[i].pCol=NULL;	
        }
	//memcpy(&cd,pColDesc,sizeof(Column_Desc)*_colnm);
	colct=_colnm;
	ReverseCD();
	if(!BuildStmt(NULL,_maxrows,true,true)) return false;
	RestorePtr();
	memset(pbf,0,rowlen*maxrows);
	long off=0;
	if(_maxrows>0 && pData && _rowct>0) {
		for(ub4 i=0;i<colct;i++) {
			memcpy(cd[i].pCol,(char *)pData+off,cd[i].dtsize*(long)_rowct);
			off+=cd[i].dtsize*(long)_rowct;
		}
		rowct=_rowct;
	}
	else rowct=0;
	ReverseByteOrder();
	return true;
}

void DataTable::CopySortRow(unsigned int from, unsigned int to)
{
	for(unsigned int i=0;i<nSortColumn;i++) {
		int col=sortcolumn[i];
		SetNull(col,to,IsNull(col,from));
		switch(cd[col].type) {
		case SQLT_CHR:
		case SQLT_STR:
		case SQLT_AFC:
		case SQLT_AVC:
			strcpy(cd[col].pCol+cd[col].dtsize*(long)to,cd[col].pCol+cd[col].dtsize*(long)from);
			break;
		case SQLT_FLT:
		case SQLT_NUM:
			*((double *)(cd[col].pCol+cd[col].dtsize*(long)to))=
				*((double *)(cd[col].pCol+cd[col].dtsize*(long)from));
			break;
		case SQLT_INT:
			*((int *)(cd[col].pCol+cd[col].dtsize*(long)to))=
				*((int *)(cd[col].pCol+cd[col].dtsize*(long)from));
			break;
		case SQLT_LNG:
			*((LONG64 *)(cd[col].pCol+cd[col].dtsize*(long)to))=
				*((LONG64 *)(cd[col].pCol+cd[col].dtsize*(long)from));
			break;
		case SQLT_DAT:
			memcpy(cd[col].pCol+cd[col].dtsize*(long)to,cd[col].pCol+cd[col].dtsize*(long)from,7);
			break;
		default:
			//Not impletemented.
			ReturnVoidErr(DT_ERR_INVALIDCOLUMNTYPE,cd[col].type,cd[col].colname);
		}
	}
}


void DataTable::GetColumnTitle(int col, char *str, int len)
{
	char fmt[300];
	str[0]=0;
	int l=0;
	char fmttmp[20];
	sprintf(fmttmp,"%%%ds",cd[col].dspsize);
	sprintf(fmt,fmttmp,cd[col].dispname);
	len=strlen(fmt);
	if(l<len)
		strcat(str,fmt);
}

bool DataTable::GetCell(unsigned int row, int col, char *str,bool rawpos)
{
	str[0]=0;
	if(!rawpos) {
		if(pSortedPos) {
			row=pSortedPos[row];
		}
		else if(pPKSortedPos) {
			row=pPKSortedPos[row];
		}
	}
	if((ub4 )row>rowct-1) ReturnErr(DT_ERR_OUTOFROW,row,"DataTable::GetCell") ;
	switch(cd[col].type) {
	case SQLT_CHR:
	case SQLT_STR:
	case SQLT_AFC:
	case SQLT_AVC:
		sprintf(str+strlen(str),"%-*s",cd[col].dspsize-1,cd[col].pCol+(long)row*cd[col].dtsize);
		break;
	case SQLT_NUM:
		sprintf(str+strlen(str),"%-*.*f",cd[col].prec,cd[col].scale,((double *)cd[col].pCol)[row]);
		break;
	case SQLT_FLT:
		{
			double v=((double *)cd[col].pCol)[row];
			LONG64 vl=(LONG64)v;
			if(v==vl)
			 sprintf(str+strlen(str),"%-18lld",vl);
			else sprintf(str+strlen(str),"%-18f",v);
		}
		break;
	case SQLT_INT:
		sprintf(str+strlen(str),"%-*d",10,((int *)cd[col].pCol)[row]);
		break;
	case SQLT_LNG:
		sprintf(str+strlen(str),"%-*lld",16,((LONG64 *)cd[col].pCol)[row]);
		break;
	case SQLT_DAT:
		{
			char fmt[300];
			WOCIDate dt(cd[col].pCol+row*7);
			dt.GetString(fmt);
			strcat(str,fmt);
		}
		break;
	default:
		ReturnErr(DT_ERR_INVALIDCOLUMNTYPE,cd[col].type,cd[col].colname);
		break;
	}
	return true;
}

int DataTable::GetColumnDispWidth(int col)
{
	if((unsigned )col<colct) 
		return int(cd[col].dspsize);
	else ReturnIntErr(DT_ERR_MISMATCHCOLID,col,"DataTable::GetColumnDispWidth");
	return 0;
}

bool DataTable::GeneTable(const char *tabname,WDBISession *psess)
{
	char crttab[2000];
	WDBISession *ps=psess;
	if(psess==NULL)
		ps=pstmt->GetSession();
	sprintf(crttab,"create table %s ",tabname);
	ps->GetColumnInfo(cd,colct,crttab+strlen(crttab));
	WDBIStatement *st=ps->NewStatement();
	st->Prepare(crttab);
	st->Execute(1);
	delete st;
	return true;
}


bool DataTable::CopyToTab(const char *tabname, WDBISession *ps,bool autocommit,bool withcolname)
{
	char isttab[2000];
	char tmp[200];
	unsigned int col;
	//sprintf(isttab,"insert into %s values (",tabname);
	sprintf(isttab,"insert into %s ",tabname);
	if(withcolname) {
	 strcat(isttab," ( ");
	 for(col=0;col<colct;col++) {
		strcat(isttab,cd[col].colname);
		if(col<colct-1) strcat(isttab,",");
		else strcat(isttab,") ");
	 }
	}
	strcat(isttab," values ( ");
	for(col=0;col<colct;col++) {
		ps->FillParam(tmp,col+1);
		//		sprintf(tmp,":id%d ",col+1);
		strcat(isttab,tmp);
		if(col<colct-1) strcat(isttab,",");
	}
	strcat(isttab,")");
	WDBISession *pss;
	if(ps) pss=ps;
	else pss=pstmt->GetSession();
	WDBIStatement *st=pss->NewStatement();
	//st.SetEcho(false);
	st->Prepare(isttab);
	long i;
	for( i=StartRows;i<rowct;i+=50000) {
		long rn=(rowct-i)>50000?50000:(rowct-i);
		for(col=0;col<colct;col++) {
			st->SetBindNullFlag(col+1,pnullind+col*maxrows+i,rn);
			switch(cd[col].type) {
			case SQLT_CHR:
			case SQLT_STR:
			case SQLT_AFC:
			case SQLT_AVC:
				st->BindByPos(col+1,(char *)(cd[col].pCol+i*cd[col].dtsize),cd[col].dtsize);
				break;
			case SQLT_NUM:
			case SQLT_FLT:
				st->BindByPos(col+1,(double *)(cd[col].pCol+i*cd[col].dtsize));
				break;
			case SQLT_INT:
				st->BindByPos(col+1,(int *)(cd[col].pCol+i*cd[col].dtsize));
				break;
			case SQLT_LNG:
				st->BindByPos(col+1,(LONG64 *)(cd[col].pCol+i*cd[col].dtsize));
				break;
			case SQLT_DAT:
				st->BindByPos(col+1,(char *)(cd[col].pCol+i*cd[col].dtsize));
				break;
			default:
				ReturnErr(DT_ERR_INVALIDCOLUMNTYPE,cd[col].type,cd[col].colname);
				break;
			}
		}
		st->Execute(rn);
		if(echo) {
			lgprintf(".");
#ifdef __unix
			if(__output_to_console)
				fflush(stdout);
#endif
		}
		if(autocommit) ps->Commit();
	}
	if(echo) lgprintf("%d rows inserted.\n",rowct);
	delete st;
	return true;
}


int DataTable::ConvertColStrToInt(const char *colsnm, int *pbf,int *partlen)
{
	char cn[COLNAME_LEN+1];
	if(colsnm==NULL) {
		int i;
        for(i=0;i<colct;i++) pbf[i]=i;
		pbf[i]=-1;
        if(partlen!=NULL) memset(partlen,0,colct*sizeof(int));
		return colct;
	}
	int len=strlen(colsnm);
	int np=0;
	int ip=0;
	int i;
	for( i=0;i<len;i++) {
		if(colsnm[i]==',') {
			if(np>0) {
				cn[np]=0;
				if(cn[np-1]==')') {
					while(np>0 && cn[np]!='(') np--;
					if(np==0) 
						ReturnErr(DT_ERR_INVALIDCOLNAME,0,colsnm);
					if(partlen!=NULL) 
						//ReturnErr(DT_ERR_INVALIDCOLLEN,0,colsnm);
						partlen[ip]=abs(atoi(cn+np+1));
					cn[np]=0;
				}
				pbf[ip++]=GetColumnId(cn);
				np=0;
			}
		}
		else if(colsnm[i]!=' ') {
			cn[np++]=colsnm[i];
			if(np>COLNAME_LEN-1) 
				ReturnErr(DT_ERR_OUTOFCOLNAME,0,colsnm);
		}
	}
	if(np>0) { // last column
		cn[np]=0;
		if(cn[np-1]==')') {
			while(np>0 && cn[np]!='(') np--;
			if(np==0) 
				ReturnErr(DT_ERR_INVALIDCOLNAME,0,colsnm);
			if(partlen!=NULL) 
				//ReturnErr(DT_ERR_INVALIDCOLLEN,0,colsnm);
				partlen[ip]=abs(atoi(cn+np+1));
			cn[np]=0;
		}
		pbf[ip++]=GetColumnId(cn);
	}
	pbf[ip]=-1;
	return ip;
}

int DataTable::GetColumnId(const char *cnm)
{
	unsigned int i;
	for( i=0;i<colct;i++) 
		if(STRICMP(cd[i].colname,cnm)==0) break;
		if(i==colct) 
			ReturnIntErr(DT_ERR_INVALIDCOLNAME,0,cnm);
		return i;
}

bool DataTable::SetSortColumn(const char *colsnm)
{
	int gpr[MAX_COLUMN];
	int partlen[MAX_COLUMN];
	memset(partlen,0,sizeof(int)*MAX_COLUMN);
	if(!ConvertColStrToInt(colsnm,gpr,partlen)) return false;
	memcpy(sortcolumnpartlen,partlen,sizeof(int)*MAX_COLUMN);
	return SetSortColumn(gpr);
}

void DataTable::SetStartRows(unsigned int st)
{
	StartRows=st;
}

//�����ֶ����ͣ�׷�ӵ���ǰ��Ķ�����
bool DataTable::CopyColumnDefine(const char *_colsname, DataTable *tbFrom)
{
	if(pbf) ReturnErr(DT_ERR_CLEARBEFOREADD,0,"DataTable::CopyColumnDefine");
	if(colct>=MAX_COLUMN) ReturnErr(DT_ERR_OUTOFCOLUMN,0,_colsname);
	int grp[MAX_COLUMN];
	int partlen[MAX_COLUMN];
	memset(partlen,0,sizeof(int)*MAX_COLUMN);
	if(!tbFrom->ConvertColStrToInt(_colsname,grp,partlen)) return false;
	Column_Desc *pcd;
	int cdlen,colnm;
	tbFrom->GetColumnDesc((void **)&pcd,cdlen,colnm);
	for(int i=0;grp[i]!=-1;i++) 
	{
		int j;
		for(j=0;j<colct;j++) {
			if(STRICMP(cd[j].colname,pcd[grp[i]].colname)==0) break;
		}
		if(j!=colct) continue;
		memcpy(cd+colct,pcd+grp[i],sizeof(Column_Desc));
		colct++;
		if(colct>=MAX_COLUMN) ReturnErr(DT_ERR_OUTOFCOLUMN,0,_colsname);
	}
	return true;
}


int DataTable::TransToInt(void *ptr,int off,int ct)
{
	if(ct==SQLT_INT) return ((int *)ptr)[off];
	else if(ct==SQLT_FLT || ct==SQLT_NUM) {
		double v=((double*)ptr)[off];
		if(v!=int(v)) ReturnErr(DT_ERR_VALUELOSS,int(v),NULL);
		return (int)v;
	}
	else if(ct==SQLT_LNG) {
		LONG64 v=((LONG64*)ptr)[off];
		if(v!=int(v)) ReturnErr(DT_ERR_VALUELOSS,int(v),NULL);
		return (int)v;
	}
	ReturnErr(DT_ERR_INVDATACOLTYPE,0,NULL);
}

double DataTable::TransToFlt(void *ptr,int off,int ct)
{
	if(ct==SQLT_INT) return (double)(((int *)ptr)[off]);
	else if(ct==SQLT_FLT || ct==SQLT_NUM) return ((double *)ptr)[off];
	else if(ct==SQLT_LNG) {
		LONG64 v=((LONG64*)ptr)[off];
		if(v!=double(v)) ReturnErr(DT_ERR_VALUELOSS,int(v),NULL);
		return (double)v;
	}
	ReturnErr(DT_ERR_INVDATACOLTYPE,0,NULL);
}

LONG64 DataTable::TransToLng(void *ptr,int off,int ct)
{
	if(ct==SQLT_INT) return (LONG64)(((int *)ptr)[off]);
	else if(ct==SQLT_FLT || ct==SQLT_NUM) {
		double v=((double*)ptr)[off];
		if(v!=(LONG64)v) ReturnErr(DT_ERR_VALUELOSS,int(v),NULL);
		return (LONG64)v;
	}
	else if(ct==SQLT_LNG) return ((LONG64*)ptr)[off];
	ReturnErr(DT_ERR_INVDATACOLTYPE,0,NULL);
}

void * DataTable::TransType(void *buf,void *ptr,int off,int ct1,int ct2)
{
	if(ct1==SQLT_LNG) *(LONG64 *)buf=TransToLng(ptr,off,ct2);
	else if(ct1==SQLT_INT) *(int *)buf=TransToInt(ptr,off,ct2);
	else *(double *)buf=TransToFlt(ptr,off,ct2);
	return buf;
}

void DataTable::TransSet(void *buf,void *ptr,int off,int ct1,int ct2,int op,int collen)
{
	if(ct1==SQLT_LNG) {
		if(op==VALUESET_SET)
			*(LONG64 *)buf=TransToLng(ptr,off,ct2);
		else if(op==VALUESET_ADD)
			*(LONG64 *)buf+=TransToLng(ptr,off,ct2);
		else if(op==VALUESET_SUB)
			*(LONG64 *)buf-=TransToLng(ptr,off,ct2);
	}
	else if(ct1==SQLT_INT) {
		if(op==VALUESET_SET)
			*(int *)buf=TransToInt(ptr,off,ct2);
		else if(op==VALUESET_ADD)
			*(int *)buf+=TransToInt(ptr,off,ct2);
		else if(op==VALUESET_SUB)
			*(int *)buf-=TransToInt(ptr,off,ct2);
	}
	else if(ct1==SQLT_FLT || ct1==SQLT_NUM) {
		if(op==VALUESET_SET)
			*(double *)buf=TransToFlt(ptr,off,ct2);
		else if(op==VALUESET_ADD)
			*(double *)buf+=TransToFlt(ptr,off,ct2);
		else if(op==VALUESET_SUB)
			*(double *)buf-=TransToFlt(ptr,off,ct2);
	}
	else memcpy(buf,(char *)ptr+off*collen,collen);
}

//destKey : true �ؼ��ֶ���Ŀ���ڴ����ָ��
//			flase �ؼ��ֶ���Դ�ڴ����ָ��
// ʹ�÷ֺ�(;)�ָ��ؼ��ֶκ���ֵ�ֶ�
//  ���ֶμ��ö���(,)�ָ�
// ʹ�ùؼ��ֶε��ڴ��Ҫ�������ؼ��ֶ��趨��˳�����������
//��ʵ���з�����ֵ���ֶθ�ʽת��������: LONGLONG <->INT <->DOUBLE
//  �����ڹؼ����е�ת�����������ֶ�.
bool DataTable::ValuesSetTo(const char *_colto, const char *_colfrom, DataTable *tbFrom,bool destKey,int op)
{
	//������ this ,����Ŀ���ڴ��.
	bool usePK=false;
	char colto[MAX_COLSNAMES],colfrom[MAX_COLSNAMES];
	strcpy(colto,_colto);strcpy(colfrom,_colfrom);
	int ckto[MAX_COLUMN],cdto[MAX_COLUMN]; //column dest(to) key and value(data)
	long p;
	
	//�ֽ�Ŀ���ֶ�
	const char *strp=colto;
	while(*strp) {if(*strp==';' && *strp) break;strp++;}
	p=strp-colto;
	if(p>=strlen(colto))  //�Ҳ����ֺ�.
		ReturnErr(DT_ERR_DATACOLNULL,0,_colto);
	//ȡ��ֵ�ֶ�
	if(!ConvertColStrToInt(colto+p+(p==0?0:1),cdto)) return false;
	int dstdct=0; // source key column count
	while(cdto[dstdct]!=-1) dstdct++;
	if(dstdct<1) ReturnErr(DT_ERR_DATACOLNULL,0,_colto);
	//�ض��ַ���
	colto[p]=0;
	
	//Ŀ�������ֶα���Ϊ��ֵ��??
	//  ����Ϊ�ַ���������,��ֻ����SET,��������
	for(p=0;p<dstdct;p++) 
		if(cd[cdto[p]].type!=SQLT_NUM && 
			cd[cdto[p]].type!=SQLT_FLT && 
			cd[cdto[p]].type!=SQLT_INT &&
			cd[cdto[p]].type!=SQLT_LNG) {
			if(op!=VALUESET_SET)
			ReturnErr(DT_ERR_INVDATACOLTYPE,0,_colto);
			
		}
		//ȡ�ؼ��ֶ�
		if(!ConvertColStrToInt(colto,ckto)) return false;
		//�õ��ؼ��ֵ��ֶ���
		int dstkct=0; // source key column count
		while(ckto[dstkct]!=-1) dstkct++;
		if(dstkct<1) ReturnErr(DT_ERR_KEYCOLNULL,dstkct,colto);
		//�Ƿ�ʹ��PK,����Ƿ�������
		bool ttt=IsPKSet();
		if(destKey && IsPKSet()) 
			usePK=true;
		else if(destKey) {
			//if(dstkct!=nSortedColumn) ReturnErr(DT_ERR_SOMESORTEDCOLNOTUSED);
			if(!pSortedPos) ReturnErr(DT_ERR_COLUNSORTED,0,_colto);
			//for(p=0;p<dstkct;p++) 
			//	if(sortcolumn[p]!=ckto[p]) ReturnErr(DT_ERR_MISMATCHSORTED);
		}
		//�ֽ�Դ���ֶ�
		int cdfrom[MAX_COLUMN],ckfrom[MAX_COLUMN];
		strp=colfrom;
		while(*strp) {if(*strp && *strp==';') break;strp++;}
		p=strp-colfrom;
		if(p>=strlen(colfrom)) //�Ҳ����ֺ�.
			ReturnErr(DT_ERR_DATACOLNULL,0,_colfrom);
		//ȡ��ֵ�ֶ�
		if(!tbFrom->ConvertColStrToInt(colfrom+p+(p==0?0:1),cdfrom)) return false;
		int srcdct=0; // source key column count
		while(cdfrom[srcdct]!=-1) srcdct++;
		if(srcdct!=dstdct) ReturnErr(DT_ERR_DATACOLMISMATCH,0,_colto);
		//�ض��ַ���
		colfrom[p]=0;
		//Ҫ���õ�Դ�ֶα�������ֵ��
		for(p=0;p<srcdct;p++) 
			if(tbFrom->cd[cdfrom[p]].type!=SQLT_NUM && 
				tbFrom->cd[cdfrom[p]].type!=SQLT_FLT && 
				tbFrom->cd[cdfrom[p]].type!=SQLT_INT &&
				tbFrom->cd[cdfrom[p]].type!=SQLT_LNG
				){
			     if(op!=VALUESET_SET || tbFrom->cd[cdfrom[p]].type!=cd[cdto[p]].type || tbFrom->cd[cdfrom[p]].dtsize!=cd[cdto[p]].dtsize)
				ReturnErr(DT_ERR_INVDATACOLTYPE,0,_colto);
			}
			//ȡ�ؼ��ֶ�
			if(!tbFrom->ConvertColStrToInt(colfrom,ckfrom)) return false;
			//�õ��ؼ��ֵ��ֶ���
			int srckct=0; // source key column count
			while(ckfrom[srckct]!=-1) srckct++;
			if(destKey && srckct==0) ReturnErr(DT_ERR_KEYCOLUMNEMPTY,0,_colto);
			if(!destKey && dstkct==0) ReturnErr(DT_ERR_KEYCOLUMNEMPTY,0,_colto);
			//if(srckct!=dstkct) ReturnErr(DT_ERR_KEYCOLMISMATCH);
			//�Ƿ�ʹ��PK,����Ƿ�������
			if(!destKey && dstkct==1 && tbFrom->IsPKSet() &&
				GetColumnType(ckto[0])==SQLT_INT) {
				usePK=true;
			}
			else if(destKey && srckct==1 && IsPKSet() &&
				tbFrom->GetColumnType(ckfrom[0])==SQLT_INT) {
				usePK=true;
			}
			else {
				if(destKey) {//ʹ��Ŀ���ڴ�����ؼ�����
					if(!pSortedPos) ReturnErr(DT_ERR_COLUNSORTED,0,_colto);
					for(p=0;p<srckct;p++) 
						if(tbFrom->GetColumnType(ckfrom[p]) !=
							GetColumnType(sortcolumn[p]) && 
							!(tbFrom->IsValueCol(cdfrom[p]) && IsValueCol(sortcolumn[p])) )
							ReturnErr(DT_ERR_MISMATCHSORTED,0,_colto);
				}
				else {//ʹ��Դ�ڴ�����ؼ�����
					if(!tbFrom->pSortedPos) ReturnErr(DT_ERR_COLUNSORTED,0,"DataTable::ValuesSetTo");
					for(p=0;p<dstkct;p++) 
						if(GetColumnType(ckto[p]) !=
							tbFrom->GetColumnType(tbFrom->sortcolumn[p]) && 
							!(tbFrom->IsValueCol(sortcolumn[p]) && IsValueCol(ckto[p])) )
							ReturnErr(DT_ERR_MISMATCHSORTED,0,_colto);
				}
			}
			
			void *dptr[MAX_COLUMN];
			void *kptr[MAX_COLUMN];
			int kcoltype[MAX_COLUMN];
			int dcoltype[MAX_COLUMN];
			double trans_key[MAX_COLUMN];
			long klen[MAX_COLUMN];
			for(p=0;p<srcdct;p++) {
				tbFrom->GetAddr(cdfrom[p],&dptr[p],0);
				dcoltype[p]=tbFrom->GetColumnType(cdfrom[p]);
			}
			int rn=0;
			if(destKey) {//Ŀ��(���ڴ��)������
				rn=tbFrom->GetRows();
				for(p=0;p<srckct;p++) {
					tbFrom->GetAddr(ckfrom[p],&kptr[p],0);
					kcoltype[p]=tbFrom->GetColumnType(ckfrom[p]);
					klen[p]=tbFrom->cd[ckfrom[p]].dtsize;
				}
				for(p=0;p<rn;p++) {
					int schd=-1;
					if(usePK) {
						schd=(int )SearchPK(TransToInt(kptr[0],p,kcoltype[0]));
					}
					else {
						void *rckptr[MAX_COLUMN];
						for(int i=0;i<srckct;i++) {
							if(cd[sortcolumn[i]].type!=kcoltype[i]) {
								rckptr[i]=TransType(trans_key+i,kptr[i],p,cd[sortcolumn[i]].type,kcoltype[i]);
							}
							else rckptr[i]=(char *)kptr[i]+p*klen[i];
						}
						schd=Search(rckptr);
					}
					if(schd!=-1) { 
						//��������
						for(int i=0;i<srcdct;i++) {
							//if(cd[cdto[i]].type==SQLT_INT)
								TransSet(cd[cdto[i]].pCol+(long)schd*cd[cdto[i]].dtsize,dptr[i],p,cd[cdto[i]].type,dcoltype[i],op,cd[cdto[i]].dtsize);
							//else
							//	TransSet(cd[cdto[i]].pCol+schd*sizeof(double),dptr[i],p,cd[cdto[i]].type,dcoltype[i],op);
						}
					}
				}
			} 
			else	{
				rn=GetRows();
				for(p=0;p<dstkct;p++) {
					GetAddr(ckto[p],&kptr[p],0);
					kcoltype[p]=GetColumnType(ckto[p]);
					klen[p]=cd[ckto[p]].dtsize;
				}
				for(p=0;p<rn;p++) {
					int schd=-1;
					if(usePK) 
						schd=(int )tbFrom->SearchPK(TransToInt(kptr[0],p,kcoltype[0]));
					else {
						void *rckptr[MAX_COLUMN];
						for(int i=0;i<srckct;i++) 
							if(tbFrom->cd[tbFrom->sortcolumn[i]].type!=kcoltype[i]) {
								rckptr[i]=TransType(trans_key+i,kptr[i],p,tbFrom->cd[tbFrom->sortcolumn[i]].type,kcoltype[i]);
							}
							else rckptr[i]=(char *)kptr[i]+(long)p*klen[i];
							schd=tbFrom->Search(rckptr);
					}
					if(schd!=-1) { 
						//��������
						for(int i=0;i<srcdct;i++) {
							//if(cd[cdto[i]].type==SQLT_INT) 
								TransSet(cd[cdto[i]].pCol+p*cd[cdto[i]].dtsize,dptr[i],schd,cd[cdto[i]].type,dcoltype[i],op,cd[cdto[i]].dtsize);
							//else
							//	TransSet(cd[cdto[i]].pCol+p*sizeof(double),dptr[i],schd,cd[cdto[i]].type,dcoltype[i],op);
						}
					}
				}
			}
			return true;
}

bool DataTable::GetAddr(int colid, void **buf,int off)
{
	if(colid>=colct)
		ReturnErr(DT_ERR_OUTOFCOLUMN,colid,"DataTable::GetAddr");
	if( off>=rowct ) ReturnErr(DT_ERR_OUTOFROW,off,"DataTable::GetAddr");
	char *tp=LocateCell(colid,0);
	tp+=(long)off*cd[colid].dtsize;
	*buf=tp;
	return true;
}

int DataTable::Search(void **ptr,int schopt)
{
	if(schopt==0 && ptree) {
		int *pos=ptree->search(ptr);
		if(pos==NULL) return -1;
		return *pos;
		//Search(ptr,
	}
	int head=0;
	int tail=rowct-1;
	int mid=0;
	if(rowct==0) {
		if(schopt==2) return 0;
		return -1;
	}
	if(pSortedPos==NULL) ReturnIntErr(DT_ERR_COLUNSORTED,0,"DataTable::Search");
	if(!isinorder) ReInOrder();
	for(;head<tail-1;) {
		mid=(head+tail)/2;
		int r=CompareSortedColumn(pSortedPos[mid],ptr);
		if(r>0) tail=mid;
		else if(r<0) head=mid;
		else return (schopt!=0)?mid:pSortedPos[mid];
	}
	if(CompareSortedColumn(pSortedPos[tail],ptr)==0) 
		return (schopt!=0)?tail:pSortedPos[tail];
	if(CompareSortedColumn(pSortedPos[head],ptr)==0) 
		return (schopt!=0)?head:pSortedPos[head];
	if(schopt==2) {
		if(CompareSortedColumn(pSortedPos[tail],ptr)<0)
			return tail+1;
		if(CompareSortedColumn(pSortedPos[mid],ptr)<0) 
			return mid+1;
		if(CompareSortedColumn(pSortedPos[head],ptr)<0) 
			return head+1;
		return head;
	}
	return -1;
}

// -1 :row1<row2 ; 0 : row1=row2 ; 1 : row1>row2
int DataTable::CompareSortRow(unsigned int _row1, unsigned int _row2)
{
	static uint rem[]={1,10,100,1000,10000,100000,1000000,10000000,100000000};
	int cmp;
        long row1=_row1,row2=_row2;
	double dres;
	int ires;
	LONG64 lres=0;
	int pl=0;
	for(unsigned int i=0;i<nSortColumn;i++) {
		int col=sortcolumn[i];
		pl=sortcolumnpartlen[i];
		switch(cd[col].type) {
		case SQLT_CHR:
		case SQLT_STR:
		case SQLT_AFC:
		case SQLT_AVC:
			if(pl>0)
				cmp=memcmp(cd[col].pCol+cd[col].dtsize*(long)row1,cd[col].pCol+cd[col].dtsize*(long)row2,pl);
			else 
				cmp=strcmp(cd[col].pCol+cd[col].dtsize*(long)row1,cd[col].pCol+cd[col].dtsize*(long)row2);
			if(cmp!=0) return cmp;
			break;
		case SQLT_FLT:
		case SQLT_NUM:
			dres=*((double *)(cd[col].pCol+cd[col].dtsize*(long)row1))-
				*((double *)(cd[col].pCol+cd[col].dtsize*(long)row2));
			if(dres<0) return -1;
			else if(dres>0) return 1;
			break;
		case SQLT_INT:
			if(pl>0) {
				int i1=*(int *)(cd[col].pCol+cd[col].dtsize*(long)row1);
				i1-=i1%rem[pl];
				int i2=*(int *)(cd[col].pCol+cd[col].dtsize*(long)row2);
				i2-=i2%rem[pl];
				ires=i1<i2?-1:((i1>i2)?1:0);//i1-i2;
			}
			else {
				int i1=*(int *)(cd[col].pCol+cd[col].dtsize*(long)row1);
				int i2=*(int *)(cd[col].pCol+cd[col].dtsize*(long)row2);
				ires=i1<i2?-1:((i1>i2)?1:0);
				//ires=*((int *)(cd[col].pCol+cd[col].dtsize*(long)row1))-
				//*((int *)(cd[col].pCol+cd[col].dtsize*(long)row2));
			}
			if(ires<0) return -1;
			else if(ires>0) return 1;
			break;
		case SQLT_LNG:
			if(pl>0) {
				LONG64 i1=*(LONG64 *)(cd[col].pCol+cd[col].dtsize*(long)row1);
				i1-=i1%rem[pl];
				LONG64 i2=*(LONG64 *)(cd[col].pCol+cd[col].dtsize*(long)row2);
				i2-=i2%rem[pl];
				lres=ires=i1<i2?-1:((i1>i2)?1:0);//(i1-i2);
			}
			else {
				LONG64 i1=*(LONG64 *)(cd[col].pCol+cd[col].dtsize*(long)row1);
				LONG64 i2=*(LONG64 *)(cd[col].pCol+cd[col].dtsize*(long)row2);
				lres=i1<i2?-1:((i1>i2)?1:0);
				//lres=*((LONG64 *)(cd[col].pCol+cd[col].dtsize*(long)row1))-
				//*((LONG64 *)(cd[col].pCol+cd[col].dtsize*(long)row2));
			}
			if(lres<0) return -1;
			else if(lres>0) return 1;
			break;
		case SQLT_DAT:
			//FORCE to USE PART Datetime sorting ,to use gongan test in xi'an,
			// REMEMBER TO��DELETE!!!!!
			pl=4;//����������ʱ�޸� ��ǵ�һ��Ҫɾ��
			if(pl>0) {
				cmp=memcmp(cd[col].pCol+cd[col].dtsize*(long)row1,cd[col].pCol+cd[col].dtsize*(long)row2,3);
				if(cmp==0) {
					char *dt=cd[col].pCol+cd[col].dtsize*(long)row1;
					int parttm1=dt[3]*1000000+dt[4]*10000+dt[5]*100+dt[6];
					parttm1-=parttm1%rem[pl];
					dt=(char *)cd[col].pCol+cd[col].dtsize*(long)row2;
					int parttm2=dt[3]*1000000+dt[4]*10000+dt[5]*100+dt[6];
					parttm2-=parttm2%rem[pl];
					cmp=parttm1-parttm2;
				}
			}
			else 
				cmp=memcmp(cd[col].pCol+cd[col].dtsize*(long)row1,cd[col].pCol+cd[col].dtsize*(long)row2,7);
			if(cmp!=0) return cmp;
			break;
		default:
			//Not impletemented.
			ReturnErr(DT_ERR_INVALIDCOLUMNTYPE,cd[col].type,cd[col].colname);
		}
	}
	return 0;
}

int DataTable::CompareSortedColumn(unsigned int _row1, void **ptr)
{
	static uint rem[]={1,10,100,1000,10000,100000,1000000,10000000,100000000};
	int cmp;
        long row1=_row1;
	double dres;
	int ires;
	LONG64 lres=0;
	int pl=0;
	for(unsigned int i=0;i<nSortColumn;i++) {
		int col=sortcolumn[i];
		pl=sortcolumnpartlen[i];
		switch(cd[col].type) {
		case SQLT_CHR:
		case SQLT_STR:
		case SQLT_AFC:
		case SQLT_AVC:
			if(pl>0)
				cmp=memcmp(cd[col].pCol+cd[col].dtsize*row1,(char *)ptr[i],sortcolumnpartlen[i]);
			else 
				cmp=strcmp(cd[col].pCol+cd[col].dtsize*row1,(char *)ptr[i]);
			if(cmp!=0) return cmp;
			break;
		case SQLT_FLT:
		case SQLT_NUM:
			dres=*((double *)(cd[col].pCol+cd[col].dtsize*row1))-
				*((double *)(ptr[i]));
			if(dres<0) return -1;
			else if(dres>0) return 1;
			break;
		case SQLT_INT:
			if(pl>0) {
				int i1=*(int *)(cd[col].pCol+cd[col].dtsize*row1);
				i1-=i1%rem[pl];
				int i2=*(int *)(ptr[i]);
				i2-=i2%rem[pl];
				ires=i1-i2;
			}
			else 
				ires=*((int *)(cd[col].pCol+cd[col].dtsize*row1))-
				*((int *)(ptr[i]));
			if(ires<0) return -1;
			else if(ires>0) return 1;
			break;
		case SQLT_LNG:
			if(pl>0) {
				LONG64 i1=*(LONG64 *)(cd[col].pCol+cd[col].dtsize*row1);
				i1-=i1%rem[pl];
				LONG64 i2=*(LONG64 *)(ptr[i]);
				i2-=i2%rem[pl];
				lres=(i1-i2);
			}
			else 
				lres=*((LONG64 *)(cd[col].pCol+cd[col].dtsize*row1))-
				*((LONG64 *)(ptr[i]));
			if(lres<0) return -1;
			else if(lres>0) return 1;
			break;
		case SQLT_DAT:
			if(pl>0) {
				cmp=memcmp(cd[col].pCol+cd[col].dtsize*row1,ptr[i],3);
				if(cmp==0) {
					char *dt=cd[col].pCol+cd[col].dtsize*row1;
					int parttm1=dt[3]*1000000+dt[4]*10000+dt[5]*100+dt[6];
					parttm1-=parttm1%rem[pl];
					dt=(char *)ptr[i];
					int parttm2=dt[3]*1000000+dt[4]*10000+dt[5]*100+dt[6];
					parttm2-=parttm2%rem[pl];
					cmp=parttm1-parttm2;
				}
			}
			else 
				cmp=memcmp(cd[col].pCol+cd[col].dtsize*row1,ptr[i],7);
			if(cmp!=0) return cmp;
			break;
		default:
			//Not impletemented.
			ReturnErr(DT_ERR_INVALIDCOLUMNTYPE,cd[col].type,cd[col].colname);
		}
	}
	return 0;
}

long DataTable::GetMemUsed()
{
	long used=bflen;
	if(pSortedPos) 
		used+=sizeof(int)*maxrows;
	if(pPKSortedPos)
		used+=sizeof(int)*maxrows;
	if(pQDelKey) 
		used+=sizeof(int)*maxrows;
	if(pnullind) 
		used+=sizeof(int)*maxrows;
	return used;
}

bool DataTable::DeleteRow(int _rown)
{
	long rown=_rown;
	if(rown>=rowct || rown<0)
		ReturnErr(DT_ERR_OUTOFROW,rown,"DataTable::DeleteRow");
	int i;
	if(pPKSortedPos) {
		int key=((int *)(cd[pkid].pCol))[rown];
		int pos=SearchPK(key,1);
		if(pos<0) ReturnErr(DT_ERR_OUTOFROW,pos,"DataTable::DeleteRow");
		memmove(pPKSortedPos+pos,pPKSortedPos+pos+1,
			(rowct-pos-1)*sizeof(int));
		for(i=0;i<rowct-1;i++)
			if(pPKSortedPos[i]>=rown)
				pPKSortedPos[i]--;
			
	}
	if(pSortedPos) {
		//��PK������ɾ����¼������pSortedPos��ֵ��
		char *ptr[MAX_COLUMN];
		for(i=0;i<nSortColumn;i++) 
			ptr[i]=cd[sortcolumn[i]].pCol+rown*cd[sortcolumn[i]].dtsize;
		int pos=Search((void **)ptr,1);
		if(pos<0) ReturnErr(DT_ERR_OUTOFROW,pos,"DataTable::DeleteRow");
		memmove(pSortedPos+pos,pSortedPos+pos+1,
			(rowct-pos-1)*sizeof(int));
		for(i=0;i<rowct-1;i++)
			if(pSortedPos[i]>=rown)
				pSortedPos[i]--;
	}
	for( i=0;i<colct;i++)  {
		memmove(cd[i].pCol+rown*cd[i].dtsize,
		cd[i].pCol+(rown+1)*cd[i].dtsize,
		(rowct-rown-1)*cd[i].dtsize);
		if(pnullind) {
			memmove(pnullind+i*maxrows+rown,
			   pnullind+i*maxrows+rown+1,(rowct-rown-1)*sizeof(int));	
		}
	}
	rowct--;
	return true;
}

bool DataTable::InsertRows(void **ptr, const char *colsname,int num)
{
	int colrf[MAX_COLUMN],i;
	if(rowct+num>maxrows) ReturnErr(DT_ERR_MEMFULL,num,colsname);
	if(colsname!=NULL) 
		ConvertColStrToInt(colsname,colrf);
	else {
		for(i=0;i<colct;i++) 
			if(ptr[i]!=NULL)
				colrf[i]=i;
			else break;
			colrf[i]=-1;
	}
	int colnum=0;
	while(colrf[colnum]!=-1) colnum++;
	for(i=0;i<colnum;i++) {
		int col=colrf[i];
		memcpy(cd[col].pCol+(long)rowct*cd[col].dtsize,
		       ptr[i],cd[col].dtsize*(long)num);
		memset(pnullind+i*maxrows+rowct,0,sizeof(int)*num);
	        for(int j=0;j<num;j++) {
			 switch(cd[col].type) {
		case SQLT_CHR:
		case SQLT_STR:
		case SQLT_AFC:
		case SQLT_AVC:
		case SQLT_DAT:
			break;
		case SQLT_NUM:
		case SQLT_FLT:
			if(  ((double *)ptr[i])[j]==NULLVALUE_DOUBLE)
			  SetNull(col,rowct+j);
			 break;
		case SQLT_INT:
			if(  ((int *)ptr[i])[j]==NULLVALUE_INT)
			  SetNull(col,rowct+j);
			break;
		case SQLT_LNG:
			if(  ((LONG64 *)ptr[i])[j]==NULLVALUE_LONG)
			  SetNull(col,rowct+j);
			break;
		default:
			break;
		}
	         }
	}
	BuildAppendIndex(num);
	return true;
}

void DataTable::BuildAppendIndex(int num) {	
	int i;
	for( i=0;i<num;i++) {
		if(pPKSortedPos) {
			int key=((int *)(cd[pkid].pCol))[rowct];
			int pos=SearchPK(key,2);
			memmove(pPKSortedPos+pos+1,pPKSortedPos+pos,
				(rowct-pos)*sizeof(int));
			pPKSortedPos[pos]=rowct;
		}
		if(pSortedPos) {
			if(ptree) {
				ptree->add_item(rowct);
				isinorder=false;
			}
			else {
				char *ptr[MAX_COLUMN];
				for(int j=0;j<nSortColumn;j++) 
					ptr[j]=cd[sortcolumn[j]].pCol+(long)rowct*cd[sortcolumn[j]].dtsize;
				
				int pos=Search((void **)ptr,2);
				
				//static int ct=0,tot=0;
				//tot+=(rowct-pos)*sizeof(int);
				//if(++ct==99900) 
				//	printf("Total moved %d\n",tot);
				//pos=rowct;
				
				memmove(pSortedPos+pos+1,pSortedPos+pos,
					(rowct-pos)*sizeof(int));
				pSortedPos[pos]=rowct;
			}
		}
		rowct++;
	}
}

bool DataTable::BindToStatment(WDBIStatement *_pstmt, const char *colsname, int _rowst)
{
	long rowst=_rowst;
        if(!_pstmt) ReturnErr(DT_ERR_EMPTYSTMT,rowst,colsname);
	if(rowst>=rowct || rowst<0) ReturnErr(DT_ERR_OUTOFROW,rowst,colsname);
	int colrf[MAX_COLUMN],i;
	if(colsname!=NULL) 
		ConvertColStrToInt(colsname,colrf);
	else {
		for(i=0;i<colct;i++) 
			colrf[i]=i;
		colrf[i]=-1;
	}
	int colnum=0;
	while(colrf[colnum]!=-1) colnum++;
	for(i=0;i<colnum;i++) {
		_pstmt->SetBindNullFlag(i+1,pnullind+i*maxrows+rowst,(rowct-rowst));
		switch(cd[colrf[i]].type) {
		case SQLT_CHR:
		case SQLT_STR:
		case SQLT_AFC:
		case SQLT_AVC:
			_pstmt->BindByPos(i+1,cd[colrf[i]].pCol+cd[colrf[i]].dtsize*(long)rowst
				,cd[colrf[i]].dtsize);
			break;
		case SQLT_NUM:
		case SQLT_FLT:
			_pstmt->BindByPos(i+1,(double *)(cd[colrf[i]].pCol+cd[colrf[i]].dtsize*rowst));
			break;
		case SQLT_INT:
			_pstmt->BindByPos(i+1,(int *)(cd[colrf[i]].pCol+cd[colrf[i]].dtsize*rowst));
			break;
		case SQLT_LNG:
			_pstmt->BindByPos(i+1,(LONG64 *)(cd[colrf[i]].pCol+cd[colrf[i]].dtsize*rowst));
			break;
		case SQLT_DAT:
			_pstmt->BindByPos(i+1,cd[colrf[i]].pCol+cd[colrf[i]].dtsize*rowst);
			break;
		default:
			ReturnErr(DT_ERR_INVALIDCOLUMNTYPE,cd[i].type,cd[i].colname);
			break;
		}
	}
	return true;
}


bool DataTable::QDeleteRow(int rownm)
{
	//if(pkid==-1) ReturnErr(DT_ERR_PKNOTDEFINED);
	if(!pQDelKey) {
		pQDelKey=new int [maxrows];
		if(!pQDelKey) ReturnErr(DT_ERR_OUTOFMEMORY,rownm,"DataTable::QDeleteRow");
		memset(pQDelKey,0,maxrows*sizeof(int));
	}
	if(pQDelKey[rownm]==1) return false;
	pQDelKey[rownm]=1;
	qdelct++;
	/*
	if(IsQDelete(rownm)) return false;
	int key=rownm;//((int *)cd[pkid].pCol)[rownm];
	int ins=SearchQDel(key,2);
	memmove(pQDelKey+ins+1,pQDelKey+ins,(qdelct-ins)*sizeof(int));
	pQDelKey[ins]=key;
	qdelct++;
	if(qdelct>qdelmax-20) {
	int *ptr=new int[qdelmax+1000];
	memcpy(ptr,pQDelKey,qdelct*sizeof(int));
	delete pQDelKey;
	pQDelKey=ptr;
	qdelmax+=1000;
	}
	*/
	return true;
}

int DataTable::QDeletedRows()
{
	return qdelct;
}

bool DataTable::IsQDelete(int rowrn)
{
	//int key=((int *)cd[pkid].pCol)[rowrn];
	return pQDelKey==NULL?false:pQDelKey[rowrn]==1;//SearchQDel(rowrn,0)!=-1;
}

bool DataTable::IsNull(int col,unsigned int rowrn)
{
	return pnullind==NULL?false:pnullind[col*maxrows+rowrn]==DB_NULL_DATA;
}

void DataTable::SetNull(int col,unsigned int rowrn,bool isnull)
{
	if(pnullind) {
	  pnullind[col*maxrows+rowrn]=isnull?DB_NULL_DATA:0;
	  switch(cd[col].type) {
		case SQLT_CHR:
		case SQLT_STR:
		case SQLT_AFC:
		case SQLT_AVC:
		case SQLT_DAT:
			break;
		case SQLT_NUM:
		case SQLT_FLT:
			*((double *)(cd[col].pCol+cd[col].dtsize*rowrn))=NULLVALUE_DOUBLE;
			break;
		case SQLT_INT:
			*((int *)(cd[col].pCol+cd[col].dtsize*rowrn))=NULLVALUE_INT;
			break;
		case SQLT_LNG:
			*((LONG64 *)(cd[col].pCol+cd[col].dtsize*rowrn))=NULLVALUE_LONG;
			break;
		default:
			break;
		}
	   }
	 
}

void DataTable::SetNullValue(unsigned int rows,unsigned int st)
{
	//by pass null temporary
	//memset(pnullind,0,sizeof(int)*colct*maxrows);// reset buffer---assume all recorad is not null
	for(long i=st;i<st+rows;i++) {
	  for(int col=0;col<colct;col++) {
	  if(IsNull(col,i)) {
	  	switch(cd[col].type) {
		case SQLT_CHR:
		case SQLT_STR:
		case SQLT_AFC:
		case SQLT_AVC:
		case SQLT_DAT:
			break;
		case SQLT_NUM:
		case SQLT_FLT:
			*((double *)(cd[col].pCol+cd[col].dtsize*i))=NULLVALUE_DOUBLE;
			break;
		case SQLT_INT:
			*((int *)(cd[col].pCol+cd[col].dtsize*i))=NULLVALUE_INT;
			break;
		case SQLT_LNG:
			*((LONG64 *)(cd[col].pCol+cd[col].dtsize*i))=NULLVALUE_LONG;
			break;
		default:
			break;
		}
	   }
	}
       }
}

int DataTable::SearchQDel(int key, int schopt)
{

	if(qdelct==0 || pQDelKey==NULL) return schopt==2?0:-1;
	//if(pkid==-1) ReturnIntErr(DT_ERR_PKNOTDEFINED);
	int head=0;
	int tail=qdelct-1;
	int mid=0;
	for(;head<tail-1;) {
		mid=(head+tail)/2;
		if(pQDelKey[mid]>key) tail=mid;
		else if(pQDelKey[mid]<key) head=mid;
		else return mid;
	}
	if(pQDelKey[tail]==key) 
		return tail;
	if(pQDelKey[head]==key) 
		return head;
	if(schopt==2) {
		if(pQDelKey[tail]<key)
			return tail+1;
		if(pQDelKey[mid]<key) 
			return mid+1;
		if(pQDelKey[head]<key) 
			return head+1;
		return head;
	}
	return -1;
}

bool DataTable::CompressBf()
{
	if(qdelct<=0 || pQDelKey==NULL) return true;
	long mt=0,mts=0,mtct=0;//MoveToPosition,MovoToStart(From),MoveToCount
	//int *kbf=(int *)cd[pkid].pCol;
	long i;
	for( i=0;i<rowct;i++) {
		if(pQDelKey[i]==1) { //SearchQDel(i,0)!=-1) {
			//Found a deleted row
			if(mtct>0) { 
				//Previous row(s) not be deleted.Do move...
				for(int j=0;j<colct;j++) {
					memmove(cd[j].pCol+mt*cd[j].dtsize,
						cd[j].pCol+mts*cd[j].dtsize,mtct*cd[j].dtsize);
					if(pnullind) {
						memmove(pnullind+j*maxrows+mt,
							pnullind+j*maxrows+mts,mtct*sizeof(int));
					}	
				}
				//i-=mtct;
				mt+=mtct;
				mts=i+1;mtct=0;
			}
			else
				//Previous row(s) be deleted too.
				mts++;
		}
		else 
			//Found a undeleted row
			mtct++;
	}
	if(mtct>0) {
		//The lastest rows will be moved.
		for(int j=0;j<colct;j++) {
			memmove(cd[j].pCol+mt*cd[j].dtsize,
				cd[j].pCol+mts*cd[j].dtsize,mtct*cd[j].dtsize);
			if(pnullind) {
				memmove(pnullind+j*maxrows+mt,
					pnullind+j*maxrows+mts,mtct*sizeof(int));
			}	
		}
	}
	rowct-=qdelct;
	qdelct=0;
	memset(pQDelKey,0,maxrows*sizeof(int));
	if(pkid!=-1)
		OrderByPK();
	if(pSortedPos)
		Sort();
	return true;
}

int DataTable::GetRawrnByPK(int ind)
{
	if(pkid==-1) ReturnIntErr(DT_ERR_PKNOTDEFINED,ind,"DataTable::GetRawrnByPK");
	if(ind<0 || ind >rowct) ReturnIntErr(DT_ERR_OUTOFROW,ind,"DataTable::GetRawrnByPK");
	if(pPKSortedPos) return pPKSortedPos[ind];
	return ind;
}

int DataTable::GetRawrnBySort(int ind)
{
	if(!pSortedPos) return ind;//ReturnIntErr(DT_ERR_COLUNSORTED);
	if(ind<0 || ind >rowct) ReturnIntErr(DT_ERR_OUTOFROW,ind,"DataTable::GetRawrnBySort");
	if(!isinorder) ReInOrder();
        return pSortedPos[ind];
}

bool DataTable::FreshRowNum()
{
	rowct=pstmt->GetRows();
	if(rowct>maxrows) {
		errprintf("***Warning : Memory table is full on FreshRowNum!\n");
		//if(echo) {
		//printf("Press q to exit or any other key to continue ...");
		//fflush(stdout);
		//int ch=getch();
		//if(ch=='Q' || ch=='q') ReturnErr(DT_ERR_MEMFULL);
		//printf("\n");
		//}
		ReturnErr(DT_ERR_MEMFULL,rowct,"DataTable::FreshRowNum");
	}
	return true;
}

int DataTable::GetMaxRows()
{
	return maxrows;
}


void DataTable::GetMTName(char *bf)
{
	strcpy(bf,dtname);
}

void _wdbiSetDateTime(char *date,int year,int mon,int day,int hour,int min,int sec) ;

int DataTable::ReadFromTextFile(const char *fn, int rowst, int rownm)
{
	FILE *fp=fopen(fn,"rt");
	if(fp==NULL) ReturnErr(DT_ERR_CANNOTOPENTEXTFILE,0,fn);
	long linect=1;
	if(rownm>(maxrows-rowct) || rownm==0) rownm=maxrows-rowct;
	while(linect++<rowst)
		while(fgetc(fp)!='\n');
		char fmt[MAX_COLUMN][200];
		char *ptr[MAX_COLUMN];
		int i;
		for( i=0;i<colct;i++) {
			//if(i!=0) {
			//	strcat(fmt,"%*[ ,\t]");
			ptr[i]=(char *)cd[i].pCol+cd[i].dtsize*(long)rowct;
			switch(cd[i].type) {
			case SQLT_CHR:
			case SQLT_STR:
			case SQLT_AFC:
			case SQLT_AVC:
				sprintf(fmt[i],"%%%dc",cd[i].dtsize-1);
				break;
			case SQLT_NUM:
			case SQLT_FLT:
				strcpy(fmt[i],"%f");
				break;
			case SQLT_INT:
				strcpy(fmt[i],"%d");
				break;
			case SQLT_LNG:
				strcpy(fmt[i],"%lld");
				break;
			case SQLT_DAT:
				strcpy(fmt[i],"%20c");
				break;
			default:
				ReturnErr(DT_ERR_INVALIDCOLUMNTYPE,cd[i].type,cd[i].colname);
				break;
			}
		}
		int rdrowct=0;
		try {
			while(linect++<rowst+rownm) {
				for(i=0;i<colct;i++) {
					ptr[i]+=cd[i].dtsize;
					if(cd[i].type==SQLT_DAT) {
						int y,m,d,h,mi,s;
						fscanf(fp,"%4d/%2d/%2d %2d:%2d:%2d ",&y,&m,&d,&h,&mi,&s);
						_wdbiSetDateTime(ptr[i],y,m,d,h,mi,s);
					}
					else
						fscanf(fp,fmt[i],ptr[i]);
					fscanf(fp,"%*[ ,\t]");
				}
				while(fgetc(fp)!='\n');
				rdrowct++;
			}	 
		}
		catch (WDBIError &e) {
			fclose(fp);
			e.Throw();
		}
		catch (...) {
			errprintf("Read from text file failed,%d row readed.\n",rdrowct);
		}
		fclose(fp);
		rowct+=rdrowct;
		return rdrowct;
}

int DataTable::ReplaceStmt(WDBIStatement *_pstmt)
{
	pstmt=_pstmt;
	return true;
}

double DataTable::Calculate(const char *colname,int op) {
	if(rowct>0) {
	 int ind=GetColumnIndex(colname);
	 unsigned short tp=GetColumnType(ind);
	 if(tp!=SQLT_NUM && tp!=SQLT_FLT && tp!=SQLT_INT && tp!=SQLT_LNG)
		ReturnErr(DTG_ERR_NEEDNUMERICCOLTYPE,op,colname);	
	 double sum=0,maxv,minv,val=0;
	 GetValues(ind,0,1,&maxv);
	 minv=maxv;
	 for(int i=0;i<rowct;i++)
	 {
	  	GetValues(ind,i,1,&val);
	  	if(val>maxv) maxv=val;
	  	if(val<minv) minv=val;
	  	sum+=val;
	 }
	 switch(op) {
		case CAL_SUM:return sum;
		case CAL_MAX:return maxv;
		case CAL_MIN:return minv;
		case CAL_AVG:return sum/rowct;
	 }
	}
	return 0.0;
}

int DataTable::LoadSort(FILE * fp)
{
	int len=0;
	fread(&len,sizeof(int),1,fp);
	if(len<0 || len!=rowct || len>maxrows) return -1;
	if(!pSortedPos)
		pSortedPos=new unsigned int [maxrows];
	if(fread(pSortedPos,sizeof(int),rowct,fp)!=rowct) return -2;
	return 1;
}

int DataTable::SaveSort(FILE * fp)
{
	if(!pSortedPos) return -1;
	fwrite(&rowct,sizeof(int),1,fp);
	if(fwrite(pSortedPos,sizeof(int),rowct,fp)!=rowct) return -2;
	return 1;
}

void DataTable::ReInOrder()
{
	if(!pSortedPos || !ptree) return;
	ptree->inorder((int *)pSortedPos);
	isinorder=true;
	
}

void DataTable::destroyptree()
{
	if(ptree) {
		delete ptree;
		ptree=NULL;
	}
}

void DataTable::AddrFresh(char **colval, int *collen,int *coltp)
{
	for(int i=0;i<colct;i++) {
		colval[i]=cd[i].pCol;
		collen[i]=cd[i].dtsize;//GetColumnLen(i);
		coltp[i]=cd[i].type;
	}
}

int DataTable::GetCreateTableSQL(char *crttab,const char *tabname,bool ismysql)
{
	sprintf(crttab,"create table %s ",tabname);
	GetColumnInfo(crttab+strlen(crttab),ismysql);
	strcat(crttab,  ismysql?" row_format= dynamic ":" ");
	return strlen(crttab);
}

int DataTable::GetColumnInfo(char *crttab,bool ismysql) {
	char fmt[200];
	strcpy(crttab,"(");
	for(unsigned int col=0;col<colct;col++) {
		switch(cd[col].type) {
		case SQLT_CHR:
		case SQLT_STR:
		case SQLT_AFC:
		case SQLT_AVC:
			if(ismysql) {
				//if(cd[col].dtsize>256)
				//	ReturnIntErr(DT_ERR_OUTOFCOLUMNWIDE,0,"DataTable::GetColumnInfo");
				if(cd[col].dtsize-1<4)
					sprintf(fmt,"%s char(%d) ",cd[col].colname,cd[col].dtsize-1);
				else
					sprintf(fmt,"%s varchar(%d) ",cd[col].colname,cd[col].dtsize-1);
			}
			else sprintf(fmt,"%s varchar2(%d) ",cd[col].colname,cd[col].dtsize-1);
			strcat(crttab,fmt);
			break;
		case SQLT_NUM:
			if(ismysql) {
				sprintf(fmt,"%s decimal(%d,%d) ",cd[col].colname,cd[col].prec>cd[col].scale?cd[col].prec:(cd[col].scale+1),
					cd[col].scale);
			}
			else
				sprintf(fmt,"%s number(%d,%d) ",cd[col].colname,cd[col].prec,
				cd[col].scale);
			strcat(crttab,fmt);
			break;
		case SQLT_FLT:
			if(ismysql) 
			 sprintf(fmt,"%s double ",cd[col].colname);
			else sprintf(fmt,"%s float ",cd[col].colname);
			strcat(crttab,fmt);
			break;
		case SQLT_INT:
			if(cd[col].prec>0) {
				if(ismysql) 
					sprintf(fmt,"%s integer(%d) ",cd[col].colname,cd[col].prec);
				else
					sprintf(fmt,"%s number(%d) ",cd[col].colname,cd[col].prec);
			}
			else sprintf(fmt,"%s integer ",cd[col].colname);
			strcat(crttab,fmt);
			break;
		case SQLT_LNG:
			if(cd[col].prec>0) {
				if(ismysql) 
					sprintf(fmt,"%s bigint(%d) ",cd[col].colname,cd[col].prec);
				else
					sprintf(fmt,"%s number(%d) ",cd[col].colname,cd[col].prec);
			}
			//δ֪�ľ���
			else sprintf(fmt,"%s integer ",cd[col].colname);
			strcat(crttab,fmt);
			break;
		case SQLT_DAT:
			if(ismysql)
				sprintf(fmt,"%s datetime ",cd[col].colname);
			else
				sprintf(fmt,"%s date ",cd[col].colname);
			strcat(crttab,fmt);
			break;
		default:
			if(errbuf[0]>0 &&errcode!=0) {
				sprintf(fmt,"%s type:%d(%d) ",cd[col].colname,cd[col].type,cd[col].dtsize-1);
				strcat(crttab,fmt);
			}
			else ReturnIntErr(DT_ERR_INVALIDCOLUMNTYPE,cd[col].type,cd[col].colname);
			break;
		}
		if(col<colct-1) strcat(crttab,",");
	}
	strcat(crttab,")");
	return strlen(crttab);
}

void DataTable::ReverseByteOrder(int rowst)
{
#ifdef WORDS_BIGENDIAN
	double *pdv;
	int *pdi;
	LONG64 *pdl;
	int i=0;
	for(unsigned int col=0;col<colct;col++) {
		switch(cd[col].type) {
		case SQLT_CHR:
		case SQLT_STR:
		case SQLT_AFC:
		case SQLT_AVC:
		case SQLT_DAT:
			break; //No actions
		case SQLT_FLT:
		case SQLT_NUM:
			pdv=((double *)cd[col].pCol)+rowst;
			for(i=rowst;i<rowct;i++,pdv++)
				revDouble(pdv);
			break;
		case SQLT_INT:
			pdi=((int *)cd[col].pCol)+rowst;
			for(i=rowst;i<rowct;i++,pdi++)
				revInt(pdi);
			break;
		case SQLT_LNG:
			pdl=((LONG64 *)cd[col].pCol)+rowst;
			for(i=rowst;i<rowct;i++,pdl++)
				revDouble(pdl);
			break;
		default:
			//Not impletemented.
			ReturnVoidErr(DT_ERR_INVALIDCOLUMNTYPE,cd[col].type,cd[col].colname);
		}
	}
#endif
}

bool DataTable::IsFixedMySQLBlock() {
	return false;
	/*	  for(unsigned int col=0;col<colct;col++) {
		  switch(cd[col].type) {
		  case SQLT_CHR:
		  case SQLT_STR:
		  case SQLT_AFC:
		  case SQLT_AVC:
			       if(cd[col].dtsize-1>=4) return false;
				   }
				   }  	
				   return true;
	*/
} 
#define SetBit(bf,colid) bf[colid/8]|=(0x1<<(colid%8))
#define ResetBit(bf,colid) bf[colid/8]&=~(0x1<<(colid%8))
#define MAX_RECLEN	50*1024
void DataTable::CopyToMySQL(unsigned int startrow, unsigned int rownum, FILE *fp)
{
	char bhdr[60];
	char varflag[40];
	char nullflag[40];
	char fmttmp[100];
	char fmt[100];
	char recbuf[MAX_RECLEN];
	if(rownum==0) 
		rownum=rowct-startrow;
	if(startrow+rownum>rowct) ReturnVoidErr(DT_ERR_OUTOFROW,rownum,"DataTable::CopyToMySQL");
	long i;
	bool fixmode=IsFixedMySQLBlock();
	
	for( i=startrow;i<startrow+rownum;i++) {
		long pos=i;
		if(pkid!=-1) pos=GetRawrnByPK(i);
		else if(pSortedPos) pos=GetRawrnBySort(i);
		long dtlen=0;
		char *src;
		long slen=0;
		memset(varflag,0,40);
		memset(nullflag,0,40);
		int varcol=0;
		for(unsigned int col=0;col<colct;col++,varcol++) {
			switch(cd[col].type) {
			case SQLT_CHR:
			case SQLT_STR:
			case SQLT_AFC:
			case SQLT_AVC:
				src=cd[col].pCol+cd[col].dtsize*(long)pos;
				slen=strlen(src);
				if(slen+1>cd[col].dtsize) {
					slen=0;
				}
				if(cd[col].dtsize-1<4) {
					strcpy(recbuf+dtlen,src);
					if(cd[col].dtsize>slen-1)
						memset(recbuf+dtlen+slen,' ',cd[col].dtsize-slen-1);
					dtlen+=cd[col].dtsize-1;
					if( (IsNull(col,pos) || slen==0) && !fixmode) {
						SetBit(nullflag,col);
					}
					//ResetBit(varflag,varcol);
					//ResetBit(nullflag,col);
					varcol--;
				}
				else {
					if( (IsNull(col,pos) || slen==0) && !fixmode) {
#ifdef MYSQL_VER_51
						varcol--;
#else
						SetBit(varflag,varcol);
#endif
						SetBit(nullflag,col);
						recbuf[dtlen++]=0;
					}
#ifdef MYSQL_VER_51
#else
					else if(cd[col].dtsize-slen<3 || fixmode) {
						strcpy(recbuf+dtlen,src);
						//_strnset(recbuf,' ',cd[col].dtsize-1-slen);
						memset(recbuf+dtlen+slen,' ',cd[col].dtsize-1-slen);
						//ResetBit(varflag,varcol);
						//ResetBit(nullflag,col);
						dtlen+=cd[col].dtsize-1;
					}
#endif
					else {
#ifdef MYSQL_VER_51
						if(cd[col].dtsize > 255 && slen > 254) {
							recbuf[dtlen]=0xff;
							recbuf[dtlen+1]=(char) (slen >>8);
							recbuf[dtlen+2]=(char) (slen & 0xff);
							dtlen++;dtlen++;
						}
#else
						if(cd[col].dtsize > 255 && slen > 127) {
							recbuf[dtlen]=(char) ((slen & 127)+128);
							recbuf[dtlen+1]=(char) (slen >> 7);
							dtlen++;
						}
#endif						
						else
							recbuf[dtlen]=slen;
						strcpy(recbuf+dtlen+1,src);
						//recbuf[dtlen]=slen;
#ifdef MYSQL_VER_51
						varcol--;	
#else
						SetBit(varflag,varcol);
#endif
						//ResetBit(nullflag,col);
						dtlen+=slen+1;
					}
				}
				break;
			case SQLT_FLT:
			   	
			   	if(IsNull(col,pos)&& !fixmode) {
					//SetBit(varflag,varcol);
					SetBit(nullflag,col);
				}
				else  if(((double *)cd[col].pCol)[pos]==0 && !fixmode) {
					SetBit(varflag,varcol);
          		    	}
          		    	else {
					double v=((double *)cd[col].pCol)[pos];
					revDouble(&v);
					memcpy(recbuf+dtlen,&v,sizeof(double));
					slen=sizeof(double);
					dtlen+=slen;
				}
				break;
			case SQLT_NUM:
			        
				//sprintf(fmttmp,"%%1.%df ",cd[col].scale);
				if(IsNull(col,pos)&& !fixmode) {
#ifdef MYSQL_VER_51
					int binlen=decimal_bin_size(cd[col].prec,cd[col].scale);
					decimal_digit_t dec_buf[9];
					decimal_t dec;
					dec.buf=dec_buf;
					dec.len= 9;
					double2decimal((double)0,&dec);
					decimal2bin(&dec,(uchar *)recbuf+dtlen,cd[col].prec,cd[col].scale);
					slen=binlen;
					dtlen+=binlen;
					SetBit(nullflag,col);
#else					
					SetBit(varflag,varcol);
					SetBit(nullflag,col);
					recbuf[0]=1;
					recbuf[1]='0';
					dtlen+=2;
#endif
				}
				else {
#ifdef MYSQL_VER_51
					int binlen=decimal_bin_size(cd[col].prec,cd[col].scale);
					decimal_digit_t dec_buf[9];
					decimal_t dec;
					dec.buf=dec_buf;
					dec.len= 9;
					double2decimal(*(double *)(cd[col].pCol+pos*sizeof(double)),&dec);
					decimal2bin(&dec,(uchar *)recbuf+dtlen,cd[col].prec,cd[col].scale);
					slen=binlen;
					dtlen+=binlen;
#else					
					int prec=cd[col].prec;
					if(prec<=cd[col].scale)
					 prec=cd[col].scale+1;
					if(*(double *)(cd[col].pCol+pos*sizeof(double))>=pow(10.0,(double)prec) ||
						*(double *)(cd[col].pCol+pos*sizeof(double))<=-pow(10.0,double(prec)) )
						ReturnVoidErr(DT_ERR_VALUELOSS,pos+1,cd[col].colname);
					sprintf(recbuf+dtlen,"%*.*f",prec+2,cd[col].scale,*(double *)(cd[col].pCol+pos*sizeof(double)));
					slen=prec+2;
					dtlen+=slen;
#endif
				}
				/*
				if(slen>cd[col].prec) ReturnVoidErr(DT_ERR_OUTOFCOLUMNWIDE,slen,cd[col].colname);
				if(slen==cd[col].prec || fixmode) {
				recbuf[dtlen]=' ';
				strcpy(recbuf+dtlen+1,fmt);
				//ResetBit(varflag,varcol);
				//ResetBit(nullflag,col);
				dtlen+=slen+1;
				}
				else {
				strcpy(;
				recbuf[dtlen]=slen;
				dtlen+=slen+1;
				SetBit(varflag,varcol);
				//ResetBit(nullflag,col);
				}
				*/
				break;
			case SQLT_INT:
				//if(cd[col].type==SQLT_INT) {
				//sprintf(fmttmp,"%%%dd ",10);
				//if(cd[col].prec==0) {
				//���������pa-riscƽ̨�����BUS-ERROR
				//*(int *)(recbuf+dtlen)=((int *)cd[col].pCol)[pos];//*sizeof(int));
				//revInt((recbuf+dtlen))
				if(IsNull(col,pos)&& !fixmode) {
					SetBit(varflag,varcol);
					SetBit(nullflag,col);
				}
				else if(((int *)cd[col].pCol)[pos]==0 && !fixmode) {
					SetBit(varflag,varcol);
					//ResetBit(nullflag,col);
				}
				else 
				{
					int v=((int *)cd[col].pCol)[pos];
					revInt(&v);
					memcpy(recbuf+dtlen,&v,sizeof(int));
					slen=sizeof(int);
					dtlen+=slen;
					//ResetBit(varflag,varcol);
					//ResetBit(nullflag,col);
				}
				break;
				//}
				//sprintf(fmt,"%d",*(int *)(cd[col].pCol+pos*sizeof(int)));
				//}
			case SQLT_LNG:
				if(IsNull(col,pos)&& !fixmode) {
					SetBit(varflag,varcol);
					SetBit(nullflag,col);
				}
				else if(((LONG64 *)cd[col].pCol)[pos]==0 && !fixmode) {
					SetBit(varflag,varcol);
					//ResetBit(nullflag,col);
          		    }
          		    else {
						LONG64 v=((LONG64 *)cd[col].pCol)[pos];
						revDouble(&v);
						memcpy(recbuf+dtlen,&v,sizeof(LONG64));
						slen=sizeof(LONG64);
						dtlen+=slen;
						//ResetBit(varflag,varcol);
						//ResetBit(nullflag,col);
					}
					break;
					//}
					//sprintf(fmt,"%d",*(int *)(cd[col].pCol+pos*sizeof(int)));
					//}
			case SQLT_DAT:
				src=cd[col].pCol+cd[col].dtsize*(long)pos;
				if((IsNull(col,pos) || *src==0 || (unsigned char )*src<101 || (unsigned char )*src>199)&& !fixmode) {
					SetBit(varflag,varcol);
					SetBit(nullflag,col);
				}
				else {
					LONG64 mdt=0;
					mdt=LLNY*(((unsigned char)src[0]-LL(100))*100+(unsigned char)src[1]-100);
					mdt+=LLHMS*src[2];
					mdt+=LL(1000000)*src[3];
					mdt+=LL(10000)*(src[4]-1);
					mdt+=100*(src[5]-1);
					mdt+=src[6]-1;
					memcpy(recbuf+dtlen,&mdt,sizeof(LONG64));
					rev8B((recbuf+dtlen));
					dtlen+=sizeof(LONG64);
					//ResetBit(varflag,varcol);
					//ResetBit(nullflag,col);
				}
				break;
				//memcpy(cd[col].pCol+cd[col].dtsize*pos,cd[col].pCol+cd[col].dtsize*pos,7);
				//break;
			default:
				//Not impletemented.
				ReturnVoidErr(DT_ERR_INVALIDCOLUMNTYPE,cd[col].type,cd[col].colname);
		}
		}
		//2005/03/03 �䳤�ֶ�ָʾ�����ֽ�������ʵ��ʹ���ֶ�������
		int valbytes=(varcol+7)/8;
		int colbytes=(colct+7)/8;
		dtlen+=colbytes+valbytes;
		long dtlen1=dtlen+4;
		//dtlen+=colbytes*2;
		int blockflag=3;
		int extrabytes=0;
		if(dtlen1<20 && !fixmode) {
			extrabytes=20-dtlen1;
			blockflag=3;
		}
		else {
			extrabytes=4-dtlen1%4;
			if(extrabytes==3 || fixmode) // type 1 flag
			{
				extrabytes=0;
				blockflag=1;
				//colbytes=valbytes=0;
			}
			else
			{
				if(extrabytes==4) extrabytes=0;
				blockflag=3;
			}
		}
		int hdl=0;
		bhdr[hdl++]=blockflag;
		if(!fixmode) {
			bhdr[hdl++]=(dtlen)>>8;
			bhdr[hdl++]=(dtlen)&0xff;
			if(blockflag==3) 
				bhdr[hdl++]=extrabytes;
			memcpy(bhdr+hdl,&varflag,valbytes);hdl+=valbytes;
			memcpy(bhdr+hdl,&nullflag,colbytes);hdl+=colbytes;
		}
		else bhdr[hdl++]=0xfc;
		fwrite(bhdr,1,hdl,fp);
		fwrite(recbuf,1,dtlen-colbytes-valbytes,fp);
		if(extrabytes>0 && !fixmode) {
			memset(recbuf,0,extrabytes);
			fwrite(recbuf,1,extrabytes,fp);
		}
	}
}

bool DataTable::SortHeap()
{
	if(nSortColumn<1) ReturnErr(DT_ERR_NOCOLUMNTOSORT,0,"DataTable::SortHeap");
	//if(pkid==-1) ReturnErr(DT_ERR_ORDERONNULLPK);
	if(!pSortedPos) {
		pSortedPos=new unsigned int [maxrows];
	}
	if(ptree) delete ptree;
	ptree=NULL;
	int i;
	for( i=0;i<rowct;i++) 
		pSortedPos[i]=i;
	// Create heap;
	
	if(rowct<1) return true;
	int n=rowct-1;
	int m=rowct/2-1;
	for(i=m;(int)i>=0;i--) 
		RebuildHeap(i,n);
	// Order heap
	for(i=n;i>0;i--)
	{
		//int x=pk[i]; //exchange first one and last one.
		//pk[i]=pk[0];pk[0]=x;
		unsigned int x=pSortedPos[i]; //exchange first one and last one.
		pSortedPos[i]=pSortedPos[0];pSortedPos[0]=x;
		RebuildHeap(0,i-1);
	}
	isinorder=true;
	return true;
}
