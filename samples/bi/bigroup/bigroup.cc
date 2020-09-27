#ifdef WIN32
#include <process.h>
#else
#include <unistd.h>
#include <malloc.h>
#include <pthread.h>
#endif
#include <stdlib.h>
#include <stdio.h>
#include "AutoHandle.h"
#include "dt_common.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <iostream>
#include <vector>
#include <string>
#include "GetPot"

using namespace std;

class wdbiparam {
public:
	string un,pwd,dsn;
	int type;
	wdbiparam()
	{
		type=-1;
	}
	bool validParam() {return type!=-1;}
	void SetParam(vector<string> &pa) {
		if(pa.size()!=4) 
			ThrowWith("����:���ݿ����Ӳ�����ʽ����,��Ҫ�ĸ�������ֻ��%d��",pa.size());
		if(strcasecmp(pa[3].c_str(),"odbc")==0) 
			type=1;
		else if(strcasecmp(pa[3].c_str(),"oracle")==0) 
		         type=0;
		else ThrowWith("����:���ݿ����Ӳ�����ʽ����,����ֻ��ѡodbc��oracle,������'%s'.",pa[3].c_str());
		un=pa[0];pwd=pa[1];dsn=pa[2];
	}
	int buildConn() {
		//printf("Build wociCreateSession('%s','%s','%s','%s')",
		//    un.c_str(),pwd.c_str(),dsn.c_str(),type==0?"DTDBTYPE_ORACLE":"DTDBTYPE_ODBC");
		char dpwd[300];
		strcpy(dpwd,pwd.c_str());
		return wociCreateSession(un.c_str(),dpwd,dsn.c_str(),type==0?DTDBTYPE_ORACLE:DTDBTYPE_ODBC);
	}
};

struct grpvars {
	string varname;
	vector<string> values;
	int pos;
	int getSize() {return values.size();}
	grpvars() {pos=0;depoffset=-1;}
	int depoffset;
};


class varsarray {
	vector<grpvars> vars;
	vector<int> lpvar;
	int lplevel;
public:
	int getlpcount() {
		int s=lpvar.size();
		int rt=1;
		for(int i=0;i<s;i++) {
		 rt*=vars[lpvar[i]].getSize();
		}
		return rt;
	}
		
	//���б�����ȡֵ,�ո�ָ�
	string getlpvars() {
		string res;
		res="";
		int s=lpvar.size();
		for(int i=0;i<s;i++) {
		 int off=lpvar[i];
		 res+=vars[off].values[vars[off].pos];
		 res+=" ";
		}
		return res;
	}
	
	void pushint(const char *varname,int value) {
		vector<string> var;
		var.clear();
		char svalue[20];
		sprintf(svalue,"%d",value);
		var.push_back(svalue);
		push(varname,var);
	}
	varsarray() {
		vars.clear();lplevel=-1;
		char curdate[10];
		char mthvalue[20];
		wociGetCurDateTime(curdate);
		int mth=wociGetMonth(curdate);
		int pmth=mth==1?12:(mth-1);
		int nmth=mth==12?1:(mth+1);
		pushint("curr_date",wociGetYear(curdate)*10000+mth*100+wociGetDay(curdate));
		pushint("this_yearmonth",wociGetYear(curdate)*100+mth);
		pushint("prev_yearmonth",mth==1?((wociGetYear(curdate)-1)*100+12):(wociGetYear(curdate)*100+mth-1));
		pushint("next_yearmonth",mth==12?((wociGetYear(curdate)+1)*100+1):(wociGetYear(curdate)*100+mth+1));
		pushint("this_month",mth);
		pushint("prev_month",pmth);
		pushint("next_month",nmth);
		pushint("this_month_m6",(mth+5)%6+1);
		pushint("prev_month_m6",(pmth+5)%6+1);
		pushint("next_month_m6",(nmth+5)%6+1);
		pushint("this_month_m4",(mth+3)%4+1);
		pushint("prev_month_m4",(pmth+3)%4+1);
		pushint("next_month_m4",(nmth+3)%4+1);
		pushint("this_month_m2",mth%2);
		pushint("prev_month_m2",pmth%2);
		pushint("next_month_m2",nmth%2);
	
	}
	
	bool checkvarexist(const char *varn) {
		int s= vars.size();
		int i=0;
		for(i=0;i<s;i++) {
		  if(vars[i].varname.compare(varn)==0) return true;
		}
		return false;
	}
	
	void push(const char *varn,vector<string> &var) {
		if(checkvarexist(varn))
		    ThrowWith("����:����'%s'�ظ������ʹ���˹ؼ���������.",varn);
		grpvars v;
		v.varname=varn;
		v.values=var;
		vars.push_back(v);
	}
	//call following routine after push all objects
	void buildrelation()
	{
		int s= vars.size();
		int i=0;
		for(i=0;i<s;i++) {
		  if(strncmp(vars[i].values[0].c_str(),"conn:",strlen("conn:"))==0)
		   {
		   	string dep=vars[i].values[0];
		   	dep.replace(0,strlen("conn:"),"");
		   	int j;
		   	for(j=0;j<s;j++)
		   	{
		   		if(vars[j].varname.compare(dep)==0) {
		   		  if(strncmp(vars[j].values[0].c_str(),"conn:",strlen("conn:"))==0 || vars[j].depoffset>=0)
		   		   	ThrowWith("����:����'%s'������δ���1('%s'�й���Ҫ��),������.",vars[i].varname.c_str(),vars[j].varname.c_str());
		   		  if(vars[j].getSize()!=vars[i].getSize()-1) 
		   		   	ThrowWith("����:����'%s'��������'%s'��һ��',�޷�����.",vars[i].varname.c_str(),vars[j].varname.c_str());
	   			  vars[i].depoffset=j;
	   			  break;
	   			}
	   		}
	   		if(j==s) 
	   			ThrowWith("����:����'%s'�Ҳ������ӱ���'%s'.",vars[i].varname.c_str(),dep.c_str());
	   		vars[i].values.erase(vars[i].values.begin());
	   	   }
	   	}
	   	for(i=0;i<s;i++) {
		  if(vars[i].depoffset==-1)
		   lpvar.push_back(i);
		}	
	   	lplevel=lpvar.size()-1;
	}
	
	bool changepos(int off,int pos=-1) {
		if(pos==-1) pos=vars[off].pos+1;
		if(vars[off].getSize()<=pos) return false;
		int s=vars.size();
		vars[off].pos=pos;
		for(int i=0;i<s;i++) {
		  if(vars[i].depoffset==off)		
		   vars[i].pos=pos;
		}
		return true;
	}
	
	void first() {
		lplevel=lpvar.size()-1;
		int s=vars.size();
		for(int i=0;i<s;i++) {
		   vars[i].pos=0;
		}
	}
	
	bool next() {
		if(!changepos(lpvar[lplevel]))
		{ //check up first
			do{
			 lplevel--;
			 if(lplevel<0) return false;
			}
			while(!changepos(lpvar[lplevel]));
			for(int i=lplevel+1;i<lpvar.size();i++) {
			   changepos(lpvar[i],0);
			}
			lplevel=lpvar.size()-1;
		}	  
		return true;
	}
	
	const char * getvar(const char *varname) {
		int s=vars.size();
		for(int i=0;i<s;i++) {
		   if(vars[i].varname.compare(varname)==0) return vars[i].values[vars[i].pos].c_str();
		}
	   	ThrowWith("����:�Ҳ�������'%s'.",varname);
	   	return "NO Value";
	}
	
	string getvars() {
		string res;
		res="";
		int s=vars.size();
		for(int i=0;i<s;i++) {
		 res+=vars[i].values[vars[i].pos];
		 res+="  ";
		}
		return res;
	}
	
	void format(char *buf,const char *fmt,vector<string> usedvars)
	{
	  //va_list vlist;
	  const char *p[100];
	  int s=usedvars.size();
 	  for(int i=0;i<s;i++) {
		 p[i]=(const char*)getvar(usedvars[i].c_str());
	  }
          int of=0;
          while(*fmt) {
            if(*fmt=='%') {
             fmt++;
             if(*fmt=='%') *buf++='%';
             else if(*fmt!='s') ThrowWith("format error ,%%%c",*fmt);
             else {strcpy(buf,p[of]);
                   buf+=strlen(p[of++]);
             }
             fmt++;
            }
            else *buf++=*fmt++;
          }
	  *buf=0;
	  //vlist=(va_list )p;
	  //vsprintf(buf,fmt,vlist);
	}
};

		
	
class grpparam {
public:
	bool contonerr;
	int repdelay;// minutes
	bool haverefdata; 
	bool verboselog;
	bool logecho;
	int smgmode;	//# 0����֪ͨ 1������֪ͨ����������ʱ֪ͨ 2����Ҫ֪ͨ�����������ȫ��ִ�����ʱ֪ͨ 
			//# 3: ��ϸ֪ͨ,����ִ�е�ÿ���׶�
	string smgroup;
	string logfile;
	string title;
	
	int startoffset; //��ʼƫ������Ĭ��Ϊ1���ӵ�һ����ʼ��
	
	varsarray vararr;
	
	int srcrowcount;
	vector<string> srcvars;
	wdbiparam srcdb;
	string srccolsforref;
	string srcsql;
	string srcgroupcols;
	string srccalcols;
	
	int refrowcount;
	vector<string> refvars;
	wdbiparam refdb;
	string refordercols;
	string refgroupcols;
	string refsql;
	
	int resrowcount;
	vector<string> resvars;
	string restabname;
	wdbiparam resdb;
	//1:�ؽ� 2:׷�� 3:����
	int resstoretype;
	string cleancondition;
	int cleanRows;

private:	
	bool getWDBIParam(const char *vpath,wdbiparam &p,GetPot &cl)
	{
	  vector<string> tmp;
	  getArray(vpath,tmp,cl);
	  if(tmp.size()==0) return false;
	  p.SetParam(tmp);
	  return true;
	}
	
	void getArray(const char *vpath,vector<string> &res,GetPot &cl) {
	 res.clear();
   	 int vnum=cl.vector_variable_size(vpath);
   	 int lp=0;
   	 while(lp<vnum) 
     	  res.push_back(cl(vpath,"",lp++));
	}
	
	void pushVarArray(vector<string> &varnames,GetPot &cl) {
		int s=varnames.size();
		string varpath;
		for(int i=0;i<s;i++) {
			if(vararr.checkvarexist(varnames[i].c_str())) continue;
			varpath="����/";
			varpath+=varnames[i];
			vector<string> res;
			getArray(varpath.c_str(),res,cl);
			if(res.size()<1) 
   	 		 ThrowWith("�Ҳ���������: '%s'",varpath.c_str());
			vararr.push(varnames[i].c_str(),res);
		}
	}
public:
  string getlpvars() {
  	return vararr.getlpvars();
  }
  
  const char * getvar(const char *varname) {
  	return vararr.getvar(varname);
  }	
  
  string getvars() {
  	return vararr.getvars();
  }

  void first() {
  	vararr.first();
  }
  
  bool next() {
  	return vararr.next();
  }
  
  grpparam(const char *filename) {
	GetPot cl(filename);
   	title=cl("����ѡ��/ͳ�Ʊ���","���÷���ͳ��");
   	logfile=cl("����ѡ��/��־�ļ�",""); //bigroup/common.log
   	if(strlen(logfile.c_str())<1)
   	 logfile="bigroup/"+title+".log";
   	
   	smgmode=cl("����ѡ��/����֪ͨ",0);
	startoffset=cl("����ѡ��/ѭ����ʼ",1);
   	smgroup=cl("����ѡ��/������","���÷���ͳ��");
   	haverefdata=cl("����ѡ��/��������",0)==1;
   	repdelay=cl("����ѡ��/�������Լ��",0);
   	contonerr=cl("����ѡ��/����ʱ����ѭ��",0)==1;
   	verboselog=cl("����ѡ��/��ϸ���",0)==1;
   	logecho=cl("����ѡ��/��־����",1)==1;
   	
   	
   	srcrowcount=cl("Դ����/��¼��",50000);
   	if(srcrowcount<1)
      		ThrowWith("����:ȱ�� Դ����/��¼��.\n");
   	if(!getWDBIParam("Դ����/���Ӳ���",srcdb,cl))
   		ThrowWith("����:Դ����/���Ӳ��� ȱ�ٻ��ʽ����!");
   	
   	getArray("Դ����/����",srcvars,cl);
   	if(srcvars.size()>0)
   	 pushVarArray(srcvars,cl);
   	if(haverefdata)
   	  srccolsforref=cl("Դ����/���ù����ֶ�","");
   	srcsql=cl("Դ����/SQL","");
   	srcgroupcols=cl("Դ����/�����ֶ�","");
	srccalcols=cl("Դ����/�����ֶ�","");
   	 	
   	if(haverefdata) {
	 refrowcount=cl("��������/��¼��",50000);
  	if(refrowcount<1)
      		ThrowWith("����:ȱ�� ��������/��¼��.\n");
	 getArray("��������/����",refvars,cl);
   	if(refvars.size()>0)
   	 pushVarArray(refvars,cl);
   	 refsql=cl("��������/SQL","");
   	 if(!getWDBIParam("��������/���Ӳ���",refdb,cl))
   	 	lgprintf("δָ�� ��������/���Ӳ���,Ĭ��ʹ������Դ������!");
	 refordercols=cl("��������/�����ֶ�","");
	 refgroupcols=cl("��������/�����ֶ�","");
	}
	
	resrowcount=cl("�������/��¼��",50000);
  	if(resrowcount<1)
      		ThrowWith("����:ȱ�� �������/��¼��.\n");
	getArray("�������/����",resvars,cl);
	if(resvars.size()>0)
	 pushVarArray(resvars,cl);
	restabname=cl("�������/�������","");
   	if(!getWDBIParam("�������/���Ӳ���",resdb,cl) )
 	 lgprintf("δָ�� �������/���Ӳ���,Ĭ��ʹ������Դ������!");
	
// /ǿ���ؽ� 1 /�״��ؽ� 2/׷�� 3 /���� 4(4�ݲ�֧��) /�������ļ� 5/�ı��ļ� 6/
//���������,����Ҫд��������
//�����׷�Ӻ�����ʽ������������ڣ����Զ�����
//������ؽ���ʽ�����۽�����Ƿ���ڣ������½�����
//
//�״��ؽ�,ͳ�ƽ���ѭ�������е�һ��ʹ�ñ�ʱ�ؽ�. ע��ֻ������ʹ�ñ�ĵ�һ���ؽ�,Ҳ����˵,����
//  �ǵڶ���ʹ�ñ�,�����ӵ�ǰһ�α�����ͬ,���ɾ���ؽ�.
//
//  �����5/6(�ļ���ʽ),��һ��Ϊ�����ؽ� 

	resstoretype=cl("�������/������洢",2);
	if(resstoretype==3) {
		cleancondition=cl("�������/��������","");
		cleanRows=cl("�������/�����ύ��¼",10000);
	}
	
      	// This must the last call of variable reading.
      	vararr.buildrelation();
	first();
  }
};	
	

int smprintf(const char *smgroup,const char *format,...){
	va_list vlist;
	va_start(vlist,format);
	static char smcontent[8000];
	vsprintf(smcontent,format,vlist);
	va_end(vlist);
	//trim length of content:
	strcpy(smcontent+70,"...");
	AutoHandle dst;
	dst.SetHandle(wociCreateSession("dtuser","readonly","//130.86.12.80/dtagt",DTDBTYPE_ORACLE));
	AutoMt mt(dst,100);
	mt.FetchFirst("select gid from sms_group where gname='%s'",smgroup);
	if(mt.Wait()!=1) 
		ThrowWith("Error: short message send to a unknown group '%s',content '%s'.",smgroup,smcontent);
	lgprintf("����Ϣ֪ͨ: %s(%s).",smcontent,smgroup);
	try{
	 AutoStmt st(dst);
	 st.DirectExecute("insert into sms_content values (%d,'%s',sysdate,null,1998)",*mt.PtrInt(0,0),smcontent);
	}
	catch(...) {
		wociRollback(dst);
		throw;
	}
	return strlen(smcontent);
}


//������־�ļ���
#define LOGPATH "bi/group"
int Start(void *ptr);
int argc;
char  **argv;  
int main(int _argc,char *_argv[]) {
    int nRetCode = 0;
    argc=_argc;
    argv=_argv;
    if(argc!=2) {
    	printf("ʹ�÷���:bigroup <control_file>.\n");
    	return 1;
    }
    WOCIInit(LOGPATH);
    nRetCode=wociMainEntrance(Start,true,argv[1],2);
    WOCIQuit(); 
    return nRetCode;
}


int Start(void *ctrlfile) {
    wociSetTraceFile("bigroupcommon.log");
    wociSetOutputToConsole(TRUE);
    wociSetEcho(TRUE);
    grpparam gp((const char *)ctrlfile);
    wociSetTraceFile(gp.logfile.c_str());
    wociSetOutputToConsole(TRUE);
    wociSetEcho(gp.verboselog);
    lgprintf("��ʼ����ͳ��: %s.",gp.title.c_str());
    AutoHandle dts;
    lgprintf("���ӵ�Դ���ݿ�...");
    
    dts.SetHandle(gp.srcdb.buildConn());
    mytimer mt_1;
    mt_1.Start();
    AutoHandle dtref;
    if(gp.haverefdata && gp.refdb.validParam())
    {
      lgprintf("���ӵ��������ݿ�...");
      dtref.SetHandle(gp.refdb.buildConn());
    }
    AutoHandle dtresult;
    if(gp.resdb.validParam()) {
     lgprintf("���ӵ�������ݿ�...");
     dtresult.SetHandle(gp.resdb.buildConn());
    }
    char sqlbuf[8000];
    int lpct=0;
    double totrows=0,lprows=0,refrows=0;
    char lastrestable[1000];
    memset(lastrestable,0,sizeof(lastrestable));
    AutoMt mtref(gp.refdb.validParam()?dtref:dts,gp.refrowcount);
    do{
	if(gp.startoffset>lpct+1) {
      lgprintf("��%d��ѭ������.",lpct+1);
      gp.vararr.format(lastrestable,gp.restabname.c_str(),gp.resvars);
      lpct++;
      continue;
     }
     mytimer mt_2;
     mt_2.Start();
     try {
     TradeOffMt mt(0,gp.srcrowcount);
     AutoStmt stmt(dts);
     gp.vararr.format(sqlbuf,gp.srcsql.c_str(),gp.srcvars);
     stmt.Prepare(sqlbuf);
     mt.Cur()->Build(stmt);
     mt.Next()->Build(stmt);
     AutoMt result(dts,gp.resrowcount);
     if(gp.haverefdata) {
     	if(gp.refvars.size()>0 || refrows<1) {
     	 lgprintf("ȡ����������...");
     	 gp.vararr.format(sqlbuf,gp.refsql.c_str(),gp.refvars);
     	 mtref.FetchAll(sqlbuf);
     	 mtref.Wait();
     	 lgprintf("������������...");
     	 wociSetSortColumn(mtref,gp.refordercols.c_str());
     	 wociSort(mtref);
     	 refrows=mtref.GetRows();
     	}
        result.SetGroupParam(*mt.Cur(),gp.srcgroupcols.c_str(),gp.srccalcols.c_str(),gp.srccolsforref.c_str(),mtref,gp.refgroupcols.c_str());
     }
     else result.SetGroupParam(*mt.Cur(),gp.srcgroupcols.c_str(),gp.srccalcols.c_str());
     mt.FetchFirst();
     int rn=mt.Wait();
     while (rn>0)
     {
    	totrows+=rn;
    	mt.FetchNext();
	wociSetGroupSrc(result,*mt.Cur());
        wociGroup(result,0,rn);
    	rn=mt.Wait();
     }
     if(result.GetRows()>0) {
      gp.vararr.format(sqlbuf,gp.restabname.c_str(),gp.resvars);
      bool forcecreate=false;
      if(gp.resstoretype==1 || (gp.resstoretype==2 && strcmp(sqlbuf,lastrestable)!=0) ) forcecreate=true;
      result.CreateAndAppend(sqlbuf,gp.resdb.validParam()?dtresult:dts,forcecreate);
      wociCommit(gp.resdb.validParam()?dtresult:dts);
      strcpy(lastrestable,sqlbuf);
     }
     mt_2.Stop();
     }
     catch(...) {
     	if(gp.smgmode>0) smprintf(gp.smgroup.c_str(),"ϵͳ֪ͨ:ͳ��''%s''��%d������ʱ�쳣!",gp.title.c_str(),lpct+1);
	lgprintf("����:ͳ��'%s'��%d������ʱ�쳣!",gp.title.c_str(),lpct+1);
     	if(gp.contonerr) {
     		if(gp.repdelay>0) {
     		 lgprintf("%d���Ӻ����...",gp.repdelay);
     		 sleep(gp.repdelay*60);
     		 continue;
     		}
     	}
     	throw;
     }
     if(gp.smgmode==3) smprintf(gp.smgroup.c_str(),"ϵͳ֪ͨ:ͳ��''%s''��%d������,%.0f��.",gp.title.c_str(),lpct+1,mt_2.GetTime());
     lgprintf("��%d��ѭ������ʱ��%f�� ��������",++lpct,mt_2.GetTime());
   } while(gp.next());
   mt_1.Stop();
   lgprintf("������ʱ��%f�� ��������",mt_1.GetTime());
   if(gp.smgmode==2) smprintf(gp.smgroup.c_str(),"ϵͳ֪ͨ:ͳ��''%s''������%d��,%.0f��.",gp.title.c_str(),lpct+1,mt_1.GetTime());
   return 0;
}
