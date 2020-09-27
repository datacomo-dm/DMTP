#include <iostream>
#include <vector>
#include <string>
#include "GetPot"

void ThrowWith(const char *format,...) {
	va_list vlist;
	va_start(vlist,format);
	static char msg[3000];
	vsprintf(msg,format,vlist);
	printf(msg);
	va_end(vlist);
	throw msg;
}

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
		return wociCreateSession('%s','%s','%s','%s')",
		    un.c_str(),pwd.c_str(),dsn.c_str(),type==0?"DTDBTYPE_ORACLE":"DTDBTYPE_ODBC");
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
	
	varsarray() {
		vars.clear();lplevel=-1;
	}
	void push(const char *varn,vector<string> &var) {
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
	  va_list vlist;
	  const char *p[100];
	  int s=usedvars.size();
 	  for(int i=0;i<s;i++) {
		 p[i]=(const char*)getvar(usedvars[i].c_str());
	  }
	  vlist=(char *)p;
	  vsprintf(buf,fmt,vlist);
	}
};

		
	
class grpparam {
public:
	bool contonerr;
	int repdelay;// minutes
	bool haverefdata; 
	int smgmode;	//# 0����֪ͨ 1������֪ͨ����������ʱ֪ͨ 2����Ҫ֪ͨ�����������ȫ��ִ�����ʱ֪ͨ 
			//# 3: ��ϸ֪ͨ,����ִ�е�ÿ���׶�
	string loggroup;
	string logfile;
	string title;
	
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
   	title=cl("����ѡ��/ͳ�Ʊ���","");
   	logfile=cl("����ѡ��/��־�ļ�","");
   	
   	smgmode=cl("����ѡ��/����֪ͨ",0);
   	loggroup=cl("����ѡ��/��־��","");
   	haverefdata=cl("����ѡ��/��������",0)==1;
   	repdelay=cl("����ѡ��/�������Լ��",0);
   	contonerr=cl("����ѡ��/����ʱ����ѭ��",0)==1;
   	loggroup=cl("����ѡ��/��־��","");
   	
   	srcrowcount=cl("Դ����/��¼��",0);
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
	 refrowcount=cl("��������/��¼��",0);
  	if(refrowcount<1)
      		ThrowWith("����:ȱ�� ��������/��¼��.\n");
	 getArray("��������/����",refvars,cl);
   	if(refvars.size()>0)
   	 pushVarArray(refvars,cl);
   	 refsql=cl("��������/SQL","");
   	 if(!getWDBIParam("��������/���Ӳ���",refdb,cl))
   	 	printf("δָ�� ��������/���Ӳ���,Ĭ��ʹ������Դ������!\n");
	 refordercols=cl("��������/�����ֶ�","");
	 refgroupcols=cl("��������/�����ֶ�","");
	}
	
	resrowcount=cl("�������/��¼��",0);
  	if(resrowcount<1)
      		ThrowWith("����:ȱ�� �������/��¼��.\n");
	getArray("�������/����",resvars,cl);
	if(resvars.size()>0)
	 pushVarArray(resvars,cl);
	restabname=cl("�������/�������","");
   	if(!getWDBIParam("�������/���Ӳ���",resdb,cl) )
 	 printf("δָ�� �������/���Ӳ���,Ĭ��ʹ������Դ������!\n");

	
	//1:�ؽ� 2:׷�� 3:����
	resstoretype=cl("�������/������洢",0);
	if(resstoretype==3) {
		cleancondition=cl("�������/��������","");
		cleanRows=cl("�������/�����ύ��¼",0);
	}
	
   	resrowcount=cl("�������/��¼��",0);
   	if(resrowcount<1)
      		ThrowWith("����:ȱ�� �������/��¼��.\n");
      	
      	// This must the last call of variable reading.
      	vararr.buildrelation();
	first();
  }
};	
	
/*
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
*/

int main () {
 grpparam gp("group.ini");
 do{
   string v;
   v=gp.getvars();
   printf("VARS: %s.\n",v.c_str());
   v=gp.getlpvars();
   printf("  LOOPVARS: %s.\n",v.c_str());
 } while(gp.next());
 
 return 1; 
}
   

