#include "dtio.h"
#include "dt_common.h"
#include "mysqlconn.h"
#include "dt_svrlib.h"
#include "dt_cmd_base.h"
#ifdef WIN32
#include <conio.h>
#endif

#define MAX_TABLES 1000
const char *helpmsg=
  " dpio�����ݱ��ݻָ����ߣ�����������ģʽ��������ʽ�����ʽ��\n"
  " \n"
  "���� : \n"
  " \n"
  " ������ʽ ��dpio����֮��ֱ�����빦�����ƣ����뽻��ʽ���档�������ƣ�\n"
  "pack  : ���ݱ��� \n"
  "unpack : ���������ָ� \n"
  "listpack : �г��ѱ����ļ�����ϸ����\n"
  "restore : ���ٻָ� \n"
  "bindpack : �����ļ����� \n"
  " \n"
  "-at Ŀ��������б�\n"
  "    ȱʡֻ�г�δ���ݻ��ϴα��ݺ��޸Ĺ��ı�\n"
  " \n"
  " \n"
  " ���ʽ����������ʱָ�������Ĳ���: \n"
  "-f {pack,restore,unpack,list,rebuild} \n"
  "    ����ѡ�����е�һ��: \n"
  "    pack: <���ݿ���>  <����> <�����ļ���> \n"
  "        ���ݱ��� \n"
  "    restore: <���ݿ���>  <����> <�µı���> <�����ļ���> \n"
  "        ���ٻָ�(ֱ�ӵ������ļ��з���,��ռ�ü��ٵĻָ��ռ�) \n"
  "        �ָ����������µ��±����ϣ��±������Ժͱ���ʱ��������ͬ \n"
  "        �ָ�ǰȷ���±��������ݿ��в����� \n"
  "        ע�⣺���ٻָ�ֱ�ӵ������ļ��в�ѯ������ɾ������������ļ� \n"
  "              ����ǹ���洢��ʽ����ȷ���ָ�ʱ�ı����ļ���" DBPLUS_STR "���е�����\n"
  "              �Ͽ��Է��ʲ��Ҿ�����ͬ��·�����ơ�\n"
  "    unpack: <���ݿ���>  <����> <�����ļ���> \n"
  "        ���ݻָ�(�����ָ�,��ҪԤ���ָ��ռ�) \n"
  "    list: <�����ļ���> [<���ݿ���> <����> [log] ]\n "
  "        �г������ļ����߲����ļ�(.DTP)�е����� \n" 
  "	   ��ѡ������ <���ݿ���> <����> \n"
  "          �г�ָ����Ĵ洢��ϸ�� \n"
  "          δָ����ʱ���г�ȫ����������ļ���Ŀ¼��\n"
  "        ��ѡ������log \n"
  "          ͬʱ�г�ָ�������־���ݡ� \n"
  "    rebuild: <���ݿ���>  <����> \n"
  "        �ؽ�Ŀ���Ļ��������ļ�(.DTP�ļ�) \n"
  "\n "
  "\n "
  "�����ѡ�����������ģʽ����Ч\n "
  "-dict {sys,dp,all} �����ֵ�ѡ��\n"
  "    ����ѡ�����е�һ��: \n"
  "    sys: ��mysqlϵͳ�� \n"
  "    dp: ��dp������ \n"
  "    all:��mysqlϵͳ���dp������\n"
  "     ���ѡ����Ʊ���ʱ�Ƿ����ϵͳ��Ͳ�����.\n"
  "     ���Ҫ�ָ�ϵͳ������������ʹ��dpunpack����.\n"
  "-basepath [path] \n"
  "    ���ݿ�Ļ���·��(����·��).\n"
  "    ȱʡ�Ӳ������ѯ \n"
  "-datapath [path] \n"
  "    �����ָ�ʱ�������ļ����·��.\n"
  "    ȱʡΪ����ʱ��·��\n"
  "-s <50-20000> \n"
  "    �����ļ�����󳤶�,ȱʡ��4500,��λMB\n"
  "-ai	\n"
  "    ����ȫ����������.\n"
  "    ȱʡֻ���������� \n"
  "-mh  (dsn)\n"
  "    ������" DBPLUS_STR " Server ��ODBC ������ \n"
  "-mun (username) \n"
  "    ������" DBPLUS_STR " Server ���û���\n"
  "-mpwd [password] \n"
  "    ������ " DBPLUS_STR " Server ����������\n"
  "-v	\n"
  "    ��ϸ���ģʽ.\n"
  "-nv	\n"
  "    ȡ����ϸ���ģʽ.\n"
  "-e	\n"
  "   �������̨���ִ����Ϣ.\n"
  "-ne	\n"
  "   ȡ���������̨���ִ����Ϣ.\n"
  "-h \n"
  "    ������Ϣ \n"
  "\n---------------------------------------------------------\n"
  "ʹ�����»�������: \n"
  "DP_SERVERIP \n"
  "   ��������IP��ַ \n"
  "DP_MPASSWORD  \n"
  "    ������" DBPLUS_STR " Server ����������\n"
  "    Ҫ����Ϊ���ܺ������\n"
  "    ʹ�ò��� -enc <��������> ����\n"
  "DP_MUSERNAME \n"
  "    ������" DBPLUS_STR " Server ���û���\n"
  "DP_DSN \n"
  "    ������" DBPLUS_STR " Server ��ODBC ������ \n"
  "DP_ECHO \n"
  "   ����̨���ִ����Ϣ.1:��,0:�ر�\n"
  "DP_VERBOSE \n"
  "    ��ϸ���ģʽ.1:��,0:�ر�\n"
  ;

struct   cmdparam:public cmd_base {
  char basedir[PARAMLEN];
  char dbname[PARAMLEN];
  char tabname[PARAMLEN];
  char newtabname[PARAMLEN];
  char filename[PARAMLEN];
  char datadir[PARAMLEN];
  bool all_index;
  int splitlen;
  bool all_tables;
  bool withlog;
  enum {
  	INC_NULL=0,INC_SYS,INC_DP,INC_ALL,INC_LAST
  } inc;
  enum {
  	DP_NULL=0,DP_PACK,DP_RESTORE,DP_UNPACK,DP_LIST,DP_REBUILD,DP_LAST
  } funcid;
  
  cmdparam():cmd_base() {
  	splitlen=4500;
  	inc=INC_NULL;
  	funcid=DP_NULL;
  	basedir[0]=dbname[0]=tabname[0]=newtabname[0]=filename[0]=datadir[0]=0;
  	all_index=false;
  	all_tables=false;
  }
  
  void GetEnv() {
        cmd_base::GetEnv();
  	funcid=DP_NULL;
  }
  
  bool checkvalid() {
  	bool ret=cmd_base::checkvalid();
  	if(funcid==DP_NULL || funcid>=DP_LAST) {
  	 printf("����Ĺ��ܴ���,��ʹ��-h�����鿴����..\n");
  	  ret=false;
  	}
  	if(inc<INC_NULL || inc>=INC_LAST) {
  	 printf("����İ�������,��ʹ��-h�����鿴����..\n");
  	  ret=false;
  	}
  	if(splitlen>20000 || splitlen<50) {
  	 printf("������ļ���С%d,������50-20000..\n",splitlen);
  	 ret=false;
  	}
  	return ret;
  }
 
  bool ExtractParam(int &p,int n,char **argv) {
  	if(cmd_base::ExtractParam(p,n,argv)) return true;
  	if(strcmp(argv[p],"-f")==0) {
  	  if(p+1>=n) ThrowWith("ȱ�ٹ��ܴ���,ʹ��'pack,restore,unpack,list,rebuild'֮һ");
  	     p++;
  	  if(strcmp(argv[p],"pack")==0) {
  	    if(p+1>=n || argv[p+1][0]=='-') ThrowWith("������Ҫָ�����ݿ���.");
	    if(p+2>=n || argv[p+2][0]=='-') ThrowWith("������Ҫָ������.");
	    if(p+3>=n || argv[p+3][0]=='-') ThrowWith("������Ҫָ���ļ���.");
  	    strcpy(dbname,argv[++p]);
  	    strcpy(tabname,argv[++p]);
  	    strcpy(filename,argv[++p]);
	    funcid=DP_PACK;
  	  }
  	  else if(strcmp(argv[p],"restore")==0) {
  	    if(p+1>=n || argv[p+1][0]=='-') ThrowWith("���ٻָ���Ҫָ�����ݿ���.");
	    if(p+2>=n || argv[p+2][0]=='-') ThrowWith("���ٻָ���Ҫָ������.");
	    if(p+3>=n || argv[p+3][0]=='-') ThrowWith("���ٻָ���Ҫָ���ָ��ı���.");
	    if(p+4>=n || argv[p+4][0]=='-') ThrowWith("���ٻָ���Ҫָ���ļ���.");
  	    strcpy(dbname,argv[++p]);
  	    strcpy(tabname,argv[++p]);
  	    strcpy(newtabname,argv[++p]);
  	    strcpy(filename,argv[++p]);
  	  	funcid=DP_RESTORE;
  	  }
  	  else if(strcmp(argv[p],"unpack")==0) {
  	    if(p+1>=n || argv[p+1][0]=='-') ThrowWith("�ָ���Ҫָ�����ݿ���.");
	    if(p+2>=n || argv[p+2][0]=='-') ThrowWith("�ָ���Ҫָ������.");
	    if(p+3>=n || argv[p+3][0]=='-') ThrowWith("�ָ���Ҫָ���ļ���.");
  	    strcpy(dbname,argv[++p]);
  	    strcpy(tabname,argv[++p]);
  	    strcpy(filename,argv[++p]);
  	  	funcid=DP_UNPACK;
  	  }
  	  else if(strcmp(argv[p],"list")==0) {
	    if(p+1>=n || argv[p+1][0]=='-') ThrowWith("list��Ҫָ���ļ���.");
  	     strcpy(filename,argv[++p]);
  	    if(p+2<n) {
  	     strcpy(dbname,argv[++p]);
  	     strcpy(tabname,argv[++p]);
  	     if(p+1<n && strcmp(argv[++p],"log")==0)
  	     	withlog=true;
  	     else withlog=false;
  	    }
  	    else dbname[0]=tabname[0]=0;
  	    funcid=DP_LIST;
  	  }
  	  else if(strcmp(argv[p],"rebuild")==0) {
  	    if(p+1>=n || argv[p+1][0]=='-') ThrowWith("rebuild��Ҫָ�����ݿ���.");
	    if(p+2>=n || argv[p+2][0]=='-') ThrowWith("rebuild��Ҫָ������.");
  	    strcpy(dbname,argv[++p]);
  	    strcpy(tabname,argv[++p]);
  	    funcid=DP_REBUILD;
  	  }
  	  else ThrowWith("����Ĺ��ܴ��� '%s',ʹ��'pack,restore,unpack,list,rebuild'֮һ",argv[p]);
  	}
  	else if(strcmp(argv[p],"-dict")==0) {
  	  if(p+1>=n) ThrowWith("ȱ�ٰ�������,ʹ��'sys,dp,all'֮һ");
  	     p++;
  	  if(strcmp(argv[p],"sys")==0) inc=INC_SYS;
  	  else if(strcmp(argv[p],"dp")==0) inc=INC_DP;
  	  else if(strcmp(argv[p],"all")==0) inc=INC_ALL;
  	  else ThrowWith("����İ������� '%s',ʹ��'sys,dp,all'֮һ",argv[p]);
  	}
  	else if(strcmp(argv[p],"-ai")==0) {
  	 all_index=true;
	}
  	else if(strcmp(argv[p],"-at")==0) {
  	 all_tables=true;
	}
  	else if(strcmp(argv[p],"-basepath")==0) {
  	  if(p+1>=n || argv[p+1][0]=='-') ThrowWith(" -basepath ѡ����Ҫָ��·��.");
  	  strcpy(basedir,argv[++p]);
	}
  	else if(strcmp(argv[p],"-datapath")==0) {
  	  if(p+1>=n || argv[p+1][0]=='-') ThrowWith(" -datapath ѡ����Ҫָ��·��.");
  	  strcpy(datadir,argv[++p]);
	}
  	else if(strcmp(argv[p],"-s")==0) {
  	  if(p+1>=n || argv[p+1][0]=='-') ThrowWith("-s ѡ����Ҫָ���ļ���С(MB).");
  	  splitlen=atoi(argv[++p]);
	}  		
	
  	else ThrowWith("������������ :'%s'",argv[p]);
	return true;
  }

};  	
int cmd_base::argc=0;
char **cmd_base::argv=(char **)NULL;


int Start(void *p);
enum RUNMODE {
 DTIO_NULL=0,DTIO_PACK=10,DTIO_UNPACK,DTIO_LIST,DTIO_BIND ,DTIO_DETACH };
RUNMODE runmode;
bool alltables=false;
int main(int argc,char **argv) {
    
    int nRetCode = 0;
    if(argc<2 || (argc==2 && (strcmp(argv[1],"-h")==0 ||  strcmp(argv[1],"--h")==0 || 
         strcmp(argv[1],"--help")==0 || strcmp(argv[1],"help")==0 || strcmp(argv[1],"-help")==0) ) ){
    	printf("������Ϣ. \n\n %s",helpmsg);
    	return 0;
    }
   if(argc==3 && strcmp(argv[1],"pack")==0 && strcmp(argv[2],"-at")==0)  {
   	runmode=DTIO_PACK;alltables=true;
   }
   else if(argc==2) {
     if(strcmp(argv[1],"pack")==0) runmode=DTIO_PACK;
     else if(strcmp(argv[1],"unpack")==0) runmode=DTIO_UNPACK;
     else if(strcmp(argv[1],"listpack")==0) runmode=DTIO_LIST;
     else if(strcmp(argv[1],"bindpack")==0) runmode=DTIO_BIND;
     else if(strcmp(argv[1],"restore")==0) runmode=DTIO_DETACH;
   }
   else runmode=DTIO_NULL;
    cmd_base::argc=argc;
    cmd_base::argv=(char **)argv;
    WOCIInit("dp_pack");
    int corelev=2;
    const char *pcl=getenv("DP_CORELEVEL");
    if(pcl!=NULL) corelev=atoi(pcl);
    if(corelev<0 || corelev>2) corelev=2;
    nRetCode=wociMainEntrance(Start,true,NULL,corelev);
    WOCIQuit(); 
    printf("\n");
    return nRetCode;
}
const char *GetDPLibVersion();
cmdparam *pcp=NULL;
int Start(void *p) {
   wociSetOutputToConsole(TRUE);
   DbplusCert::initInstance();
   DbplusCert::getInstance()->printlogo();
    printf(DBPLUS_STR " ������汾 :%s \n",GetDPLibVersion());
   cmdparam cp;
   cp.all_tables=alltables;
   pcp=&cp;
   cp.GetEnv();
   wociSetEcho(cp.verbosemode);
   wociSetOutputToConsole(cp.echomode);
   dtioMain dm;
   char date[20];
  	wociGetCurDateTime(date);
  	int y,m,d;
        DbplusCert::getInstance()->GetExpiredDate(y,m,d);
        if(y>0 && wociGetYear(date)*100+wociGetMonth(date)>y*100+m) 
  	  ThrowWith("���õ�" DBPLUS_STR "�汾̫�ϣ�����º�ʹ��(Your " DBPLUS_STR " is too old,please update it)!");
   switch(runmode) {
   	case DTIO_PACK:
   	 dm.BackupPrepare();
   	 dm.Backup();
   	 break;
   	case DTIO_UNPACK:
   	 dm.RestorePrepare();
   	 dm.PreviousRestore();
   	 break;
   	case DTIO_LIST:
   	 dm.List();
   	 break;
   	case DTIO_BIND:
   	 dm.BindPrepare();
   	 dm.PreviousBind();
   	 break;
   	case DTIO_DETACH:
   	 dm.DetachPrepare();
   	 dm.PreviousDetach();
   	 break;
   	case DTIO_NULL:
   	{
          cp.GetParam();
	   if(!cp.checkvalid()) return -1;
   	  dm.RunByParam(&cp);
   	}
   	 break;
   	default:
   	 printf("��֧�ֵ�����ģʽ.\n");
   }
   DbplusCert::ReleaseInstance();
   return 0;
}

void checkPathEnd(char *basedir)
{
	 if(basedir[strlen(basedir)-1]!=PATH_SEPCHAR)
	  strcat(basedir,PATH_SEP);
}

void getBasePath(AutoMt &mt,char *basedir) {
	 mt.FetchAll("select * from dp.dp_path where pathtype='msys'");
	 if(mt.Wait()!=1) ThrowWith("���һ���Ŀ¼ʧ�ܣ�����dp_path���ͬ��ʶ���.");
	 strcpy(basedir,mt.PtrStr("pathval",0));
	 checkPathEnd(basedir);
	}

void dtioMain::RunByParam(void *_cp)
{
	cmdparam *cp=(cmdparam *)_cp;
	if(cp->funcid!=cmdparam::DP_LIST) {
	 dts.SetHandle(wociCreateSession(cp->musrname,cp->mpswd,cp->mhost,DTDBTYPE_ODBC));
	 conn.Connect(cp->serverip,cp->musrname,cp->mpswd,NULL,cp->port);
	 myparam=dtparam=false;
	 if(cp->inc==cmdparam::INC_SYS || cp->inc==cmdparam::INC_ALL) 
	 	myparam=true;
	 else if(cp->inc==cmdparam::INC_DP || cp->inc==cmdparam::INC_ALL) 
	 	dtparam=true;
	 psoledidxmode=!cp->all_index;
	 AutoMt mt(dts);
	  if(strlen(cp->basedir)>0) {
	 	strcpy(basedir,cp->basedir);
	 	checkPathEnd(basedir);
	  }
	  else getBasePath(mt,basedir);
	}
	switch(cp->funcid) {
	case cmdparam::DP_PACK:
		dtdata=true;
		DEFAULT_SPLITLEN=(int8B)cp->splitlen*1024*1000;
		strcpy(streamPath,cp->filename);
		Backup(cp->dbname,cp->tabname);
		break;
	case cmdparam::DP_UNPACK:
	 	myparam=dtparam=false;
		dtdata=true;
		strcpy(streamPath,cp->filename);
		PreviousRestore(cp->dbname,cp->tabname,cp->datadir[0]!=0?cp->datadir:NULL);
		break;
	case cmdparam::DP_REBUILD:
		CreateBind(cp->dbname,cp->tabname);
		break;
	case cmdparam::DP_LIST:
		List(cp->filename,cp->withlog,cp->dbname[0]==0?NULL:cp->dbname,cp->tabname[0]==0?NULL:cp->tabname);
		break;
	case cmdparam::DP_RESTORE:
	 	myparam=dtparam=false;
		dtdata=true;
		strcpy(streamPath,cp->filename);
		PreviousDetach(cp->dbname,cp->tabname,cp->newtabname);
		break;
	}
}

dtioMain::dtioMain() {
	myparam=dtparam=false;configfiles=psoledidxmode=false;
	basedir[0]=streamPath[0]=destdir[0]=0;pdtio=NULL;
}

dtioMain::~dtioMain() {
	if(pdtio!=NULL) delete pdtio;
}

void dtioMain::BindPrepare() {
	printf(DBPLUS_STR "�����ļ����� \n"
		" ʹ�� " DBPLUS_STR "�����ļ����ɳ���,���԰�ĳ��" DBPLUS_STR "��Ĳ����洢���ļ��С�\n"
	    " ע��:  �����������Ŀ��" DBPLUS_STR " ���ݿ����������У�\n");
	char un[30],pwd[80],sn[100];
	//printf("���ӵ�DBPlus���ݿ⣬��ȷ�����ӵ�����DBPlusʵ��.\n");
	//getdbcstr(un,pwd,sn,"�û���");
  dts.SetHandle(wociCreateSession(pcp->musrname,pcp->mpswd,pcp->mhost,DTDBTYPE_ODBC));
	conn.Connect(pcp->serverip,pcp->musrname,pcp->mpswd,NULL,pcp->port);
  //dts.SetHandle(wociCreateSession(un,pwd,sn,DTDBTYPE_ODBC));
	//conn.Connect("localhost",un,pwd);
	AutoMt mt(dts,MAX_TABLES);
        getBasePath(mt,basedir);

	printf(DBPLUS_STR "����·��:%s.\n",basedir);
	myparam=dtparam=false;
	psoledidxmode=false;
	mt.FetchAll("select tabid ,databasename ,tabname ,recordnum ,(totalbytes/1024/1024) "
	//mt.FetchAll("select tabid as \"����\",databasename as \"���ݿ�\",tabname as \"����\",recordnum as \"��¼��\",(totalbytes/1024/1024) as\"������\""
		" from dp.dp_table where recordnum>0");
	mt.Wait();
	wociSetColumnDisplay(mt,NULL,0,"����",-1,-1);
	wociSetColumnDisplay(mt,NULL,1,"���ݿ�",-1,-1);
	wociSetColumnDisplay(mt,NULL,2,"����",-1,-1);
	wociSetColumnDisplay(mt,NULL,3,"��¼��",-1,0);
	wociSetColumnDisplay(mt,NULL,4,"������",-1,0);

	//dtmt.SetMt(mt,false);//��ѡ
	dtmt.SetMt(mt);
	dtmt.DoSel("Ŀ���ѡ��(��������λΪMB)");
}

void dtioMain::PreviousBind() {
	int sel=dtmt.GetFirstSel();
	int mt=dtmt.GetMt();
	while(sel!=-1) {
		char dbn[200];
		char tbn[200];
		wociGetStrValByName(mt,"databasename",sel,dbn);
		wociGetStrValByName(mt,"tabname",sel,tbn);
		CreateBind(dbn,tbn);
		sel=dtmt.GetNextSel(sel);
	}
}

void dtioMain::CreateBind(const char *dbn,const char *tbn) {
	sprintf(streamPath,"%s%s/%s.DTP",basedir,dbn,tbn);
	printf("׼������'%s.%s'�Ĳ����ļ�...\n",dbn,tbn);
	if(pdtio!=NULL) delete pdtio;
	pdtio=new dtioStreamFile(basedir);
	pdtio->SetStreamName(streamPath);
	pdtio->SetWrite(false);
	pdtio->StreamWriteInit(DTP_BIND);
	DoBind(dbn,tbn);
	printf("����������...\n");
	//wociMTCompactPrint(pdtio->GetContentMt()->GetMt(),0,NULL);
	printf("��ʼ���ɲ����ļ�...\n");
	pdtio->SetWrite(true);
	pdtio->StreamWriteInit(DTP_BIND);
	DoBind(dbn,tbn);
	char fullpath[300];
	sprintf(fullpath,"%s.%s",dbn,tbn);
	conn.FlushTables(fullpath);
	printf("����...\n");
	//wociMTCompactPrint(pdtio->GetContentMt()->GetMt(),0,NULL);
	//delete pdtio;
}

void dtioMain::DoBind(const char *dbn,const char *tbn) {
	//	 assert(pdtio);
	//���汸������Դ��MySQL��������Ŀ¼
	pdtio->SetOutDir(basedir);
	if(pdtio==NULL) 
		ThrowWith("pdtio is nil in dtioMain::DoBind.");
	dtioDTTable dtt(dbn,tbn,pdtio,false);
	dtt.FetchParam(dts);
	dtt.SerializeParam();
}

void dtioMain::BackupPrepare() {
	printf(DBPLUS_STR "����\n"
		" ʹ�� " DBPLUS_STR "���ݳ���,���԰����ݿ����ñ�" DBPLUS_STR "���ñ�" DBPLUS_STR "���ݵ��ļ�ϵͳ��Զ���������ۺϱ���ϵͳ��\n"
	    " ע��:  ����������ڱ��ݻ�ָ���Ŀ��" DBPLUS_STR "���ݿ����������У�\n");
	char un[30],pwd[80],sn[100];
//	printf("���ӵ�DBPlus���ݿ⣬��ȷ�����ӵ�����DBPlusʵ��.\n");
//	getdbcstr(un,pwd,sn,"�û���");
//	dts.SetHandle(wociCreateSession(un,pwd,sn,DTDBTYPE_ODBC));
  dts.SetHandle(wociCreateSession(pcp->musrname,pcp->mpswd,pcp->mhost,DTDBTYPE_ODBC));
         if(pcp->all_tables)
           printf("��ȫ����.\n");
         else 
	   printf("��������.\n");
	   
	// BEGIN: DM-192 ,modify by liujs
	#if 0 
	AutoMt mt(dts,MAX_TABLES);
	#else
	AutoMt mt(dts,200);	
	#endif
	// End : DM-192
	
	getBasePath(mt,basedir);
	printf(DBPLUS_STR "����·��:%s.\n",basedir);
	getString("Ŀ��·�����ļ���:","dpdump.dtp",streamPath);
	DEFAULT_SPLITLEN=(int8B)getOption("�ļ����(50MB-20000MB):",4500,50,20000)*1024*1000;
	myparam=false;//GetYesNo("�Ƿ񱸷����ݿ����ñ�(Y/N)<no>:",false);
	dtparam=false;//GetYesNo("�Ƿ񱸷�DBPlus���ñ�(Y/N)<no>:",false);
	
	dtdata=true;//GetYesNo("�Ƿ񱸷�DBPlus��(Y/N)<yes>:",true);
	// BEGIN: DM-192 -- add by liujs
	int fetchConditonLen = 0;
	char fetchCondition[1024] = {0};
    memset(fetchCondition,0x0,1024);
    char sqlTxt[2048] = {0};
    memset(sqlTxt,0x0,2048);
    fetchConditonLen = getString("���ݲ�ѯ����(����:tabid = 1 and tabname = 'tbname_1'):","",fetchCondition);
    // END : DM-192 
    
	if(dtdata) {
		psoledidxmode=GetYesNo("������" DBPLUS_STR "��������(Y/N)<yes>:",true);
		// BEGIN: DM-192 -- modify by liujs  
		#if 0
		mt.FetchAll("select tabid ,databasename ,tabname,recordnum ,(totalbytes/1024/1024)"
		//mt.FetchAll("select tabid \"����\",databasename \"���ݿ�\",tabname \"����\",recordnum \"��¼��\",totalbytes/1024/1024 \"������\""
			" from dp.dp_table where recordnum>0 %s",pcp->all_tables?"":" and cdfileid=2");
	    #else		
	    sprintf(sqlTxt,"select tabid ,databasename ,tabname,recordnum ,(totalbytes/1024/1024) from dp.dp_table where recordnum>0 %s",pcp->all_tables?"":" and cdfileid=2");
	    if(fetchConditonLen)
	    {
	    	sprintf(sqlTxt+strlen(sqlTxt)," and %s",fetchCondition);
	    }
		mt.FetchFirst(sqlTxt);
	    #endif			
	    // END: DM-192
	    
		mt.Wait();
		if(mt.GetRows()<1) 
			ThrowWith("%sû�ҵ���Ҫ���ݵı�.",pcp->all_tables?"":"��������ʱ");
		wociSetColumnDisplay(mt,NULL,0,"����",-1,-1);
		wociSetColumnDisplay(mt,NULL,1,"���ݿ�",-1,-1);
		wociSetColumnDisplay(mt,NULL,2,"����",-1,-1);
		wociSetColumnDisplay(mt,NULL,3,"��¼��",-1,0);
		wociSetColumnDisplay(mt,NULL,4,"������",-1,0);
		dtmt.SetMt(mt);
		dtmt.DoSel("����Ŀ���ѡ��(��������λΪMB)");
	}
}

void dtioMain::Backup(const char *dbname,const char *tabname) {
	printf("׼������...\n");
	if(pdtio==NULL) pdtio=new dtioStreamFile(basedir);
	pdtio->SetStreamName(streamPath);
	pdtio->SetWrite(false);
	pdtio->StreamWriteInit();
	DoBackup(dbname,tabname);
	printf("����������...\n");
	//wociGeneTable(pdtio->GetContentMt()->GetMt(),"dt_test",dts);
	//wociAppendToDbTable(pdtio->GetContentMt()->GetMt(),"dt_test",dts,true);
	//wociMTCompactPrint(pdtio->GetContentMt()->GetMt(),0,NULL);
	printf("��ʼ����...\n");
	pdtio->SetWrite(true);
	pdtio->StreamWriteInit();
	DoBackup(dbname,tabname);
	if(dtdata) 
	{
	        AutoStmt stmt(dts);
		if(dbname==NULL) {
		 int sel=dtmt.GetFirstSel();
		 int mt=dtmt.GetMt();
		 while(sel!=-1) {
			char dbn[200];
			char tbn[200];
			wociGetStrValByName(mt,"databasename",sel,dbn);
			wociGetStrValByName(mt,"tabname",sel,tbn);
			stmt.DirectExecute("update dp.dp_table set cdfileid=3 where databasename=lower('%s') and tabname=lower('%s')",dbn,tbn);
			sel=dtmt.GetNextSel(sel);
		 }
		}
		else stmt.DirectExecute("update dp.dp_table set cdfileid=3 where databasename=lower('%s') and tabname=lower('%s')",dbname,tabname);
	}
	printf("���ݽ���...\n");
	//wociMTCompactPrint(pdtio->GetContentMt()->GetMt(),0,NULL);
}

void dtioMain::DoBackup(const char *dbname,const char *tabname) {
	//	 assert(pdtio);
	//���汸������Դ��MySQL��������Ŀ¼
	pdtio->SetOutDir(basedir);
	if(pdtio==NULL) 
		ThrowWith("pdtio is nil in dtioMain::DoBackup.");
	if(myparam) {
		dtioMySys msys(pdtio);
		msys.AddTables();
	}
	if(dtparam) {
		dtioDTSys dtsys(pdtio);
		dtsys.AddTables();
	}
	if(dtdata) 
	{
		dtioDTTableGroup dtg(dts,DTIO_UNITTYPE_NORMDTTAB,"Normal " DBPLUS_STR " table",pdtio,psoledidxmode);
		if(dbname==NULL) {
		 int sel=dtmt.GetFirstSel();
		 int mt=dtmt.GetMt();
		 while(sel!=-1) {
			char dbn[200];
			char tbn[200];
			wociGetStrValByName(mt,"databasename",sel,dbn);
			wociGetStrValByName(mt,"tabname",sel,tbn);
			dtg.AddTable(dbn,tbn);
			sel=dtmt.GetNextSel(sel);
		 }
		}
		else dtg.AddTable(dbname,tabname);
	}
}

void dtioMain::List(const char *filename,bool withlog,const char *dbn,const char *tbn) {
	if(filename==NULL) getString("Ŀ��·�����ļ���:","dpdump.dtp",streamPath);
	else strcpy(streamPath,filename);
	if(pdtio==NULL) pdtio=new dtioStreamFile(basedir);
	pdtio->SetStreamName(streamPath);
	pdtio->SetWrite(false);
	pdtio->StreamReadInit();
	
	int mt,rn,i;
	char dbname[PATH_LEN],tabname[PATH_LEN];
	void *ptr[3];
	ptr[0]=dbname;ptr[1]=tabname;ptr[2]=NULL;
	mt=wociCreateMemTable();
	{
		dtioMySys dm(pdtio);
		rn=dm.GetTableNum();
		if(rn==0) {
			printf("ָ���ı����ļ��в������ݿ������.\n");
		}
		else {
		wociAddColumn(mt,"databasename","���ݿ�",COLUMN_TYPE_CHAR,PATH_LEN,0);
		wociAddColumn(mt,"tabname","����",COLUMN_TYPE_CHAR,PATH_LEN,0);
		wociBuild(mt,rn);
		for(i=0;i<rn;i++) {
			dm.SearchTable(i,dbname,tabname);
		        wociInsertRows(mt,ptr,NULL,1);
		}
		printf("���ݿ�ϵͳ��:\n");
		wociMTCompactPrint(mt,0,NULL);
		}
	}
	{
		dtioDTSys dm(pdtio);
		rn=dm.GetTableNum();
		if(rn==0) {
			printf("ָ���ı����ļ��в���" DBPLUS_STR "����������.\n");
		}
		else {
		wociClear(mt);
		wociAddColumn(mt,"databasename","���ݿ�",COLUMN_TYPE_CHAR,60,0);
		wociAddColumn(mt,"tabname","����",COLUMN_TYPE_CHAR,80,0);
		wociBuild(mt,rn);
		for(i=0;i<rn;i++) {
			dm.SearchTable(i,dbname,tabname);
			wociInsertRows(mt,ptr,NULL,1);
		}
		printf(DBPLUS_STR "������:\n");
		wociMTCompactPrint(mt,0,NULL);
		}
	}
	{
		double st,len;
		dtioUnitMap *map=pdtio->GetContentMt();
		rn=map->GetItemNum(DTIO_UNITTYPE_DTPARAM);
		if(rn==0) {
			printf("ָ���ı����ļ��в���" DBPLUS_STR "������.\n");
			dtdata=false;
		}
		else {
		wociClear(mt);
		wociAddColumn(mt,"databasename","���ݿ�",COLUMN_TYPE_CHAR,60,0);
		wociAddColumn(mt,"tabname","����",COLUMN_TYPE_CHAR,80,0);
		wociBuild(mt,rn);
		for(i=0;i<rn;i++) {
			map->GetItem(DTIO_UNITTYPE_DTPARAM,i,dbname,tabname,st,len);
			//if((dbn==NULL || strcmp(dbname,dbn)==0) &&(tbn==NULL || strcmp(tabname,tbn))
			     wociInsertRows(mt,ptr,NULL,1);
		}
		printf( DBPLUS_STR "��:\n");
		wociMTCompactPrint(mt,0,NULL);
		}
	}
	wocidestroy(mt);
	printf( DBPLUS_STR "�洢��Ŀ:\n");
	wociMTCompactPrint(pdtio->GetContentMt()->GetMt(),0,NULL);
	if(dbn!=NULL && tbn!=NULL) {
	 dtioDTTable dtt(dbn,tbn,pdtio,false);
 	 dtt.DeserializeParam();
	 printf( DBPLUS_STR "������ϸ:\n");
	 dtt.GetParamMt()->PrintDetail(withlog);
	}
	delete pdtio;
	pdtio=NULL;
}

void dtioMain::RestorePrepare()
{
	printf( DBPLUS_STR "�ָ� \n"
		" ʹ��" DBPLUS_STR "�ָ�����,���Իָ����ݿ����ñ�" DBPLUS_STR "���ñ�" DBPLUS_STR "��\n"
	    " ע��:  �������������Ҫ�ָ���Ŀ��" DBPLUS_STR "���ݿ����������У�\n");
	char un[30],pwd[80],sn[100];
	printf("���ӵ�" DBPLUS_STR "���ݿ⣬��ȷ�����ӵ�����" DBPLUS_STR "ʵ��.\n");
	//��������
	//strcpy(un,"dtsys");strcpy(pwd,"gcmanager");strcpy(sn,"dtagt");
	//getdbcstr(un,pwd,sn,"�û���");
	//dts.SetHandle(wociCreateSession(un,pwd,sn,DTDBTYPE_ODBC));
	dts.SetHandle(wociCreateSession(pcp->musrname,pcp->mpswd,pcp->mhost,DTDBTYPE_ODBC));
	conn.Connect(pcp->serverip,pcp->musrname,pcp->mpswd,NULL,pcp->port);

	//printf("���ӵ�DT�����ݿ�(MySQL)����ȷ�����ӵ�����DTʵ��.\n");
	//getdbcstr(un,pwd,sn,"�û���");
	//strcpy(un,"root");strcpy(pwd,"cdr0930");strcpy(sn,"dtsvr");
	sn[0]=0;
	//conn.Connect("localhost",un,pwd);
	AutoMt mt(dts,MAX_TABLES);
	 getBasePath(mt,basedir);
	
	//getString("���ݿ����Ŀ��·��:",basedir,basedir);
	//����һ�������ʱʹ��
	//getString("���ݿ����Ŀ��·��:","/dbsturbo/dttemp/",basedir);
	if(*(basedir+strlen(basedir)-1)!=PATH_SEPCHAR) strcat(basedir,PATH_SEP);
	getString("Ŀ��·�����ļ���:","dpdump.dtp",streamPath);
	myparam=false;//GetYesNo("�Ƿ�ָ����ݿ����ñ�(Y/N)<no>:",false);
	dtparam=false;//GetYesNo("�Ƿ�ָ�DBPlus���ñ�(Y/N)<no>:",false);
	dtdata=true;//GetYesNo("�Ƿ�ָ�DBPlus��(Y/N)<yes>:",true);
}

int dtioMain::SetSequence() {
	AutoMt mt(dts,100);
	mt.FetchAll("select max(tabid) mtid from dp.dp_table");
	mt.Wait();
	int mxid=mt.GetInt("mtid",0);
	mt.FetchAll("select max(sysid) maxid from dp.dp_datasrc");
	mt.Wait();
	mxid=max(mt.GetInt("maxid",0),mxid);
	mt.FetchAll("select max(pathid) maxid from dp.dp_path");
	mt.Wait();
	mxid=max(mt.GetInt("maxid",0),mxid);
	mt.FetchAll("select max(mdfid) maxid from dp.dp_middledatafile");
	mt.Wait();
	mxid=max(mt.GetInt("maxid",0),mxid);

	mt.FetchAll("select * from dp.dp_seq");
	mt.Wait();
	AutoStmt stmt(dts);
	if(mt.GetInt("id",0)<mxid) 
		stmt.DirectExecute("update dp.dp_seq set id=%d",mxid);
	return 0;
}

//����ʱ��Ŀ�����ܺ��ж��������������ֻ�����������������Ҫ�ؽ�����
// ����DTP�ļ���ǰ��mysqld������Ŀ����ǿձ� ����ֻ��Ŀ����Ͻ������������޸�
// 2008-06-06: mysql51��ֲ�� �����3.2.9�ı����ļ� ���ܽ����������µ�5.1��ʽ�����������ļ���ʽ���������
int dtioMain::CreateDataTableAndIndex(const char *dbname,const char *tbname,dtparams_mt *pdtm) {
	//AutoMt mt(dts,100);
	char fulltbname[300];
	//sql��仺�壬����Ŀ�������кܶ��ֶΣ�Ԥ���㹻�ռ�
	char sqlbf[3000];
	sprintf(fulltbname,"%s.%s",dbname,tbname);
	AutoMt destmt(dts,10);
	destmt.FetchFirst("select * from %s",fulltbname);
	destmt.Wait();
	/*
	mt.FetchAll("select tabid from dt_table where databasename='%s' and tabname='%s'",dbname,tbname);
	int rn=mt.Wait();
	if(rn<1) ThrowWith("�Ҳ���Ŀ���");
	int tabid=mt.GetInt("tabid",0);
	mt.FetchAll("select filename from dt_datafilemap where tabid=%d and fileflag=0 limit 2",tabid);
	rn=mt.Wait();
	if(rn<1) ThrowWith("����Ŀ���ṹʱ�Ҳ��������ļ���");
	dt_file idf;
	idf.Open(mt.PtrStr("filename",0),0);
	AutoMt destmt(0,10);
	destmt.SetHandle(idf.CreateMt(1));
	bool exist=conn.TouchTable(fulltbname);
	if(exist) {
	*/
	//printf("table %s has exist,dropped.\n",fulltbname);
	//2008 06-06 : ��������ʱ��Ķ��壬���ؽ�
	//sprintf(sqlbf,"drop table %s",fulltbname);
	//conn.DoQuery(sqlbf);
	//}
	//����Ŀ��꼰���ṹ�������ļ�
	//wociGetCreateTableSQL(destmt,sqlbf,fulltbname,true);
	//printf("%s.\n",sqlbf);
	//conn.DoQuery(sqlbf);
	//mSleep(300);
	int i;
	while(true) { 
	 AutoMt idxmt(dts,10);
	 idxmt.FetchFirst("show index from %s",fulltbname);
	 if(idxmt.Wait()<1) break;
	 sprintf(sqlbf,"drop index %s on %s",
		 idxmt.PtrStr("key_name",0),fulltbname);
	  lgprintf("ɾ������:%s.",sqlbf);
	  conn.DoQuery(sqlbf);
	}
	int rn=pdtm->GetTotIndexNum();
	if(rn<1)
	 ThrowWith("�Ҳ���%s���������",fulltbname);
	for(i=0;i<rn;i++) { 
	  //������������
	  //int id=mt.GetInt("indexid",i);
	  //char colsname[300];
	  //strcpy(colsname,mt.PtrStr("columnsname",i));
	  sprintf(sqlbf,"create index %s_%d on %s(%s)",
		  tbname,i+1,
		  fulltbname,pdtm->GetString(GetDPIndexAll(pdtio),"columnsname",i));
	  lgprintf("��������:%s.",sqlbf);
	  conn.DoQuery(sqlbf);
	}
	return 0;
}

void dtioMain::PreviousRestore(const char *dbn,const char *tbn,const char *ndtpath)
{
	printf("׼���ָ�...\n");
	bool cmdmode=dbn!=NULL;
	if(pdtio==NULL) pdtio=new dtioStreamFile(basedir);
	pdtio->SetStreamName(streamPath);
	pdtio->SetWrite(false);
	pdtio->StreamReadInit();
	//wociMTCompactPrint(pdtio->GetContentMt()->GetMt(),0,NULL);
	char dbname[PATH_LEN],tabname[PATH_LEN];
	void *ptr[3];
	ptr[0]=dbname;ptr[1]=tabname;ptr[2]=NULL;
	int mt,rn,i;
	MTChooser mysel,syssel,dtsel;
	mt=wociCreateMemTable();
	if(!cmdmode && myparam  && GetYesNo("��������Ȩ���������ڵ�MySQL���������滻,ȷ����?(Y/N)<no>:",false)) {
		printf("��������Ȩ���������ڵ�MySQL���������滻,ȷ����");
		dtioMySys dm(pdtio);
		rn=dm.GetTableNum();
		if(rn==0) {
			printf("ָ���ı����ļ��в������ݿ������.\n");
			myparam=false;
		}
		else {
		  wociAddColumn(mt,"databasename","���ݿ�",COLUMN_TYPE_CHAR,PATH_LEN,0);
		  wociAddColumn(mt,"tabname","����",COLUMN_TYPE_CHAR,PATH_LEN,0);
		  wociBuild(mt,rn);
		  for(i=0;i<rn;i++) {
		  	dm.SearchTable(i,dbname,tabname);
		  	wociInsertRows(mt,ptr,NULL,1);
		  }
		  mysel.SetMt(mt);
		  mysel.DoSel("���ݿ�ϵͳ��ָ�ѡ��");
		  int sel=mysel.GetFirstSel();
		  while(sel!=-1) {
		  	dm.Restore(conn,sel);		
		  	sel=mysel.GetNextSel(sel);
		  }
		  dm.AfterRestoreAll(conn);
		}
	}
	if(!cmdmode && dtparam && GetYesNo("�ָ����" DBPLUS_STR "��������ʧ�ϴα��������������޸�,����ȷ����" DBPLUS_STR "�����ļ��Ǻ�,ȷ��Ҫ�ָ���?(Y/N)<no>:",false)) {
		dtioDTSys dm(pdtio);
		rn=dm.GetTableNum();
		if(rn==0) {
			printf("ָ���ı����ļ��в���" DBPLUS_STR "����������.\n");
			dtparam=false;
		}
		else {
		wociClear(mt);
		wociAddColumn(mt,"databasename","���ݿ�",COLUMN_TYPE_CHAR,60,0);
		wociAddColumn(mt,"tabname","����",COLUMN_TYPE_CHAR,80,0);
		wociBuild(mt,rn);
		for(i=0;i<rn;i++) {
			dm.SearchTable(i,dbname,tabname);
			wociInsertRows(mt,ptr,NULL,1);
		}
		syssel.SetMt(mt);
		syssel.DoSel(DBPLUS_STR "������ָ�ѡ��");
		int sel=syssel.GetFirstSel();
		while(sel!=-1) {
			dm.Restore(conn,sel);
			sel=syssel.GetNextSel(sel);
		}
		dm.AfterRestoreAll(conn);
		}
	}
	if(dtdata || cmdmode) {
		double st,len;
		int sel=-1;
		dtioUnitMap *map=pdtio->GetContentMt();
		rn=map->GetItemNum(DTIO_UNITTYPE_DTPARAM);
		if(rn==0) {
			printf("ָ���ı����ļ��в���" DBPLUS_STR "������.\n");
			dtdata=false;
		}
		else {
		if(!cmdmode) {
		   wociClear(mt);
		   wociAddColumn(mt,"databasename","���ݿ�",COLUMN_TYPE_CHAR,60,0);
		   wociAddColumn(mt,"tabname","����",COLUMN_TYPE_CHAR,80,0);
		   wociBuild(mt,rn);
		   for(i=0;i<rn;i++) {
		   	map->GetItem(DTIO_UNITTYPE_DTPARAM,i,dbname,tabname,st,len);
		   	wociInsertRows(mt,ptr,NULL,1);
		   }
		   dtsel.SetMt(mt);
		   dtsel.DoSel( DBPLUS_STR "Ŀ���ָ�ѡ��");
		   sel=dtsel.GetFirstSel();
		}
		char fulldbn[300];
		char optpath[PATH_LEN];
		char fullpath[PATH_LEN];
		optpath[0]=0;
		
		while(sel!=-1 || cmdmode) {
			if(!cmdmode) {
			 wociGetStrValByName(mt,"databasename",sel,dbname);
			 wociGetStrValByName(mt,"tabname",sel,tabname);
			 sel=dtsel.GetNextSel(sel);
			 char prmpt[100];
			 if(optpath[0]==0) {
			 	sprintf(prmpt,"����'%s.%s'������ݻָ�·��<ȱʡ������ʱ·��>:",dbname,tabname);
			 	getString(prmpt,"",optpath);
			 }
			 else {
			 	sprintf(prmpt,"����'%s.%s'������ݻָ�·��:",dbname,tabname);
			 	getString(prmpt,optpath,optpath);
			 }
			}
			else {
				strcpy(dbname,dbn);
				strcpy(tabname,tbn);
				if(ndtpath!=NULL) strcpy(optpath,ndtpath);
			}
			dtioDTTable dtt(dbname,tabname,pdtio,false);
			bool overwritable,duprecords;
			char msg[3000];
			sprintf(msg,"%s.%s",dbname,tabname);
			dtt.DeserializeParam();
			if(conn.TouchTable(msg)) 
				ThrowWith("��'%s'�Ѵ���,�޷��ָ�.\n",msg);
			dtt.GetParamMt()->restoreCheck(dts,overwritable,duprecords,msg);
			if(duprecords && !overwritable) {
				printf("%s\n",msg);
				if(cmdmode) break;
				continue;
			}
			if(duprecords) {
			 printf("%s\n",msg);
			 if(cmdmode) break;
			 if(!GetYesNo("Ҫ�ָ��ı��Ѵ���,���ݷǿ�,������? Yes/No(no):",false)) continue;
			}
			SetSequence();
			try {
			if(optpath[0]==0)
				dtt.Deserialize(dts);
			else {
				if(*(optpath+strlen(optpath)-1)!=PATH_SEPCHAR) strcat(optpath,PATH_SEP);
				dtt.Deserialize(dts,optpath);
			}
			printf("�ؽ�Ŀ���ṹ:'%s.%s'.\n",dbname,tabname);
			if(pdtio->GetVersion()==DTIO_VERSION)
			 CreateDataTableAndIndex(dbname,tabname,dtt.GetParamMt());
			else 
				lgprintf("��Ҫ��ʾ�� ��ԭ�а汾�ı����ļ��ָ����ݣ���ע������������ṹ��һ����!");
		  }
		  catch (...) {
		  	printf("���ݻָ��쳣��ɾ����%s.%s:\n",dbname,tabname);
		    sprintf(msg,"dpadmin -rm %s %s",dbname,tabname);
		    system(msg);
		    throw;
		  }
			sprintf(fullpath,"%s%s/%s.DTP",basedir,dbname,tabname);
			dtioStreamFile *ldtio=NULL;
			ldtio=new dtioStreamFile(basedir);
			ldtio->SetStreamName(fullpath);
			ldtio->SetWrite(false);
			ldtio->StreamWriteInit(DTP_BIND);
			ldtio->SetOutDir(basedir);
			dtt.SerializeParam(ldtio);
			printf("���ɲ����ļ�...\n");
			ldtio->SetWrite(true);
			ldtio->StreamWriteInit(DTP_BIND);
			ldtio->SetOutDir(basedir);
			dtt.SerializeParam(ldtio);
			delete ldtio;
			ldtio=NULL;
			
			sprintf(fulldbn,"%s.%s",dbname,tabname);
			conn.FlushTables(fulldbn);
			if(strlen(msg)>0)
			lgprintf(msg);
			lgprintf("�ָ����");
			if(cmdmode) break;
		}
	     }
	}
	wocidestroy(mt);
	
	//delete pdtio;
}

void dtioMain::DoRestore()
{
}

void dtioMain::DetachPrepare()
{
	printf( DBPLUS_STR "���ٻָ� \n"
		" ʹ��" DBPLUS_STR "���ٻָ�����,��Ҫ��ԭʼ�����ļ�һ��������������ݡ�\n"
	    " ע��:  �������������Ҫ�ָ���Ŀ��" DBPLUS_STR "���ݿ����������У�\n");
	char un[30],pwd[80],sn[100];
	printf("���ӵ�" DBPLUS_STR "���ݿ⣬��ȷ�����ӵ�����" DBPLUS_STR "ʵ��.\n");
	//��������
	//strcpy(un,"dtsys");strcpy(pwd,"gcmanager");strcpy(sn,"dtagt");
	//getdbcstr(un,pwd,sn,"�û���");
	//dts.SetHandle(wociCreateSession(un,pwd,sn,DTDBTYPE_ODBC));
	dts.SetHandle(wociCreateSession(pcp->musrname,pcp->mpswd,pcp->mhost,DTDBTYPE_ODBC));
	conn.Connect(pcp->serverip,pcp->musrname,pcp->mpswd,NULL,pcp->port);

	//printf("���ӵ�DBPlus�����ݿ�(MySQL)����ȷ�����ӵ�����DBPlusʵ��.\n");
	//getdbcstr(un,pwd,sn,"�û���");
	//strcpy(un,"root");strcpy(pwd,"cdr0930");strcpy(sn,"dtsvr");
	sn[0]=0;
	//conn.Connect("localhost",un,pwd);
	AutoMt mt(dts,MAX_TABLES);
	 getBasePath(mt,basedir);
	
	//getString("���ݿ����Ŀ��·��:",basedir,basedir);
	//����һ�������ʱʹ��
	//getString("���ݿ����Ŀ��·��:","/dbsturbo/dttemp/",basedir);
	if(*(basedir+strlen(basedir)-1)!=PATH_SEPCHAR) strcat(basedir,PATH_SEP);
	getString("Ŀ��·�����ļ���:","dpdump.dtp",streamPath);
	myparam=dtparam=false;
	dtdata=true;
}

void dtioMain::PreviousDetach(const char *dbn,const char *tbn,const char *newtbn)
{
	printf("׼���ָ�...             \n");
	bool cmdmode=dbn!=NULL;
	if(pdtio==NULL) pdtio=new dtioStreamFile(basedir);
	pdtio->SetStreamName(streamPath);
	pdtio->SetWrite(false);
	pdtio->StreamReadInit();
	if(pdtio->GetStreamType()!=FULL_BACKUP) 
		ThrowWith("ָ�����ļ�����ԭʼ�����ļ�!");
	
	//wociMTCompactPrint(pdtio->GetContentMt()->GetMt(),0,NULL);
	char dbname[PATH_LEN],tabname[PATH_LEN];
	void *ptr[3];
	ptr[0]=dbname;ptr[1]=tabname;ptr[2]=NULL;
	int mt,rn,i;
	MTChooser dtsel;
	mt=wociCreateMemTable();
	if(dtdata) {
		double st,len;
		dtioUnitMap *map=pdtio->GetContentMt();
		rn=map->GetItemNum(DTIO_UNITTYPE_DTPARAM);
		if(rn==0) {
			printf("ָ���ı����ļ��в���" DBPLUS_STR "������.\n");
			dtdata=false;
		}
		else {
		int sel=-1;
		if(!cmdmode) {
		 wociClear(mt);
		 wociAddColumn(mt,"databasename","���ݿ�",COLUMN_TYPE_CHAR,60,0);
		 wociAddColumn(mt,"tabname","����",COLUMN_TYPE_CHAR,80,0);
		 wociBuild(mt,rn);
		 for(i=0;i<rn;i++) {
			map->GetItem(DTIO_UNITTYPE_DTPARAM,i,dbname,tabname,st,len);
			wociInsertRows(mt,ptr,NULL,1);
		 }
		 dtsel.SetMt(mt);
		 dtsel.DoSel( DBPLUS_STR "Ŀ���ָ�ѡ��");
		 sel=dtsel.GetFirstSel();
		}
		char fullpath[300];
		
		dtioStream *ldtio=NULL;
		while(sel!=-1 || cmdmode) {
			char desttabname[300];
			if(!cmdmode) {
			 wociGetStrValByName(mt,"databasename",sel,dbname);
			 wociGetStrValByName(mt,"tabname",sel,tabname);
			 sel=dtsel.GetNextSel(sel);
			 getString("�ָ�������:",tabname,desttabname);
			}
			else {
				strcpy(dbname,dbn);
				strcpy(tabname,tbn);
				strcpy(desttabname,newtbn==NULL?tabname:newtbn);
			}
			sprintf(fullpath,"%s%s/%s.DTP",basedir,dbname,desttabname);
			
			dtioDTTable dtt(dbname,tabname,pdtio,false);
			bool overwritable,duprecords;
			char msg[3000];
			dtt.DeserializeParam();
			sprintf(msg,"%s.%s",dbname,desttabname);
			if(conn.TouchTable(msg)) 
				ThrowWith("��'%s'�Ѵ���,�޷��ָ�.\n",msg);
			//printf("����������...\n");
			//wociMTCompactPrint(pdtio->GetContentMt()->GetMt(),0,NULL);
			
			dtt.DeserializeDestTab(desttabname);
			dtt.DeserializeIndex(desttabname);
			CreateDataTableAndIndex(dbname,desttabname,dtt.GetParamMt());

			printf("��ʼ���ɲ����ļ�...\n");
			if(ldtio!=NULL) delete ldtio;
			ldtio=new dtioStreamFile(basedir);
			ldtio->SetStreamName(fullpath);
			ldtio->SetWrite(false);
			ldtio->StreamWriteInit(DTP_DETACH,streamPath);
			ldtio->SetOutDir(basedir);
			dtt.GetParamMt()->restoreCheck(dts,overwritable,duprecords,msg);
			dtt.SerializeParam(ldtio,desttabname);

			ldtio->SetWrite(true);
			ldtio->StreamWriteInit(DTP_DETACH,streamPath);
			ldtio->SetOutDir(basedir);
			dtt.SerializeParam(ldtio,desttabname);
			
			delete ldtio;
			
			ldtio=NULL;
			sprintf(fullpath,"%s.%s",dbname,desttabname);
			conn.FlushTables(fullpath);
			printf("����...\n");
			//wociMTCompactPrint(pdtio->GetContentMt()->GetMt(),0,NULL);
			//delete pdtio;
			if(cmdmode) break;
		}
		if(ldtio!=NULL) delete ldtio;
		}
	}
	wocidestroy(mt);
	
	//delete pdtio;
}

