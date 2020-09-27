#include "dt_common.h"
//tt
#include "dt_lib.h"
#include "dt_cmd_base.h"
#ifdef WIN32
#include <process.h>
#else
#include <unistd.h>
#include <malloc.h>
#include <pthread.h>
#include <ctype.h>
#endif
#include <stdlib.h>
#include <stdio.h>
int Start(void *ptr);
void ShowDoubleByte() ;
//#define dtdbg(a) lgprintf(a)
#define dtdbg(a)
#ifdef __unix
pthread_t hd_pthread_loop;
#endif
const char *helpmsg=
    "���� : \n"
//  "-f {check,dump,mload,dload,dc,all} \n"
    "-f {dump,mload,dload,dc,all} \n"
    "    ����ѡ��,ȱʡΪdump�������й��� \n"
    "    ����ѡ�����е�һ��: \n"
    "    dump: ���ݳ�ȡ,������-dsѡ�����Դϵͳ���� \n"
    "    mload: ����Ԥ���� \n"
    "    dload: ����װ�� \n"
    "    dc: ����ѹ������ \n"
    "    all: (ȱʡ),mload+dload+dc \n"
    " -ds '1,2,3' \n"
    "   ֻ��dp_code����code_type=9���г������ͳ�ȡ����\n"
    "-rm [���ݿ���] [����] \n"
    "    ɾ��" DBPLUS_STR "��\n"
    "-mv [Դ���ݿ�] [Դ��] [Ŀ�����ݿ�] [Ŀ���]\n"
    "    ���ƶ�(����)\n"
//  "-mb [database name] [table name] \n"
//  "    ת��" DBPLUS_STR "���� MyISAM ���ʽ\n"
//  "-hmr [���ݿ���] [����] \n"
//  "    ͬ��(����Ҫ��������)�����ؽ�.\n"
    "-ck [���ݿ���] [����]\n"
    "    �����ṹ������.\n"
    "-cl [�ο�����] [�ο�����] [��Դ���ݿ���] [��Դ����] [��Ŀ�����ݿ���] [��Ŀ�����] [����ʼʱ��]\n"
    "    ���ο������ƴ���Դ��/����/Ŀ���/����/������Ϣ.\n"
    "    ��ʼʱ���������now��ʾ���ڵ�ϵͳʱ�䣬\n"
    "       ������\"2000-01-01 12:00:00\"��ʽ(ע����Ҫ˫����).\n"
    "-clf [�ο�����] [�ο�����] [��Դ���ݿ���] [��Դ����] [��Ŀ�����ݿ���] [��Ŀ�����] [����ʼʱ��]\n"
    "    ���ο������ƴ���Դ��/����/Ŀ���/����/������Ϣ,������ʽ��.\n"
    "    ��ʼʱ���������now��ʾ���ڵ�ϵͳʱ�䣬\n"
    "       ������\"2000-01-01 12:00:00\"��ʽ(ע����Ҫ˫����).\n"

    "-csit [Դ���ݿ��������] [Դ���ݿ��û���] [Դ���ݿ��û�����] [Դ���ݿ���] [Դ����] [Ŀ�����ݿ���] [Ŀ�����] \n"
    " [\"����ʼʱ��,������now() ������ 2013-01-01 12:00:01 \"] [��ʱ·�������ܴ��ڿո�] [����·�������ܴ��ڿո�] \n"
    " [ѹ������(cmptype):��ͨ-1 ����-5 ����-8 ����-10)] [�����б��ļ���] \n"
    " [\"��ȡ���,Ĭ��ֵ��default\"]\n"
    " ����: -csit //192.168.1.10:1523/dtagt1 user_name user_passwd srcDbName srcTabName dstDbName dstTabName \"now()\" /tmp/temp_test /data/dest_test 1 /tmp/createtable.ctl \"default\" \n"

    "-enc (text)\n"
    "    ������� \n"
    "-mh  (dsn)\n"
    "    ������" DBPLUS_STR " Server ��ODBC ������ \n"
    "-mun (username) \n"
    "    ������" DBPLUS_STR " Server ���û���\n"
    "-mpwd [password] \n"
    "    ������" DBPLUS_STR " Server ����������\n"
//  "-osn (oracle service name) \n"
// "    for Oracle AgentServer connection\n"
//  "-oun (oracle username) \n"
//  "    for Oracle AgentServer connection \n"
//  "-opwd [Oracle password] \n"
//  "    for Oracle AgentServer connection \n"
    "-lp 	\n"
    "   ѭ��ִ��ģʽ\n"
//  "-fr    \n"
//  "    calculate freeze time. \n"
    "-v	\n"
    "    ��ϸ���ģʽ.\n"
    "-nv	\n"
    "    ȡ����ϸ���ģʽ.\n"
    "-e	\n"
    "   ��������̨���ִ����Ϣ.\n"
    "-ne	\n"
    "   ȡ����������̨���ִ����Ϣ.\n"
    "-wt ( seconds) \n"
    "   ѭ��ִ��ģʽ��˯��ʱ��(��)\n"
    "-thn (threadnum) \n "
    "   ��������ѹ���߳���(1-16)\n"
    "-h \n"
    "    ������Ϣ \n"
    "\n---------------------------------------------------------\n"
    "ʹ�����»�������:\n"
//  "DP_INDEXNUM \n"
//  "   ����װ��ʱ�Ŀ��С:50-300(MB)\n"
    "DP_SERVERIP \n"
    "   ��������IP��ַ \n"
    "DP_MTLIMIT \n"
    "   ���ݳ�ȡʱ�Ŀ��С:50-20000(MB)\n"
//  "DP_BLOCKSIZE \n"
//  "   " DBPLUS_STR "���ݿ��С:100-512 (KB)\n"
    "DP_LOADTIDXSIZE \n"
    "   �м������ڴ�����:50-20000 (KB)\n"
    "DP_WAITTIME \n"
    "   ѭ��ִ��ģʽ��˯��ʱ��(��)\n"
    "DP_ECHO \n"
    "   ����̨���ִ����Ϣ.1:��,0:�ر�\n"
    "DP_VERBOSE \n"
    "    ��ϸ���ģʽ.1:��,0:�ر�\n"
    "DP_LOOP \n"
    "   ѭ��ִ��ģʽ\n"
    "DP_THREADNUM \n"
    "   ��������ѹ���߳���(1-16)\n"
    "DP_FUNCODE {DUMP MLOAD DLOAD DC ALL} \n"
    "    ����ѡ��,ȱʡΪDUMP�������й��� \n"
    "    ����ѡ�����е�һ��: \n"
    "    DUMP: ���ݳ�ȡ \n"
    "    MLOAD: ����Ԥ���� \n"
    "    DLOAD: ����װ�� \n"
    "    DC: ����ѹ������ \n"
    "    ALL: (ȱʡ),MLOAD+DLOAD+DC \n"
    "DP_MPASSWORD  \n"
    "    ������" DBPLUS_STR " Server ����������\n"
    "    Ҫ����Ϊ���ܺ������\n"
    "    ʹ�ò��� -enc <��������> ����\n"
    "DP_MUSERNAME \n"
    "    ����" DBPLUS_STR " Server ���û���\n"
    "DP_DSN \n"
    "    ����" DBPLUS_STR " Server ��ODBC ������ \n"
//  "DP_ORAPASSWORD \n"
//  "   password  for Oracle AgentServer connection\n"
//  "DP_ORAUSERNAME \n"
//  "   username for Oracle AgentServer connection\n"
//  "DP_ORASVCNAME \n"
//  "   service name for Oracle AgentServer connection\n"
    ;

//>> Begin:DM-228 ����������������������ṹ
typedef struct CrtTaskTableAssistParam
{
    int  cmp_type;                             // ѹ������
    char tmp_path[PARAMLEN];                   // ��ʱ·��
    char back_path[PARAMLEN];                  // ����·��
    char solid_index_list_file[PARAMLEN];      // ���������б��ļ�����
    char ext_sql[4000];                        // ����sql���
    CrtTaskTableAssistParam()
    {
        tmp_path[0] = 0;
        back_path[0] = 0;
        solid_index_list_file[0] = 0;
        ext_sql[0] = 0;
        cmp_type = 1;
    };
} CrtTaskTableAssistParam,*CrtTaskTableAssistParamPtr;
void Str2Lower(char *str)
{
    while(*str!=0)
    {
        *str=tolower(*str);
        str++;
    }
}
//<< End:DM-228

struct   cmdparam:public cmd_base
{
    //>> Begin:DM-228
    char orasvcname[PARAMLEN];
    char orausrname[PARAMLEN];
    char orapswd[PARAMLEN];
    CrtTaskTableAssistParam   objCrtTskTabAssistParam;
    //<< End: DM-228

    char srcdbn[PARAMLEN];
    char srctbn[PARAMLEN];
    char dstdbn[PARAMLEN];
    char dsttbn[PARAMLEN];
    char srcowner[PARAMLEN];
    char srctable[PARAMLEN];
    char taskdate[41];
    char dumpstrains[100];
    //char lgfname[PARAMLEN];
    enum
    {
        DP_NULL=0,DP_CHECK,DP_DUMP,DP_MDLDR,DP_DSTLDR,DP_DEEPCMP,DP_ALL,DP_MOVE,DP_REMOVE,DP_MYSQLBLOCK,DP_ENCODE,DP_HOMO_REINDEX,DP_CREATELIKE,DP_CREATELIKE_PRESERVE_FORMAT,DP_LAST,DP_CREAT_SOLID_INDEX_TABLE,
    } funcid;

    int load_tidxmem;
    int indexnum;
    int mtlimit;
    int threadnum;
    int blocksize;
    bool directIOSkip,useOldBlock;
    cmdparam():cmd_base()
    {
        threadnum=1;
        freezemode=false;
        blocksize=512;
    }

    void GetEnv()
    {
        cmd_base::GetEnv();
        const char *fncode=getenv("DP_FUNCODE");
        dumpstrains[0]=0;
        //set DP_FUNCODE or -f(-f check,-f dump,-f mload,-f dload,-f dc,-f all) parameter.
        if(fncode==NULL)
            funcid=DP_ALL;
        //else if(strcmp(fncode,"CHECK")==0) funcid=DP_CHECK;
        else if(strcmp(fncode,"DUMP")==0) funcid=DP_DUMP;
        else if(strcmp(fncode,"MLOAD")==0) funcid=DP_MDLDR;
        else if(strcmp(fncode,"DLOAD")==0) funcid=DP_DSTLDR;
        else if(strcmp(fncode,"DC")==0) funcid=DP_DEEPCMP;
        else if(strcmp(fncode,"ALL")==0) funcid=DP_ALL;
        else funcid=DP_ALL;
        SetValue(threadnum,getenv("DP_THREADNUM"));
        SetValue(mtlimit,getenv("DP_MTLIMIT"));
//      SetValue(indexnum,getenv("DP_INDEXNUM"));
        SetValue(blocksize,getenv("DP_BLOCKSIZE"));
        if(blocksize<1) blocksize=512; // default block size is 512 KB
        SetValue(load_tidxmem,getenv("DP_LOADTIDXSIZE"));
        SetValue(directIOSkip,getenv("DP_SKIP_DIRECT_IO"));
        SetValue(useOldBlock,getenv("DP_USE_OLD_BLOCK"));
    }
    bool checkvalid()
    {
        bool ret=cmd_base::checkvalid();
        if(funcid==DP_NULL || funcid>DP_CREAT_SOLID_INDEX_TABLE)
        {
            printf("Invalid function selection code ,set DP_FUNCODE or -f (dump,mload,dload,dc,all) option..\n");
            ret=false;
        }
        if(load_tidxmem<50)
        {
            printf("�м�������¼�ڴ�ʹ��%dMB ,�޸�Ϊ 50MB\n",load_tidxmem);
            load_tidxmem=50;
        }
        if(load_tidxmem<50)
        {
            printf("�м�������¼�ڴ�ʹ��%dMB ,�޸�Ϊ 50MB\n",load_tidxmem);
            load_tidxmem=50;
        }
        if(load_tidxmem>MAX_LOADIDXMEM)
        {
            printf("�м�������¼����ʹ��%dMB ,�޸�Ϊ %dMB\n",load_tidxmem,MAX_LOADIDXMEM);
            load_tidxmem=MAX_LOADIDXMEM;
        }
        //indexnum�̶�ʹ��10MB
        indexnum=10;
        /*if(indexnum<50) {
            printf("�����ڴ�ʹ��%d ,�޸�Ϊ 50MB\n",indexnum);
            indexnum=50;
        }
        if(indexnum>300) {
            printf("�����ڴ�ʹ��%d ,�޸�Ϊ 300MB\n",indexnum);
            indexnum=300;
        }
        */
        if(mtlimit<50)
        {
            printf("���ݳ�ȡ�ڴ�%d, �޸�Ϊ 50MB\n",mtlimit);
            mtlimit=50;
        }
        if(mtlimit>MAX_LOADIDXMEM)
        {
            printf("���ݳ�ȡ�ڴ�%d, �޸�Ϊ %dGB\n",mtlimit,MAX_LOADIDXMEM/1000);
            mtlimit=MAX_LOADIDXMEM;
        }
        if(blocksize<100)
        {
            printf("���ݿ��С%d, �޸�Ϊ 100KB\n",mtlimit);
            blocksize=100;
        }
        if(blocksize>512)
        {
            printf("���ݿ��С%d, �޸�Ϊ 512KB\n",mtlimit);
            blocksize=512;
        }
        //mtlimit=650000000;
        if(threadnum<1 || threadnum>16)
        {
            printf("��������ѹ���߳���ֻ����1-16,�޸�Ϊ1.\n");
            threadnum=1;
        }
        if(strlen(serverip)<1)
        {
            printf("δָ��DP_SERVERIP����,ʹ��Ĭ��ֵlocalhost.\n");
            strcpy(serverip,"localhost");
        }

        return ret;
    }

    bool ExtractParam(int &p,int n,char **argv)
    {
        if(cmd_base::ExtractParam(p,n,argv)) return true;
        if(strcmp(argv[p],"-f")==0)
        {
//        if(p+1>=n) ThrowWith("Need function code,use one of 'check,dump,mload,dload,dc,all'");
            if(p+1>=n) ThrowWith("ȱ�ٹ��ܴ���,ʹ��'dump,mload,dload,dc,all'֮һ");
            p++;
            if(strcmp(argv[p],"dump")==0) funcid=DP_DUMP;
            //else if(strcmp(argv[p],"check")==0) funcid=DP_CHECK;
            else if(strcmp(argv[p],"mload")==0) funcid=DP_MDLDR;
            else if(strcmp(argv[p],"dload")==0) funcid=DP_DSTLDR;
            else if(strcmp(argv[p],"dc")==0) funcid=DP_DEEPCMP;
            else if(strcmp(argv[p],"all")==0) funcid=DP_ALL;
//        else ThrowWith("Invalid function code '%s',use one of 'check,dump,mload,dload,dc,all'",argv[p]);
            else ThrowWith("����Ĺ��ܴ��� '%s',ʹ��'dump,mload,dload,dc,all'֮һ",argv[p]);
        }
        else if(strcmp(argv[p],"-thn")==0)
        {
            if(p+1>=n || argv[p+1][0]=='-') ThrowWith("-thn ѡ����Ҫָ���߳���.");
            threadnum=atoi(argv[++p]);
        }
        else if(strcmp(argv[p],"-ds")==0)
        {
            if(p+1>=n || argv[p+1][0]=='-') ThrowWith("-ds ѡ����Ҫָ����������.");
            strcpy(dumpstrains,argv[++p]);
        }
        else if(strcmp(argv[p],"-hmr")==0)
        {
            funcid=DP_HOMO_REINDEX;
            if(p+1>=n || argv[p+1][0]=='-') ThrowWith("ͬ�������ؽ�(-hmr)������Ҫָ�������ͱ���.");
            if(p+2>=n || argv[p+2][0]=='-') ThrowWith("ͬ�������ؽ�(-hmr)������Ҫָ�������ͱ���.");
            strcpy(srcdbn,argv[++p]);
            strcpy(srctbn,argv[++p]);
        }
        else if(strcmp(argv[p],"-ck")==0)
        {
            funcid=DP_CHECK;
            if(p+1>=n || argv[p+1][0]=='-') ThrowWith("���ݼ��(-ck)��Ҫָ�������ͱ���.");
            if(p+2>=n || argv[p+2][0]=='-') ThrowWith("���ݼ��(-ck)��Ҫָ�������ͱ���.");
            strcpy(srcdbn,argv[++p]);
            strcpy(srctbn,argv[++p]);
        }
        else if(strcmp(argv[p],"-mv")==0)
        {
            funcid=DP_MOVE;
            if(p+1>=n || argv[p+1][0]=='-') ThrowWith("���ƶ�(-mv)��Ҫָ�� ��Դ/Ŀ�� �����ͱ���.");
            if(p+2>=n || argv[p+2][0]=='-') ThrowWith("���ƶ�(-mv)��Ҫָ�� ��Դ/Ŀ�� �����ͱ���.");
            if(p+3>=n || argv[p+3][0]=='-') ThrowWith("���ƶ�(-mv)��Ҫָ�� ��Դ/Ŀ�� �����ͱ���.");
            if(p+4>=n || argv[p+4][0]=='-') ThrowWith("���ƶ�(-mv)��Ҫָ�� ��Դ/Ŀ�� �����ͱ���.");
            strcpy(srcdbn,argv[++p]);
            strcpy(srctbn,argv[++p]);
            strcpy(dstdbn,argv[++p]);
            strcpy(dsttbn,argv[++p]);
        }
        else if(strcmp(argv[p],"-cl")==0 || strcmp(argv[p],"-clf")==0)
        {
            if(strcmp(argv[p],"-cl")==0)
                funcid=DP_CREATELIKE;
            else funcid=DP_CREATELIKE_PRESERVE_FORMAT;
            if(p+1>=n || argv[p+1][0]=='-') ThrowWith("���ƴ�����Ҫָ���ο����ݿ���.");
            if(p+2>=n || argv[p+2][0]=='-') ThrowWith("���ƴ�����Ҫָ���ο�����.");
            if(p+3>=n || argv[p+3][0]=='-') ThrowWith("���ƴ�����Ҫָ����Դ���ݿ���.");
            if(p+4>=n || argv[p+4][0]=='-') ThrowWith("���ƴ�����Ҫָ����Դ����.");
            if(p+5>=n || argv[p+5][0]=='-') ThrowWith("���ƴ�����Ҫָ�������ݿ���.");
            if(p+6>=n || argv[p+6][0]=='-') ThrowWith("���ƴ�����Ҫָ���±���.");
            if(p+7>=n || argv[p+7][0]=='-') ThrowWith("���ƴ�����Ҫָ������ʼʱ��.");
            strcpy(srcdbn,argv[++p]);
            strcpy(srctbn,argv[++p]);
            strcpy(srcowner,argv[++p]);
            strcpy(srctable,argv[++p]);
            strcpy(dstdbn,argv[++p]);
            strcpy(dsttbn,argv[++p]);
            strcpy(taskdate,argv[++p]);
        }
        //>>Begin:DM-228
        else if(strcmp(argv[p],"-csit") == 0)
        {
            funcid = DP_CREAT_SOLID_INDEX_TABLE;
            /*
            example:
            dpadmin -csit localhost:1522 dbuser dbpasswd srcdb srctable1 dstdb dsttable1 "now()"
              /tmp/temp_path /tmp/back_path 5 createtable.lst "select * from srcdb.srctable1"
            */
            // ��14������
            if(p+14 != n)
            {
                ThrowWith("�������������������������. ��ο�:dpadmin --h");
            }

            // ora svc ,user,name
            strcpy(orasvcname,argv[++p]);
            Trim(orasvcname);
            strcpy(orausrname,argv[++p]);
            Trim(orausrname);
            strcpy(orapswd,argv[++p]);
            Trim(orapswd);

            // src db & table
            strcpy(srcdbn,argv[++p]);
            Trim(srcdbn);
            strcpy(srctbn,argv[++p]);
            Trim(srctbn);

            // dst db & table
            strcpy(dstdbn,argv[++p]);
            Trim(dstdbn);
            Str2Lower(dstdbn);
            strcpy(dsttbn,argv[++p]);
            Trim(dsttbn);
            Str2Lower(dsttbn);

            // tack date
            strcpy(taskdate,argv[++p]);

            // temp path,backup path,
            strcpy(objCrtTskTabAssistParam.tmp_path,argv[++p]);
            strcpy(objCrtTskTabAssistParam.back_path,argv[++p]);

            // compress type,solid index file name
            objCrtTskTabAssistParam.cmp_type = atoi(argv[++p]);
            strcpy(objCrtTskTabAssistParam.solid_index_list_file,argv[++p]);

            // get the extsql
            if(strcasestr(argv[p+1],"default") != NULL)
            {
                sprintf(objCrtTskTabAssistParam.ext_sql,"select * from %s.%s",srcdbn,srctbn);
            }
            else
            {
                strcpy(objCrtTskTabAssistParam.ext_sql,argv[p+1]);
            }
            p++;
        } // end  else if(strcmp(argv[p],"-csit")
        //<<End:Dm-228
        else if(strcmp(argv[p],"-enc")==0)
        {
            funcid=DP_ENCODE;
            if(p+1>=n || argv[p+1][0]=='-') ThrowWith("No text to encode!.");
            strcpy(srcdbn,argv[++p]);
        }
        else if(strcmp(argv[p],"-rm")==0)
        {
            funcid=DP_REMOVE;
            if(p+1>=n || argv[p+1][0]=='-') ThrowWith("ɾ������Ҫָ�����ݿ���.");
            if(p+2>=n || argv[p+2][0]=='-') ThrowWith("ɾ������Ҫָ������.");
            strcpy(srcdbn,argv[++p]);
            strcpy(srctbn,argv[++p]);
        }
        else if(strcmp(argv[p],"-mb")==0)
        {
            funcid=DP_MYSQLBLOCK;
            if(p+1>=n || argv[p+1][0]=='-') ThrowWith("���ʽת����Ҫָ�����ݿ���.");
            if(p+2>=n || argv[p+2][0]=='-') ThrowWith("���ʽת����Ҫָ������.");
            strcpy(srcdbn,argv[++p]);
            strcpy(srctbn,argv[++p]);
        }
        else ThrowWith("������������ :'%s'",argv[p]);
        return true;
    }

};
int cmd_base::argc=0;
char **cmd_base::argv=(char **)NULL;

cmdparam cp;

const char *GetDPLibVersion();
int main(int argc,char *argv[])
{
    int nRetCode = 0;
    if(argc==2 && (strcmp(argv[1],"-h")==0 || strcmp(argv[1],"-help")==0 || strcmp(argv[1],"--h")==0 ||
                   strcmp(argv[1],"--help")==0 || strcmp(argv[1],"help")==0
                  ) )
    {
        printf("������Ϣ. \n\n %s",helpmsg);
        return 0;
    }
    if(argc==3 && (strcmp(argv[1],"-3396709")==0 ) )
    {
        char str[100];
        strcpy(str,argv[2]);
        decode(str);
        printf("������Ϣ. \n\n %s",str);
        return 0;
    }
    cmd_base::argc=argc;
    cmd_base::argv=(char **)argv;
    int corelev=2;
    const char *pcl=getenv("DP_CORELEVEL");
    if(pcl!=NULL) corelev=atoi(pcl);
    if(corelev<0 || corelev>2) corelev=2;
    int failretry=0;
    do
    {
        WOCIInit("dpadmin");
        nRetCode=wociMainEntrance(Start,true,NULL,corelev);
        WOCIQuit();
        if(cp.loopmode)
        {
            lgprintf("�����쳣����,����!");
            lgprintf("������5���Ӻ����.");
            mySleep(5*60);
            failretry++;
        }
        else break;
    }
    while(true);
    return nRetCode;
}
#ifdef __unix
#define thread_rt void *
#define thread_end return NULL
#else
#define thread_rt void
#define thread_end return
#endif

/*
thread_rt LoopMonite(void *ptr) {
    bool *loopend=(bool *)ptr;
    printf("\n���� 'quit' �˳�ѭ��ִ��ģʽ\n.");
    while(1)
    {
        char bf[1000];
        gets(bf);
        if(strcmp(bf,"quit")==0) {
            *loopend=true;
            printf("\nѭ��ִ��ģʽ���˳��������ڽ�����ǰ�������ֹ.\n");
            break;
        }
        else if(strlen(bf)>0) {
            printf("\n����'%s'����ʶ��ֻ������'quit'��ֹѭ��ִ��ģʽ.\n",bf);
        }
    }
    thread_end;
}
*/

int Start(void *ptr)
{
    wociSetOutputToConsole(TRUE);
    DbplusCert::initInstance();
    DbplusCert::getInstance()->printlogo();
    printf(DBPLUS_STR " ������汾 :%s \n",GetDPLibVersion());
    int rp=0;
    cp.GetEnv();
    cp.GetParam();
    //corelev=0;
    if(!cp.checkvalid()) return -1;
    wociSetEcho(cp.verbosemode);
    wociSetOutputToConsole(cp.echomode);
    if(cp.funcid==cmdparam::DP_ENCODE)
    {
        encode(cp.srcdbn);
        return 1;
    }
    printf("���ӵ�" DBPLUS_STR "...\n");
    char date[20];
    wociGetCurDateTime(date);
    int y,m,d;
    DbplusCert::getInstance()->GetExpiredDate(y,m,d);
    if(y>0 && wociGetYear(date)*100+wociGetMonth(date)>y*100+m)
        ThrowWith("���õ�" DBPLUS_STR "�汾̫�ϣ�����º�ʹ��(Your " DBPLUS_STR " is too old,please update it)!");
    if(cp.freezemode)
    {
        char dtnow[19],dtstart[19];
        int y=0,m=0,d=0,h=0,mi=0,s=0;
        wociGetCurDateTime(dtnow);
        printf("Input start year(%d) :",wociGetYear(dtnow));
        scanf("%d",&y);
        if(y==0) y=wociGetYear(dtnow);
        if(y<2000 || y>2100)
        {
            printf("Invalid input.\n");
            return -1;
        }
        printf("Input start month(%d) :",wociGetMonth(dtnow));
        if(m==0) m=wociGetMonth(dtnow);
        scanf("%d",&m);
        if(m<1 || m>12)
        {
            printf("Invalid input.\n");
            return -1;
        }
        printf("Input start day(%d) :",wociGetDay(dtnow));
        scanf("%d",&d);
        if(d==0) d=wociGetDay(dtnow);
        if(d<1 || d>31)
        {
            printf("Invalid input.\n");
            return -1;
        }
        printf("Input start hour :");
        scanf("%d",&h);
        if(h<0 || h>53)
        {
            printf("Invalid input.\n");
            return -1;
        }
        printf("Input start minute :");
        scanf("%d",&mi);
        if(mi<0 || mi>59)
        {
            printf("Invalid input.\n");
            return -1;
        }
        printf("Input start sec(0) :");
        scanf("%d",&s);
        if(s<0 || s>59)
        {
            printf("Invalid input.\n");
            return -1;
        }
        wociGetCurDateTime(dtnow);
        wociSetDateTime(dtstart,y,m,d,h,mi,s);
        int sl=wociDateDiff(dtstart,dtnow);
        if(sl<1)
        {
            printf("�ȴ� %d ��,(Y/N)?",sl);
            int ans=getch();
            if(ans!='Y' && ans!='y') return -1;
        }
        else
        {
            printf("�ȴ� %d ��...",sl);
            mySleep(sl);
            printf("��ʼ���!\n");
        }
    }


    //DirFullAccessTest(dts);
    //UserQueryTest();
    //StatiTest();
    //FullAccessTest();
    //BatchUserQueryTest(cp.orausrname,cp.orapswd,cp.orasvcname);
    //return 1;


    bool normal=true;
    //building wdbi connect to dbplus server
    printf("ȡ���в���...\n");
    {
        //this code block use a separate database connection,release at end of code block
        AutoHandle dts;
        dts.SetHandle(wociCreateSession(cp.musrname,cp.mpswd,cp.mhost,DTDBTYPE_ODBC));
        {
            {
                AutoStmt st(dts);
                st.DirectExecute("update dp.dp_seq set id=id+1");
                st.DirectExecute("lock tables dp.dp_seq write");
            }
            AutoMt seq(dts,10);
            seq.FetchAll("select id as fid from dp.dp_seq");
            seq.Wait();
            {
                AutoStmt st(dts);
                st.DirectExecute("unlock tables");
            }
        }
        //building direct api connect to dbplus server
        printf("���ӵ�:%s\n",cp.serverip);
        SysAdmin sa (dts,cp.serverip,cp.musrname,cp.mpswd,NULL,cp.port);
        sa.Reload();
        SysAdmin *psa=&sa;
        /****Not loopable********************/
        if(cp.funcid==cmdparam::DP_CREATELIKE )
        {
            MiddleDataLoader dl(psa);
            printf("DP���ƴ�����'%s.%s %s %s %s' ...\n",cp.srcdbn,cp.srctbn,cp.srctable,cp.dsttbn,cp.taskdate);
            dl.CreateLike(cp.srcdbn,cp.srctbn,cp.srcowner,cp.srctable,cp.dstdbn,cp.dsttbn,cp.taskdate,false);
            return 0;
        }
        if(cp.funcid==cmdparam::DP_CREATELIKE_PRESERVE_FORMAT )
        {
            MiddleDataLoader dl(psa);
            printf("DP���ƴ�����'%s.%s %s %s %s' ...\n",cp.srcdbn,cp.srctbn,cp.srctable,cp.dsttbn,cp.taskdate);
            dl.CreateLike(cp.srcdbn,cp.srctbn,cp.srcowner,cp.srctable,cp.dstdbn,cp.dsttbn,cp.taskdate,true);
            return 0;
        }

        //>> Begin: DM-228
        if(cp.funcid == cmdparam::DP_CREAT_SOLID_INDEX_TABLE)
        {
            MiddleDataLoader dl(psa);
            printf("DP����������'%s.%s %s %s %s' ...\n",cp.srcdbn,cp.srctbn,cp.dstdbn,cp.dsttbn,cp.taskdate);
            dl.CreateSolidIndexTable(cp.orasvcname,cp.orausrname,cp.orapswd,
                                     cp.srcdbn,cp.srctbn,cp.dstdbn,cp.dsttbn,
                                     cp.objCrtTskTabAssistParam.cmp_type,
                                     cp.objCrtTskTabAssistParam.tmp_path,
                                     cp.objCrtTskTabAssistParam.back_path,
                                     cp.taskdate,
                                     cp.objCrtTskTabAssistParam.solid_index_list_file,
                                     cp.objCrtTskTabAssistParam.ext_sql);
            return 0;
        }
        //<< End:Dm-228

        if(cp.funcid==cmdparam::DP_REMOVE)
        {
            DestLoader dstl(&sa);
            lgprintf("��ɾ��...");
            dstl.RemoveTable(cp.srcdbn,cp.srctbn);
            return 0;
        }
        if(cp.funcid==cmdparam::DP_MYSQLBLOCK)
        {
            DestLoader dstl(&sa);
            lgprintf("��ʽת��...");
            dstl.ToMySQLBlock(cp.srcdbn,cp.srctbn);
            return 0;
        }

        if(cp.funcid==cmdparam::DP_MOVE)
        {
            DestLoader dstl(&sa);
            lgprintf("�������ƶ�...");
            dstl.MoveTable(cp.srcdbn,cp.srctbn,cp.dstdbn,cp.dsttbn);
            return 0;
        }
        if(cp.funcid==cmdparam::DP_HOMO_REINDEX)
        {
            printf("����--ͬ�������ؽ�����֧�֣�'%s.%s' ...\n",cp.srcdbn,cp.srctbn);
            //MiddleDataLoader dl(psa);
            //dl.homo_reindex(cp.srcdbn,cp.srctbn);
            return 0;
        }
//  if(cp.funcid==cmdparam::DP_CHECK || cp.funcid==cmdparam::DP_ALL) {
        if(cp.funcid==cmdparam::DP_CHECK )
        {
            MiddleDataLoader dl(psa);
            printf("DP�ļ���飺'%s.%s' ...\n",cp.srcdbn,cp.srctbn);
            dl.dtfile_chk(cp.srcdbn,cp.srctbn);
            return 0;
        }
    } // block end,the connection release. below will build a new connection.
    bool loopend=false;
    /*
    if(cp.loopmode) {
    #ifdef WIN32
        _beginthread(LoopMonite,81920,(void *)&loopend);
    #else
        pthread_create(&hd_pthread_loop,NULL,LoopMonite,(void *)&loopend);
        pthread_detach(hd_pthread_loop);
    #endif
    }
    */
    int isbusy;
    while(true)
    {
        isbusy=0;
        // Why build a new connect on every loop?
        //  .   in case of more table opened in a connect thread.
        //  ..  rebuild connect will server is done.
        //  ... release connect on sleep
        {
            AutoHandle dts;
            dts.SetHandle(wociCreateSession(cp.musrname,cp.mpswd,cp.mhost,DTDBTYPE_ODBC));
            //building direct api connect to dbplus server
            SysAdmin sa (dts,cp.serverip,cp.musrname,cp.mpswd,NULL,cp.port);
            SysAdmin *psa=&sa;

            sa.Reload();
            sa.SetNormalTask(normal);
            printf("���Ŀ����ṹ...\n");
            try
            {
                int tabid;
                while(sa.GetBlankTable(tabid) && sa.CreateDT(tabid))
                {
                    isbusy++;
                }
                if(isbusy>0) sa.Reload();
            }
            catch(...)
            {
            };
            if(cp.funcid==cmdparam::DP_DUMP)   // || cp.funcid==cmdparam::DP_ALL) {
            {
                if(cp.blocksize>512) cp.blocksize=512;
                if(cp.blocksize<100) cp.blocksize=100;
                DataDump dd(psa->GetDTS(),cp.mtlimit,cp.blocksize);
                printf("������ݵ�������...\n");
                //if(dd.DoDump(*psa)==0)
                //  dd.heteroRebuild(*psa);
                isbusy+=dd.DoDump(*psa,cp.dumpstrains[0]==0?NULL:cp.dumpstrains);
            }
            if(cp.funcid==cmdparam::DP_MDLDR || cp.funcid==cmdparam::DP_ALL)
            {
                MiddleDataLoader dl(psa);
                printf("���������������...\n");
                isbusy+=dl.Load(cp.indexnum,cp.load_tidxmem,cp.useOldBlock);
            }
            if(cp.funcid==cmdparam::DP_DSTLDR || cp.funcid==cmdparam::DP_ALL)
            {
                DestLoader dstl(&sa);
                printf("������ݵ�������...\n");
                while(dstl.Load(cp.directIOSkip));
                isbusy+=dstl.RecreateIndex(&sa);
            }
            /***********************/
            if(cp.funcid==cmdparam::DP_DEEPCMP || (cp.funcid==cmdparam::DP_ALL && isbusy==0) )
            {
                DestLoader dstl(&sa);
                printf("������ݶ���ѹ������...\n");
                isbusy+=dstl.ReCompress(cp.threadnum);
                isbusy+=dstl.ReLoad();
                isbusy+=dstl.RecreateIndex(&sa);
            }
            //����������̳����쳣�������ǵ���������
            //�����Ҫ��ǿ���˳���
            if(loopend) break;
            //�����������Ч����ֱ�Ӽ��� ��
            if(isbusy>0) continue;
            //δ������Ч�������δ�����쳣���񣬳��Դ���
            if(normal)
            {
                normal=false;
                printf("���Իָ��쳣����ִ��...\n");
                continue;
            }
            //���쳣����Ҳ������
            normal=true;
            if( !cp.loopmode) break;
        }// connection release here
        mySleep(cp.waittime)
    }
    //wait LoopMonite thread end.
    mySleep(1)
    SvrAdmin::ReleaseInstance();
    SvrAdmin::ReleaseDTS();
    /**********************/
    printf("����������\n");
    DbplusCert::ReleaseInstance();
    return 0;
}

