#include "tbexport_helper.h"


tbexport::tbexport()
{
    m_psql = NULL;
    m_mt = NULL;
    m_curRowIdx = 0;
    m_srcst = NULL;
    m_dtd = NULL;
    m_psql = (char*)malloc(conAppendMem5MB);
    pthread_mutex_init(&m_exitLock,NULL);
    m_status = exp_unstart;
    m_rowSum = 0;
}

tbexport::~tbexport()
{
    if(m_mt != NULL)
    {
        delete m_mt;
        m_mt = NULL;
    }
    if(m_srcst != NULL)
    {
        delete m_srcst;
        m_srcst = NULL;	
    }
    if(m_dtd != NULL)
    {
        delete m_dtd;
        m_dtd = NULL;
    }
    m_curRowIdx = 0;
    ReleaseBuff(m_psql);
    m_rowSum = 0;
    pthread_mutex_destroy(&m_exitLock);
}

void StrToLower(char *str) {
    while(*str!=0) 
    {
        *str=tolower(*str);
        str++;
    }
}
/*
-1: 数据库连接失败 
1: 已经初始化过，不能重复调用
0: 成功
*/
int   tbexport::start(_InitInfoPtr pInitObj,const char* psql,const char* logpth)
{
    if (m_status != exp_unstart&& m_status != exp_hasstoped)
    {
        return 1;
    }
    // 拷贝对象
    memcpy(&m_stInitInfo,pInitObj,sizeof(m_stInitInfo));
    _InitInfoPtr pobj = &m_stInitInfo;
    
    int logpthlen = strlen(logpth);
    WOCIInit(logpthlen>2 ? logpth:"tbexporter_log");
    wociSetOutputToConsole(TRUE);
    wociSetEcho(true);
    
    m_tm.Start();
    
    //--1.  连接目标数据库
    try
    {
        if(m_dtd != NULL)
        {
            delete m_dtd;
            m_dtd = NULL; 	
        }
        m_dtd = new AutoHandle();
        m_dtd->SetHandle(wociCreateSession(pobj->user,pobj->pwd,pobj->dsn,DTDBTYPE_ODBC));
    }
    catch(...)
    {
        lgprintf("can not connect db[%s-->%s:%s]",pobj->dsn,pobj->user,pobj->pwd);
        return -1;
    }
    
    //--2. 获取总的记录数
    strcpy(m_psql,psql);
    StrToLower(m_psql);
    std::string strsql = m_psql;
    int len = strsql.size();
    int from_pos = strsql.find_first_of("from");
    int limit_pos = strsql.find_first_of("limit");
    AutoMt _mt(*m_dtd,10);
    try
    {
        if(limit_pos == std::string::npos) // 存在limit的情况
        {
            _mt.FetchFirst("select count(1) ct from ( %s ) __www_datacomo_com_tbexport_",strsql.c_str());
        }
        else  // 不存在limit的情况
        {           
            _mt.FetchFirst("select count(1) ct from %s",strsql.substr(from_pos+5,len-from_pos).c_str());
        }
        _mt.Wait();
        m_rowSum = _mt.GetLong("ct",0);
    }
    catch(...)
    {
        lgprintf("execute sql:[select count(1) ct from  %s] error.",strsql.substr(from_pos+5,len-from_pos-5).c_str());
        return -1;	
    }
    
    //--3. 构建目标数据库表MT结构
    if(m_mt == NULL)
    {
        delete m_mt;
        m_mt = NULL;
    }
    m_mt = new TradeOffMt(0,_MAX_ROWS);
    
    if(m_srcst == NULL)
    {
        delete m_srcst;
        m_srcst = NULL;
    }	
    m_srcst = new AutoStmt(*m_dtd);
    m_srcst->Prepare(m_psql);
    m_mt->Cur()->Build(*m_srcst);
    m_mt->Next()->Build(*m_srcst);
    
    m_status = exp_hasstarted;
    m_running = true;
    
    // wociMainEntrance(startLoadData,true,NULL,2);   // 等待退出的线程
    return 0; 
}

int   tbexport::stop()
{     
    if(!m_running){
    	return 0;
    }
    if(m_status != exp_doing){
        lgprintf("do not begin to export data .");
        return -1;
    }
    while(1)
    {
        lgprintf("wait export thread finish...");
        
        pthread_mutex_lock(&m_exitLock);
        if(m_running){
            m_running = false;
            pthread_mutex_unlock(&m_exitLock);
            
            sleep(1);
            break;
        }
        else
        {
            pthread_mutex_unlock(&m_exitLock);
            break;
        }	   	
    }
    
    /*压缩文件过程*/
    
    
    m_status = exp_hasstoped;
    if(m_mt != NULL)
    {
        delete m_mt;
        m_mt = NULL;
    }
    if(m_srcst != NULL)
    {
        delete m_srcst;
        m_srcst = NULL;	
    }
    if(m_dtd != NULL)
    {
        delete m_dtd;
        m_dtd = NULL;
    }
    lgprintf("test finish.\n");
    WOCIQuit(); // 退出wdbi
    
    m_running = false;
    return 0;
}

// 启动线程开始导出数据
int   tbexport::doStart()
{
    if(m_status == exp_doing)
    {
        lgprintf("do exporting...");
        return 0;
    }
    
    if(m_status != exp_hasstarted)
    {
        lgprintf("can not do exporting,status = %d",m_status);
        return -1;
    }
    
    pthread_create(&m_exportThread,NULL,procExportData,(void*)this);
    
    lgprintf("begin to exporting cvs .");
    
    m_status = exp_doing;
    m_tm.Start();
    return 0;
}


void* tbexport::procExportData(void * ptr)
{
    tbexport* pobj = (tbexport*)ptr;
    
    pobj->m_mt->FetchFirst();
    int rrn = pobj->m_mt->Wait();
    while(rrn>0)
    {
        pobj->m_mt->FetchNext();
        wociMTToTextFileStr(*pobj->m_mt->Cur(),pobj->m_stInitInfo.fn_ext,rrn,NULL);
        pobj->m_curRowIdx+=rrn;
        
        pthread_mutex_lock(&pobj->m_exitLock);
        if(!pobj->m_running){			
            pthread_mutex_unlock(&pobj->m_exitLock);
            break;
        }
        pthread_mutex_unlock(&pobj->m_exitLock);
        
        rrn=pobj->m_mt->Wait();
    }
    pobj->m_tm.Stop();
    lgprintf("\n共处理%d行(时间%.2f秒)。\n",pobj->m_curRowIdx,pobj->m_tm.GetTime());
    pobj->m_running = exp_hasstoped;
    return NULL;
}

