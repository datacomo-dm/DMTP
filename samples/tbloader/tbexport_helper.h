#ifndef TBEXPORT_DEF_H
#define	TBEXPORT_DEF_H
#include "tbloader_impl.h"

/********************************************************************
  file : tbexport.h
  desc : define the export table to cvs file internal function
  author: liujianshun,201304
********************************************************************/
enum EXPROT_STATUS{exp_unstart,exp_hasstarted,exp_doing,exp_hasstoped};
class tbexport
{
public:
    tbexport();
    virtual ~tbexport();

public:
    int   start(_InitInfoPtr pInitObj,const char* psql,const char* logpth);   // 初始化
    int   stop();                                                       // 卸载
    int   doStart();     // 开始启动线程导出数据
    inline long getRowSum(){ return m_rowSum;};
    inline long getCurrentRows(){ return m_curRowIdx;};
    
    inline bool getRunFlag(){return m_running;};
    inline void setRunFlag(bool b) { m_running = b;};
    inline EXPROT_STATUS getStatus(){return m_status;};
    inline void setId(int id){m_id = id;};
    inline int  getId(){ return m_id;};
	
protected:
    pthread_t    m_exportThread;                 // 导出数据线程
    static void* procExportData(void * ptr);     // 导出数据线程函数    
    
protected:	
    // 初始化信息
    _InitInfo            m_stInitInfo;  
    char           *m_psql;   
    AutoHandle     *m_dtd;
    long            m_rowSum;       // 文件行数
    long            m_curRowIdx;    // 当前导出行数
    AutoStmt       *m_srcst;
    mytimer         m_tm;
    TradeOffMt     *m_mt;
	
protected:
    bool            m_running;                   // 在使用状态
    EXPROT_STATUS   m_status;                    // 内初始化状态
    pthread_mutex_t m_exitLock;                  // 退出锁
    int             m_id;                        // id
};

#endif
