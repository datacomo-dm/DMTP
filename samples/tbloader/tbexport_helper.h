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
    int   start(_InitInfoPtr pInitObj,const char* psql,const char* logpth);   // ��ʼ��
    int   stop();                                                       // ж��
    int   doStart();     // ��ʼ�����̵߳�������
    inline long getRowSum(){ return m_rowSum;};
    inline long getCurrentRows(){ return m_curRowIdx;};
    
    inline bool getRunFlag(){return m_running;};
    inline void setRunFlag(bool b) { m_running = b;};
    inline EXPROT_STATUS getStatus(){return m_status;};
    inline void setId(int id){m_id = id;};
    inline int  getId(){ return m_id;};
	
protected:
    pthread_t    m_exportThread;                 // ���������߳�
    static void* procExportData(void * ptr);     // ���������̺߳���    
    
protected:	
    // ��ʼ����Ϣ
    _InitInfo            m_stInitInfo;  
    char           *m_psql;   
    AutoHandle     *m_dtd;
    long            m_rowSum;       // �ļ�����
    long            m_curRowIdx;    // ��ǰ��������
    AutoStmt       *m_srcst;
    mytimer         m_tm;
    TradeOffMt     *m_mt;
	
protected:
    bool            m_running;                   // ��ʹ��״̬
    EXPROT_STATUS   m_status;                    // �ڳ�ʼ��״̬
    pthread_mutex_t m_exitLock;                  // �˳���
    int             m_id;                        // id
};

#endif
