#ifndef TBLOADER_HELPER_DEF_H
#define TBLOADER_HELPER_DEF_H

#include "tbloader_impl.h"

/********************************************************************
  file : tbloader_helper.h
  desc : define the tbloader java call interface
  author: liujianshun,201304
  note: support multiple thread use
********************************************************************/

enum STATUS{status_unknown,unstart,hasstarted,parserheader,writedata,hasstoped};
enum OPER_TYPE{oper_unknown,write_files,insert_db};
class tbloader_helper
{
public:
    tbloader_helper();
    virtual ~tbloader_helper();

public:
    int   start(_InitInfoPtr pInitObj,int operType,const char* logpth);   // ��ʼ��
    int   stop();                                                         // ж��
    int   parserColumnInfo(const HerderPtr header,const int headerlen);   // ������ͷ
    int   writePackData(const char* porigin_buff,const long origin_len,const int rownum);  // д�����ݿ� 
    inline bool getRunFlag(){return m_running;};
    inline void setRunFlag(bool b) { m_running = b;};
    inline STATUS getStatus(){return m_status;};
    inline OPER_TYPE getOperType(){ return m_opertype;};
    inline void setId(int id){m_id = id;};
    inline int  getId(){ return m_id;};
    void  getMd5Sum(char * pmd5sum);
	
protected:	
    // ��ʼ����Ϣ
    _InitInfo            m_stInitInfo;   
    /*
    ��һ����
    ͷ�ļ�λ�� ��int�� 4�ֽ� [X]���� ͷ�����ȣ���ͷ����Ϣ���ܵĳ�
    ͷ�ļ� (String) X�ֽ� [head[6]] 
    ͷ�ļ����� (ʹ��;�з�)
    columnName(����);
    columnDBType(���ݿ�������);
    columnType(java����);
    columnCName;
    columnPrecision;
    columnScale 
    ���磺c1,c2;INT,INT;INT,INT;col1,col2;2,2;12,12
    */
    _ColumnInfoPtrVector  m_colInfoVec; 	   // �洢��ͷ������Ϣ	
    /*
    �������֣�
    �����ݿ�λ�� (Long)[] ��һ�����ݿ�λ�� filesize - 16 -([y]*8) ,�ڶ������ݿ�λ�� filesize - 16 -([y-1]*8) �Դ�����
    �������� ��Long�� 8�ֽ�
    ���ݿ����� ��int�� 4�ֽ�
    ���ݿ���� ��int�� 4�ֽ� [y]
    */
    _BlockSumary   m_blkSumaryInfo; 	// ��ĸſ���Ϣ
    
    /*
    �ڶ��������ݿ�ṹ��
    ���ݿ��С ��int�� 4�ֽ�
    ������ʼλ�ã�Long�� 8�ֽ�
    ���ݽ���λ�� ��Long�� 8�ֽ�
    ����MD5(String) 32�ֽ�
    �������ݣ�GZIPѹ����
    
    ÿһ�����ݽṹ ��ѹ������ݸ�ʽ��
    �����ݴ�С ��int�� 4�ֽ� 
    �п�ֵ��� ��byte[columnName.length]�� 1 == ��ֵ 0== �ǿ� 
    ������ 011000 ������columnType ȡ����Ӧ�������ͣ��ṩȡֵ��
    ���� �������������ͳ��ȶ�ȡ���ַ������ͣ�ǰ4�ֽ�Ϊ�ַ������ȣ�
    */
    unsigned char  *m_pCompressed_data;	
    long            m_compressed_len;
    AutoHandle     *m_dtd;
    int             m_colNum;       // ����
    AutoStmt       *m_srcst;
    mytimer         m_tm;
    TradeOffMt     *m_mt;
    bool            m_firstEnter;
    bool            m_DirectWriteTable;    
	
protected:// д�ļ���
    unsigned char   m_md5sum[conMd5Len];
    FILE           *m_pFile;
	
protected:// dm ��ز���
    _DmInitInfo     m_dmInitInfo;
    AutoHandle     *m_dmdt; 
    MySQLConn      *m_pConnector;
    char            m_sysPath[256];

protected:
    bool            m_running;                   // ��ʹ��״̬
    STATUS          m_status;                    // �ڳ�ʼ��״̬
    OPER_TYPE       m_opertype;                  // �������ͣ�д���ݿ�����ļ�
    int             m_id;                        // id
};

#endif
