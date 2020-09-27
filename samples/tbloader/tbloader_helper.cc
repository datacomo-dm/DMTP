#include "tbloader_helper.h"
#ifdef DEBUG
#define lgprintf printf
#else
#define lgprintf 
#endif
tbloader_helper::tbloader_helper()
{
    m_pCompressed_data = NULL;
    m_pConnector = NULL;
    m_srcst = NULL;
    m_mt = NULL;
    m_status = status_unknown;
    m_compressed_len = 0;
    m_firstEnter = false;
    memset(m_md5sum,0,conMd5Len);
	memset(m_sysPath,0,256);
    m_running = false;
    m_pFile = NULL;
    m_dtd = NULL;
    m_dmdt = NULL;
}

tbloader_helper::~tbloader_helper()
{
    if(m_pCompressed_data != NULL){
       free(m_pCompressed_data);
       m_pCompressed_data = NULL;
    }
    if (NULL != m_pConnector)
    {
        delete m_pConnector;
        m_pConnector = NULL;
    }
    if (NULL != m_mt)
    {
        delete m_mt;
        m_mt = NULL;
    }
    if (NULL != m_srcst)
    {
        delete m_srcst;
        m_srcst = NULL;
    }
    if(m_dtd != NULL)
    {
    	delete m_dtd;
        m_dtd = NULL;
    }
    if(m_dmdt != NULL)
    {
       delete m_dmdt;
       m_dmdt = NULL;
    }
    CLOSE_FILE(m_pFile);
}

int   tbloader_helper::start(_InitInfoPtr pInitObj,int operType,const char* logpth)
{
    if (m_status != status_unknown && m_status != hasstoped)
    {
        return 1;
    }
    
    m_opertype = (OPER_TYPE)operType;
    // ��������
    memcpy(&m_stInitInfo,pInitObj,sizeof(m_stInitInfo));
    _InitInfoPtr pobj = &m_stInitInfo;
    
    int logpthlen = strlen(logpth);
    WOCIInit(logpthlen>2 ? logpth:"tbloader_log");
    wociSetOutputToConsole(TRUE);
    wociSetEcho(true);
    memset(m_md5sum,0,conMd5Len);
    
    m_tm.Start();    
    if(m_opertype == insert_db)
    {
        try
        {
            //--1.  ����Ŀ�����ݿ�
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
            lgprintf("can not connect database [%s--->%s:%s].\n",pobj->dsn,pobj->user,pobj->pwd);
            return -1;
        }
        //--1.1 ����dm���ݿ�
        // �ӻ��������л�ȡ����dm����ز�����Ϣ
        strcpy(m_dmInitInfo._musrname,getenv("DP_MUSERNAME"));
        strcpy(m_dmInitInfo._mpswd,getenv("DP_MPASSWORD"));
        decode(m_dmInitInfo._mpswd);
        strcpy(m_dmInitInfo._mhost,getenv("DP_DSN"));
        strcpy(m_dmInitInfo._serverip,getenv("DP_SERVERIP"));
        const char *_strport = getenv("DP_MPORT");
        if(_strport == NULL || atoi(_strport)<3306){m_dmInitInfo._port = 3306;}
        else{m_dmInitInfo._port = atoi(_strport);}
        
        char *pDirectWriteTable = getenv("DirectWriteTable");
        m_DirectWriteTable = false;
        if (pDirectWriteTable != NULL && (atoi(pDirectWriteTable) == 1)){
            m_DirectWriteTable = true;
        }
    
    	try
    	{
            //--1.  ����Ŀ�����ݿ�
            if(m_dmdt != NULL)
            {
                delete m_dmdt;
                m_dmdt = NULL;  	
            }
            m_dmdt = new AutoHandle();
            m_dmdt->SetHandle(wociCreateSession(m_dmInitInfo._musrname,m_dmInitInfo._mpswd,m_dmInitInfo._mhost,DTDBTYPE_ODBC));
            {
				// ��ȡ·��
                AutoMt  path_mt(*m_dmdt,10);
                path_mt.FetchFirst("select pathval from dp.dp_path where pathtype = 'msys' order by pathid limit 5");
                int rn = path_mt.Wait();
                if(rn <=0) {
                    lgprintf("get syspath error\n");
                    return -1;
                }
                strcpy(m_sysPath,path_mt.PtrStr("pathval",0));
            }
        }
    	catch(...)
    	{
    	    lgprintf("can not connect database  [%s--->%s:%s].\n",m_dmInitInfo._mhost,m_dmInitInfo._musrname,m_dmInitInfo._mpswd);
    	    return -1;
    	}
    
    	if(m_pConnector != NULL)
    	{
            delete m_pConnector;
            m_pConnector = NULL;
    	}
		m_pConnector = new MySQLConn();
		m_pConnector->Connect(m_dmInitInfo._serverip,m_dmInitInfo._musrname,m_dmInitInfo._mpswd,NULL,m_dmInitInfo._port);

    	//--2. ����Ŀ�����ݿ��MT�ṹ
    	if(m_mt != NULL)
    	{
            delete m_mt;
            m_mt = NULL;
    	}
    	m_mt = new TradeOffMt(0,_MAX_ROWS);
    	
    	if(m_srcst != NULL)
    	{
            delete m_srcst;
            m_srcst = NULL;
    	}
    	m_srcst = new AutoStmt(*m_dtd);
    	m_srcst->Prepare("select * from %s.%s limit 2",pobj->dbname,pobj->tbname);
    	wociBuildStmt(*m_mt->Cur(),*m_srcst,_MAX_ROWS);
    	wociBuildStmt(*m_mt->Next(),*m_srcst,_MAX_ROWS);
    	// m_mt->SetPesuado(true);//ap use , tp do not use 
    	m_mt->Cur()->AddrFresh();
    	m_mt->Next()->AddrFresh();
    }
    else  // д���ļ�����
    {
        lgprintf(" tbloader_helper::start init file oper.\n");
        CLOSE_FILE(m_pFile);
        
        m_compressed_len = conAppendMem5MB;
        if(m_pCompressed_data != NULL){
           free(m_pCompressed_data);
           m_pCompressed_data = NULL;
        }
        m_pCompressed_data = (unsigned char *)malloc(m_compressed_len);
    }
    
    // ��� m_blkSumaryInfo
    m_blkSumaryInfo.blockNum = 0;
    m_blkSumaryInfo.rowNum= 0;
    m_blkSumaryInfo.rowSum = 0;
    m_blkSumaryInfo.BlkStartPosVec.clear();
    
    // ���m_colInfoVec
    ReleaseColInfo(m_colInfoVec);
        
    m_status = hasstarted;
    lgprintf("tbloader_helper::start finish.");
    m_running = true;
    // wociMainEntrance(startLoadData,true,NULL,2);   // �ȴ��˳����߳�
    return 0; 
}

int   tbloader_helper::stop()
{
    if(!m_running){
        return 0;	
    }
    // �������һ������
    if(m_opertype == insert_db)
    {
        try
        {
            if(!m_firstEnter && m_mt!=NULL && m_mt->Cur()->GetRows()>0)
            {
                if(m_DirectWriteTable)
                {   // ͨ��ֱ��д���ļ��ķ�ʽ��������д�����ݿ��� 
                	//lgprintf("id=[%d],start tbloader_helper::stop to write mt to file,mt rows = %d.\n",m_id,m_mt->Cur()->GetRows());
                    if(-1 == DirectWriteMt2Table(m_stInitInfo.dbname,m_stInitInfo.tbname,*m_mt->Cur(),m_pConnector,m_sysPath,true))
                    {
                        lgprintf("insert to table [%s.%s] error.",m_stInitInfo.dbname,m_stInitInfo.tbname);
                        return -1;
                    }
                    //lgprintf("id=[%d],end tbloader_helper::stop to write mt to file,mt rows = %d.\n",m_id,m_mt->Cur()->GetRows());
                }
                else
                {   // ͨ��insert�������ݿ��м�¼
                    char tb[500];
                    sprintf(tb,"%s.%s",m_stInitInfo.dbname,m_stInitInfo.tbname);
                    //lgprintf("id=[%d],start tbloader_helper::stop to write mt into table,mt rows = %d.\n",m_id,m_mt->Cur()->GetRows());
                    wociAppendToDbTable(*m_mt->Cur(),tb,*m_dtd,true);
                    //lgprintf("id=[%d],end tbloader_helper::stop to write mt into table,mt rows = %d.\n",m_id,m_mt->Cur()->GetRows());
                }
                
                wociReset(*m_mt->Cur());
            }
        }
        catch(...)
        {
         	lgprintf("id=[%d],tbloader_helper::stop insert table[%s.%s] errro , m_DirectWriteTable = %d.",
         	     m_id,m_stInitInfo.dbname,m_stInitInfo.tbname,m_DirectWriteTable?1:0);
            return 1;   	
        }
    }
    else
    {   // д�������ļ�2013/4/19 18:03:24
        lgprintf("id=[%d],tbloader_helper::writePackData packnum = %d,rowNum=%d,rowSum=%d\n",
        m_id,m_blkSumaryInfo.blockNum,m_blkSumaryInfo.rowNum,m_blkSumaryInfo.rowSum);
        if(-1 == WriteFileSumaryInfo(m_pFile,m_blkSumaryInfo)){
            return -1;
        }			       
    }
    m_tm.Stop();    
    lgprintf("\nid=[%d],������%d��(ʱ��%.2f��)��\n",m_id,m_blkSumaryInfo.rowSum,m_tm.GetTime());
    
    // ��� m_blkSumaryInfo
    m_blkSumaryInfo.blockNum = 0;
    m_blkSumaryInfo.rowNum= 0;
    m_blkSumaryInfo.rowSum = 0;
    m_blkSumaryInfo.BlkStartPosVec.clear();
    
    // ���m_colInfoVec
    ReleaseColInfo(m_colInfoVec);
    if(m_pCompressed_data != NULL){
       free(m_pCompressed_data);   
       m_pCompressed_data = NULL; 
    }                             
    
    // �ر��ļ�
    CLOSE_FILE(m_pFile);
    
    //--2. ����Ŀ�����ݿ��MT�ṹ
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
    
    if(m_pConnector!= NULL)
    {
        delete m_pConnector;
        m_pConnector = NULL;   	
    }
    
    if(m_dmdt != NULL)
    {
        delete m_dmdt;
        m_dmdt = NULL;
    }
    
    if(m_dtd != NULL)
    {
        delete m_dtd;
        m_dtd = NULL; 	
    }
    
    m_status = hasstoped;
    m_running = false;
    
    WOCIQuit(); // �˳�wdbi
    return 0;
}

/*
return 
0: �ɹ�
-1: �ļ�д��ʧ��
1: �ļ�ͷ�����ظ�����д��
2: ���ݳ���У��ʧ��
3: δ��ʼ�� tl_initStorerForFile ���� tl_initStorerForDB
4: ����ͷ���ݽ�������
5: ����ͷ��Ŀ���ṹ��һ��
*/
int   tbloader_helper::parserColumnInfo(const HerderPtr header,const int headerlen)
{
    if (m_status < hasstarted)
    {
        lgprintf("%s:%d error",__FILE__,__LINE__);
        return 3;
    }
    if (m_status == parserheader)
    {
        lgprintf("%s:%d error",__FILE__,__LINE__);
        return 1;
    }
    /*if (strlen(header) != headerlen)
    {
        lgprintf("%s:%d error",__FILE__,__LINE__);
        return 2;
    }
    */
    /*
    columnName(����);
    columnDBType(���ݿ�������);
    columnType(java����);
    columnCName;
    columnPrecision;
    columnScale 
    ���磺col1,col2;dbtype1,dbtype2;type1,type2;cc1,cc2;prec1,prec2;scale1,scale2;
    */
    //lgprintf("id=[%d],begin ParserColumnInfo opertype = %d\n",m_id,m_opertype);
    if (-1 == ParserColumnInfo(header,m_colInfoVec)){
        lgprintf("Parser column header error \n");
        lgprintf("%s:%d error",__FILE__,__LINE__);
        return 4;
    }
    //lgprintf("id=[%d],begin ParserColumnInfo opertype = %d\n",m_id,m_opertype);
   
    //lgprintf("id=[%d],begin tbloader_helper::parserColumnInfo opertype = %d\n",m_id,m_opertype);
    if(m_opertype == insert_db)   // �������ݿ��У���ҪУ��ͷ��
    {
    	try
        {
            //--1. �����ȡ�ļ�����Ҫ�Ļ���buff,���п��
            if(m_dtd == NULL)
            {
                lgprintf("id[%d],tbloader_helper::parserColumnInfo m_dtd == NULL",m_id);
            	return 5;
            }
            AutoMt mt2(*m_dtd,10);
            mt2.FetchFirst("select * from %s.%s limit 2",m_stInitInfo.dbname,m_stInitInfo.tbname);
            mt2.Wait();
            m_colNum = wociGetColumnNumber(mt2);
            lgprintf("У���ṹ��ʽ..\n");
            if(m_colNum == m_colInfoVec.size())
            {
                char cn[256];      
                for(int col = 0;col < m_colNum;col++)
                {        
            		wociGetColumnName(mt2,col,cn);   
            		if(strcasecmp(m_colInfoVec[col]->columnName,cn) !=0)
            		{
                        lgprintf("����ͷ�ṹ��� %s.%s �ṹ��һ��.\n",m_stInitInfo.dbname,m_stInitInfo.tbname);
                        lgprintf("%s:%d error",__FILE__,__LINE__);
                        return 5;
            		}	
            	} 	
            }
            else
            {
                lgprintf("����ͷ�ṹ��� %s.%s �ṹ��һ��.\n",m_stInitInfo.dbname,m_stInitInfo.tbname);
                lgprintf("%s:%d error",__FILE__,__LINE__);
                return 5;
            }
         }
         catch(...)
         {
             lgprintf("tbloader_helper::parserColumnInfo insert_db table %s.%s error.\n",m_stInitInfo.dbname,m_stInitInfo.tbname);
         	 return 5;
         }
    }
    else     
    {   
         // д���ļ��У�ֱ��д���ļ�����
         CLOSE_FILE(m_pFile);
         m_pFile = fopen(m_stInitInfo.fn_ext,"wb");
         if( NULL == m_pFile)
         {
    	       lgprintf("Open file %s error \n",m_stInitInfo.fn_ext);
    	       lgprintf("%s:%d error",__FILE__,__LINE__);
    	       return -1;	
         }
    
         // д���ļ�ͷ����
        int _headerlen = headerlen;
        if(-1 == WriteColumnHeaderLen(m_pFile,_headerlen))
        {   
    	     lgprintf("%s:%d error",__FILE__,__LINE__);   
             return -1;
        }
    	
    	// д���ļ�ͷ����
        if(-1 == WriteColumnHeader(m_pFile,header,_headerlen))
        { 
    	     lgprintf("%s:%d error",__FILE__,__LINE__);
             return -1;
        }       
    }
    //lgprintf("id=[%d],end tbloader_helper::parserColumnInfo opertype = %d\n",m_id,m_opertype);
    m_status = parserheader;
    m_firstEnter = true;
    return 0;
}

// ����md5У��ֵ���ַ�����32�ַ�
void  tbloader_helper::getMd5Sum(char * pmd5sum)
{
    char md5array[conMd5StrLen];
    memset(md5array,0,conMd5StrLen);
    for(int i=0;i<conMd5Len;i++)
    {
        sprintf(md5array+strlen(md5array),"%02x",m_md5sum[i]);
    }
    strcpy(pmd5sum,md5array);
}

/*
����ֵ:
0: �ɹ�
-1: �ļ�д��ʧ��
1: �ļ�ͷδд���,tl_writeHeadInfoδ���óɹ���
2: ���ݳ���У��ʧ��
3: δ��ʼ�� tl_initStorerForFile ���� tl_initStorerForDB
4: ���ݰ�����
*/
int   tbloader_helper::writePackData(const char* porigin_buff,const long origin_len,const int rownum)
{
    if (m_status == status_unknown)
    {
        lgprintf("%s:%d error",__FILE__,__LINE__);
        return 3;
    }
    if (m_status < parserheader)
    {
        lgprintf("%s:%d error",__FILE__,__LINE__);
        return 1;
    }
    
    // �����¼����
    int parserRows = 0;
    
    // У��md5ֵ
    MD5((unsigned char*)porigin_buff,origin_len,m_md5sum);
    
    if(m_opertype == write_files) //<< �����ļ�����
    {
        if (m_compressed_len == 0 || (m_compressed_len <= (origin_len+ conAppendMem5MB/2)) )
        {
            if(m_pCompressed_data != NULL){
               free(m_pCompressed_data);
               m_pCompressed_data = NULL;
            }                    
            m_compressed_len = conAppendMem5MB + origin_len;
            m_pCompressed_data = (unsigned char *)malloc(m_compressed_len);
        }
        
        // �����ʼλ��
        long _blokStartPos = ftell(m_pFile);
        m_blkSumaryInfo.BlkStartPosVec.push_back(_blokStartPos);
        
        // �޸�ѹ������
        long compressed_len = m_compressed_len;
        //lgprintf("id=[%d],begin tbloader_helper::writePackData  to write data compressed_len = %d,origin_len=%d.\n",m_id,compressed_len,origin_len);
        if(0 != WriteBlockData(m_pFile,_blokStartPos,(char*)m_pCompressed_data,compressed_len,(const char*)porigin_buff,origin_len))
        {
            lgprintf("WriteBlockData return error.\n");
            return -31;
        }
        //lgprintf("id=[%d],end tbloader_helper::writePackData  to write data compressed_len = %d,origin_len=%d.\n",m_id,compressed_len,origin_len);
       
        // �ð��ļ�¼����
        parserRows = rownum;
    	
    }
    else //<< �������ݿ����
    {                      
        try
        {
            if(!m_firstEnter && ((m_mt->Cur()->GetRows() + m_blkSumaryInfo.rowNum) >= m_mt->Cur()->GetMaxRows()))
            {
                if(m_DirectWriteTable)
                {   // ͨ��ֱ��д���ļ��ķ�ʽ��������д�����ݿ��� 
                    //lgprintf("id=[%d],start tbloader_helper::writePackData to write mt to file,mt rows = %d.\n",m_id,m_mt->Cur()->GetRows());
                    if(-1 == DirectWriteMt2Table(m_stInitInfo.dbname,m_stInitInfo.tbname,*m_mt->Cur(),m_pConnector,m_sysPath,false))
                    {
                        lgprintf("insert to table [%s.%s] error.",m_stInitInfo.dbname,m_stInitInfo.tbname);
                        return -21;
                    }
                    //lgprintf("id=[%d],end tbloader_helper::writePackData to write mt to file,mt rows = %d.\n",m_id,m_mt->Cur()->GetRows());
                }
                else
                {   // ͨ��insert�������ݿ��м�¼
                    char tb[500];
                    sprintf(tb,"%s.%s",m_stInitInfo.dbname,m_stInitInfo.tbname);
                    //lgprintf("id=[%d],start tbloader_helper::writePackData to write mt into table,mt rows = %d.\n",m_id,m_mt->Cur()->GetRows());
                    wociAppendToDbTable(*m_mt->Cur(),tb,*m_dtd,true);
                    //lgprintf("id=[%d],start tbloader_helper::writePackData to write mt into table,mt rows = %d.\n",m_id,m_mt->Cur()->GetRows());
                }
                
                wociReset(*m_mt->Cur());
            }
            
            m_firstEnter = false;
            //lgprintf("id=[%d],start tbloader_helper::writePackData  to parse data origin_len = %d.\n",m_id,origin_len);
            parserRows = ParserOriginData((char*)porigin_buff,origin_len,*m_mt->Cur(),m_colInfoVec);
            //lgprintf("id=[%d],end tbloader_helper::writePackData  to parse data origin_len = %d.\n",m_id,origin_len);
            if (parserRows<=0)
            {
                lgprintf("Do ParserOriginData return error.\n");
                return parserRows;
            }
            if(parserRows != rownum)
            {
                lgprintf("ParserOriginData return rows %d != input row %d .\n",parserRows,rownum); 	
            }
        }
        catch(...)
        {
        	lgprintf("tbloader_helper::stop insert table[%s.%s] errro , m_DirectWriteTable = %d.\n",m_stInitInfo.dbname,m_stInitInfo.tbname,m_DirectWriteTable?1:0);                   
            return -22;	
        }
    }
    
    m_blkSumaryInfo.rowSum += parserRows;
    m_blkSumaryInfo.blockNum ++;
    if(m_blkSumaryInfo.blockNum+1 % 1000 ==0){
        lgprintf("write pack %d ....",m_blkSumaryInfo.blockNum);
    }
    
    //lgprintf("id=[%d],tbloader_helper::writePackData packnum = %d,rowNum=%d,rowSum=%d\n",m_id,m_blkSumaryInfo.blockNum,m_blkSumaryInfo.rowNum,m_blkSumaryInfo.rowSum);
    
    // ���ö��¼����
    if(m_blkSumaryInfo.rowNum <= parserRows){
        m_blkSumaryInfo.rowNum = parserRows;
    }
    
    m_status = writedata;    
    return 0;
}

