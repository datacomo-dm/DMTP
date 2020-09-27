#include "tbloader_helper.h"

/*
     func:Start_loadfile(void *ptr)
     desc:���ļ�װ�����
     param:
          ptr[in]:�������ݿ�������Ϣ
*/
int Start_loadfile(void *ptr);

/*
     func:Start_loadfile_new(void* ptr)
     desc : ���ļ�װ�����ݿ�
     param:
         ptr[in]:�������ݿ�������Ϣ
*/
int Start_loadfile_new(void* ptr);

/*
     func:Start_genfile(void *ptr)
     desc:���ļ���ȡ�ļ����������ļ�
     param:
          ptr[in]:�������ݿ�������Ϣ
*/
int Start_genfile(void *ptr);


/*
   func:HelperDisplay()
   desc:������Ϣ��ʾ
*/
void HelperDisplay();

int main(int argc,char *argv[])  
{                   
    if(argc < 2){
        printf("tbloader:���ò����������.\n");
		HelperDisplay();
        return 0;	
    }
    int index = 1;
    _InitInfo stInitInfo;
	strcpy(stInitInfo.funcode,argv[index++]);
	if(strcasecmp(stInitInfo.funcode,"-h") == 0 || strcasecmp(stInitInfo.funcode,"-help") == 0)
	{
        HelperDisplay();
		return 0;
	}
	else if(strcasecmp(stInitInfo.funcode,"-load_file") == 0 && argc == 9)
	{
        strcpy(stInitInfo.dsn,argv[index++]);
        strcpy(stInitInfo.user,argv[index++]);
        strcpy(stInitInfo.pwd,argv[index++]);
        strcpy(stInitInfo.dbname,argv[index++]);
        strcpy(stInitInfo.tbname,argv[index++]);
        if(strcasecmp("MyISAM",argv[index]) == 0)
        {
            stInitInfo.tbEngine = MyISAM;
        }
        else if(strcasecmp("BrightHouse",argv[index]) == 0)
        {
            stInitInfo.tbEngine = Brighthouse;        
        }
        else
        {              
            printf("tbloader:���ò����������.���ݿ����������<MyISAM,BrightHouse>����.\n");
            HelperDisplay();
            return 0;
        }
        index++;
        strcpy(stInitInfo.fn,argv[index++]);
	}
	else if(strcasecmp(stInitInfo.funcode,"-gen_file") == 0 && argc == 9)
	{
        strcpy(stInitInfo.dsn,argv[index++]);
        strcpy(stInitInfo.user,argv[index++]);
        strcpy(stInitInfo.pwd,argv[index++]);
        strcpy(stInitInfo.dbname,argv[index++]);
        strcpy(stInitInfo.tbname,argv[index++]);
        strcpy(stInitInfo.fn,argv[index++]);
		strcpy(stInitInfo.fn_ext,argv[index++]);
	}
	else
	{
        printf("tbloader:���ò����������.\n");
		HelperDisplay();
        return 0;	
	}
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
    int nRetCode = 0;
#ifndef TBLOADER_HELPER_TEST
    WOCIInit("tbloader");
    wociSetOutputToConsole(TRUE);
    wociSetEcho(true);	
	if(strcasecmp(stInitInfo.funcode,"-load_file") == 0 ){
        nRetCode=wociMainEntrance(Start_loadfile,true,(void*)&stInitInfo,2);
	}
	else if(strcasecmp(stInitInfo.funcode,"-gen_file") == 0){
		nRetCode=wociMainEntrance(Start_genfile,true,(void*)&stInitInfo,2);
	}
#else
    if(strcasecmp(stInitInfo.funcode,"-load_file") == 0 ){
        nRetCode=Start_loadfile_new((void*)&stInitInfo);
	}
#endif

#ifndef TBLOADER_HELPER_TEST
    WOCIQuit(); 
#endif

    return nRetCode;
}


/*
     func:Start_loadfile(void *ptr)
     desc:���ļ�װ�����
     param:
          ptr[in]:�������ݿ�������Ϣ
*/
int Start_loadfile(void *ptr) 
{ 
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
	_ColumnInfoPtrVector  g_colInfoVec; 	   // �洢��ͷ������Ϣ
	
	
	/*
	�������֣�
	�����ݿ�λ�� (Long)[] ��һ�����ݿ�λ�� filesize - 16 -([y]*8) ,�ڶ������ݿ�λ�� filesize - 16 -([y-1]*8) �Դ�����
	�������� ��Long�� 8�ֽ�
	���ݿ����� ��int�� 4�ֽ�
	���ݿ���� ��int�� 4�ֽ� [y]
	*/
	_BlockSumary   g_blkSumaryInfo; 	// ��ĸſ���Ϣ
	
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
	unsigned char  *g_pCompressed_data=NULL;			  // shared comprssed data buffer
	unsigned char  *g_pOrigin_data=NULL;				  // shared origin data buffer

	_InitInfoPtr pobj = (_InitInfoPtr)ptr;
    AutoHandle dtd;
	mytimer tm;
	tm.Start();
    
	//--1.  ����Ŀ�����ݿ�
    dtd.SetHandle(wociCreateSession(pobj->user,pobj->pwd,pobj->dsn,DTDBTYPE_ODBC));

	//--1.1 ����dm���ݿ�
    AutoHandle dm_dt;
	// �ӻ��������л�ȡ����dm����ز�����Ϣ
    const char *_musrname = getenv("DP_MUSERNAME");
    char _mpswd[100];
	strcpy(_mpswd,getenv("DP_MPASSWORD"));
	decode(_mpswd);
    const char *_mhost = getenv("DP_DSN");
    const char *_serverip = getenv("DP_SERVERIP");
    const char *_strport = getenv("DP_MPORT");
    int  _port = 0;
    char m_sysPath[256];
    if(_strport == NULL || atoi(_strport)<3306){_port = 3306;}
	else{_port = atoi(_strport);}
	
    dm_dt.SetHandle(wociCreateSession(_musrname,_mpswd,_mhost,DTDBTYPE_ODBC));
    {
	  	// ��ȡ·��
        AutoMt  path_mt(dm_dt,10);
        path_mt.FetchFirst("select pathval from dp.dp_path where pathtype = 'msys' order by pathid limit 5");
        int rn = path_mt.Wait();
        if(rn <=0) {
            lgprintf("get syspath error\n");
            return -1;
        }
        strcpy(m_sysPath,path_mt.PtrStr("pathval",0));		
    }
    MySQLConn m_Connector;
    m_Connector.Connect(_serverip,_musrname,_mpswd,NULL,_port);
 
	
    //--2. ����Ŀ�����ݿ��MT�ṹ
    TradeOffMt mt(0,_MAX_ROWS);
    AutoStmt srcst(dtd);
    srcst.Prepare("select * from %s.%s limit 2",pobj->dbname,pobj->tbname);
    wociBuildStmt(*mt.Cur(),srcst,_MAX_ROWS);
    wociBuildStmt(*mt.Next(),srcst,_MAX_ROWS);
//    mt.SetPesuado(true);
    mt.Cur()->AddrFresh();
    mt.Next()->AddrFresh();
    
    //--3. ���ļ�
    FILE *pFile = NULL;
	pFile = fopen(pobj->fn,"rb");
	if( NULL == pFile){
		lgprintf("Open file %s error \n",pobj->fn);
		return -1;	
	}

    //--4. ��ȡ�ļ�������Ϣ(�����Ƶ���Ϣ,����Ŀ,�������)
    if (-1 == GetColumnInfo2Vector(pFile,g_colInfoVec)){ 
        ReleaseColInfo(g_colInfoVec);
        fclose(pFile);pFile = NULL;
        return -1;
    }
    
    //--5. ��ȡ�ļ��ĸſ���Ϣ(�ļ���С�����ݿ���Ŀ��ÿһ�����ݿ�������ÿһ�����ݿ����ʼλ��)
    if (-1 == GetFileSumaryInfo(pFile,g_blkSumaryInfo)){
		ReleaseColInfo(g_colInfoVec);
		fclose(pFile);pFile = NULL;
		return -1;
    }
    
    //--6. �����ȡ�ļ�����Ҫ�Ļ���buff,���п��
	int rowLen = 0;
    int colNum = 0;
    AutoMt mt2(dtd,10);
    mt2.FetchFirst("select * from %s.%s limit 2",pobj->dbname,pobj->tbname);
	mt2.Wait();
	rowLen = wociGetRowLen(mt2);
    colNum = wociGetColumnNumber(mt2);
    lgprintf("У���ṹ��ʽ...");
    if(colNum == g_colInfoVec.size())
    {
       char cn[256];      
       for(int col = 0;col < colNum;col++)
       {        
       	   wociGetColumnName(mt2,col,cn);   
           if(strcasecmp(g_colInfoVec[col]->columnName,cn) !=0)
           {
           	  lgprintf("�ļ�%s �ṹ��� %s.%s �ṹ��һ�£�������ѡ���ļ�.",pobj->fn,pobj->dbname,pobj->tbname);
       	      ReleaseColInfo(g_colInfoVec);
		      fclose(pFile);pFile = NULL;
		      return -1;
           }	
       } 	
    }
    else
    {
    	lgprintf("�ļ�%s �ṹ��� %s.%s �ṹ��һ�£�������ѡ���ļ�.",pobj->fn,pobj->dbname,pobj->tbname);
       	ReleaseColInfo(g_colInfoVec);
		fclose(pFile);pFile = NULL;
		return -1;
    }
	long  _compressed_len,compressed_len;
	long  _origin_len,origin_len;
    long  _min_rows = g_blkSumaryInfo.rowNum > g_blkSumaryInfo.rowSum ? g_blkSumaryInfo.rowSum :g_blkSumaryInfo.rowNum;
	_compressed_len = LocateBuff(_min_rows,rowLen,colNum,&g_pCompressed_data);
	_origin_len = LocateBuff(_min_rows,rowLen,colNum,&g_pOrigin_data);

    //--7. ���ļ��н������ݣ����MT����MT������Append�����ݿ���
    lgprintf("��ȡ�ļ��������...");
	bool  has_error = false;
	bool  firstEnter = true;
	long  deal_rows = 0;   // �Ѿ�����ļ�¼����
	long  rn = 0;
	int  blkIdx = 0;       // block ��������ʼ��������
	long  block_start_pos;
    bool  dealfinish = false;  // �������
	while ( (rn>0 && !has_error && !firstEnter) || firstEnter)
	{
		if(!firstEnter)
		{
            char *pDirectWriteTable = getenv("DirectWriteTable");
            bool b_DirectWriteTable = false;
            if (pDirectWriteTable != NULL && (atoi(pDirectWriteTable) == 1)){
                b_DirectWriteTable = true;
            }
            if(b_DirectWriteTable)
            {   // ͨ��ֱ��д���ļ��ķ�ʽ��������д�����ݿ��� 
                if(-1 == DirectWriteMt2Table(pobj->dbname,pobj->tbname,*mt.Cur(),&m_Connector,m_sysPath,dealfinish/*�Ƿ������һ��mt*/))
				{
				    lgprintf("insert to table [%s.%s] error.",pobj->dbname,pobj->tbname);
					has_error = true;
				}
            }
			else
            {   // ͨ��insert�������ݿ��м�¼
			    char tb[500];
                sprintf(tb,"%s.%s",pobj->dbname,pobj->tbname);
                wociAppendToDbTable(*mt.Cur(),tb,dtd,true);
            }
			
            wociReset(*mt.Cur());
		}
		firstEnter = false;
        if(dealfinish){
        	break;
        }

		while(1)
		{
			compressed_len = origin_len = _compressed_len;
			if (dealfinish){
				break;			
			}

			block_start_pos = g_blkSumaryInfo.BlkStartPosVec[blkIdx++];
			
            if (blkIdx == g_blkSumaryInfo.BlkStartPosVec.size()){  // ���һ�����ݿ鴦��
				dealfinish = true;
            }

			if(0 != GetBlockData(pFile,block_start_pos,(char*)g_pCompressed_data,compressed_len,(char*)g_pOrigin_data,origin_len))
			{
				lgprintf("get GetBlockData return error.");
				has_error = true;
				break;
			}
			else
			{
				int parserRows = ParserOriginData((char*)g_pOrigin_data,origin_len,*mt.Cur(),g_colInfoVec);
				if (parserRows<=0)
				{
					lgprintf("get ParserOriginData return error.");
					has_error = true;
					break;
				}
				else
				{
					deal_rows += parserRows;
					if ( (mt.Cur()->GetRows() + g_blkSumaryInfo.rowNum) >= mt.Cur()->GetMaxRows()) // �жϼ�¼���Ƿ�����
					{
						// һ�� mt �Ѿ�װ�������ˣ�׼������д��������
						break;
					}					 
				}
			}// end else
		} // end while(1)
		rn = mt.Cur()->GetRows();
	} // while (rn > 0 && !has_error)     

	tm.Stop();
	if(!has_error){
        printf("\n������%d��(ʱ��%.2f��)��\n",deal_rows,tm.GetTime());
	}

    //--8. �ͷ��Ѿ�����õ��ڴ���Դ
    ReleaseBuff(g_pCompressed_data);
    ReleaseBuff(g_pOrigin_data);
	ReleaseColInfo(g_colInfoVec);
	fclose(pFile);pFile = NULL;

    return 1;
}



/*
     func:Start_genfile(void *ptr)
     desc:���ļ���ȡ�ļ����������ļ�
     param:
          ptr[in]:�������ݿ�������Ϣ
*/
int Start_genfile(void *ptr)
{ 
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
	_ColumnInfoPtrVector  g_colInfoVec; 	   // �洢��ͷ������Ϣ
	
	
	/*
	�������֣�
	�����ݿ�λ�� (Long)[] ��һ�����ݿ�λ�� filesize - 16 -([y]*8) ,�ڶ������ݿ�λ�� filesize - 16 -([y-1]*8) �Դ�����
	�������� ��Long�� 8�ֽ�
	���ݿ����� ��int�� 4�ֽ�
	���ݿ���� ��int�� 4�ֽ� [y]
	*/
	_BlockSumary   g_blkSumaryInfo; 	// ��ĸſ���Ϣ
	_BlockSumary   g_blkSumaryInfo_ext;     // �����ļ��Ŀ�ĸſ���Ϣ
	
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
	unsigned char  *g_pCompressed_data=NULL;			  // shared comprssed data buffer
	unsigned char  *g_pOrigin_data=NULL;				  // shared origin data buffer
	
	_InitInfoPtr pobj = (_InitInfoPtr)ptr;
    AutoHandle dtd;
	mytimer tm;
	tm.Start();
    
	//--1.  ����Ŀ�����ݿ�
    dtd.SetHandle(wociCreateSession(pobj->user,pobj->pwd,pobj->dsn,DTDBTYPE_ODBC));
    
    //--2. ���ļ�
    FILE *pFile = NULL;
	pFile = fopen(pobj->fn,"rb");
	if( NULL == pFile){
		lgprintf("Open file %s error \n",pobj->fn);
		return -1;	
	}
	FILE *pFileExt = NULL;
	pFileExt = fopen(pobj->fn_ext,"wb");
	if( NULL == pFileExt){
		lgprintf("Open file %s error \n",pobj->fn_ext);
		CLOSE_FILE(pFile);
		return -1;	
	}

    //--3. ��ȡ�ļ�������Ϣ(�����Ƶ���Ϣ,����Ŀ,�������)
	// 3.1 read header len
	int headLen = 0;
	headLen = GetColumnHeaderLen(pFile);
    if(headLen == -1){
		CLOSE_FILE(pFile);
		CLOSE_FILE(pFileExt);
		return -1;
    }
    if(-1 == WriteColumnHeaderLen(pFileExt,headLen))
    {    
		CLOSE_FILE(pFile);
		CLOSE_FILE(pFileExt);
        return -1;
	}
	
	// 3.2 read header
	HerderPtr header = NULL;
    header = (HerderPtr)malloc(headLen+1);
    if(headLen != GetColumnHeader(pFile,header,headLen)){
		ReleaseBuff(header);		
		CLOSE_FILE(pFile);
		CLOSE_FILE(pFileExt);
		return -1;
	}
    if(-1 == WriteColumnHeader(pFileExt,header,headLen))
    {
		ReleaseBuff(header);		
		CLOSE_FILE(pFile);
		CLOSE_FILE(pFileExt);
		return -1;
    }
	
	// 3.3 parser column info
    if (-1 == ParserColumnInfo(header,g_colInfoVec)){
		lgprintf("Parser file header error \n");
		CLOSE_FILE(pFile);
		CLOSE_FILE(pFileExt);
		ReleaseBuff(header);
		return -1;
    }
    ReleaseBuff(header);

    
    //--4. ��ȡ�ļ��ĸſ���Ϣ(�ļ���С�����ݿ���Ŀ��ÿһ�����ݿ�������ÿһ�����ݿ����ʼλ��)
    if (-1 == GetFileSumaryInfo(pFile,g_blkSumaryInfo)){
		ReleaseColInfo(g_colInfoVec);
		fclose(pFile);pFile = NULL;
		return -1;
    }
    
    //--5. �����ȡ�ļ�����Ҫ�Ļ���buff,���п��
	int rowLen = 0;
    int colNum = 0;
    AutoMt mt2(dtd,10);
    mt2.FetchFirst("select * from %s.%s limit 2",pobj->dbname,pobj->tbname);
	mt2.Wait();
	rowLen = wociGetRowLen(mt2);
    colNum = wociGetColumnNumber(mt2);
    lgprintf("У���ṹ��ʽ...");
    if(colNum == g_colInfoVec.size())
    {
       char cn[256];      
       for(int col = 0;col < colNum;col++)
       {        
       	   wociGetColumnName(mt2,col,cn);   
           if(strcasecmp(g_colInfoVec[col]->columnName,cn) !=0)
           {
           	  lgprintf("�ļ�%s �ṹ��� %s.%s �ṹ��һ�£�������ѡ���ļ�.",pobj->fn,pobj->dbname,pobj->tbname);
       	      ReleaseColInfo(g_colInfoVec);
		      fclose(pFile);pFile = NULL;
		      return -1;
           }	
       } 	
    }
    else
    {
    	lgprintf("�ļ�%s �ṹ��� %s.%s �ṹ��һ�£�������ѡ���ļ�.",pobj->fn,pobj->dbname,pobj->tbname);
       	ReleaseColInfo(g_colInfoVec);
		fclose(pFile);pFile = NULL;
		return -1;
    }
	long  _compressed_len,compressed_len;
	long  _origin_len,origin_len;
    long  _min_rows = g_blkSumaryInfo.rowNum > g_blkSumaryInfo.rowSum ? g_blkSumaryInfo.rowSum :g_blkSumaryInfo.rowNum;
	_compressed_len = LocateBuff(_min_rows,rowLen,colNum,&g_pCompressed_data);
	_origin_len = LocateBuff(_min_rows,rowLen,colNum,&g_pOrigin_data);

    //--7. ���ļ��н������ݣ�ѹ����д���ļ�
    lgprintf("��ȡ�ļ�����ת���ļ�...");
	bool  has_error = false;
	long  rn = 0;
	int  blkIdx = 0;       // block ��������ʼ��������
	long  block_start_pos;
    bool  dealfinish = false;  // �������
	while (!has_error)
	{
	    compressed_len = origin_len = _compressed_len;
		if (dealfinish){
			break;			
		}

		block_start_pos = g_blkSumaryInfo.BlkStartPosVec[blkIdx++];
        if (blkIdx == g_blkSumaryInfo.BlkStartPosVec.size()){  // ���һ�����ݿ鴦��
			dealfinish = true;
        }

		if(0 != GetBlockData(pFile,block_start_pos,(char*)g_pCompressed_data,compressed_len,(char*)g_pOrigin_data,origin_len))
		{
			lgprintf("get GetBlockData return error.");
			has_error = true;
			break;
		}
		else
		{
		    // �����ʼλ��
		    long _blokStartPos = ftell(pFileExt);
			g_blkSumaryInfo_ext.BlkStartPosVec.push_back(_blokStartPos);

			// �޸�ѹ������
			compressed_len = _compressed_len;
			if(0 != WriteBlockData(pFileExt,_blokStartPos,(char*)g_pCompressed_data,compressed_len,(const char*)g_pOrigin_data,origin_len))
			{
                lgprintf("get WriteBlockData return error.");
  			    has_error = true;
				break;
			}
            if(blkIdx %(g_blkSumaryInfo.blockNum / 10 + 1) == 0 || blkIdx == g_blkSumaryInfo.blockNum){
			    lgprintf("����ת�������%d����������%d����...",blkIdx,g_blkSumaryInfo.blockNum);
            }
		}// end else
	} // end while(1)
	
    //--8. ���ļ��н������ݣ�ѹ����д���ļ�
    if(!has_error)
	{
        g_blkSumaryInfo_ext.blockNum = g_blkSumaryInfo.blockNum;
    	g_blkSumaryInfo_ext.rowNum = g_blkSumaryInfo.rowNum;
	    g_blkSumaryInfo_ext.rowSum = g_blkSumaryInfo.rowSum;	
        if(-1 == WriteFileSumaryInfo(pFileExt,g_blkSumaryInfo_ext)){
            has_error = true;
        }			
	}
	tm.Stop();
	if(!has_error){
        printf("\n������%d��(ʱ��%.2f��)��\n",g_blkSumaryInfo.rowSum,tm.GetTime());
	}

    //--9. �ͷ��Ѿ�����õ��ڴ���Դ
    ReleaseBuff(g_pCompressed_data);
    ReleaseBuff(g_pOrigin_data);
	ReleaseColInfo(g_colInfoVec);
	CLOSE_FILE(pFile);
	CLOSE_FILE(pFileExt);

    return 1;
}




/*
   func:HelperDisplay()
   desc:������Ϣ��ʾ
*/
void HelperDisplay()
{
   const char * pMsg = "tbloader ������Ϣ��ʾ\n"
   	"������Ϣ:\n\n"
   	"-help \n"
   	"         ������Ϣ\n\n"
    "-load_file\n"
    "         [ODBC����Դ][���ݿ��û���][���ݿ�����][���ݿ�����][������][��������][װ����ļ�]\n"
    "         example : tbloader -load_file dsn user pwd dbname tbname engine<MyISAM,BrightHouse> fn\n\n"
#ifndef TBLOADER_HELPER_TEST
    "-gen_file\n"    
    "         [ODBC����Դ][���ݿ��û���][���ݿ�����][���ݿ�����][������][װ����ļ�][���ɵ��ļ�]\n"
    "         example : tbloader -gen_file dsn user pwd dbname tbname fin fout\n\n";
#else
    "tbloader_test ����\n";
#endif
   printf(pMsg);
}

//---------------------------------------------------------------------------------

/*
     func:Start_loadfile_new(void* ptr)
     desc : ���ļ�װ�����ݿ�
     param:
         ptr[in]:�������ݿ�������Ϣ
*/
int Start_loadfile_new(void* ptr)
{	
    _BlockSumary   g_blkSumaryInfo; 	// ��ĸſ���Ϣ
	
	unsigned char  *g_pCompressed_data=NULL;			  // shared comprssed data buffer
	unsigned char  *g_pOrigin_data=NULL;				  // shared origin data buffer

	_InitInfoPtr pobj = (_InitInfoPtr)ptr;

	// tbloader_helper ����
	tbloader_helper tbloader_obj;
   	
	tbloader_obj.setId(1);
	if(0 != tbloader_obj.start(pobj,insert_db,"tbloader"))
    {
        return -1;
    }             
    //--3. ���ļ�
    FILE *pFile = NULL;
	pFile = fopen(pobj->fn,"rb");
	if( NULL == pFile){
		lgprintf("Open file %s error \n",pobj->fn);
		return -1;	
	}

       // 3.1 read header len
	int headLen = 0;
	headLen = GetColumnHeaderLen(pFile);
    if(headLen == -1){
	    CLOSE_FILE(pFile);
	    return -1;
    }	
	// 3.2 read header
	HerderPtr header = NULL;
    header = (HerderPtr)malloc(headLen+1);
    if(headLen != GetColumnHeader(pFile,header,headLen)){
		ReleaseBuff(header);		
		CLOSE_FILE(pFile);
		return -1;
	}

    // ����ͷ��
    if(0 != tbloader_obj.parserColumnInfo(header, headLen))
    {		
       ReleaseBuff(header);		
       CLOSE_FILE(pFile);
       return -1;
    }
    
    //--5. ��ȡ�ļ��ĸſ���Ϣ(�ļ���С�����ݿ���Ŀ��ÿһ�����ݿ�������ÿһ�����ݿ����ʼλ��)
    if (-1 == GetFileSumaryInfo(pFile,g_blkSumaryInfo)){
		CLOSE_FILE(pFile);
		return -1;
    }
    
    //--6. �����ȡ�ļ�����Ҫ�Ļ���buff,���п��
    long  _compressed_len,compressed_len;
    long  _origin_len,origin_len;

    // ����ǲ��Եģ��ڴ�����Ϊ10M
    _compressed_len = _origin_len= conAppendMem5MB*2;
	
    g_pCompressed_data = (unsigned char *)malloc(_compressed_len);
    g_pOrigin_data = (unsigned char *)malloc(_compressed_len);

    //--7. ���ļ��н������ݣ����MT����MT������Append�����ݿ���
    lgprintf("��ȡ�ļ��������...");
	bool  has_error = false;
	long  deal_rows = 0;   // �Ѿ�����ļ�¼����
	long  rn = 0;
	int  blkIdx = 0;       // block ��������ʼ��������
	long  block_start_pos;
    bool  dealfinish = false;  // �������
    while(!has_error)
    {
    	compressed_len = origin_len = _compressed_len;
    	if (dealfinish){
    		break;			
    	}
    
    	block_start_pos = g_blkSumaryInfo.BlkStartPosVec[blkIdx++];
    	
        if (blkIdx == g_blkSumaryInfo.BlkStartPosVec.size()){  // ���һ�����ݿ鴦��
    		dealfinish = true;
        }
    	                     
    	if(0 != GetBlockData(pFile,block_start_pos,(char*)g_pCompressed_data,compressed_len,(char*)g_pOrigin_data,origin_len))
    	{
    		lgprintf("get GetBlockData return error.");
    		has_error = true;
    		break;
    	}
    	else
    	{
    	     int _test_row_num = -1; // -1��ʾ����У���¼����
    	     if(0!=tbloader_obj.writePackData((const char*)g_pOrigin_data,origin_len,_test_row_num))
    	     {
    	         has_error= true;
    	         break;
    	     }
             deal_rows += _test_row_num;
    	}// end else
    } // end while(1)
    
	tbloader_obj.stop();


    //--8. �ͷ��Ѿ�����õ��ڴ���Դ
    ReleaseBuff(g_pCompressed_data);
    ReleaseBuff(g_pOrigin_data);
	CLOSE_FILE(pFile);
    
    return 1;

}
