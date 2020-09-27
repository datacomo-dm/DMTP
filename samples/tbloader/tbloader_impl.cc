#include "tbloader_impl.h"

/*
    func:ReleaseBuff 
    desc:release malloc locate memory
*/
void ReleaseBuff(void* p){
    if(NULL != p){
        free(p); p = NULL;
    }      
}

/*
    func: LocateBuff(int rowNum,int rowLen,int colnum)
    desc: locate buffer for data block use
    params:
        rowNum[in]: data block row number
        rowLen[in]: row len
        colnum[in]: data block columns
        pbuff[in/out]: the data buffer 
    return: the buffer len
*/
int LocateBuff(const int rowNum,const int rowLen,const int colnum/* ��*/,unsigned char** pbuff)
{
   ReleaseBuff(*pbuff);	
   int len = rowNum*rowLen +(colnum * 4/* �ַ������ͣ�ǰ4�ֽ�Ϊ�ַ������� */ + 4/* ÿ�����ݴ�С */ + colnum/*�п����*/);
   len += conAppendMem5MB;   // �Ӵ�5MB
   *pbuff = (unsigned char*)malloc(len);
   return len;
}


/*
    func: GetColumnHeaderLen(FILE* pFile,char * pHeader)
    desc: ��ȡ�ļ���ͷ������
    param:
           pFile[in]: ���ݿ��ļ����
    return: failed---> -1
            success----> header Len
*/
int  GetColumnHeaderLen(FILE* pFile)
{
    // read header len
    int headLen = 0;
    if(1 != fread(&headLen,sizeof(int),1,pFile)){
        lgprintf("Read file header len error\n");
        return -1;
    }
    _revInt(&headLen);
    return headLen;
}
int  WriteColumnHeaderLen(FILE* pFile,int headerlen)
{
    _revInt(&headerlen);
    if(1 != fwrite(&headerlen,sizeof(int),1,pFile)){
        lgprintf("Write file header len error\n");
        return -1;
    }
    _revInt(&headerlen);
    return headerlen;
}


/*
    func: GetColumnHeader(FILE* pFile,char * pHeader)
    desc: ��ȡ�ļ���ͷ��
    param:
           pFile[in]: ���ݿ��ļ����
           pHeader[in/out]: �ļ�ͷ��
    return: failed---> -1
            success----> header Len
*/
int  GetColumnHeader(FILE* pFile,HerderPtr pHeader,const int headerlen)
{
    if(1 != fread(pHeader,headerlen,1,pFile)){
        lgprintf("Read file header error \n");
        return -1;
    }
    return headerlen;
}
int  WriteColumnHeader(FILE* pFile,HerderPtr pHeader,const int headerlen)
{
    if(1 != fwrite(pHeader,headerlen,1,pFile)){
        lgprintf("Write file header error \n");
        return -1;
    }
    return headerlen;
}


/*   
     func: GetColumnInfo2Vector(FILE* pFile,_ColumnInfoPtrVector& colInfoVec)
     desc: ��ȡ�е������Ϣ�����������
     param:
           pFile[in]: ���ݿ��ļ��򿪾��
           colInfoVec[in/out]:�洢���ݿ�����Ϣ�Ķ���
     return: success 0,error -1
*/
int  GetColumnInfo2Vector(FILE* pFile,_ColumnInfoPtrVector& colInfoVec)
{
    // read header len
    int headLen = 0;
    headLen = GetColumnHeaderLen(pFile);
    if(headLen == -1){
        return -1;
    }
    
    
    // read header
    HerderPtr header = NULL;
    header = (HerderPtr)malloc(headLen+1);
    if(headLen != GetColumnHeader(pFile,header,headLen)){
        ReleaseBuff(header);
        return -1;
    }
    
    /*
    columnName(����);
    columnDBType(���ݿ�������);
    columnType(java����);
    columnCName;
    columnPrecision;
    columnScale 
    ���磺col1,col2;dbtype1,dbtype2;type1,type2;cc1,cc2;prec1,prec2;scale1,scale2;
    */
    if (-1 == ParserColumnInfo(header,colInfoVec)){
        lgprintf("Parser file header error \n");
        ReleaseBuff(header);
        return -1;
    }
    ReleaseBuff(header);
    return 0;
}

/*
     func: ReleaseColInfo(_ColumnInfoPtrVector& colInfoVec)
     desc: �ͷŴ洢�������Ϣ���ж���
     param:
           colInfoVec[in/out]:�洢���ݿ�����Ϣ�Ķ���
*/
void ReleaseColInfo(_ColumnInfoPtrVector& colInfoVec)
{
    while(!colInfoVec.empty())
    {
       for(_ColumnInfoPtrIter iter=colInfoVec.begin();iter != colInfoVec.end();iter++){
           _ColumnInfoPtr pObj = NULL;
           pObj = (_ColumnInfoPtr)(*iter);
           colInfoVec.erase(iter);
           if(NULL != pObj){
              delete pObj;
              pObj = NULL;
           }
           break;	   
       }
   }
}

/*
    func: ParserColumnInfo(const char* header,_ColumnInfoPtrVector& colInfoVec)
    desc: ��������Ϣ
    param:
          header[in] : column header info 
          colInfoVec[in/out]:�洢���ݿ�����Ϣ�Ķ���
    return : success 0,error -1 
*/
int  ParserColumnInfo(const HerderPtr header,_ColumnInfoPtrVector& vec)
{
    std::string parserHeader = header;
    std::string columnsNameInfo;    // �������:col1,col2,col3
    std::string columnsDBType;      // ���������
    std::string columnItem;
    size_t separater_pos = 0;
    int  column_num = 0; 
    int  colIndex = 0;
    
    // get columnNames : col1,col2,col3
    separater_pos = parserHeader.find_first_of(separater_columns);
    if (separater_pos == string::npos){
        return -1;
    }
    columnsNameInfo = parserHeader.substr(0,separater_pos);
    
    // ignore the columnType 
    parserHeader = parserHeader.substr(separater_pos+1,parserHeader.size()-separater_pos-1);
    separater_pos = parserHeader.find_first_of(separater_columns);
    
    // get columnDBTypes : ct1,ct2,ct3
    parserHeader = parserHeader.substr(separater_pos+1,parserHeader.size()-separater_pos-1);
    separater_pos = parserHeader.find_first_of(separater_columns);
    if (separater_pos == string::npos)
    {
        return -1;
    }
    columnsDBType = parserHeader.substr(0,separater_pos);
    
    // get columns number 
    const char *p = columnsNameInfo.c_str();
    while (*p++){
        if (*p == separater_items){
            column_num++;    // get column num
        }
    }
    column_num++;            // tail
    
    // generate the column objects vector
    for (int i=0;i<column_num;i++){
        _ColumnInfoPtr obj = NULL;
        obj = new _ColumnInfo();
        if(NULL != obj){
           vec.push_back(obj);
        }
    }
    
    // parser the column name
    separater_pos = columnsNameInfo.find_first_of(separater_items);
    colIndex = 0;
    while (separater_pos != string::npos)
    { 
        columnItem = columnsNameInfo.substr(0,separater_pos);
        columnsNameInfo = columnsNameInfo.substr(separater_pos+1,columnsNameInfo.size()-separater_pos-1);
        separater_pos = columnsNameInfo.find_first_of(separater_items);
        
        // copy column name
        strcpy(vec[colIndex]->columnName,columnItem.c_str());
        colIndex++;
    }
    strcpy(vec[colIndex]->columnName,columnsNameInfo.c_str());   // add tail
    
    // parser the column type
    separater_pos = columnsDBType.find_first_of(separater_items);
    colIndex = 0;
    while (separater_pos != string::npos)
    { 
        columnItem = columnsDBType.substr(0,separater_pos);
        columnsDBType = columnsDBType.substr(separater_pos+1,columnsDBType.size()-separater_pos-1);
        separater_pos = columnsDBType.find_first_of(separater_items);
        
        // set column type
        if (strcmp(columnItem.c_str(),"STRING") == 0)
        {
            vec[colIndex]->columnDBType = vec[colIndex]->columnType = _STRING;
        }
        else if (strcmp(columnItem.c_str(),"INT") == 0)
        {
            vec[colIndex]->columnDBType = vec[colIndex]->columnType = _INT;
        }
        else if (strcmp(columnItem.c_str(),"LONG") == 0)
        {
            vec[colIndex]->columnDBType = vec[colIndex]->columnType = _LONG;
        }
        else if (strcmp(columnItem.c_str(),"FLOAT") == 0)
        {
            vec[colIndex]->columnDBType = vec[colIndex]->columnType = _FLOAT;
        }
        else if (strcmp(columnItem.c_str(),"DOUBLE") == 0)
        {
            vec[colIndex]->columnDBType = vec[colIndex]->columnType = _DOUBLE;			
        }
        else if (strcmp(columnItem.c_str(),"BYTE") == 0)
        {
            vec[colIndex]->columnDBType = vec[colIndex]->columnType = _BYTE;
        }
        else
        {
            printf("column type error \n");
            return -1;
        }
        colIndex++;
    }
    if(true)
    {   // add tail 
        // set column type
        if (strcmp(columnsDBType.c_str(),"STRING") == 0)
        {
            vec[colIndex]->columnDBType = vec[colIndex]->columnType = _STRING;
        }
        else if (strcmp(columnsDBType.c_str(),"INT") == 0)
        {
            vec[colIndex]->columnDBType = vec[colIndex]->columnType = _INT;
        }
        else if (strcmp(columnsDBType.c_str(),"LONG") == 0)
        {
            vec[colIndex]->columnDBType = vec[colIndex]->columnType = _LONG;
        }
        else if (strcmp(columnsDBType.c_str(),"FLOAT") == 0)
        {
            vec[colIndex]->columnDBType = vec[colIndex]->columnType = _FLOAT;
        }
        else if (strcmp(columnsDBType.c_str(),"DOUBLE") == 0)
        {
            vec[colIndex]->columnDBType = vec[colIndex]->columnType = _DOUBLE;			
        }
        else if (strcmp(columnsDBType.c_str(),"BYTE") == 0)
        {
            vec[colIndex]->columnDBType = vec[colIndex]->columnType = _BYTE;
        }
        else
        {
            printf("column type error \n");
            return -1;
        }
    }
    
    return 0;
}

/*
     func: GetFileSumaryInfo(FILE* pFile,_BlockSumary& blkSumaryInfo)
	 desc: ��ȡ�ļ��ĸ�Ҫ��Ϣ
	 param:
           pFile[in]: ���ݿ��ļ��򿪾��
           blkSumaryInfo[in/out]:�ļ���Ҫ��Ϣ�ṹ(����������������ܼ�¼��������ʼλ��)

     note:
	 �����ݿ�λ�� (Long)[] ��һ�����ݿ�λ�� filesize - 16 -([y]*8) ,�ڶ������ݿ�λ�� filesize - 16 -([y-1]*8) �Դ�����
	 �������� ��Long�� 8�ֽ�
	 ���ݿ����� ��int�� 4�ֽ�
	 ���ݿ���� ��int�� 4�ֽ� [y]
*/
int  GetFileSumaryInfo(FILE* pFile,_BlockSumary& blkSumaryInfo)
{
    fseek(pFile,0,SEEK_END); // seek end
    blkSumaryInfo.filesize = ftell(pFile);  // get file size

	//-- file block number
	int Pos = 0;
	Pos -= sizeof(blkSumaryInfo.blockNum);
	fseek(pFile,Pos,SEEK_END);
	if (1 != fread(&blkSumaryInfo.blockNum,sizeof(blkSumaryInfo.blockNum),1,pFile)){
        lgprintf("read block number error");
        return -1;
	}
	_revInt(&blkSumaryInfo.blockNum);
    
	//-- file row number
	Pos -= sizeof(blkSumaryInfo.rowNum);
	fseek(pFile,Pos,SEEK_END); 
	if (1 != fread(&blkSumaryInfo.rowNum,sizeof(blkSumaryInfo.rowNum),1,pFile)){
		lgprintf("read row number error");
		return -1;
	}
    _revInt(&blkSumaryInfo.rowNum);
	
	//-- file rows sum
	Pos -= sizeof(blkSumaryInfo.rowSum);
    fseek(pFile,Pos,SEEK_END);
	if (1 != fread(&blkSumaryInfo.rowSum,sizeof(blkSumaryInfo.rowSum),1,pFile)){
		lgprintf("read row sum error");
		return -1;
	}
    _revDouble(&blkSumaryInfo.rowSum);

	//-- file block start possition vector
	//-- �����ȡ���ݣ�ÿһ����8�ֽڣ�˳���ȡ����
	unsigned long firstBlockLocate = CalculateBlockStartPos(blkSumaryInfo.filesize,blkSumaryInfo.blockNum,0);
    fseek(pFile,firstBlockLocate,SEEK_SET);
    for (int i=0;i<blkSumaryInfo.blockNum;i++){
        unsigned long blockStartPos = 0;
        if(1 != fread(&blockStartPos,sizeof(long),1,pFile))
        {
        	lgprintf("read block start position error .");
            break;	
        }
        _revDouble(&blockStartPos);
		blkSumaryInfo.BlkStartPosVec.push_back(blockStartPos);
    }   
	
	return 0;
}

/*
     func: GetFileSumaryInfo(FILE* pFile,_BlockSumary& blkSumaryInfo)
	 desc: ��ȡ�ļ��ĸ�Ҫ��Ϣ
	 param:
           pFile[in]: ���ݿ��ļ��򿪾��
           blkSumaryInfo[in/out]:�ļ���Ҫ��Ϣ�ṹ(����������������ܼ�¼��������ʼλ��)

     note:
	 �����ݿ�λ�� (Long)[] ��һ�����ݿ�λ�� filesize - 16 -([y]*8) ,�ڶ������ݿ�λ�� filesize - 16 -([y-1]*8) �Դ�����
	 �������� ��Long�� 8�ֽ�
	 ���ݿ����� ��int�� 4�ֽ�
	 ���ݿ���� ��int�� 4�ֽ� [y]
*/
int WriteFileSumaryInfo(FILE* pFile,const _BlockSumary blkSumaryInfo)
{
	//-- file block start possition vector
	if(blkSumaryInfo.blockNum != blkSumaryInfo.BlkStartPosVec.size())
	{
	    lgprintf("block number check error.");
        return -1;
	}
	
	//-- д��ÿһ�������ʼλ��
    unsigned long blockStartPos = 0;
    for (int i=0;i<blkSumaryInfo.blockNum;i++){
		blockStartPos = blkSumaryInfo.BlkStartPosVec[i];
        _revDouble(&blockStartPos);
        if(1 != fwrite(&blockStartPos,sizeof(long),1,pFile))
        {
        	lgprintf("write block start position error .");
            break;	
        }
    }   

	//-- file rows sum
	long _rowSum = blkSumaryInfo.rowSum;
	_revDouble(&_rowSum);
	if (1 != fwrite(&_rowSum,sizeof(_rowSum),1,pFile)){
		lgprintf("write row sum error");
		return -1;
	}

	//-- file row number
	int _rowNum = blkSumaryInfo.rowNum;
	_revInt(&_rowNum);
	if (1 != fwrite(&_rowNum,sizeof(_rowNum),1,pFile)){
		lgprintf("write row number error");
		return -1;
	}

    //-- file block number
    int _blockNum = blkSumaryInfo.blockNum;
	_revInt(&_blockNum);
	if (1 != fwrite(&_blockNum,sizeof(_blockNum),1,pFile)){
        lgprintf("write block number error");
        return -1;
	}	
	return 0;
}


/*
    func: GetBlockData(FILE* pFile,long blockStartPos,char* pcompressed_buff,const int compressed_len,char* porigin_buff,int& origin_len)
    desc: Get block data from file and decompress data to origin data
    param:
         pFile[in/out] : file handle
		 blockStartPos[in]: start pos
         pcompressed_buff[in/out]: compressed buffer
		 compressed_len[in/out]: compressed buffer len
		 porigin_buff[in/out]: origin buffer
         origin_len[in/out]: origin len
     return 0:success -1:error

	 ���ݿ��С ��int�� 4�ֽ�,���ݰ���С
	 ������ʼλ�ã�Long�� 8�ֽ�
	 ���ݽ���λ�� ��Long�� 8�ֽ�
	 ����MD5(String) 32�ֽ�
	 �������ݣ�ZIPѹ����
*/
int GetBlockData(FILE* pFile,long blockStartPos,char* pcompressed_buff,long& compressed_len,char* porigin_buff,long& origin_len)
{ 
     fseek(pFile,blockStartPos,SEEK_SET);
	 
     _BlockInfo blkInfo;
	 //-- origin block data size
	 if (1 != fread(&blkInfo.blocksize,sizeof(blkInfo.blocksize),1,pFile)){
         lgprintf("Read block info error,read origin data size error.");
         return -1;
	 }
    _revInt(&blkInfo.blocksize);
     
	 //-- read compressed start pos ,end pos,and get compressed size
	 if (1 != fread(&blkInfo.startPos,sizeof(blkInfo.startPos),1,pFile)){
		 lgprintf("Read block info error,read start pos error.");
		 return -1;
	 }
	 if (1 != fread(&(blkInfo.endPos),sizeof(blkInfo.endPos),1,pFile)){
		 lgprintf("Read block info error,read end pos error.");
		 return -1;
	 }
	 _revDouble(&blkInfo.startPos);  // ��Ч
	 _revDouble(&blkInfo.endPos);    // ��Ч

	 //-- read origin data md5sum 
     if(1 != fread(blkInfo.md5sum,conMd5StrLen,1,pFile)){
		 lgprintf("Read block info error,read md5sum error.");
		 return -1;
	 }
     
     // ѹ�������ݴ�С
	 blkInfo.compressed_data_size = blkInfo.blocksize - ( conMd5StrLen + sizeof(blkInfo.startPos) + sizeof(blkInfo.startPos));
	 
	 if(blkInfo.compressed_data_size > compressed_len - 100)
	 {
	     lgprintf("Compressed data len error.");
	     return -1;	
	 }
	 //-- read compressed data
     if (1 != fread(pcompressed_buff,blkInfo.compressed_data_size,1,pFile)){
		 lgprintf("Read block info error,read compressed buff error.");
		 return -1;
     }
	 compressed_len = blkInfo.compressed_data_size;

	 //-- get origin data
	 if(0 != UnzipDataBlock(pcompressed_buff,compressed_len,porigin_buff,origin_len)){
		 lgprintf("Read block info error,unzip data block error.");
		 return -1;
	 }
	 
	 // check data md5sum
	 
     unsigned char md[conMd5Len] = {0}; // 16 bytes     ,���ܶ����char,��������sprintf������md5array�д�������
	 // get md5 hush code from data data source
	 MD5((unsigned char*)porigin_buff,origin_len,md);
	 char md5array[conMd5StrLen];
	 memset(md5array,0,conMd5StrLen);
     for(int i=0;i<conMd5Len;i++)
     {
         sprintf(md5array+strlen(md5array),"%02x",md[i]);
     }
     
     if(strncmp(md5array,blkInfo.md5sum,conMd5StrLen) != 0)
     {
         lgprintf("data md5 check error.");
         return -1;	
     }
     
     return 0;
}

/*
    func: GetBlockData(FILE* pFile,int blockStartPos,char* pcompressed_buff,const int compressed_len,char* porigin_buff,int& origin_len)
    desc: Get block data from file and decompress data to origin data
    param:
         pFile[in/out] : file handle
		 blockStartPos[in]: start pos
         pcompressed_buff[in/out]: compressed buffer
		 compressed_len[in/out]: compressed buffer len
		 porigin_buff[in]: origin buffer
         origin_len[in]: origin len
     return 0:success -1:error

   	 ���ݿ��С ��int�� 4�ֽ�,���ݰ���С
	 ������ʼλ�ã�Long�� 8�ֽ�
	 ���ݽ���λ�� ��Long�� 8�ֽ�
	 ����MD5(String) 32�ֽ�
	 �������ݣ�ZIPѹ����
*/
int WriteBlockData(FILE* pFile,long blockStartPos,char* pcompressed_buff,long& compressed_len,const char* porigin_buff,const long origin_len)
{
    fseek(pFile,blockStartPos,SEEK_SET);
    
    //-- ѹ������
    if(0 != ZipDataBlock(porigin_buff,origin_len,pcompressed_buff,compressed_len)){
        return -1;
    }
    
    _BlockInfo blkInfo;
    //-- write block data size
    blkInfo.blocksize = compressed_len + ( conMd5StrLen + sizeof(blkInfo.startPos) + sizeof(blkInfo.startPos));
    _revInt(&blkInfo.blocksize);
    if (1 != fwrite(&blkInfo.blocksize,sizeof(blkInfo.blocksize),1,pFile)){
        lgprintf("Write block info error,write origin data size error.");
        return -1;
    }
    _revInt(&blkInfo.blocksize);
    
    //-- write compressed start pos ,end pos,and get compressed size,2��ֵ������Ч��
    _revDouble(&blkInfo.startPos);
    if (1 != fwrite(&blkInfo.startPos,sizeof(blkInfo.startPos),1,pFile)){
        lgprintf("Write block info error,read start pos error.");
        return -1;
    }
    _revDouble(&blkInfo.endPos);
    if (1 != fwrite(&(blkInfo.endPos),sizeof(blkInfo.endPos),1,pFile)){
        lgprintf("Write block info error,read end pos error.");
        return -1;
    }
    
    //-- write origin data md5sum 
    unsigned char md[conMd5Len] = {0}; // 16 bytes     ,���ܶ����char,��������sprintf������md5array�д�������
    // get md5 hush code from data data source
    MD5((unsigned char*)porigin_buff,origin_len,md);
    for(int i=0;i<conMd5Len;i++)
    {
        sprintf(blkInfo.md5sum+strlen(blkInfo.md5sum),"%02x",md[i]);
    }
    if(1 != fwrite(blkInfo.md5sum,conMd5StrLen,1,pFile)){
        lgprintf("Write block info error,write md5sum error.");
        return -1;
    }
    
    // ѹ�������ݴ�С
    blkInfo.compressed_data_size = blkInfo.blocksize - ( conMd5StrLen + sizeof(blkInfo.startPos) + sizeof(blkInfo.startPos));
    compressed_len = blkInfo.compressed_data_size;
    
    //-- write compressed data
    if (1 != fwrite(pcompressed_buff,blkInfo.compressed_data_size,1,pFile)){
        lgprintf("Write block info error,Write compressed buff error.");
        return -1;
    }
    
    return 0;   
}



/*
    func: ParserOriginData(const char* porigin_buff,const int origin_len,AutoMt* mt,_ColumnInfoPtrVector& colInfoVec)
    desc: Parser origin data into AutoMt
    param:
		 porigin_buff[in]: origin buffer
         origin_len[in]: origin len
		 mt:AutoMt
		 colInfoVec:columnInfo
     return: parse rows,-1 == error 
            
	 ÿһ�����ݽṹ ��  
	 [len][null_info][data]
	 [---------len--------]
	 �����ݴ�С ��int�� 4�ֽ� 
	 �п�ֵ��� ��byte[columnName.length]�� 1 == ��ֵ 0== �ǿ� 
	 ������ 011000 ������columnType ȡ����Ӧ�������ͣ��ṩȡֵ��
	 ���� �������������ͳ��ȶ�ȡ���ַ������ͣ�ǰ4�ֽ�Ϊ�ַ������ȣ�
	 
	 ��ע�⣺ʱ����ַ��������ַ�������ʽ�������ݣ���������ַ������͵�����
	 ��Ҫ�жϸ����Ƿ���ʱ�����ͣ����������ʱ�����ͣ���Ҫ�����ݽ���ת������
	 
*/
int ParserOriginData(const char* porigin_buff,const int origin_len,int memtab,_ColumnInfoPtrVector& colInfoVec)
{
    AutoMt mt(0,10);
    mt.SetHandle(memtab,true);
    int deal_rows = 0;
    // current row index in mt
    long mtrows = mt.GetRows();
    int null_array_num = colInfoVec.size();
    int line_len = 0;
    int origin_pos = 0;
    int retCode = 0;
    int colindex = 0;
    while(origin_pos < origin_len)
    {
        // line ����
        memcpy(&line_len,porigin_buff+origin_pos,sizeof(line_len));
        _revInt(&line_len);
        origin_pos += sizeof(line_len);
        
        // column ��ֵ���
        const NullPtr pnulls = (const NullPtr)porigin_buff+origin_pos;
        origin_pos += null_array_num;
        
        // һ�����ж�Ӧ��ֵ����Ϣ
        const LinePtr plineInfo = (const LinePtr)porigin_buff+origin_pos;
        origin_pos += (line_len - null_array_num - sizeof(line_len));   //�г���
        int line_pos = 0;
        
        int col_len = 0;
        int col_type = 0;
        // ����ֵ���
        for (colindex=0;colindex<null_array_num;colindex++)
        {                         
            // �ٴ��ж��Ƿ�Խ��
            if(origin_pos > origin_len)
            {
                lgprintf("ParserOriginData len error , origin_pos = %d,origin_len = %d,colindex = %d,deal_rows = %d,line_len = %d\n",
                        origin_pos,origin_len,colindex+1,deal_rows+1,line_len);  
                retCode = -11;
                break;
            }
            if (pnulls[colindex] == EMPTY_YES) // �����ǿ�ֵ
            {
                // �������κδ���
                continue;
            }
            else
            {
                // �ж��е����ͣ��������ͽ����������
                int strLen = 0;
                switch (colInfoVec[colindex]->columnDBType)
                {
                case _INT:
                    _revInt(plineInfo+line_pos);
                    memcpy(mt.PtrInt(colindex,mtrows),plineInfo+line_pos,sizeof(int));
                    line_pos += sizeof(int);
                    break;
                case _LONG:
                    _revDouble(plineInfo+line_pos);
                    memcpy(mt.PtrLong(colindex,mtrows),plineInfo+line_pos,sizeof(long));					
                    line_pos += sizeof(long);
                    break;
                case _FLOAT:
                    _revInt(plineInfo+line_pos);
                    memcpy(mt.PtrDouble(colindex,mtrows),plineInfo+line_pos,sizeof(float));
                    line_pos += sizeof(float);
                    break;
                case _DOUBLE:
                    _revDouble(plineInfo+line_pos);
                    memcpy(mt.PtrDouble(colindex,mtrows),plineInfo+line_pos,sizeof(double));
                    line_pos += sizeof(double);
                    break;
                case _STRING:
                    _revInt(plineInfo+line_pos);
                    memcpy(&strLen,plineInfo+line_pos,sizeof(int));
                    line_pos += sizeof(int);
                    
                    //--------------------------------------------------------------------------------
                    col_len = mt.getColLen(colindex);
                    col_type = mt.getColType(colindex);
                    
                    if(col_type == COLUMN_TYPE_DATE)//ʱ�����ʹ���
                    {
                        // ʱ�������ַ�����Ӧ�ô���19
                        if(strLen > 19){
                            lgprintf("ParserOriginData error col[%d] len[%d], data len[%d] �����������ݸ�ʽ����.",colindex,col_len,strLen);
                            retCode = -12; 	
                            break;
                        }
                        int of=0,y,m,d,hh,mm,ss;
                        char *dptr=(char*)plineInfo+line_pos;
                        while(*dptr==' ') dptr++;
                        if(*dptr<'0' || *dptr>'9') {
                        	lgprintf("ParserOriginData error col[%d] len[%d], data len[%d] �����������ݸ�ʽ����.",colindex,col_len,strLen);
                        	retCode = -12;
                            break;
                        }
                        if(dptr[4]>='0' && dptr[4]<='9') {
                        	//û�зָ���,���磺2012-11-12 12:12:12,2012/12/12 12/12/12
                        	char r=dptr[4];
                        	dptr[4]=0;
                        	y=atoi(dptr);dptr+=4;
                        	*dptr=r;r=dptr[2];dptr[2]=0;
                        	m=atoi(dptr);dptr+=2;
                        	*dptr=r;r=dptr[2];dptr[2]=0;
                        	d=atoi(dptr);dptr+=2;
                        	*dptr=r;r=dptr[2];dptr[2]=0;
                        	hh=atoi(dptr);dptr+=2;
                        	*dptr=r;r=dptr[2];dptr[2]=0;
                        	mm=atoi(dptr);dptr+=2;
                        	*dptr=r;r=dptr[2];dptr[2]=0;
                        	ss=atoi(dptr);dptr+=2;
                        }
                        else {
                        	// ���ڷָ��������磺2012-11-12 12:12:12,2012/12/12 12/12/12
                        	y=atoi(dptr+of);of+=5;
                        	m=atoi(dptr+of);of+=(dptr[of+1]>='0' && dptr[of+1]<='9')?3:2;
                        	d=atoi(dptr+of);of+=(dptr[of+1]>='0' && dptr[of+1]<='9')?3:2;
                        	hh=atoi(dptr+of);of+=(dptr[of+1]>='0' && dptr[of+1]<='9')?3:2;
                        	mm=atoi(dptr+of);of+=(dptr[of+1]>='0' && dptr[of+1]<='9')?3:2;
                        	ss=atoi(dptr+of);
                        }
                        if(y<1900 || m<1 || m>12 || d<1 || d>31 ||hh<0 || hh>60 || mm<0 || mm>60 ||ss<0 || ss>60) {
                        	lgprintf("ParserOriginData error col[%d] len[%d], data len[%d] ������������[%04d-%02d-%02d %02d-%02d-%02d]��ʽ����.",
                        	     colindex,col_len,strLen,y,m,d,hh,mm,ss); 
                            retCode = -14;
                            break;
                        }
                        wociSetDateTime(mt.PtrDate(colindex,mtrows),y,m,d,hh,mm,ss);                    	
                    }
                    else // �ַ�������
                    {
                        if( col_len < strLen){
                            lgprintf("ParserOriginData error col[%d] len[%d], data len[%d] �ַ������ͳ��ȸ�ʽ����.",colindex,col_len,strLen);
                    	    retCode = -15;
                        }                                          	
                        strncpy(mt.PtrStr(colindex,mtrows),(char*)plineInfo+line_pos,strLen);
                        mt.PtrStr(colindex,mtrows)[strLen] = 0;	
                    }           
                    //------------------------------------------------------------------------------------                                     
                    // ʱ���������ַ��������ģ����ܳ�����19������:2012-12-12 12:12:12
                    line_pos += strLen;
                    break;
                default:
                    lgprintf("column type error , column index %d ",colindex); 	
                    retCode = -16;
                }// end switch  
               
                if(0!=retCode){
                   break; 
                }                  
            } // end else(EMPTY_NO)
        }// end for (int colindex=0;colindex<null_array_num;colindex++)
                
        // ���ݳ��Ȳ���׷��
        if(line_pos != (line_len - null_array_num - sizeof(line_len)))
        {
           lgprintf("ParserOriginData len error , origin_pos = %d,origin_len = %d,colindex = %d,deal_rows = %d,line_len = %d,line_pos = %d.\n",
                        origin_pos,origin_len,colindex+1,deal_rows+1,line_len,line_pos);  
           retCode = -17;
        }
        
        if(retCode != 0){
            break;	
        }
        
        deal_rows++; 
        mtrows++;      
    }// end while(origin_pos < origin_len)
    
    if(retCode == 0){ // ������ȷ
       _wdbiSetMTRows(mt,mtrows);
    }
    else
    {
       // ������������Ϊ����
       for (colindex=0;colindex<null_array_num;colindex++){
          wociSetNull(mt,colindex,mtrows,true);
       }
       mtrows -= 1;  // ����һ��
       _wdbiSetMTRows(mt,mtrows);
       #if 0
       return retCode;
       #else 
       return deal_rows;
       #endif
    }
    return deal_rows;
}

/*
     func:DirectWriteMt2Table(const char* dbname,const char* tbname,int memtb)
     desc:ͨ���ļ��ļ���ʽ��ֱ�ӽ�mtд�����ݿ��ļ�(����insert����)
     param:
          dbname[in]:���ݿ�����
          tbname[in]:������
          memtb[in]:memtable�ṹ���ڲ��洢���Ѿ����õ�����
          pConnetor[in]:����mysql���ݿ⣬ˢ�±��ʱ��ʹ��
          syspath[���ݿ��Ŀ¼]
          lastmt[in]:�Ƿ������һ��memtable,true:���һ��mt,��ʱ����Ҫѹ���ļ�
     return:
          deal rows: successed 
          -1: failed 
*/
int DirectWriteMt2Table(const char* dbname,const char* tbname,int memtb,MySQLConn* pConnetor,const char* sysPath,bool lastmt)
{
    char fn_myd[300];    // �����ļ�
    char fn_tmd[300];    // ��ʱ�ļ�
    char fn_myi[300];    // �����ļ�
    const char *pathval=sysPath;
    int  mt_rows = 0;
    // ��ȡ���ݿ���ļ�����,����:/app/dma/var/dp/test_01.MYI
    sprintf(fn_myi,"%s/%s/%s.MYI",pathval,dbname,tbname);   // ��������
    
    // 1. ��MYI�ļ�
    FILE *fp_myi =NULL;
    fp_myi=fopen(fn_myi,"rb");
    if(fp_myi==NULL){
        lgprintf("�������ݿ�%s.%s,���ļ�%sʧ��.",dbname,tbname,fn_myi);
        return -1;	 
    }
    
    // 2. ��ȡ�ļ���MYI�ļ�¼��
    fseek(fp_myi,28,SEEK_SET);
    LONG64 file_row_num = 0;
    if(1 != fread(&file_row_num,8,1,fp_myi)){
        fclose(fp_myi);fp_myi = NULL;
        lgprintf("�������ݿ�%s.%s,��ȡ�ļ�%sʧ��.",dbname,tbname,fn_myi);
        return -1;	 
    }
    _revDouble(&file_row_num);
    
    fclose(fp_myi);
    fp_myi = NULL;
    
    // ��ȡ���ݿ���ļ�����,����:/app/dma/var/dp/test_01.MYD
    sprintf(fn_myd,"%s/%s/%s.MYD",pathval,dbname,tbname);   // �����ļ�
    
    // 3. ��MYD�ļ�
    FILE *fp_myd =NULL;
    fp_myd=fopen(fn_myd,"ab");
    if(fp_myd==NULL){
        lgprintf("�������ݿ�%s.%s,���ļ�%sʧ��.",dbname,tbname,fn_myd);
        return -1;	 
    }
    
    // 4. д��MYD�ļ���������
    fseek(fp_myd,0,SEEK_END);
    wociCopyToMySQL(memtb,0,0,fp_myd);
    
    // ��¼�������
    mt_rows =  wociGetMemtableRows(memtb);
    file_row_num += mt_rows;
    
    // 5. ��ȡMYD�ļ���С���ر�MYD�ļ�
    LONG64 myd_file_size = 0;
    fseek(fp_myd,0,SEEK_END);
    myd_file_size = ftell(fp_myd);	
    fclose(fp_myd);fp_myd = NULL;
    
    // 6. ����¼����,MYD�ļ���Сд��MYI�ļ���
    fp_myi=fopen(fn_myi,/*"wb"*/ "r+b");
    if(fp_myi==NULL){
        lgprintf("�������ݿ�%s.%s,���ļ�%sʧ��.",dbname,tbname,fn_myi);
        return -1;	 
    }			
    
    //7. д���¼����	  
    fseek(fp_myi,28,SEEK_SET);
    _revDouble(&file_row_num);
    dp_fwrite(&file_row_num,1,8,fp_myi);
    
    // reset deleted records count.
    char tmp[20];
    memset(tmp,0,20);
    dp_fwrite(tmp,1,8,fp_myi);
    
    // д���ļ���С
    fseek(fp_myi,68,SEEK_SET);	
    _revDouble(&myd_file_size);
    dp_fwrite(&myd_file_size,1,8,fp_myi);
    
    // �ر��ļ�
    fseek(fp_myi,0,SEEK_END);
    fclose(fp_myi); 
    fp_myi = NULL;
    
    // 8. FlushTable(ˢ�±�)
    char _tb[300];
    sprintf(_tb,"%s.%s",dbname,tbname);
    lgprintf("д���%s,%d�м�¼...",_tb,mt_rows);
    // pConnetor->FlushTables(_tb);
    
    _revDouble(&myd_file_size);
    
    // 9 ѹ������,���һ��mt������ɺ�
    char cmdline[500];
    int rt;
    if(lastmt && myd_file_size>1024*1024)
    {
        // 9.0 ɾ����ʱ�ļ�(��һ��ִ��ʧ�ܵ�)
        sprintf(fn_tmd,"%s/%s/%s.TMD",pathval,dbname,tbname);   // ��������
        unlink(fn_tmd);
        
        // 9.1 myisampack ѹ����		
        lgprintf("ѹ�����:%s....",_tb);
        printf("pack:%s\n",fn_myi);
        sprintf(cmdline,"myisampack -v %s",fn_myi);
        rt=system(cmdline) ;
        lgprintf("���ݿ��%s.%s���ļ�%sѹ��%s.",dbname,tbname,fn_myi,(rt != 0 ? "ʧ��":"�ɹ�"));
        if(rt){
            lgprintf("myisampack ִ��ʧ��:cmd = %s ",cmdline);
            return -1;
        }
    }
    if(lastmt)
    {
        // 9.2 myisamchk MYI ��
        sprintf(cmdline,"myisamchk -rqvn --tmpdir=\"%s\" %s ",pathval,fn_myi);
        printf(cmdline);
        rt=system(cmdline);
        if(rt){
            lgprintf("myisamchk ִ��ʧ��:cmd = %s ",cmdline);
            return -1;
        }		
		lgprintf("\n");
        pConnetor->FlushTables(_tb);
    }
    
    return 0;
}


/*
    func:ZipDataBlock(const char* origin_data,const long origin_len,char * compressed_data,long& compressed_len);l
    desc:compressed the zip data
    param:
        compressed_data[in/out]: compressed data
        compressed_len[in/out]: compressed data len
        origin_data[in]: origin data buff
        origin_len[in]: orgint data len
    return success 0,failed -1
	note : use compress2 ,zip algorithm, throw the zlib library 
*/
int ZipDataBlock(const char* origin_data,const long origin_len,char * compressed_data,long& compressed_len)
{
    int rt = 0;
    long _compressed_len = compressed_len;
    rt=compress2((Bytef *)compressed_data,(uLongf*)&compressed_len,(Bytef *)origin_data,origin_len,1);
    if(rt!=Z_OK) 
    {
        lgprintf("Compress failed,return code:%d\n",rt);
        return -1;
    }
    if(compressed_len >= _compressed_len){
        lgprintf("Compress failed,data len error[buffer len %d != compressed len %d].\n",_compressed_len ,compressed_len);
        return -1;  
    }
    return 0;
}


/*
    func: DecompressDataBlock(const char* compressed_data,const int compressed_len,char * origin_data,int& origin_len);
    desc: Decompress the zip data to origin data
    param:
        compressed_data[in]: compressed data
        compressed_len[in]: compressed data len
        origin_data[in/out]: origin data buff
        origin_len[in/out]: orgint data len
     return success 0,failed -1
	 note : use uncompress ,zip algorithm, throw the zlib library
*/
int  UnzipDataBlock(const char* compressed_data,const long compressed_len,char * origin_data,long& origin_len)
{
    int rt = 0;
    rt=uncompress((Bytef *)origin_data,(uLongf*)&origin_len,(Bytef *)compressed_data,compressed_len);
    if(rt!=Z_OK) 
    {
        lgprintf("Decompress failed,return code:%d\n",rt);
        return -1;
    }
    return 0;
}

