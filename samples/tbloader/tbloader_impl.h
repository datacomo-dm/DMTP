#ifndef TBLOADER_IMPL_DEF_H
#define TBLOADER_IMPL_DEF_H
/********************************************************************
  file : tbloader_impl.h
  desc : define the tbloader internal function
  author: liujianshun,201304
********************************************************************/
#ifdef WIN32
#include <process.h>
#define getch getchar
#else
#include <unistd.h>
#include <malloc.h>
#include <pthread.h>
#endif
#include <openssl/md5.h>   // MD5
#include <stdlib.h>
#include <stdio.h>
#include "AutoHandle.h"
#include "dt_common.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "zlib.h"      // zip �㷨
#include "dt_svrlib.h" // SysAdmin *psa

#include <vector>
#include <string>
using namespace std;

// �ļ�ͷ����
#define _MAX_ROWS   100000+20              // 100 ����
const int conNameLen = 256;
const int conMd5Len = 16;
const int conMd5StrLen = 16*2;
const int conAppendMem5MB = 5*1024*1024;

typedef enum _ColumnType{_INT,_LONG,_FLOAT,_DOUBLE,_STRING,_BYTE,}_ColumnType;
typedef enum _EngineType{MyISAM=1,Brighthouse}_EngineType;   // ���ݿ���������

#ifndef BIGENDIAN        // ���С��
#define _revDouble(V)
#define _revLong _revDouble
#define _revInt(V)
#define _revShort(V)
#else
#define _revDouble(V)   { char def_temp[8];\
                          ((mbyte*) &def_temp)[0]=((mbyte*)(V))[7];\
                          ((mbyte*) &def_temp)[1]=((mbyte*)(V))[6];\
                          ((mbyte*) &def_temp)[2]=((mbyte*)(V))[5];\
                          ((mbyte*) &def_temp)[3]=((mbyte*)(V))[4];\
                          ((mbyte*) &def_temp)[4]=((mbyte*)(V))[3];\
                          ((mbyte*) &def_temp)[5]=((mbyte*)(V))[2];\
                          ((mbyte*) &def_temp)[6]=((mbyte*)(V))[1];\
                          ((mbyte*) &def_temp)[7]=((mbyte*)(V))[0];\
                          memcpy(V,def_temp,sizeof(double)); }

#define _revLong _revDouble

#define _revShort(V)   { short def_temp;\
                          ((mbyte*) &def_temp)[0]=((mbyte*)(V))[1];\
                          ((mbyte*) &def_temp)[1]=((mbyte*)(V))[0];\
                          *((short *)(V))=def_temp;}
 
#define _revInt(V)	{ int def_temp;\
                          ((mbyte*) &def_temp)[0]=((mbyte*)(V))[3];\
                          ((mbyte*) &def_temp)[1]=((mbyte*)(V))[2];\
                          ((mbyte*) &def_temp)[2]=((mbyte*)(V))[1];\
                          ((mbyte*) &def_temp)[3]=((mbyte*)(V))[0];\
			  *(int *)(V) = def_temp; }
#endif

#ifdef DEBUG
#define Dbg_printf printf
#else
#define Dbg_printf 
#endif

/*
dsn  ������Դ(data source name)
user : �������ݿ��û���
pwd  : �������ݿ�����
dbname : ���ݿ�����
tbname : ������
fn   ��Ҫ���е����������ļ� 
fn_ext: �����ļ�����
tbEngine:��������
*/
typedef struct _InitInfo
{
    char funcode[128];                // -load_file,-gen_file
    char dsn[128];
    char user[128];
    char pwd[128];
    char dbname[64];
    char tbname[64];
    char fn[256];
    char fn_ext[256];
	_EngineType tbEngine;
	_InitInfo(){
		tbEngine = MyISAM;
	}
}_InitInfo,*_InitInfoPtr;

typedef struct _DmInitInfo // dm ��������������Ϣ
{
	char _musrname[128];
	char _mpswd[128];
	char _mhost[128];
	char _serverip[20];
	int _port;
}_DmInitInfo,*_DmInitInfoPtr;


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
typedef struct _ColumnInfo
{
    char columnName[conNameLen];
    _ColumnType  columnDBType;
    _ColumnType  columnType;
    char columnCName[conNameLen];          // ��������������
    int  columnPrecision;                  // ���ȣ�����
    int  columnScale;                      // ��Χ������
    _ColumnInfo(){
		memset(columnName,0,conNameLen);
		columnDBType = _INT;
		columnType = _INT;
		memset(columnCName,0,conNameLen);
		columnPrecision = 0;
		columnScale = 0;
    }
}_ColumnInfo,*_ColumnInfoPtr;
typedef vector<_ColumnInfoPtr>            _ColumnInfoPtrVector;
typedef vector<_ColumnInfoPtr>::iterator  _ColumnInfoPtrIter; 
#define separater_columns  ';'            // �м�ָ���
#define separater_items    ','            // �ڵ��ָ���  
typedef char* HerderPtr;           // ����ͷ������


#define CLOSE_FILE(filehandle)\
do{\
if((filehandle)!=NULL){\
fclose((filehandle));\
(filehandle) = NULL;\
}\
}while(0);

/*
�������֣�
�����ݿ�λ�� (Long)[] ��һ�����ݿ�λ�� filesize - 16 -([y]*8) ,�ڶ������ݿ�λ�� filesize - 16 -([y-1]*8) �Դ�����
�������� ��Long�� 8�ֽ�
���ݿ����� ��int�� 4�ֽ�
���ݿ���� ��int�� 4�ֽ� [y]
*/
#define	 CalculateBlockStartPos(filesize,blkNum,blkIdx) ((filesize)-16-(((blkNum)-(blkIdx))*8))
typedef  struct _BlockSumary
{
    unsigned long filesize;         // �ļ��ֽ���
    int        blockNum;            // ��¼�����
    int        rowNum;              // ��¼������
    long       rowSum;              // �ܵļ�¼����
    vector<long>   BlkStartPosVec;  // �������ݿ����ʼλ��
    _BlockSumary(){
		filesize = 0;
		blockNum = 0;
		rowSum = 0;
		BlkStartPosVec.clear();
    }
}_BlockSumary,*_BlockSumaryPtr;

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
typedef struct _BlockInfo
{
    int   blocksize;               // ���ݿ��С���������ݿ��С
    long  startPos;
    long  endPos;
    char  md5sum[conMd5StrLen];           // md5sum of uncompression data
    int   compressed_data_size;           // endpos - startpos
    _BlockInfo(){
	    blocksize = 0;
		startPos = 0;
		endPos = 0;
		memset(md5sum,0,conMd5StrLen);
		compressed_data_size = 0;
    }
}_BlockInfo,*_BlockInfoPtr;
typedef unsigned char* LinePtr;           // ���ݲ���
typedef unsigned char* NullPtr;           // �п�ֵ���� 
enum{EMPTY_NO = 0x0,EMPTY_YES=0x1};       // 0:�ǿգ�1:��

/*
    func:ReleaseBuff 
    desc:release malloc locate memory
*/
void ReleaseBuff(void* p);


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
int LocateBuff(const int rowNum,const int rowLen,const int colnum/* ��*/,unsigned char** pbuff);

/*
    func: ParserColumnInfo(const char* header,_ColumnInfoPtrVector& colInfoVec)
    desc: ��������Ϣ
    param:
          header[in] : column header info 
          colInfoVec[in/out]:�洢���ݿ�����Ϣ�Ķ���
    return : success 0,error -1 
*/
int  ParserColumnInfo(const HerderPtr header,_ColumnInfoPtrVector& vec);


/*
    func: GetColumnHeader(FILE* pFile,char * pHeader)
    desc: ��ȡ�ļ���ͷ��
    param:
           pFile[in]: ���ݿ��ļ����
           pHeader[in/out]: �ļ�ͷ��
           headerlen[in]:��ȡ�ĳ���
    return: failed---> -1
            success----> header Len
*/
int  GetColumnHeader(FILE* pFile,HerderPtr pHeader,const int headerlen);
int  WriteColumnHeader(FILE* pFile,HerderPtr pHeader,const int headerlen);

/*
    func: GetColumnHeaderLen(FILE* pFile,char * pHeader)
    desc: ��ȡ�ļ���ͷ������
    param:
           pFile[in]: ���ݿ��ļ����
    return: failed---> -1
            success----> header Len
*/
int  GetColumnHeaderLen(FILE* pFile);
int  WriteColumnHeaderLen(FILE* pFile,int headerlen);

/*   
     func: GetColumnInfo2Vector(FILE* pFile,_ColumnInfoPtrVector& colInfoVec)
     desc: ��ȡ�е������Ϣ�����������
     param:
           pFile[in]: ���ݿ��ļ����
           colInfoVec[in/out]:�洢���ݿ�����Ϣ�Ķ���
     return: success 0,error -1
*/
int  GetColumnInfo2Vector(FILE* pFile,_ColumnInfoPtrVector& colInfoVec);


/*
     func: GetFileSumaryInfo(FILE* pFile,_BlockSumary& blkSumaryInfo)
	 desc: ��ȡ�ļ��ĸ�Ҫ��Ϣ
	 param:
           pFile[in]: ���ݿ��ļ����
           blkSumaryInfo[in/out]:�ļ���Ҫ��Ϣ�ṹ(����������������ܼ�¼��������ʼλ��)
     returns: 0 success,-1 error
*/
int GetFileSumaryInfo(FILE* pFile,_BlockSumary& blkSumaryInfo);

/*
     func: WriteFileSumaryInfo(FILE* pFile,const _BlockSumary blkSumaryInfo)
	 desc: д���ļ��ĸ�Ҫ��Ϣ
	 param:
           pFile[in]: ���ݿ��ļ����
           blkSumaryInfo[in]:�ļ���Ҫ��Ϣ�ṹ(����������������ܼ�¼��������ʼλ��)
     returns: 0 success,-1 error
*/
int WriteFileSumaryInfo(FILE* pFile,const _BlockSumary blkSumaryInfo);

/*
    func: GetBlockData(FILE* pFile,int blockStartPos,char* pcompressed_buff,const int compressed_len,char* porigin_buff,int& origin_len)
    desc: Get block data from file and decompress data to origin data
    param:
         pFile[in/out] : file handle
		 blockStartPos[in]: start pos
         pcompressed_buff[in/out]: compressed buffer
		 compressed_len[in/out]: compressed buffer len
		 porigin_buff[in/out]: origin buffer
         origin_len[in/out]: origin len
     return 0:success -1:error
*/
int GetBlockData(FILE* pFile,long blockStartPos,char* pcompressed_buff,long& compressed_len,char* porigin_buff,long& origin_len);


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
*/
int WriteBlockData(FILE* pFile,long blockStartPos,char* pcompressed_buff,long& compressed_len,const char* porigin_buff,const long origin_len);



/*
    func: ParserOriginData(const char* porigin_buff,const int origin_len,AutoMt* mt,_ColumnInfoPtrVector& colInfoVec)
    desc: Parser origin data into AutoMt
    param:
		 porigin_buff[in]: origin buffer
         origin_len[in]: origin len
		 mt:AutoMt
		 colInfoVec:columnInfo
     return 0:success -1:error
*/
int ParserOriginData(const char* porigin_buff,const int origin_len,int memtab,_ColumnInfoPtrVector& colInfoVec);

/*
    func: DecompressDataBlock(const char* compressed_data,const int compressed_len,char * origin_data,int& origin_len);
    desc: Decompress the gzip data to origin data
    param:
        compressed_data[in]: compressed data
        compressed_len[in]: compressed data len
        origin_data[in/out]: origin data buff
        origin_len[in/out]: orgint data len
     return success 0,failed -1
*/
int  UnzipDataBlock(const char* compressed_data,const long compressed_len,char * origin_data,long& origin_len);


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
int ZipDataBlock(const char* origin_data,const long origin_len,char * compressed_data,long& compressed_len);

/*
     func: ReleaseColInfo(_ColumnInfoPtrVector& colInfoVec)
     desc: �ͷŴ洢�������Ϣ���ж���
     param:
           colInfoVec[in/out]:�洢���ݿ�����Ϣ�Ķ���
*/
void ReleaseColInfo(_ColumnInfoPtrVector& colInfoVec);


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
int DirectWriteMt2Table(const char* dbname,const char* tbname,int memtb,MySQLConn* pConnetor,const char* sysPath,bool lastmt);

#endif
