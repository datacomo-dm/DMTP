/* gprs_parser.h --

   This file is part of the DataMerger data extrator.

   Copyright (C) 2010 DataComo
   All Rights Reserved.
   
   Wang Guosheng
   <wangguosheng@datacomo.com>
   http://www.datacomo.com
   http://mobi5.cn
 */

#ifndef COMMONTXT_PARSER_H
#define COMMONTXT_PARSER_H
#include "dumpfile.h"

class ZteTxtParse: public FileParser {
	char *fieldval ;
	char *linebak;
	int yyyy,dd,mm,hour;
	int ExtractNum(int offset,const char *info,int len) ;
	bool SwitchOpt(char *deststr,char *opt,int val,int len);
	bool SwitchOpt(char *deststr,char *opt,char *value) ;
	//���طֽ����ֶ�����,colidx����ֶ�������collen��Ź̶�����
	int parserfixlen(int memtab,const char *cols,int *fixflag,int *collen);
	int parsercolumn(int memtab,const char *cols,int *colsflag);
	
	// add by liujs
	// ��ʵ���п�ȣ�����Ҫת�����У����ݳ��������ݿ���һ������Ҫת�����У����ݳ����ļ���ָ����
	int realColLenAry[MAX_COLUMNS];
	// �ļ�����Ҫת�����еĳ��ȣ����磺col1:1,col2:4,col3:5;�ڴ�洢1,4,5
	int fileColLenAry[MAX_COLUMNS];
	//ÿ�ζ�ȡ�����ֽ�����
	int fileRowLen;
	// ��ȡ�ַ��������ȣ�add by liujs ���磺 SSS:12,BBB:23,DDD:24
	// �õ���SSS,BBB,DDD   �� 12��23��24
    int GetColumnInfo(const char* pColumnWidthInfo,int memtab,/*char* pColumnsName,*/int * columnsId);
public :
	ZteTxtParse():FileParser(){
		linebak =new char[MAXLINELEN];
		fieldval =new char[MAXLINELEN];
		
		memset(realColLenAry,0x0,MAX_COLUMNS*sizeof(int));
		memset(fileColLenAry,0x0,MAX_COLUMNS*sizeof(int));
		fileRowLen = 0;
		
		yyyy = dd = mm = hour = -1;
	}
	~ZteTxtParse() {
		delete []linebak;delete []fieldval;
	}
	// fill filename member,the wrapper check conflict concurrent process,
	// if one has privillege to process current file ,then invoke DoParse
	// ����ֵ��
	//  -1 ������������������״̬�Ѿ��������������޸�Ϊȡ��������ͣ
	//  0�������ļ�
	//  1��ȡ����Ч���ļ�
	virtual int GetFile(bool frombackup,SysAdmin &sp,int tabid,int datapartid,bool checkonly=false) ;
	// if end ,update task to next step(with lock)
	// critical function !
	// checked every file parser completed.
	// �����ǲ������ļ���Ҫ�����ʱ�򣬲��ܵ��������麯��
	virtual bool ExtractEnd() ;
	virtual int WriteErrorData(const char *line) ;
	virtual int GetFileSeq(const char *filename) ;
	
	// return value:
	//  -1: a error occurs.�����ڴ���ֶνṹ��ƥ��
	//  0:memory table has been filled full,and need to parse again
	//  1:file parse end
	//while parser file ended,this function record file process log and 
	// sequence of file in dp dictions tab.
	virtual int DoParse(int memtab,SysAdmin &sp,int tabid,int datapartid) ;
};

IFileParser *BuildParser(void);
/*
int tabid,int datapartid) ;
FileParser *BuildParser(void);
int tabid,int datapartid) ;
FileParser *BuildParser(void);
*/
#endif
