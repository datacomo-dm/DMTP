/* gprs_parser.h --

   This file is part of the DataMerger data extrator.

   Copyright (C) 2010 DataComo
   All Rights Reserved.
   
   Wang Guosheng
   <wangguosheng@datacomo.com>
   http://www.datacomo.com
   http://mobi5.cn
 */
#ifndef CMNET_PARSER_H
#define CMNET_PARSER_H
#include "dumpfile.h"

class ZJCmnetParser: public FileParser {

	char yyyy[20],dd[10],mm[10];
public :
	// fill filename member,the wrapper check conflict concurrent process,
	// if one has privillege to process current file ,then invoke DoParse
	// ����ֵ��
	//  -1 ������������������״̬�Ѿ��������������޸�Ϊȡ��������ͣ
	//  0�������ļ�
	//  1��ȡ����Ч���ļ�
	// virtual int GetFile(bool frombackup,SysAdmin &sp,int tabid,int datapartid) ;
	virtual int GetFile(bool frombackup,SysAdmin &sp,int tabid,int datapartid,bool checkonly=false);
	
	// if end ,update task to next step(with lock)
	// critical function !
	// checked every file parser completed.
	// �����ǲ������ļ���Ҫ�����ʱ�򣬲��ܵ��������麯��
	virtual bool ExtractEnd() ;
	
	virtual int GetFileSeq(const char *filename) ;
	virtual int WriteErrorData(const char *line){ThrowWith("ZJCmnetParser WriteErrorData Error");return -1;};
	
	// return value:
	//  -1: a error occurs.�����ڴ���ֶνṹ��ƥ��
	//  0:memory table has been filled full,and need to parse again
	//  1:file parse end
	//while parser file ended,this function record file process log and 
	// sequence of file in dp dictions tab.
	virtual int DoParse(int memtab,SysAdmin &sp,int tabid,int datapartid) ;
};
IFileParser *BuildParser(void);
#endif
