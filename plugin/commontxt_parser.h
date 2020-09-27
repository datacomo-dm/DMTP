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
	
class CommonTxtParser: public FileParser {
	char *fieldval ;
	char *linebak;
	int yyyy,dd,mm,hour;
	int ExtractNum(int offset,const char *info,int len) ;
	bool SwitchOpt(char *deststr,char *opt,int val,int len);
	bool SwitchOpt(char *deststr,char *opt,char *value) ;
	//���طֽ����ֶ�����,colidx����ֶ�������collen��Ź̶�����
	int parserfixlen(int memtab,const char *cols,int *fixflag,int *collen);
	int parsercolumn(int memtab,const char *cols,int *colsflag);
	//>> begin:fix DMA-604,��Ҫ�����п�ȵ���
    bool colsAdjustWidth[MAX_FILE_COLUMNS];  // �洢��ֵ��colspos���Ӧ
	//<< end:fix dma-604	
public :
	CommonTxtParser():FileParser(){
		linebak =new char[MAXLINELEN];
		fieldval =new char[MAXLINELEN];
		
		yyyy = dd = mm = hour = -1;
	}
	~CommonTxtParser() {
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
#endif
