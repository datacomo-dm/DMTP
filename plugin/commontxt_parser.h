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
	//返回分解后的字段数量,colidx存放字段索引，collen存放固定长度
	int parserfixlen(int memtab,const char *cols,int *fixflag,int *collen);
	int parsercolumn(int memtab,const char *cols,int *colsflag);
	//>> begin:fix DMA-604,需要调整列宽度的列
    bool colsAdjustWidth[MAX_FILE_COLUMNS];  // 存储的值与colspos相对应
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
	// 返回值：
	//  -1 ：发生错误，例如任务状态已经在其它进程中修改为取消或者暂停
	//  0：暂无文件
	//  1：取得有效的文件
	virtual int GetFile(bool frombackup,SysAdmin &sp,int tabid,int datapartid,bool checkonly=false) ;
	// if end ,update task to next step(with lock)
	// critical function !
	// checked every file parser completed.
	// 必须是不再有文件需要处理的时候，才能调用这个检查函数
	virtual bool ExtractEnd() ;
	virtual int WriteErrorData(const char *line) ;
	virtual int GetFileSeq(const char *filename) ;
	
	// return value:
	//  -1: a error occurs.例如内存表字段结构不匹配
	//  0:memory table has been filled full,and need to parse again
	//  1:file parse end
	//while parser file ended,this function record file process log and 
	// sequence of file in dp dictions tab.
	virtual int DoParse(int memtab,SysAdmin &sp,int tabid,int datapartid) ;
};
IFileParser *BuildParser(void);
#endif
