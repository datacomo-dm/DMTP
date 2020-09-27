
/* dumpfile.h --

This file is part of the DataMerger data extrator.

Copyright (C) 2010 DataComo
All Rights Reserved.

Wang Guosheng
<wangguosheng@datacomo.com>
http://www.datacomo.com
http://mobi5.cn
*/
#ifndef DUMPFILE_H
#define DUMPFILE_H
#include "dt_common.h"
#include <stdio.h>
#include <map>
#include <string>
#include <dlfcn.h>
#include "dt_lib.h"
#include "IDumpfile.h"
#include "dmncftp.h"
#define MAX_COLUMNS 500

class file_man
{
	FILE *flist;
	bool ftpmode;
	//duplicated process file
	bool ascmode;
	//忽略本地复制文件
	bool skippickup;
	//忽略文件备份
	bool skipbackup;
	char lines[300];
	//bool firstpart;
	//char *ptr;
	char LISTFILE[300];
	char bakuperrfile[300];
	char host[100],username[100],passwd[100],path[300],localpath[300];
	char localfile[300];
	// 是否删除原文件 ,add by liujs
	enum DELETE_FILE_FLAG
	{
      DELETE_NO = 0, // 不删除文件
      DELETE_YES = 1, // 删除文件
	};
	enum REPLASE_FTP_GET_PARAM // 替换1: 表示需要替换ftpget文件的参数%2F
	{
      REPLASE_NO = 0,  // 不替换
      REPLASE_YES = 1, // 替换
	};
	int delFileFlag;           // 删除文件标识
	//>> Begin: delete src file 20130131
	char current_file[300];    // 当前正在处理的文件名称，删除源文件使用，包括完整路径
	long current_filesize;     // 记录当前文件大小
    bool is_backup_dir;        // 是否是backup目录,如果是backup目录，则不删除源文件
	//<< end:
	
	FTPClient ftpclient;
public:
	file_man();
	~file_man() ;
	void SetLocalPath(const char *p);
	void SetAscMode(bool val) ;
	void SetSkipPickup(bool val) ;
	void SetSkipBackup(bool val) ;
	void FTPDisconnect();
	void list(const char *_host,const char *_username,const char *_passwd,const char *_path,const char *filepatt);
	bool listhasopen() {return flist!=NULL;}
	void clearlist() {if(flist) fclose(flist);flist=NULL;unlink(LISTFILE);}
	void listlocal(const char *_path,const char *filepatt);	
	const char *getlocalfile(const char *fn) ;
	//获取文件并做备份，如果backupfile==null,或者backupfile[0]=0,则不备份
	//  如果文件不是.gz,则备份时做gzip压缩
	bool getfile(char *fn,char *backupfile,bool removeori,SysAdmin &sp,int tabid,int datapartid);
	bool getnextfile(char *fn);
	// 设置删除标志
	inline void SetDeleteFileFlag(const int delFlag){delFileFlag = delFlag;};
	inline bool GetFtpMode(){return ftpmode;};
	inline void SetIsBackupDir(bool isBackup){ is_backup_dir = isBackup;};
	int  DeleteSrcFile();
	inline long GetCurFileSize(){return current_filesize;};


};

//plugin must implemtation subclass of FileParser 
// construction a subclass of FileParser to process a 
//  specified table,once table parse end,destruction object,and next
//  table build another parser
#define MAXLINELEN 40000

#define MAX_FILE_COLUMNS  600
#define BACKUP_LOGFILE "backup.log"
class FileParser : public IFileParser{
protected :
	int dbc;
	int filerows;
	int fileseq;
	std::string filename;
	// file must be able to hold larger than 4GB contents
	FILE *fp;
	file_man fm;
	int curoffset;//offset line
	int tabid;
	int datapartid;
	int colspos[MAX_FILE_COLUMNS];
	void *colptr[MAX_FILE_COLUMNS];
	int colct;
	char *line;
	//不包含路径的文件名
	std::string basefilename;
	int errinfile;

	IDumpFileWrapper *pWrapper;
	//公用文件抽取函数
	//  从  FTP SERVER/本地路径/备份目录 抽取源文件，如果是压缩文件(.gz),则解压缩
	//    除备份目录外抽取文件方式以外，将文件做备份，如果源文件不是压缩的，则压缩备份文件，否则，直接备份
	//    backuppath: 备份文件的路径
	//    frombackup:是否从备份路径抽取文件
	//    filepatt:文件查找模式(可以含通配符)
	//  返回值：1, 抽取到有效文件, 0,没抽取到文件,2抽取文件错误，但应该忽略
	// 使用到的参数：
	//     files:localpath,抽取文件目标路径
	//     ftp:textmode/ftp:host/ftp:username/ftp:password/ftp:path, 抽取文件的ftp参数
	//     local:path
	// 取到有效文件时，文件名在filename成员
	int commonGetFile(const char *backuppath,bool frombackup,const char *filepatt,SysAdmin &sp,int tabid,int datapartid,bool checkonly=false) ;

public :
	FileParser() ;
	virtual ~FileParser();

	virtual int GetErrors() {return errinfile;}
	virtual int WriteErrorData(const char *line) = 0;
	//这个函数必须在对象构造后，在其它成员函数之前调用
	virtual void SetTable(int _dbc,int _tabid,int _datapartid,IDumpFileWrapper *pdfw) ;
	// fill filename member,the wrapper check conflict concurrent process,
	// if one has privillege to process current file ,then invoke DoParse
	// 返回值：
	//  -1 ：发生错误，例如任务状态已经在其它进程中修改为取消或者暂停
	//  0：暂无文件
	//  1：取得有效的文件
	virtual int GetFile(bool frombackup,SysAdmin &sp,int tabid,int datapartid,bool checkonly=false) = 0;
	// if end ,update task to next step(with lock)
	// critical function !
	// checked every file parser completed.
	// 必须是不再有文件需要处理的时候，才能调用这个检查函数
	virtual bool ExtractEnd() = 0;
	virtual int GetIdleSeconds() ;
	// return value:
	//  -1: a error occurs.例如内存表字段结构不匹配
	//  0:memory table has been filled full,and need to parse again
	//  1:file parse end
	//while parser file ended,this function record file process log and 
	// sequence of file in dp dictions tab.
	virtual int DoParse(int memtab,SysAdmin &sp,int tabid,int datapartid) = 0;
	virtual int GetFileSeq(const char *filename) = 0;
};

#endif


