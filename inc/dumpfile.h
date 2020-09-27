
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
	//���Ա��ظ����ļ�
	bool skippickup;
	//�����ļ�����
	bool skipbackup;
	char lines[300];
	//bool firstpart;
	//char *ptr;
	char LISTFILE[300];
	char bakuperrfile[300];
	char host[100],username[100],passwd[100],path[300],localpath[300];
	char localfile[300];
	// �Ƿ�ɾ��ԭ�ļ� ,add by liujs
	enum DELETE_FILE_FLAG
	{
      DELETE_NO = 0, // ��ɾ���ļ�
      DELETE_YES = 1, // ɾ���ļ�
	};
	enum REPLASE_FTP_GET_PARAM // �滻1: ��ʾ��Ҫ�滻ftpget�ļ��Ĳ���%2F
	{
      REPLASE_NO = 0,  // ���滻
      REPLASE_YES = 1, // �滻
	};
	int delFileFlag;           // ɾ���ļ���ʶ
	//>> Begin: delete src file 20130131
	char current_file[300];    // ��ǰ���ڴ�����ļ����ƣ�ɾ��Դ�ļ�ʹ�ã���������·��
	long current_filesize;     // ��¼��ǰ�ļ���С
    bool is_backup_dir;        // �Ƿ���backupĿ¼,�����backupĿ¼����ɾ��Դ�ļ�
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
	//��ȡ�ļ��������ݣ����backupfile==null,����backupfile[0]=0,�򲻱���
	//  ����ļ�����.gz,�򱸷�ʱ��gzipѹ��
	bool getfile(char *fn,char *backupfile,bool removeori,SysAdmin &sp,int tabid,int datapartid);
	bool getnextfile(char *fn);
	// ����ɾ����־
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
	//������·�����ļ���
	std::string basefilename;
	int errinfile;

	IDumpFileWrapper *pWrapper;
	//�����ļ���ȡ����
	//  ��  FTP SERVER/����·��/����Ŀ¼ ��ȡԴ�ļ��������ѹ���ļ�(.gz),���ѹ��
	//    ������Ŀ¼���ȡ�ļ���ʽ���⣬���ļ������ݣ����Դ�ļ�����ѹ���ģ���ѹ�������ļ�������ֱ�ӱ���
	//    backuppath: �����ļ���·��
	//    frombackup:�Ƿ�ӱ���·����ȡ�ļ�
	//    filepatt:�ļ�����ģʽ(���Ժ�ͨ���)
	//  ����ֵ��1, ��ȡ����Ч�ļ�, 0,û��ȡ���ļ�,2��ȡ�ļ����󣬵�Ӧ�ú���
	// ʹ�õ��Ĳ�����
	//     files:localpath,��ȡ�ļ�Ŀ��·��
	//     ftp:textmode/ftp:host/ftp:username/ftp:password/ftp:path, ��ȡ�ļ���ftp����
	//     local:path
	// ȡ����Ч�ļ�ʱ���ļ�����filename��Ա
	int commonGetFile(const char *backuppath,bool frombackup,const char *filepatt,SysAdmin &sp,int tabid,int datapartid,bool checkonly=false) ;

public :
	FileParser() ;
	virtual ~FileParser();

	virtual int GetErrors() {return errinfile;}
	virtual int WriteErrorData(const char *line) = 0;
	//������������ڶ��������������Ա����֮ǰ����
	virtual void SetTable(int _dbc,int _tabid,int _datapartid,IDumpFileWrapper *pdfw) ;
	// fill filename member,the wrapper check conflict concurrent process,
	// if one has privillege to process current file ,then invoke DoParse
	// ����ֵ��
	//  -1 ������������������״̬�Ѿ��������������޸�Ϊȡ��������ͣ
	//  0�������ļ�
	//  1��ȡ����Ч���ļ�
	virtual int GetFile(bool frombackup,SysAdmin &sp,int tabid,int datapartid,bool checkonly=false) = 0;
	// if end ,update task to next step(with lock)
	// critical function !
	// checked every file parser completed.
	// �����ǲ������ļ���Ҫ�����ʱ�򣬲��ܵ��������麯��
	virtual bool ExtractEnd() = 0;
	virtual int GetIdleSeconds() ;
	// return value:
	//  -1: a error occurs.�����ڴ���ֶνṹ��ƥ��
	//  0:memory table has been filled full,and need to parse again
	//  1:file parse end
	//while parser file ended,this function record file process log and 
	// sequence of file in dp dictions tab.
	virtual int DoParse(int memtab,SysAdmin &sp,int tabid,int datapartid) = 0;
	virtual int GetFileSeq(const char *filename) = 0;
};

#endif


