/* DumpFileWrapper.h --

�����ļ������ת�ز��֣����ļ�������ط��뿪��

Copyright (C) 2012 DataComo
All Rights Reserved.

liujianshun
<liujianshun@datacomo.com>
http://www.datacomo.com
http://mobi5.cn

JIRA : DM-201
*/
#ifndef DUMPFILEWRAPPER_DEF_H
#define DUMPFILEWRAPPER_DEF_H
#include "IDumpfile.h"
#include "dt_common.h"
#include <stdio.h>
#include <map>
#include <string>
#include <dlfcn.h>

// dynamic load as library(plugin) ,should come later.
typedef void * (*MODULE_HANDLE)();
# if (defined(__linux__)||defined(__std_plugin_sym__))
#define BUILDPARSER_SYM "_Z11BuildParserv"
# else
#define BUILDPARSER_SYM "BuildParser__Fv"
#endif

class LibAdmin 
{
protected:
	void *handle;
	IFileParser *pFileParser;
public:
	LibAdmin(const char *libname) ;
	IFileParser *getParser() ;
	~LibAdmin();//��Ҫ�ͷź������pFileParser����cc�ļ��ж���
};

class DumpFileWrapper : public IDumpFileWrapper
{
protected:
	std::map<std::string,std::string> options;
	LibAdmin *pla;
	int split(char *text,char **tokens,char delimter) ;
public :
	~DumpFileWrapper() ;
	IFileParser *getParser() ;

	// װ������ļ��������Լ��ļ��ֽ�ģ��
	DumpFileWrapper(const char *ctlfile);
	// ȡ���Ʋ���
	// keyval:���ڰ������ֿ��Ĳ�����ͳһ����section:item�ĸ�ʽ������
	//    ftp:username
	const char *getOption(const char *keyval,const char *defaults) ;
	const char getOption(const char *keyval,const char defaults) ;
	int getOption(const char *keyval,int defaults) ;
	double getOption(const char *keyval,double defaults);
};

#endif
 
