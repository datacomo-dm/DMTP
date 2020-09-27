
/* dumpfile.h --

This file is part of the DataMerger data extrator.

Copyright (C) 2010 DataComo
All Rights Reserved.

Wang Guosheng
<wangguosheng@datacomo.com>
http://www.datacomo.com
http://mobi5.cn
*/
#include "dumpfile.h"
#include "dmncftp.h"
//>> Begin: 针对FTP list & get 失败后进行重试5次操作，每次重试进行等待2分钟，DM-198 , add by liujs
#define	FTP_GET_LIST_RETRY_TIMES    5
#define	FTP_OPER_FAIL_SLEEP_SEC     120
//<< End: add by liujs

void file_man::FTPDisconnect() {
	ftpclient.Close();
}
file_man::file_man()
{
	memset(localfile,0,300);
	flist=NULL;memset(lines,0,sizeof(lines));
	ascmode=true;
	ftpmode=true;
	//天津移动本地修改
	skippickup=false;
	skipbackup=false;
	sprintf(LISTFILE,"%s/lst/filelist%d.txt",getenv("DATAMERGER_HOME"),getpid());
	strcpy(localpath,"./");
	
	bakuperrfile[0]=0;
	const char *path=NULL;
	if(path==NULL) path=getenv("WDBI_LOGPATH");
	if(path) {
		strcpy(bakuperrfile,path);

		if(path[strlen(path)-1]!=PATH_SEPCHAR) {
		 strcat(bakuperrfile,PATH_SEP);
		}
	}
	strcpy(bakuperrfile,BACKUP_LOGFILE);
	
	delFileFlag = DELETE_YES;
	memset(current_file,0,300);
	current_filesize = 0;
	
	is_backup_dir = false;
}
file_man::~file_man() {
	if(flist) fclose(flist);
	flist=NULL;
	if(-1 == unlink(LISTFILE)){
	   //lgprintf("删除文件:%s 失败，请确认是否具有删除文件权限.",LISTFILE);  
    }
    
	memset(current_file,0,300);
}
void file_man::SetLocalPath(const char *p) 
{
	strcpy(localpath,p);
}
void file_man::SetAscMode(bool val) {
	ascmode=val;
}
void file_man::SetSkipPickup(bool val) {
	skippickup=val;
}
void file_man::SetSkipBackup(bool val) {
	skipbackup=val;}

void file_man::list(const char *_host,const char *_username,const char *_passwd,const char *_path,const char *filepatt)
{
	char remotepath[500];
	strcpy(host,_host);strcpy(username,_username);strcpy(passwd,_passwd);

	//if(_path[0]=='/') 
	//	sprintf(path,"%%2F%s",_path+1);
	//else 
	strcpy(path,_path);
	if(flist) fclose(flist);
	flist=NULL;
    if(-1 == unlink(LISTFILE)){
	   //lgprintf("删除文件:%s 失败，请确认是否具有删除文件权限.",LISTFILE);  
    }
	// remote path end with '/' cause list result no path prefix,only filename
	sprintf(remotepath,"%s/%s",		path,filepatt);
	
	ftpclient.SetContext(_host,_username,_passwd);
	ftpclient.ListFile(remotepath,LISTFILE);
	lgprintf("获取文件列表 ...");

	flist=fopen(LISTFILE,"rt");
	if(flist==NULL) ThrowWith("文件列表失败.打开文件列表%s失败.");
	memset(lines,0,sizeof(lines));
	fclose(flist);flist=NULL;
	ftpmode=true;
}

void file_man::listlocal(const char *_path,const char *filepatt)
{
	char localpath[300];
	if(flist) fclose(flist);
	flist=NULL;
	strcpy(path,_path);
	if(-1 == unlink(LISTFILE)){
	   //lgprintf("删除文件:%s 失败，请确认是否具有删除文件权限.",LISTFILE);  
    }
  
	sprintf(localpath,"%s/%s",_path,filepatt);
	listfile(localpath,LISTFILE);

	flist=fopen(LISTFILE,"rt");
	if(flist==NULL) ThrowWith("文件列表失败.");
	memset(lines,0,sizeof(lines));
	fclose(flist);flist=NULL;
	ftpmode=false;
}

const char *file_man::getlocalfile(const char *fn) {
	if(fn!=NULL){
		memset(localfile,0,300);
		sprintf(localfile,"%s/%s",skippickup?path:localpath,fn);
	}
	return localfile;
}
//获取文件并做备份，如果backupfile==null,或者backupfile[0]=0,则不备份
//  如果文件不是.gz,则备份时做gzip压缩
bool file_man::getfile(char *fn,char *backupfile,bool removeori,SysAdmin &sp,int tabid,int datapartid)
{
	char ls_current_file_Info[300] = {0};
	char ls_current_file_name[300] = {0};
	long ls_current_file_size = 0;
	char tmpfile[300] = {0};
	//tmf.Start();
	char *end=fn+strlen(fn)-1;
	while(*end<' ') *end--=0;
	
	if(ftpmode) 
	{				
	  // 记录当前的文件名称(完整的文件名称)
	  sprintf(current_file,"%s/%s",path,fn);	
	  ls_current_file_size=ftpclient.FileSize(current_file);
	  sprintf(tmpfile,"%s/%s",localpath,fn);
	  unlink(tmpfile);
	  lgprintf("FTP Get %s->%s ...",current_file,tmpfile);
	  ftpclient.GetFile(current_file,tmpfile);
		if(strcmp(fn+strlen(fn)-4,".ack")==0) 
		{
			//删除本地的确认文件(空文件)
			if(-1 == unlink(getlocalfile(fn)))
			{
			   lgprintf("删除文件:%s 失败，请确认是否具有删除文件权限.",getlocalfile(fn));	
			}
			current_file[strlen(current_file)-4]=0;
			tmpfile[strlen(tmpfile)-4]=0;
			ftpclient.GetFile(current_file,tmpfile);
		}
	}
	else 
	{		
	      // 记录当前的文件名称(完整的文件名称)
	      memset(current_file,0,300);
	      sprintf(current_file,"%s/%s",path,fn);
	      sprintf(tmpfile,"%s/%s",localpath,fn);
	      ls_current_file_size=localfilesize(current_file);
	      
		  // local file
		  if(!skippickup) {
		    localcopyfile(current_file,tmpfile);
        if(strcmp(fn+strlen(fn)-4,".ack")==0) {
             //如果文件存在，删除本地的确认文件(空文件)
             if( -1 == unlink(getlocalfile(fn))){
			   // lgprintf("删除文件:%s 失败，请确认是否具有删除文件权限.",getlocalfile(fn));             	
             }
             current_file[strlen(current_file)-4]=0;
						 tmpfile[strlen(tmpfile)-4]=0;
             localcopyfile(current_file,tmpfile);
          }
		} // skippickup endif
	}
	struct stat st;
	if(stat(getlocalfile(fn),&st)){ // 成功返回0
		ThrowWith("文件采集失败:%s.",fn);
	}	

	// 对比文件大小
	if(st.st_size != ls_current_file_size)
	{
	     lgprintf("源文件大小(%ld)与采集到的文件大小(%ld)不一样，程序退出.",	ls_current_file_size,st.st_size);
	     ThrowWith("源文件大小(%ld)与采集到的文件大小(%ld)不一样，程序退出.",	ls_current_file_size,st.st_size);
	}
	current_filesize = ls_current_file_size;  // 记录文件大小，插件中存入dp.dp_filelog中用到
	
	if(backupfile && backupfile[0]!=0 && !skipbackup) {
		if(strcmp(backupfile+strlen(backupfile)-4,".ack")==0) 
			backupfile[strlen(backupfile)-4]=0;
			localcopyfile(getlocalfile(fn),backupfile);
		  if(strcmp(fn+strlen(fn)-3,".gz")!=0) {
			 //后台并行方式做gzip压缩
			 char cmd[300];
			 sprintf(cmd,"nohup gzip %s >>%s 2>&1 &",backupfile,bakuperrfile);
			 strcat(backupfile,".gz");
			 if(-1 == unlink(backupfile)){ // 如果文件已经存在就删除文件
				//lgprintf("删除文件:%s 失败，请确认是否具有删除文件权限.",backupfile);	
			 }
			 system(cmd);
		}
	}
	if(strcmp(fn+strlen(fn)-3,".gz")==0) {
		gunzipfile(getlocalfile(fn));
		fn[strlen(fn)-3]=0;
		if(stat(getlocalfile(fn),&st))
		{
			lgprintf("文件解压缩失败:%s,忽略错误...",getlocalfile(fn));
			sp.log(tabid,datapartid,106,"文件解压缩失败:%s,忽略错误...",getlocalfile(fn));
			return false;
		} 				
	}
	//tmf.Stop();
	return true;
}

bool file_man::getnextfile(char *fn)
{
	if(!flist) flist=fopen(LISTFILE,"rt");
	if(flist==NULL) ThrowWith("文件列表失败.");
	while(fgets(lines,300,flist)!=NULL) {
		int sl=strlen(lines);
		if(lines[sl-1]=='\n') lines[sl-1]=0;
		char *pfn=lines+strlen(lines)-1;
		//找到文件名的开始位置（排除路径部分)
		while(pfn>lines) if(*pfn--=='/')  {pfn+=2;break;}
		strcpy(fn,pfn);
		return true;
	}
	fclose(flist);flist=NULL;

	//>> begin: fix DM-264
    unlink(LISTFILE);
	//<< end:fix dm-264
	
	return false; // reach eof
}

//>> DM-216 , 20130131，删除源文件
int  file_man::DeleteSrcFile()
{
	if(strlen(current_file)< 1)
	{
	    lgprintf("文件不存在，删除源文件失败.");	
	    return -1;
	}	

	if(ftpmode) // ftp 模式删除FTP服务端文件
	{
        if(DELETE_YES == delFileFlag)
		{		
            lgprintf("删除文件:%s,is_backup_dir = %s",current_file,is_backup_dir?"true":"false");
			ftpclient.Delete(current_file);
		    // 文件是确认文件
		    if(strcmp(current_file+strlen(current_file)-4,".ack")==0) 
		    {
		    	current_file[strlen(current_file)-4] = 0;
			    ftpclient.Delete(current_file);
			}
		}
	}
	else  // 删除本地文件,backup目录的不删除
	{				  
		if( !is_backup_dir &&  DELETE_YES == delFileFlag)
		{		    
            lgprintf("删除文件:%s,is_backup_dir = %s",current_file,is_backup_dir?"true":"false");
			unlink(current_file);
			if(strcmp(current_file+strlen(current_file)-4,".ack")==0) {
                current_file[strlen(current_file)-4]=0;
                unlink(current_file);
            }
		} // end if(DELETE_YES == delFileFlag)
	
	} // end else
	
  return 0;	
}

//限制条件：
//  第一次调用frombackup,必须是上一个ftp/local已经处理完
int FileParser::commonGetFile(const char *backuppath,bool frombackup,const char *filepatt,SysAdmin &sp,int tabid,int datapartid,bool checkonly) {
	char localfile[300];
	AutoMt mt(dbc,10);
	if(frombackup) {
		char backfilepatt[300];
		strcpy(backfilepatt,filepatt);
		
		if(strcmp(backfilepatt+strlen(backfilepatt)-4,".ack")==0) 
			backfilepatt[strlen(backfilepatt)-4]=0;
			
		// BEGIN: fix DMA-447,20130109				
        if(strcmp(backfilepatt+strlen(backfilepatt)-3,".gz")!=0) 
            strcat(backfilepatt,".gz");
		// END : fix DMA-447

        //>> Begin: fix DM-263
        fm.SetIsBackupDir(frombackup);      // 记录当前目录是否是backup目录 
		//<< End:fix Dm-263
		
		fm.listlocal(backuppath,backfilepatt);
	}
	else if(!fm.listhasopen() && strlen(pWrapper->getOption("ftp:host",""))<1) {
		//本地文件模式
		fm.listlocal(pWrapper->getOption("local:path","./"),filepatt);
		fm.SetSkipPickup(pWrapper->getOption("files:skippickup",0)==1);
		fm.SetSkipBackup(pWrapper->getOption("files:skipbackup",0)==1);

		//>> Begin: fix DM-263
        fm.SetIsBackupDir(frombackup);      // 记录当前目录是否是backup目录 
		//<< End:fix Dm-263
	}
	else if(!fm.listhasopen())
	{
		//>> Begin: fix DM-263
        fm.SetIsBackupDir(frombackup);      // 记录当前目录是否是backup目录 
		//<< End:fix Dm-263
		
		//FTP模式	  
		fm.SetAscMode(pWrapper->getOption("ftp:textmode",1)==1);

		//>> Begin: FTP get 文件失败后,进行重试5次，每次等待2分钟 ,modify by liujs
		int ftpListTimes = 0;
		while(1)
		{
            try
            {
                ftpListTimes++; // FTP List times	
		    	      
                fm.list(pWrapper->getOption("ftp:host",""),
                pWrapper->getOption("ftp:username",""),
                pWrapper->getOption("ftp:password",""),
                pWrapper->getOption("ftp:path",""),filepatt);
		    	  	
                // list 文件正常，未抛出异常
                break;				
			}
            catch(char* e)
            {
                if(ftpListTimes <= FTP_GET_LIST_RETRY_TIMES)
                {
                    lgprintf("FTP LIST 文件失败，等待2分钟后继续重试,FTP LIST执行次数(%d),当前次数(%d)",FTP_GET_LIST_RETRY_TIMES,ftpListTimes);
                    sleep(FTP_OPER_FAIL_SLEEP_SEC);// 2 minutes
                    fm.FTPDisconnect();
                    continue;
                }
                else
                {
                    throw e;
                }
			}
			catch(...)
			{			    
	        ThrowWith("FTP LIST 文件列表失败，未知错误");
			}
		}// end while
		//<< End: modify by liujs
	}
	fm.SetLocalPath(pWrapper->getOption("files:localpath","./"));

	bool ec=wociIsEcho();
	wociSetEcho(pWrapper->getOption("files:verbose",0)==1);
	while(fm.getnextfile(localfile) ) 
	{
		char realfile[300];
		char realfile_bk[300];
		strcpy(realfile,localfile);
		if(strcmp(realfile+strlen(realfile)-4,".ack")==0)
			realfile[strlen(realfile)-4]=0;
				
        // Jira-DMA-453,去除.gz 扩展名
        strcpy(realfile_bk, realfile);
        if(strcmp(realfile+strlen(realfile)-3,".gz")==0)
	        realfile[strlen(realfile)-3] = 0;
			
		fileseq=GetFileSeq(realfile); 
		
		//Begin: crc产生序号，crc序号不校验
	    if(pWrapper->getOption("files:crcsequence",0)==0)
	    {
		    if(fileseq<=0) ThrowWith("文件序号错误(%d):%s",fileseq,localfile);
		}
		//End:src产生序号
		
		// fix dm-210,add filename query
        mt.FetchAll("select filename from dp.dp_filelog where tabid=%d and datapartid=%d and fileseq=%d and filename = '%s'",
			tabid,datapartid,fileseq,realfile);
        
        if(mt.Wait()>0) {//已经有处理记录
            if(strcmp(mt.PtrStr("filename",0),realfile)!=0) {                
                //源文件未压缩,Jira DMA-453 20130110
                if(strcmp(realfile+strlen(realfile)-3,".gz")==0){ 
                    realfile[strlen(realfile)-3]=0; 
                    if(strcmp(mt.PtrStr("filename",0),realfile)!=0) 
                        ThrowWith("文件序号(%d)错误，两个文件'%s' ---'%s' 序列号重复!",fileseq,mt.PtrStr("filename",0),realfile);
                    }
            }
            continue;
        }
		
		char backfilename[300];
		backfilename[0]=0;
		if(!frombackup) {
			sprintf(backfilename,"%s/%s",backuppath,realfile_bk);
		}
		AutoStmt st(dbc);
		try {
			if(!checkonly) {
			st.Prepare("insert into dp.dp_filelog values("
				"%d,%d,'%s','%s',0,%d,sysdate(),null,0,0,0,'NULL',0)",
				tabid,datapartid,realfile,backfilename,fileseq);
			st.Execute(1);
			st.Wait();
		  }
		}
		catch(...) {
			//另一个进程在处理这个文件？只有正确写入dp_filelog的进程可以处理文件
			continue;
		}
		bool filesuc=true;
		if(!checkonly) 
		{
            //>> Begin: FTP get 文件失败后,进行重试5次，每次等待2分钟 ,modify by liujs
            int ftpGetTimes = 0;
            while(1)
            {
                try
                {
                    ftpGetTimes++; 	
					// warning : DM-263 , 不能在此处设置
                    // fm.SetIsBackupDir(frombackup);      // 记录当前目录是否是backup目录 
                    
                    fm.getfile(localfile,backfilename,!frombackup/* 是否删除文件参数 */,sp,tabid,datapartid);                    
                    // get 文件正常，为抛出异常
                    break;				
                }
                catch(char* e)
                {
                    if(ftpGetTimes <= FTP_GET_LIST_RETRY_TIMES)
                    {
                        lgprintf("%s文件失败，等待2分钟后继续重试,%s执行次数(%d),当前次数(%d)",fm.GetFtpMode()?"FTP GET":"cp local",
                        	fm.GetFtpMode()?"FTP GET":"cp local",FTP_GET_LIST_RETRY_TIMES,ftpGetTimes);                          
                        sleep(FTP_OPER_FAIL_SLEEP_SEC);// 2 minutes
                        fm.FTPDisconnect();
                        continue;
                    }
                    else
                    {
                        throw e;
                    }
                }
                catch(...)
                {				
                    ThrowWith("%s 文件失败，未知错误", fm.GetFtpMode()?"FTP GET":"cp local");
                }
			}// END while
		    //<< End: modify by liujs

			basefilename=localfile;
			filename=fm.getlocalfile(localfile);
			if(!filesuc) {
			//忽略错误，直接修改为处理结束的提交状态
			st.DirectExecute("update dp.dp_filelog set status=2 where tabid=%d and datapartid=%d and status=0"
				,tabid,datapartid);
			}
		}
		
		// 更新文件大小
		st.DirectExecute("update dp.dp_filelog set filesize=%d where tabid=%d and datapartid=%d and fileseq=%d",
			fm.GetCurFileSize(),tabid,datapartid,fileseq);
		
		wociSetEcho(ec);
		return filesuc?1:2;
	} 
	wociSetEcho(ec);
	if(frombackup) 
		fm.clearlist();
	return 0;
}


FileParser::FileParser() {
	fp=NULL;
	line=new char[MAXLINELEN];

}
//这个函数必须在对象构造后，在其它成员函数之前调用
void FileParser::SetTable(int _dbc,int _tabid,int _datapartid,IDumpFileWrapper *pdfw) {
	dbc=_dbc;tabid=_tabid;fp=NULL;pWrapper=pdfw;datapartid=_datapartid;fileseq=0;
};
// fill filename member,the wrapper check conflict concurrent process,
// if one has privillege to process current file ,then invoke DoParse
// 返回值：
//  -1 ：发生错误，例如任务状态已经在其它进程中修改为取消或者暂停
//  0：暂无文件
//  1：取得有效的文件
//int  FileParser::GetFile(bool frombackup,SysAdmin &sp,int tabid,int datapartid,bool checkonly) {return 0;};
// if end ,update task to next step(with lock)
// critical function !
// checked every file parser completed.
// 必须是不再有文件需要处理的时候，才能调用这个检查函数
//bool FileParser::ExtractEnd() {return true;};
int FileParser::GetIdleSeconds() {return pWrapper->getOption("files:seekinterval", 5);}
// return value:
//  -1: a error occurs.例如内存表字段结构不匹配
//  0:memory table has been filled full,and need to parse again
//  1:file parse end
//while parser file ended,this function record file process log and 
// sequence of file in dp dictions tab.
//int FileParser::DoParse(int memtab,SysAdmin &sp,int tabid,int datapartid) {return -1;};
//int FileParser::GetFileSeq(const char *filename) {return 1;}

FileParser::~FileParser() {
	if(fp) fclose(fp);delete []line;
	
}


