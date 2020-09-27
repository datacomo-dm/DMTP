
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
//>> Begin: ���FTP list & get ʧ�ܺ��������5�β�����ÿ�����Խ��еȴ�2���ӣ�DM-198 , add by liujs
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
	//����ƶ������޸�
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
	   //lgprintf("ɾ���ļ�:%s ʧ�ܣ���ȷ���Ƿ����ɾ���ļ�Ȩ��.",LISTFILE);  
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
	   //lgprintf("ɾ���ļ�:%s ʧ�ܣ���ȷ���Ƿ����ɾ���ļ�Ȩ��.",LISTFILE);  
    }
	// remote path end with '/' cause list result no path prefix,only filename
	sprintf(remotepath,"%s/%s",		path,filepatt);
	
	ftpclient.SetContext(_host,_username,_passwd);
	ftpclient.ListFile(remotepath,LISTFILE);
	lgprintf("��ȡ�ļ��б� ...");

	flist=fopen(LISTFILE,"rt");
	if(flist==NULL) ThrowWith("�ļ��б�ʧ��.���ļ��б�%sʧ��.");
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
	   //lgprintf("ɾ���ļ�:%s ʧ�ܣ���ȷ���Ƿ����ɾ���ļ�Ȩ��.",LISTFILE);  
    }
  
	sprintf(localpath,"%s/%s",_path,filepatt);
	listfile(localpath,LISTFILE);

	flist=fopen(LISTFILE,"rt");
	if(flist==NULL) ThrowWith("�ļ��б�ʧ��.");
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
//��ȡ�ļ��������ݣ����backupfile==null,����backupfile[0]=0,�򲻱���
//  ����ļ�����.gz,�򱸷�ʱ��gzipѹ��
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
	  // ��¼��ǰ���ļ�����(�������ļ�����)
	  sprintf(current_file,"%s/%s",path,fn);	
	  ls_current_file_size=ftpclient.FileSize(current_file);
	  sprintf(tmpfile,"%s/%s",localpath,fn);
	  unlink(tmpfile);
	  lgprintf("FTP Get %s->%s ...",current_file,tmpfile);
	  ftpclient.GetFile(current_file,tmpfile);
		if(strcmp(fn+strlen(fn)-4,".ack")==0) 
		{
			//ɾ�����ص�ȷ���ļ�(���ļ�)
			if(-1 == unlink(getlocalfile(fn)))
			{
			   lgprintf("ɾ���ļ�:%s ʧ�ܣ���ȷ���Ƿ����ɾ���ļ�Ȩ��.",getlocalfile(fn));	
			}
			current_file[strlen(current_file)-4]=0;
			tmpfile[strlen(tmpfile)-4]=0;
			ftpclient.GetFile(current_file,tmpfile);
		}
	}
	else 
	{		
	      // ��¼��ǰ���ļ�����(�������ļ�����)
	      memset(current_file,0,300);
	      sprintf(current_file,"%s/%s",path,fn);
	      sprintf(tmpfile,"%s/%s",localpath,fn);
	      ls_current_file_size=localfilesize(current_file);
	      
		  // local file
		  if(!skippickup) {
		    localcopyfile(current_file,tmpfile);
        if(strcmp(fn+strlen(fn)-4,".ack")==0) {
             //����ļ����ڣ�ɾ�����ص�ȷ���ļ�(���ļ�)
             if( -1 == unlink(getlocalfile(fn))){
			   // lgprintf("ɾ���ļ�:%s ʧ�ܣ���ȷ���Ƿ����ɾ���ļ�Ȩ��.",getlocalfile(fn));             	
             }
             current_file[strlen(current_file)-4]=0;
						 tmpfile[strlen(tmpfile)-4]=0;
             localcopyfile(current_file,tmpfile);
          }
		} // skippickup endif
	}
	struct stat st;
	if(stat(getlocalfile(fn),&st)){ // �ɹ�����0
		ThrowWith("�ļ��ɼ�ʧ��:%s.",fn);
	}	

	// �Ա��ļ���С
	if(st.st_size != ls_current_file_size)
	{
	     lgprintf("Դ�ļ���С(%ld)��ɼ������ļ���С(%ld)��һ���������˳�.",	ls_current_file_size,st.st_size);
	     ThrowWith("Դ�ļ���С(%ld)��ɼ������ļ���С(%ld)��һ���������˳�.",	ls_current_file_size,st.st_size);
	}
	current_filesize = ls_current_file_size;  // ��¼�ļ���С������д���dp.dp_filelog���õ�
	
	if(backupfile && backupfile[0]!=0 && !skipbackup) {
		if(strcmp(backupfile+strlen(backupfile)-4,".ack")==0) 
			backupfile[strlen(backupfile)-4]=0;
			localcopyfile(getlocalfile(fn),backupfile);
		  if(strcmp(fn+strlen(fn)-3,".gz")!=0) {
			 //��̨���з�ʽ��gzipѹ��
			 char cmd[300];
			 sprintf(cmd,"nohup gzip %s >>%s 2>&1 &",backupfile,bakuperrfile);
			 strcat(backupfile,".gz");
			 if(-1 == unlink(backupfile)){ // ����ļ��Ѿ����ھ�ɾ���ļ�
				//lgprintf("ɾ���ļ�:%s ʧ�ܣ���ȷ���Ƿ����ɾ���ļ�Ȩ��.",backupfile);	
			 }
			 system(cmd);
		}
	}
	if(strcmp(fn+strlen(fn)-3,".gz")==0) {
		gunzipfile(getlocalfile(fn));
		fn[strlen(fn)-3]=0;
		if(stat(getlocalfile(fn),&st))
		{
			lgprintf("�ļ���ѹ��ʧ��:%s,���Դ���...",getlocalfile(fn));
			sp.log(tabid,datapartid,106,"�ļ���ѹ��ʧ��:%s,���Դ���...",getlocalfile(fn));
			return false;
		} 				
	}
	//tmf.Stop();
	return true;
}

bool file_man::getnextfile(char *fn)
{
	if(!flist) flist=fopen(LISTFILE,"rt");
	if(flist==NULL) ThrowWith("�ļ��б�ʧ��.");
	while(fgets(lines,300,flist)!=NULL) {
		int sl=strlen(lines);
		if(lines[sl-1]=='\n') lines[sl-1]=0;
		char *pfn=lines+strlen(lines)-1;
		//�ҵ��ļ����Ŀ�ʼλ�ã��ų�·������)
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

//>> DM-216 , 20130131��ɾ��Դ�ļ�
int  file_man::DeleteSrcFile()
{
	if(strlen(current_file)< 1)
	{
	    lgprintf("�ļ������ڣ�ɾ��Դ�ļ�ʧ��.");	
	    return -1;
	}	

	if(ftpmode) // ftp ģʽɾ��FTP������ļ�
	{
        if(DELETE_YES == delFileFlag)
		{		
            lgprintf("ɾ���ļ�:%s,is_backup_dir = %s",current_file,is_backup_dir?"true":"false");
			ftpclient.Delete(current_file);
		    // �ļ���ȷ���ļ�
		    if(strcmp(current_file+strlen(current_file)-4,".ack")==0) 
		    {
		    	current_file[strlen(current_file)-4] = 0;
			    ftpclient.Delete(current_file);
			}
		}
	}
	else  // ɾ�������ļ�,backupĿ¼�Ĳ�ɾ��
	{				  
		if( !is_backup_dir &&  DELETE_YES == delFileFlag)
		{		    
            lgprintf("ɾ���ļ�:%s,is_backup_dir = %s",current_file,is_backup_dir?"true":"false");
			unlink(current_file);
			if(strcmp(current_file+strlen(current_file)-4,".ack")==0) {
                current_file[strlen(current_file)-4]=0;
                unlink(current_file);
            }
		} // end if(DELETE_YES == delFileFlag)
	
	} // end else
	
  return 0;	
}

//����������
//  ��һ�ε���frombackup,��������һ��ftp/local�Ѿ�������
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
        fm.SetIsBackupDir(frombackup);      // ��¼��ǰĿ¼�Ƿ���backupĿ¼ 
		//<< End:fix Dm-263
		
		fm.listlocal(backuppath,backfilepatt);
	}
	else if(!fm.listhasopen() && strlen(pWrapper->getOption("ftp:host",""))<1) {
		//�����ļ�ģʽ
		fm.listlocal(pWrapper->getOption("local:path","./"),filepatt);
		fm.SetSkipPickup(pWrapper->getOption("files:skippickup",0)==1);
		fm.SetSkipBackup(pWrapper->getOption("files:skipbackup",0)==1);

		//>> Begin: fix DM-263
        fm.SetIsBackupDir(frombackup);      // ��¼��ǰĿ¼�Ƿ���backupĿ¼ 
		//<< End:fix Dm-263
	}
	else if(!fm.listhasopen())
	{
		//>> Begin: fix DM-263
        fm.SetIsBackupDir(frombackup);      // ��¼��ǰĿ¼�Ƿ���backupĿ¼ 
		//<< End:fix Dm-263
		
		//FTPģʽ	  
		fm.SetAscMode(pWrapper->getOption("ftp:textmode",1)==1);

		//>> Begin: FTP get �ļ�ʧ�ܺ�,��������5�Σ�ÿ�εȴ�2���� ,modify by liujs
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
		    	  	
                // list �ļ�������δ�׳��쳣
                break;				
			}
            catch(char* e)
            {
                if(ftpListTimes <= FTP_GET_LIST_RETRY_TIMES)
                {
                    lgprintf("FTP LIST �ļ�ʧ�ܣ��ȴ�2���Ӻ��������,FTP LISTִ�д���(%d),��ǰ����(%d)",FTP_GET_LIST_RETRY_TIMES,ftpListTimes);
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
	        ThrowWith("FTP LIST �ļ��б�ʧ�ܣ�δ֪����");
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
				
        // Jira-DMA-453,ȥ��.gz ��չ��
        strcpy(realfile_bk, realfile);
        if(strcmp(realfile+strlen(realfile)-3,".gz")==0)
	        realfile[strlen(realfile)-3] = 0;
			
		fileseq=GetFileSeq(realfile); 
		
		//Begin: crc������ţ�crc��Ų�У��
	    if(pWrapper->getOption("files:crcsequence",0)==0)
	    {
		    if(fileseq<=0) ThrowWith("�ļ���Ŵ���(%d):%s",fileseq,localfile);
		}
		//End:src�������
		
		// fix dm-210,add filename query
        mt.FetchAll("select filename from dp.dp_filelog where tabid=%d and datapartid=%d and fileseq=%d and filename = '%s'",
			tabid,datapartid,fileseq,realfile);
        
        if(mt.Wait()>0) {//�Ѿ��д����¼
            if(strcmp(mt.PtrStr("filename",0),realfile)!=0) {                
                //Դ�ļ�δѹ��,Jira DMA-453 20130110
                if(strcmp(realfile+strlen(realfile)-3,".gz")==0){ 
                    realfile[strlen(realfile)-3]=0; 
                    if(strcmp(mt.PtrStr("filename",0),realfile)!=0) 
                        ThrowWith("�ļ����(%d)���������ļ�'%s' ---'%s' ���к��ظ�!",fileseq,mt.PtrStr("filename",0),realfile);
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
			//��һ�������ڴ�������ļ���ֻ����ȷд��dp_filelog�Ľ��̿��Դ����ļ�
			continue;
		}
		bool filesuc=true;
		if(!checkonly) 
		{
            //>> Begin: FTP get �ļ�ʧ�ܺ�,��������5�Σ�ÿ�εȴ�2���� ,modify by liujs
            int ftpGetTimes = 0;
            while(1)
            {
                try
                {
                    ftpGetTimes++; 	
					// warning : DM-263 , �����ڴ˴�����
                    // fm.SetIsBackupDir(frombackup);      // ��¼��ǰĿ¼�Ƿ���backupĿ¼ 
                    
                    fm.getfile(localfile,backfilename,!frombackup/* �Ƿ�ɾ���ļ����� */,sp,tabid,datapartid);                    
                    // get �ļ�������Ϊ�׳��쳣
                    break;				
                }
                catch(char* e)
                {
                    if(ftpGetTimes <= FTP_GET_LIST_RETRY_TIMES)
                    {
                        lgprintf("%s�ļ�ʧ�ܣ��ȴ�2���Ӻ��������,%sִ�д���(%d),��ǰ����(%d)",fm.GetFtpMode()?"FTP GET":"cp local",
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
                    ThrowWith("%s �ļ�ʧ�ܣ�δ֪����", fm.GetFtpMode()?"FTP GET":"cp local");
                }
			}// END while
		    //<< End: modify by liujs

			basefilename=localfile;
			filename=fm.getlocalfile(localfile);
			if(!filesuc) {
			//���Դ���ֱ���޸�Ϊ����������ύ״̬
			st.DirectExecute("update dp.dp_filelog set status=2 where tabid=%d and datapartid=%d and status=0"
				,tabid,datapartid);
			}
		}
		
		// �����ļ���С
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
//������������ڶ��������������Ա����֮ǰ����
void FileParser::SetTable(int _dbc,int _tabid,int _datapartid,IDumpFileWrapper *pdfw) {
	dbc=_dbc;tabid=_tabid;fp=NULL;pWrapper=pdfw;datapartid=_datapartid;fileseq=0;
};
// fill filename member,the wrapper check conflict concurrent process,
// if one has privillege to process current file ,then invoke DoParse
// ����ֵ��
//  -1 ������������������״̬�Ѿ��������������޸�Ϊȡ��������ͣ
//  0�������ļ�
//  1��ȡ����Ч���ļ�
//int  FileParser::GetFile(bool frombackup,SysAdmin &sp,int tabid,int datapartid,bool checkonly) {return 0;};
// if end ,update task to next step(with lock)
// critical function !
// checked every file parser completed.
// �����ǲ������ļ���Ҫ�����ʱ�򣬲��ܵ��������麯��
//bool FileParser::ExtractEnd() {return true;};
int FileParser::GetIdleSeconds() {return pWrapper->getOption("files:seekinterval", 5);}
// return value:
//  -1: a error occurs.�����ڴ���ֶνṹ��ƥ��
//  0:memory table has been filled full,and need to parse again
//  1:file parse end
//while parser file ended,this function record file process log and 
// sequence of file in dp dictions tab.
//int FileParser::DoParse(int memtab,SysAdmin &sp,int tabid,int datapartid) {return -1;};
//int FileParser::GetFileSeq(const char *filename) {return 1;}

FileParser::~FileParser() {
	if(fp) fclose(fp);delete []line;
	
}


