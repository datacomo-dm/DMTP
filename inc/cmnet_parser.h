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
#include <stdlib.h>
#include "dumpfile.h"

class ZJCmnetParser: public FileParser {

	char yyyy[20],dd[10],mm[10];
public :
	// fill filename member,the wrapper check conflict concurrent process,
	// if one has privillege to process current file ,then invoke DoParse
	// 返回值：
	//  -1 ：发生错误，例如任务状态已经在其它进程中修改为取消或者暂停
	//  0：暂无文件
	//  1：取得有效的文件
	virtual int GetFile(bool frombackup,SysAdmin &sp,int tabid,int datapartid) {
		AutoMt mt(dbc,10);
	  mt.FetchAll("select tabname from dp.dp_table where tabid=%d",tabid);
	  if(mt.Wait()<1) ThrowWith("Table id:%d not found.",tabid);
	  char tabname[200];
	  strcpy(tabname,mt.PtrStr(0,0));
	  //取表名中的日期 YYYYMMDD
	  char filepatt[300];
	  char backuppath[300];
	  strcpy(yyyy,tabname+strlen(tabname)-6);
	  if(!isdigit(yyyy[0]) || !isdigit(yyyy[1]) ||!isdigit(yyyy[2])||!isdigit(yyyy[3]))
	  	ThrowWith("目标表 '%s' 名称必须要以年(4位)月(2位)日(2位)结尾！",tabname);
          //天津移动OnDemand导入，月表，不需要到日
	  //strncpy(dd,yyyy+6,2);
          strcpy(dd,"01");
	  dd[2]=0;
	  strncpy(mm,yyyy+4,2);
	  mm[2]=0;
	  yyyy[4]=0;
	  sprintf(filepatt,pWrapper->getOption("files:filename","usercdr_%04s-%02s-%02s*.txt.gz.ack"),yyyy,mm,dd);
		sprintf(backuppath,"%s/%s",
		   pWrapper->getOption("files:backuppath","./"),tabname);
		if(datapartid!=1) sprintf(backuppath+strlen(backuppath),"/part%d",datapartid);
		xmkdir(backuppath);
		int rt=commonGetFile(backuppath,false,filepatt,sp,tabid,datapartid);
		if(rt==0&&frombackup) rt=commonGetFile(backuppath,true,filepatt,sp,tabid,datapartid);
		return rt;
	}
	// if end ,update task to next step(with lock)
	// critical function !
	// checked every file parser completed.
	// 必须是不再有文件需要处理的时候，才能调用这个检查函数
	bool ExtractEnd() {
		//wociSetEcho(false);
		AutoMt mt(dbc,10);
		//不能加and procstate<=1 ，可能最后的一部分文件还没有进入状态1
		mt.FetchAll("select sum(recordnum) srn from dp.dp_middledatafile where tabid=%d and datapartid=%d ",
		   tabid,datapartid);
		if(mt.Wait()!=1) return false; //文件还没有处理完。
		double srn=mt.GetDouble("srn",0);
		mt.FetchAll("select case status when 1 then 2 else status end status,max(fileseq) xseq,min(fileseq) mseq,count(*) ct,sum(recordnum) srn "
		" from dp.dp_filelog where tabid=%d and datapartid=%d group by case status when 1 then 2 else status end ",
		tabid,datapartid);
		if(mt.Wait()!=1) return false; //文件还没有处理完。
		if(mt.GetDouble("status",0)!=2) return false;//文件还没有处理完。
		int maxseq=mt.GetInt("xseq",0);
		int minseq=mt.GetInt("mseq",0);
		LONG64 filect=mt.GetLong("ct",0);
		lgprintf("最小文件号:%d,最大文件号:%d,文件数:%ld.",minseq,maxseq,mt.GetLong("ct",0));
		//应移动要求，去掉序号连续性检查
		//if(minseq!=1 || maxseq-minseq+1!=mt.GetLong("ct",0)) return false;//文件不完整
		//
		//if(srn!=mt.GetDouble("srn",0)) {
	  //  lgprintf("记录数不一致，中间数据为%.0f行,数据文件为%.0f行.\n",srn,mt.GetDouble("srn",0));
	  //  return false;
	  //	}
		mt.FetchAll( "select timestampdiff(MINUTE,now(),date_add(date_add('%04s-%02s-%02s',interval 1 day),interval '%d:%d' hour_minute))",
			yyyy,mm,dd,pWrapper->getOption("files:enddelay",600)/60,pWrapper->getOption("files:enddelay",600)%60
		);
		mt.Wait();
		LONG64 tm1=mt.GetLong(0,0);
	  lgprintf("距离截止时间:%ld.\n",tm1);
		if(mt.GetLong(0,0)>=0) return false;//还没到延迟时间
		mt.FetchAll( "select timestampdiff(MINUTE,now(),date_add(max(endproctime),interval %d minute)) from dp.dp_filelog where tabid=%d and datapartid=%d",
			pWrapper->getOption("files:endcheckdelay",45),tabid,datapartid);
		if(mt.Wait()!=1) return false;
		tm1=mt.GetLong(0,0);
	  lgprintf("距离文件延迟:%ld.\n",tm1);
		if(mt.GetLong(0,0)>=0) return false;//还没到最后一个文件处理结束的延迟时间
		lgprintf("文件抽取结束，共%d个文件(序列号%d-%d)。",filect,minseq,maxseq);
		return true;
	}
	
	virtual int GetFileSeq(const char *filename) {
			// Jira-49：add two control parameters to file load .
			char seqbuf[50];		
			int backoff=pWrapper->getOption("files:sequencebackoffset",0);
			int seqlen=pWrapper->getOption("files:sequencelen",4);
			if(backoff<0) backoff=0;
			const char *pseq=filename+strlen(filename)-backoff-1;
			while(!isdigit(*pseq) && pseq>filename) pseq--;
			if(pseq-filename<seqlen) seqlen=pseq-filename;
			strncpy(seqbuf,pseq-seqlen,seqlen);
			pseq=seqbuf;
			while(*pseq) {
				if(!isdigit(*pseq)) {
					strcpy((char *)pseq,pseq+1);
				}
				else pseq++;
			}
			return atoi(seqbuf);
	}
	// return value:
	//  -1: a error occurs.例如内存表字段结构不匹配
	//  0:memory table has been filled full,and need to parse again
	//  1:file parse end
	//while parser file ended,this function record file process log and 
	// sequence of file in dp dictions tab.
	int DoParse(int memtab,SysAdmin &sp,int tabid,int datapartid) {
		AutoMt mt(0,10);
		mt.SetHandle(memtab,true);
		if(fp==NULL) {
			fp=fopen(filename.c_str(),"rt");
			if(fp==NULL) ThrowWith("文件'%s'打开失败!",filename.c_str());
			//检查字段名称
			strcpy(line,pWrapper->getOption("files:header",""));
			if(strlen(line)<1) {
			 if(fgets(line,MAXLINELEN,fp)==NULL) ThrowWith("读文件'%s'失败!",filename.c_str());
			 curoffset=1;
			}
			else curoffset=0;
			int sl=strlen(line);
			if(line[sl-1]=='\n') line[sl-1]=0;
			colct=wociConvertColStrToInt(memtab,line,colspos);
			filerows=0;
			lgprintf("文件'%s'开始处理.",filename.c_str());
		}
		int rows=mt.GetRows();
		int maxrows=mt.GetMaxRows();
		char colname[300];
		char ipaddr[20];
		while(rows<maxrows && fgets(line,MAXLINELEN,fp)){
			curoffset++;
			int sl=strlen(line);
			if(line[sl-1]=='\n') line[sl-1]=0;
			if(line[0]==0) continue; //允许空行
			char *ptrs[300];
			int ct=1;
			char *ps=line;
			ptrs[0]=line;
			while(*ps) {
			  if(*ps++==',') {
				 ps[-1]=0;ptrs[ct++]=ps;
				}
			}
			if(ct!=colct) {
				//ThrowWith("文件'%s'第%d行格式错误,需要%d个字段，实际有%d个.",filename.c_str(),
				//   curoffset,colct,ct);
				lgprintf("文件'%s'第%d行格式错误,需要%d个字段，实际有%d个,忽略...",filename.c_str(),
				   curoffset,colct,ct);
			  sp.log(tabid,datapartid,106,"文件'%s'第%d行格式错误,需要%d个字段，实际有%d个,忽略...",filename.c_str(),
				   curoffset,colct,ct);
				continue;
			}
			for(int i=0;i<colct;i++) {
				switch(mt.getColType(colspos[i])) {
 			case COLUMN_TYPE_CHAR	:
 				wociGetColumnName(mt,colspos[i],colname);
 				if(strcasecmp(colname+strlen(colname)-3,"_IP")==0 && strchr(ptrs[i],'.')==NULL) {
 					int ip=atoi(ptrs[i]);
 					unsigned char *bip=(unsigned char*)&ip;
 				  sprintf(ipaddr,"%u.%u.%u.%u",bip[3],bip[2],bip[1],bip[0]);
 				  if(ip==0) ipaddr[0]=0;
					strcpy(mt.PtrStr(colspos[i],rows),ipaddr);
 				}
				else {
					char *trimp=ptrs[i];
					int olen=strlen(trimp)-1;
					while(trimp[olen]==' ') trimp[olen--]=0;
					while(*trimp==' ') trimp++;
					if(strlen(trimp)>mt.getColLen(colspos[i])) {	
				 ThrowWith("文件'%s'第%d行第%d字段超过定义的长度(%d,%d)！",filename.c_str(),
				    curoffset,i+1,strlen(ptrs[i]),mt.getColLen(colspos[i]));
					}
				  strcpy(mt.PtrStr(colspos[i],rows),trimp);
				}
				break;
			case COLUMN_TYPE_FLOAT	:
				*mt.PtrDouble(colspos[i],rows)=atof(ptrs[i]);
				break;
			case COLUMN_TYPE_INT	:
				*mt.PtrInt(colspos[i],rows)=atoi(ptrs[i]);
				break;
			case COLUMN_TYPE_BIGINT	:
				/* HP-UX no atoll 
				*mt.PtrLong(colspos[i],rows)=atoll(ptrs[i]);
				*/
				*mt.PtrLong(colspos[i],rows)=atol(ptrs[i]);
				break;
			case COLUMN_TYPE_DATE	:{
				int of=0,y,m,d,hh,mm,ss;
				char *dptr=ptrs[i];
				y=atoi(dptr+of);of+=5;
				m=atoi(dptr+of);of+=(dptr[of+1]>='0' and dptr[of+1]<='9')?3:2;
				d=atoi(dptr+of);of+=(dptr[of+1]>='0' and dptr[of+1]<='9')?3:2;
				hh=atoi(dptr+of);of+=(dptr[of+1]>='0' and dptr[of+1]<='9')?3:2;
				mm=atoi(dptr+of);of+=(dptr[of+1]>='0' and dptr[of+1]<='9')?3:2;
				ss=atoi(dptr+of);
				wociSetDateTime(mt.PtrDate(colspos[i],rows),
				  y,m,d,hh,mm,ss);
				}				
				break;
			case COLUMN_TYPE_NUM	:
			default :
  	    ThrowWith("Invalid column type :%d,id:%d",mt.getColType(colspos[i]),colspos[i]);
		    break;
			  }
		}
		filerows++;
	  rows++;
	 }
	 _wdbiSetMTRows(mt,rows);
	 if(rows==maxrows) return 0;
	 AutoStmt st(dbc);
	 st.Prepare("update dp.dp_filelog set status=1,recordnum=%d,endproctime=now() where tabid=%d and datapartid=%d and fileseq=%d",
	  	     filerows,tabid,datapartid,fileseq);
	 st.Execute(1);
	 st.Wait();
	 lgprintf("文件'%s'处理结束，共%d行记录，文件%d行。",filename.c_str(),filerows,curoffset);
	 fclose(fp);fp=NULL;curoffset=0;
         if(pWrapper->getOption("files:keeppickfile",0)!=1)
	  unlink(filename.c_str());
	 return 1;
	}
};
FileParser *BuildParser(void);
#endif
