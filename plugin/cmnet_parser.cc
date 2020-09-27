#include "dt_common.h"
#include "AutoHandle.h"
#include "cmnet_parser.h"


IFileParser *BuildParser(void) {
 return new ZJCmnetParser();
}

int ZJCmnetParser::GetFile(bool frombackup,SysAdmin &sp,int tabid,int datapartid,bool checkonly) {
		AutoMt mt(dbc,10);
	  mt.FetchAll("select tabname from dp.dp_table where tabid=%d",tabid);
	  if(mt.Wait()<1) ThrowWith("Table id:%d not found.",tabid);
	  char tabname[200];
	  strcpy(tabname,mt.PtrStr(0,0));
	  //ȡ�����е����� YYYYMMDD
	  char filepatt[300];
	  char backuppath[300];
	  strcpy(yyyy,tabname+strlen(tabname)-8);
	  if(!isdigit(yyyy[0]) || !isdigit(yyyy[1]) ||!isdigit(yyyy[2])||!isdigit(yyyy[3]))
	  	ThrowWith("Ŀ��� '%s' ���Ʊ���Ҫ����(4λ)��(2λ)��(2λ)��β��",tabname);
	  //strncpy(dd,yyyy+6,2);
	  //GXGN
	  //strcpy(dd,"01");
	  strncpy(dd,yyyy+6,2);
	  dd[2]=0;
	  strncpy(mm,yyyy+4,2);
	  mm[2]=0;
	  yyyy[4]=0;
	  sprintf(filepatt,pWrapper->getOption("files:filename","usercdr_%04s-%02s-%02s*.txt.gz.ack"),yyyy,mm,dd);//GXGN ,dd);
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
	// �����ǲ������ļ���Ҫ�����ʱ�򣬲��ܵ��������麯��
	bool ZJCmnetParser::ExtractEnd() {
		wociSetEcho(false);
		AutoMt mt(dbc,10);
		//���ܼ�and procstate<=1 ����������һ�����ļ���û�н���״̬1
		mt.FetchAll("select sum(recordnum) srn from dp.dp_middledatafile where tabid=%d and datapartid=%d ",
		   tabid,datapartid);
		if(mt.Wait()!=1) return false; //�ļ���û�д����ꡣ
		double srn=mt.GetDouble("srn",0);
		mt.FetchAll("select case status when 1 then 2 else status end status,max(fileseq) xseq,min(fileseq) mseq,count(*) ct,sum(recordnum) srn "
		" from dp.dp_filelog where tabid=%d and datapartid=%d group by case status when 1 then 2 else status end ",
		tabid,datapartid);
		if(mt.Wait()!=1) return false; //�ļ���û�д����ꡣ
		if(mt.GetDouble("status",0)!=2) return false;//�ļ���û�д����ꡣ
		int maxseq=mt.GetInt("xseq",0);
		int minseq=mt.GetInt("mseq",0);
		LONG64 filect=mt.GetLong("ct",0);
		lgprintf("��С�ļ���:%d,����ļ���:%d,�ļ���:%ld.",minseq,maxseq,mt.GetLong("ct",0));
		//Ӧ�ƶ�Ҫ��ȥ����������Լ��
		//if(minseq!=1 || maxseq-minseq+1!=mt.GetLong("ct",0)) return false;//�ļ�������
		//
		//if(srn!=mt.GetDouble("srn",0)) {
	  //  lgprintf("��¼����һ�£��м�����Ϊ%.0f��,�����ļ�Ϊ%.0f��.\n",srn,mt.GetDouble("srn",0));
	  //  return false;
	  //	}
		mt.FetchAll( "select timestampdiff(MINUTE,now(),date_add(date_add('%04s-%02s-%02s',interval 1 day),interval '%d:%d' hour_minute))",
			yyyy,mm,dd,pWrapper->getOption("files:enddelay",600)/60,pWrapper->getOption("files:enddelay",600)%60
		);
		mt.Wait();
		LONG64 tm1=mt.GetLong(0,0);
	  lgprintf("�����ֹʱ��:%ld.\n",tm1);
		if(mt.GetLong(0,0)>=0) return false;//��û���ӳ�ʱ��
		mt.FetchAll( "select timestampdiff(MINUTE,now(),date_add(max(endproctime),interval %d minute)) from dp.dp_filelog where tabid=%d and datapartid=%d",
			pWrapper->getOption("files:endcheckdelay",45),tabid,datapartid);
		if(mt.Wait()!=1) return false;
		tm1=mt.GetLong(0,0);
	  lgprintf("�����ļ��ӳ�:%ld.\n",tm1);
		if(mt.GetLong(0,0)>=0) return false;//��û�����һ���ļ�����������ӳ�ʱ��
		lgprintf("�ļ���ȡ��������%d���ļ�(���к�%d-%d)��",filect,minseq,maxseq);
		return true;
	}
	
 int ZJCmnetParser::GetFileSeq(const char *filename) {
			// Jira-49��add two control parameters to file load .
			char seqbuf[50];		
			int backoff=pWrapper->getOption("files:sequencebackoffset",0);
			int seqlen=pWrapper->getOption("files:sequencelen",4);
			if(backoff<0) backoff=0;
			const char *pseq=filename+strlen(filename)-backoff-1;
			while(!isdigit(*pseq) && pseq>filename) pseq--;
			// last digit should be included 
			pseq++;
			if(pseq-filename<seqlen) seqlen=pseq-filename;
			strncpy(seqbuf,pseq-seqlen,seqlen);
			seqbuf[seqlen]=0;
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
	//  -1: a error occurs.�����ڴ���ֶνṹ��ƥ��
	//  0:memory table has been filled full,and need to parse again
	//  1:file parse end
	//while parser file ended,this function record file process log and 
	// sequence of file in dp dictions tab.
	int ZJCmnetParser::DoParse(int memtab,SysAdmin &sp,int tabid,int datapartid) {
		AutoMt mt(0,10);
		mt.SetHandle(memtab,true);
		if(fp==NULL) {
			fp=fopen(filename.c_str(),"rt");
			if(fp==NULL) ThrowWith("�ļ�'%s'��ʧ��!",filename.c_str());
			//����ֶ�����
			strcpy(line,pWrapper->getOption("files:header",""));
			if(strlen(line)<1) {
			 if(fgets(line,MAXLINELEN,fp)==NULL) ThrowWith("���ļ�'%s'ʧ��!",filename.c_str());
			 curoffset=1;
			}
			else curoffset=0;
			int sl=strlen(line);
			if(line[sl-1]=='\n') line[sl-1]=0;
			colct=wociConvertColStrToInt(memtab,line,colspos);
			filerows=0;
			lgprintf("�ļ�'%s'��ʼ����.",filename.c_str());
		}
		int rows=mt.GetRows();
		int maxrows=mt.GetMaxRows();
		char colname[300];
		char ipaddr[20];
		while(rows<maxrows && fgets(line,MAXLINELEN,fp)){
			curoffset++;
			int sl=strlen(line);
			if(line[sl-1]=='\n') line[sl-1]=0;
			if(line[0]==0) continue; //�������
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
				//ThrowWith("�ļ�'%s'��%d�и�ʽ����,��Ҫ%d���ֶΣ�ʵ����%d��.",filename.c_str(),
				//   curoffset,colct,ct);
				lgprintf("�ļ�'%s'��%d�и�ʽ����,��Ҫ%d���ֶΣ�ʵ����%d��,����...",filename.c_str(),
				   curoffset,colct,ct);
			  sp.log(tabid,datapartid,106,"�ļ�'%s'��%d�и�ʽ����,��Ҫ%d���ֶΣ�ʵ����%d��,����...",filename.c_str(),
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
							ThrowWith("�ļ�'%s'��%d�е�%d�ֶγ�������ĳ���(%d,%d)��",filename.c_str(),
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
			case COLUMN_TYPE_DATE	: {
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
	 lgprintf("�ļ�'%s'�����������%d�м�¼���ļ�%d�С�",filename.c_str(),filerows,curoffset);
	 fclose(fp);fp=NULL;curoffset=0;
         if(pWrapper->getOption("files:keeppickfile",0)!=1)
	  unlink(filename.c_str());
	 return 1;
	}


