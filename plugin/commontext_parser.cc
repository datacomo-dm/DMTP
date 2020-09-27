#include "dt_common.h"
#include "AutoHandle.h"
#include "commontxt_parser.h"
#include <string.h> 

extern "C" uLongCRC crc32(uLongCRC crc, const Bytef * buf, uInt len);

IFileParser *BuildParser(void) {
	return new CommonTxtParser();
}

#define ExtractDate(optionstr,value,len,func)	{\
	const char *opt=pWrapper->getOption(optionstr,"");\
	int offset=0;\
	/* value=-1;  // warning:delete 20130114, Fix bug DM-206 */  \
	if(strlen(opt)>0) {\
	offset=atoi(opt);\
	if(opt[strlen(opt)-1]=='d') {\
	value=ExtractNum(offset,dbname,len);\
	} \
		else if(opt[strlen(opt)-1]=='t') { \
		value=ExtractNum(offset,tabname,len); \
} \
		else if(opt[0]=='p') { \
		value=func(datapartdate);\
} \
		else value=atoi(opt);\
	}\
}

int CommonTxtParser::ExtractNum(int offset,const char *info,int len) {
	int value =0;
	char numchar[20];
	if(offset<0) {
		strncpy(numchar,info+strlen(info)+offset,len);
		numchar[len]=0;
		value=atoi(numchar);
	}
	else {
		strncpy(numchar,info+offset,len);
		numchar[len]=0;
		value=atoi(numchar);
	}
	return value;
}
bool CommonTxtParser::SwitchOpt(char *deststr,char *opt,int val,int len)
{
	char sourcestr[300];
	strcpy(sourcestr,deststr);
	char *strdest=strcasestr(sourcestr,opt);
	if(strdest) {
		char patt[20];
		sprintf(patt,"%%0%dd",len);
		if(val<0) ThrowWith("δ����files:%s����",opt+1);
		strncpy(deststr,sourcestr,strdest-sourcestr);
		deststr[strdest-sourcestr]=0;

		sprintf(deststr+strlen(deststr),patt,val);
		strcat(deststr,sourcestr+(strdest-sourcestr)+strlen(opt));
		return true;
	}
	return false;
}
bool CommonTxtParser::SwitchOpt(char *deststr,char *opt,char *value) {
	char sourcestr[300];
	strcpy(sourcestr,deststr);
	char *strdest=strcasestr(sourcestr,opt);
	if(strdest) {
		//char patt[10];
		//sprintf(patt,"%%0%dd",len);
		if(!value) ThrowWith("����%s��ֵ��Ч",opt+1);
		strncpy(deststr,sourcestr,strdest-sourcestr);
		deststr[strdest-sourcestr]=0;
		strcat(deststr,value);
		strcat(deststr,sourcestr+(strdest-sourcestr)+strlen(opt));
		return true;
	}
	return false;
}


// fill filename member,the wrapper check conflict concurrent process,
// if one has privillege to process current file ,then invoke DoParse
// ����ֵ��
//  -1 ������������������״̬�Ѿ��������������޸�Ϊȡ��������ͣ
//  0�������ļ�
//  1��ȡ����Ч���ļ�
// checkonly : dma-111,ֻ�����û���ļ������ɼ�
int CommonTxtParser::GetFile(bool frombackup,SysAdmin &sp,int tabid,int datapartid,bool checkonly) {
	AutoMt mt(dbc,10);
	mt.FetchAll("select databasename,tabname from dp.dp_table where tabid=%d",tabid);
	if(mt.Wait()<1) ThrowWith("Table id:%d not found.",tabid);
	char tabname[200],dbname[200];
	strcpy(dbname,mt.PtrStr(0,0));
	strcpy(tabname,mt.PtrStr(1,0));
	char datapartdate[30];
	mt.FetchAll("select begintime from dp.dp_datapart where tabid=%d and datapartid=%d",tabid,datapartid);
	if(mt.Wait()<1) ThrowWith("Table id:%d,datapart:%d not found in dp.dp_datapart.",tabid,datapartid);
	memcpy(datapartdate,mt.PtrDate(0,0),7);
	ExtractDate("files:year",yyyy,4,wociGetYear);
	ExtractDate("files:month",mm,2,wociGetMonth);  
	ExtractDate("files:day",dd,2,wociGetDay);  
	ExtractDate("files:hour",hour,2,wociGetHour);  
	char filepatt[300];
	strcpy(filepatt,pWrapper->getOption("files:filename",""));
	while(true) {
		if(SwitchOpt(filepatt,"$year",yyyy,4)) continue;
		if(SwitchOpt(filepatt,"$month",mm,2)) continue;
		if(SwitchOpt(filepatt,"$day",dd,2)) continue;
		if(SwitchOpt(filepatt,"$hour",hour,2)) continue;
		if(SwitchOpt(filepatt,"$table",tabname)) continue;
		if(SwitchOpt(filepatt,"$db",dbname)) continue;
		break;
	}
	
	// Begin:�����Ƿ�ɾ��Դ�ļ���Ĭ��ɾ��[1:ɾ����0:��ɾ��] 2012-11-02
	fm.SetDeleteFileFlag(pWrapper->getOption("files:delsrcdata",0));
	// End: �����Ƿ�ɾ��Դ�ļ���Ĭ��ɾ��[1:ɾ����0:��ɾ��]
	
	// if not set datetime ,set to now
	char now[10];
	wociGetCurDateTime(now);
	if(yyyy<0) yyyy=wociGetYear(now);
	if(mm<0) mm=wociGetMonth(now);
	if(dd<0) dd=wociGetDay(now);
	if(hour<0) hour=wociGetHour(now);
	char backuppath[300];
	sprintf(backuppath,"%s/%s",
		pWrapper->getOption("files:backuppath","./"),tabname);
	if(datapartid!=1) sprintf(backuppath+strlen(backuppath),"/part%d",datapartid);
	xmkdir(backuppath);
	int rt=commonGetFile(backuppath,false,filepatt,sp,tabid,datapartid,checkonly);
	if(rt==0&&frombackup) rt=commonGetFile(backuppath,true,filepatt,sp,tabid,datapartid,checkonly);
	return rt;
}
// if end ,update task to next step(with lock)
// critical function !
// checked every file parser completed.
// �����ǲ������ļ���Ҫ�����ʱ�򣬲��ܵ��������麯��
bool CommonTxtParser::ExtractEnd() {
	wociSetEcho(false);
	AutoMt mt(dbc,10);
	//���ܼ�and procstate<=1 ����������һ�����ļ���û�н���״̬1
	mt.FetchAll("select ifnull(sum(recordnum),0) srn from dp.dp_middledatafile where tabid=%d and datapartid=%d ",
		tabid,datapartid);
	mt.Wait();
	// ע������һ�� ԭ�� DMA-64
	//if(  mt.GetDouble("srn",0)==0) return false; //�ļ���û�д����ꡣ
	double srn=mt.GetDouble("srn",0);
	mt.FetchAll("select case status when 1 then 2 else status end status,max(fileseq) xseq,min(fileseq) mseq,count(*) ct,sum(recordnum) srn "
		" from dp.dp_filelog where tabid=%d and datapartid=%d group by case status when 1 then 2 else status end ",
		tabid,datapartid);
	int rn = mt.Wait();
        int maxseq=0;
        int minseq=0;
        LONG64 filect=0;
	if(rn != 0){
		if(mt.Wait()!=1) return false; //�ļ���û�д����ꡣ
		if(mt.GetDouble("status",0)!=2) return false;//�ļ���û�д����ꡣ

		maxseq=mt.GetInt("xseq",0);
		minseq=mt.GetInt("mseq",0);
		filect=mt.GetLong("ct",0);
		lgprintf("��С�ļ���:%d,����ļ���:%d,�ļ���:%ld.",minseq,maxseq,mt.GetLong("ct",0));
	}
	else{
		// TODO: nothing ,fix dma-700
	}
	//Ӧ�ƶ�Ҫ��ȥ����������Լ��
	
	// Begin: �����crc�������ļ���ţ�������У��
	if(pWrapper->getOption("files:crcsequence",0)==0 && rn>0 )
	{
		  // �Ƿ�У�����
			if(pWrapper->getOption("files:sequencecheck",1)!=0) {
				if(minseq!=1 || maxseq-minseq+1!=mt.GetLong("ct",0)) {
					lgprintf("�ļ���Ų�������Ӧ�������1-%d,Ӧ����%d���ļ���ʵ����%d���ļ�....",
					     maxseq,maxseq,mt.GetLong("ct",0));
					return false;//�ļ�������
				}
			}		
    }
  // End: �����crc�������ļ���ţ�������У��
  
	/* ��¼���ļ�����������֣�
	�ļ��Ѿ��ֽ⵽�ڴ�������ڴ��û������û��Ԥ����Ҳ��û��д��dp_middledatafile��
	��˼�¼���˶��쳣
	DMA-64
	if(pWrapper->getOption("recordnumcheck",1)!=0) {
	if(srn!=mt.GetDouble("srn",0)) {
	lgprintf("��¼����һ�£��м�����Ϊ%.0f��,�����ļ�Ϊ%.0f��.\n",srn,mt.GetDouble("srn",0));
	return false;
	}
	}
	*/
	if(pWrapper->getOption("files:forceenddump",0)!=1) {
		mt.FetchAll("select timestampdiff(MINUTE,now(),date_add(date_add('%04d-%02d-%02d',interval 1 day),interval '%d:%d' hour_minute))",
			yyyy,mm,dd,pWrapper->getOption("files:enddelay",600)/60,pWrapper->getOption("files:enddelay",600)%60);
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
	}	
	lgprintf("�ļ���ȡ��������%d���ļ�(���к�%d-%d)��",filect,minseq,maxseq);
	return true;
}

int CommonTxtParser::GetFileSeq(const char *filename) {
	// Jira-49��add two control parameters to file load .
	char seqbuf[50];		
	int backoff=pWrapper->getOption("files:sequencebackoffset",0);
	int seqlen=pWrapper->getOption("files:sequencelen",4);
	
	//Begin: �ļ����crc����
	if(pWrapper->getOption("files:crcsequence",0)==1)
	{
	    int fileSeq = 0; // �ļ����
		fileSeq = crc32(0,(const Bytef *)filename,(uInt)strlen(filename));
		return fileSeq;
	}
	//End:�ļ����crc����
	
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
int CommonTxtParser::parsercolumn(int memtab,const char *cols,int *colsflag) {
	memset(colsflag,0,sizeof(int)*wociGetColumnNumber(memtab));
	if(!cols || strlen(cols)<1) return 0;
	int colct=0;
	int colarray[MAX_COLUMN];
	int cvtct=wociConvertColStrToInt(memtab,cols,colarray);
	for(int i=0;i<cvtct;i++)
		colsflag[colarray[i]]=1;
	return cvtct;
}
// fixflag collen use 0 base index
int CommonTxtParser::parserfixlen(int memtab,const char *cols,int *fixflag,int *collen){
	//split colname
	memset(fixflag,0,sizeof(int)*wociGetColumnNumber(memtab));
	memset(collen,0,sizeof(int)*wociGetColumnNumber(memtab));
	if(!cols || strlen(cols)<3) return 0;
	char colname[COLNAME_LEN];
	const char *pcol=cols;
	int cni=0;
	int colct=0;
	memset(fixflag,0,sizeof(int)*wociGetColumnNumber(memtab));
	memset(collen,0,sizeof(int)*wociGetColumnNumber(memtab));
	while(*pcol) {
		if(*pcol==':' || !pcol[1]) {
			colname[cni]=0;
			cni=0;
			int idx=wociGetColumnPosByName(memtab,colname);
			fixflag[idx]=1;
			collen[idx]=atoi(pcol+1);
			colct++;
			while(*pcol && *pcol!=',') pcol++;
			if(!pcol) break;
		}
		else colname[cni++]=*pcol;
		pcol++;
	}
	return colct;	
}

// return value:
//  -1: a error occurs.�����ڴ���ֶνṹ��ƥ��
//  0:memory table has been filled full,and need to parse again
//  1:file parse end
//while parser file ended,this function record file process log and 
// sequence of file in dp dictions tab.
int CommonTxtParser::WriteErrorData(const char *line) {
	char errfile[300];
	strcpy(errfile,pWrapper->getOption("files:errordatapath",""));
	//δ���ô����ļ����·�����򲻱����������
	if(errfile[0]==0) return 0;
	if(errfile[strlen(errfile)-1]!='/') strcat(errfile,"/");
	strcat(errfile,basefilename.c_str());
	strcat(errfile,".err");
	FILE *err=fopen(errfile,"a+t");
	if(err==NULL) {
		lgprintf("��������ļ�'%s'ʧ�ܣ�����Ȩ��!",errfile);
	}
	//����Ҫ�ӻس�
	if(fputs(line,err)<1)
		lgprintf("��������ļ�'%s'ʧ�ܣ��������ļ�ϵͳ��!",errfile);
	fclose(err);
	return 1;
}

int CommonTxtParser::DoParse(int memtab,SysAdmin &sp,int tabid,int datapartid) {
	AutoMt mt(0,10);
	mt.SetHandle(memtab,true);
	char sepchar[10];
	errinfile=0;
	int ignore_rows = 0;
	if(fp==NULL)
	{
		fp=fopen(filename.c_str(),"rt");
		if(fp==NULL) ThrowWith("�ļ�'%s'��ʧ��!",filename.c_str());
		//����ֶ�����
		strcpy(line,pWrapper->getOption("files:header",""));
		if(strlen(line)<1) {
			if(fgets(line,MAXLINELEN,fp)==NULL) ThrowWith("���ļ�'%s'ʧ��!",filename.c_str());
			curoffset=1;  //����
		}
		else curoffset=0;  //����
		int sl=strlen(line);   //��¼����
		if(line[sl-1]=='\n') line[sl-1]=0;
		if(line[0]=='*') {
			colct=wociGetColumnNumber(memtab);  //��ȡ�ֶ���
			for(int i=0;i<colct;i++) 
				colspos[i]=i;
		}
		else colct=wociConvertColStrToInt(memtab,line,colspos);  //��ȡ�ֶ���

        //>> begin DMA-604 : ������Ҫ�����п�ȵ���,20130411
        strcpy(line,pWrapper->getOption("files:adjustcolumnswidth",""));
		if(strlen(line)<1){
		    for(int _i = 0;_i<colct;_i++){  // û����Ҫ�����п�ȵ���
				colsAdjustWidth[_i] = false;
		    }
		}
        else
        {
		    sl=strlen(line);  
            if(line[sl-1]=='\n') line[sl-1]=0;
            
		    //1. �����ַ������ж���Ҫ�������
            if(line[0]=='*')
            {
               for(int _i = 0 ;_i<colct;_i++)  // ֻ���ַ���������Ҫ����
               {
                   if( COLUMN_TYPE_CHAR == mt.getColType(colspos[_i]))
		    	   	    		colsAdjustWidth[_i] = true; 
		    	   			 else 
		    	        		colsAdjustWidth[_i] = false; 
               }
            }
            
            //2. ��������Ҫ�������
            size_t  separater_pos = 0;
            std::string columnsNameInfo = line;
		    // ���һ��","����β�������㴦�����һ��
		    columnsNameInfo += ", ";
		    #define separater_items  ','
            std::string columnItem;
            separater_pos = columnsNameInfo.find_first_of(separater_items);
            int colindex = 0;
            while (separater_pos != std::string::npos)
	        { 
		        columnItem = columnsNameInfo.substr(0,separater_pos);
      	    	columnsNameInfo = columnsNameInfo.substr(separater_pos+1,columnsNameInfo.size()-separater_pos-1);
		        separater_pos = columnsNameInfo.find_first_of(separater_items);
		    	
		        // �ж���������Ƿ���ڣ���������ھ���ʾ����
		        bool _exist = false;
                for(int _i = 0;_i<colct;_i++)
                {
                    char _cn[256];
                    wociGetColumnName(memtab,colspos[_i],_cn);
                    if(strcasecmp(_cn,columnItem.c_str()) == 0)
                    {
                        _exist = true;
            
		    			// �ж��������Ƿ���ȷ����������Ͳ���ȷ���˳�
                        if(COLUMN_TYPE_CHAR != mt.getColType(colspos[_i]))
                        {
                            ThrowWith("����[%s]�ֶ����Ͳ����ַ�����,��֧���Զ������п��.",columnItem.c_str());
                        }
		    								colsAdjustWidth[_i] = true;
                        break;
                    }
		    						else{
		    							colsAdjustWidth[_i] = false;
		    						}
                }
            
		    	// û���ҵ�����
                if(!_exist){
                    ThrowWith("����[%s]�ֶβ�����Ч�ֶΣ���֧���Զ������п��.",columnItem.c_str());
                }			
            }
        }		
		//<< end: DMA-604 ������Ҫ�����п�ȵ���
		
		filerows=0;
		lgprintf("�ļ�'%s'��ʼ����.",filename.c_str());
	}
	int rows=mt.GetRows();
	int maxrows=mt.GetMaxRows();
	char colname[300];
	char ipaddr[20];

/****************************20120920**********DM-152********************/
	char separator[6] = {0};
	int separator_len = 0;
	char enclosedby = pWrapper->getOption("files:enclosedby",'\x0');
	strcpy(separator, pWrapper->getOption("files:separator",""));
	separator_len = strlen(separator);
	bool ischaracter = strlen(separator) == 1;
/****************************20120920********DM-152**********************/

	char escape=pWrapper->getOption("files:escape",'\x0');
	bool fixedmode=pWrapper->getOption("files:fixedmode",0)==1;
	int isfixed[MAX_COLUMN],fixlen[MAX_COLUMN];
	int fixcols=parserfixlen(memtab,pWrapper->getOption("files:fixlengthcols",""),isfixed,fixlen);
	int asciicvt[MAX_COLUMN];
	bool ipcheck=pWrapper->getOption("files:ipconvert",0)==1;
	int asccvt=parsercolumn(memtab,pWrapper->getOption("files:asciivalues",""),asciicvt);
	if(fixedmode) ThrowWith("�����ļ���ʽ�ݲ�֧��!");
	int valuelen[MAX_COLUMN];
	int recordfmtcheck=pWrapper->getOption("files:recordfmtcheck",1);
	int postoperculum = pWrapper->getOption("files:postoperculum",0)==1; //0��ʾĩβ�����ָ���
	bool ignoreLastColAnalyse = pWrapper->getOption("files:ignorelastcolanalyse",0) == 1;// �������һ��ת���ַ��ͷָ����ķ�����JIRA DMA-474  

	while(rows<maxrows && fgets(line,MAXLINELEN,fp))
	{
		curoffset++; //�к�
		int sl=strlen(line); //��¼����
		strcpy(linebak,line);
/****************************20120920******DM-152************************/
		//truncate \r\n seq:
		if(line[sl-1]=='\n' || line[sl-1]=='\r') line[--sl]=0;
		if(line[sl-1]=='\n' || line[sl-1]=='\r') line[--sl]=0;
		if(postoperculum && 0 == strncmp(line + (sl - separator_len), separator, separator_len)){
	  		line[sl - separator_len]=0;
			  sl -= separator_len;
	  }
/****************************20120920*******DM-152***********************/

		if(line[0]==0) continue; //�������
		char *ptrs[MAX_FILE_COLUMNS];
		char *ps=line;   //ͷָ��
		char *psle=line + sl -1;  //βָ��
		memset(valuelen,0,colct*sizeof(int));
		ptrs[0]=line;
		bool enclose=false;
		int ct=0;  //ʵ���ֶ���
		//initial first column
		ptrs[ct]=ps;
		bool isthisfix=isfixed[colspos[ct]];
		bool skipline=false;
		//�����ֶβ���ת������
		while(*ps) 
		{
			if(isthisfix) 
			{
					//������ʽ
					char *pse=ps+fixlen[colspos[ct]]-1;
					if(pse>psle) 
					{
							if(recordfmtcheck!=0) 
							{
								ThrowWith("�ļ�'%s'��%d�и�ʽ����,�����ֶ�(%d)�������еĳ���%d.",filename.c_str(),
									curoffset,ct+1,pse-psle);
							}
							lgprintf("�ļ�'%s'��%d�и�ʽ����,�����ֶ�(%d)�������еĳ���%d.",filename.c_str(),
								curoffset,ct+1,pse-psle);
							skipline=true;
							errinfile++;
							WriteErrorData(linebak);
							/*
							sp.log(tabid,datapartid,106,"�ļ�'%s'��%d�и�ʽ����,�����ֶ�(%d)�������еĳ���%d.",filename.c_str(),
							curoffset,ct+1,pse-psle);*/
							break;
					}
					
					valuelen[colspos[ct]]=pse-ps+1;
					//�����ֶβ�֧�ַ�װ�ַ����ţ���
					//if(*ps==enclosedby && *pse==enclosedby && enclosedby!=0) {
					// valuelen[colspos[ct]]-=2;ptrs[ct]++;
					//}
					ps+=fixlen[colspos[ct]];
				
/****************************20120920***DM-152***************************/
					if(!ischaracter){
						/* string */
						/*trim left separator*/
						if(0 == strncmp(ps, separator, separator_len)){
							ps += separator_len;
						}
					}
					else{
						/* charactor */
						if(*ps==separator[0] && separator[0]!=0){ ps++;}
					}			
					ptrs[++ct]=ps;
					isthisfix=isfixed[colspos[ct]];
			}
			//���������ת�壿
		 	else 
		 	{
				//�䳤��ʽ
				if(*ps==enclosedby && enclosedby!=0) {
					ps++;
					if(/*enclose && */ps[-2]==escape &&escape!=0) {strcpy(ps-2,ps-1);continue;}
					if(enclose) {ps[-1]=0;enclose=false;continue;}
					ps[-1]=0;ptrs[ct]=ps; //delete enclosed char and 
					enclose=true;
				}
/****************************20120920*****DM-152*************************/
				if(!ischaracter)
				{
					// string 				
					/* trim left separator */
					if(0 == strncmp(ps, separator, separator_len))
					{
						ps += separator_len;
					 if(ps[-separator_len] == escape && escape!=0)
						{
							strcpy(ps - separator_len, ps); continue;
						}
						memset(ps-separator_len, 0x0, separator_len);
						ptrs[++ct]=ps;
						isthisfix=isfixed[colspos[ct]];
					}
					else ps++;						
				} // end if(!ischaracter)
				else
				{
					/*character */
					if(*ps++==separator[0] && !enclose) 
					{
						if(ps[-2]==escape && escape!=0) 
						{
							strcpy(ps-2,ps-1);continue;
						}
						ps[-1]=0;
						ptrs[++ct]=ps;
						isthisfix=isfixed[colspos[ct]];
					}	
		    }	// end if(ischaracter)
		 }// end else
		 
		 //>> Begin: fix jira dma-474
		 if(ignoreLastColAnalyse && (ct == colct-1)){
		    break;	
		 }
		    //<< End: fix jira dma-474
		}// end while(*ps)
		
		if(skipline) {continue;}
		if(!isfixed[colspos[colct-1]]) ct++;
		//POSITION;PRINTF("ct:%d; colct:%d\n", ct, colct);
		if(ct!=colct) {
			if(recordfmtcheck!=0) {
				ThrowWith("�ļ�'%s'��%d�и�ʽ����,��Ҫ%d���ֶΣ�ʵ����%d��.",filename.c_str(),
					curoffset,colct,ct);
			}
			lgprintf("�ļ�'%s'��%d�и�ʽ����,��Ҫ%d���ֶΣ�ʵ����%d��,����...",filename.c_str(),
				curoffset,colct,ct);
			/*sp.log(tabid,datapartid,106,"�ļ�'%s'��%d�и�ʽ����,��Ҫ%d���ֶΣ�ʵ����%d��,����...",filename.c_str(),
			curoffset,colct,ct);*/
			errinfile++;
			WriteErrorData(linebak);
			continue;
		}
		for(int i=0;i<colct;i++) {
			char *ptrf=ptrs[i];
			if(isfixed[colspos[i]]) {
				//�����ֶ�����û�зָ���������ֶε�ֵ�п���δ�ضϣ���Ҫ��������
				memcpy(fieldval,ptrs[i],valuelen[colspos[i]]);
				fieldval[valuelen[colspos[i]]]=0;
				ptrf=fieldval;
			}
			char *trimp=ptrf;
			int olen=strlen(trimp)-1;
			while(trimp[olen]==' ') trimp[olen--]=0;
			while(*trimp==' ') trimp++;
			
			
			if(*trimp==0) {
				wociSetNull(mt,colspos[i],rows,true);
				//continue;
			}
			
			switch(mt.getColType(colspos[i])) {
			case COLUMN_TYPE_CHAR	:
				if(ipcheck) {
					wociGetColumnName(mt,colspos[i],colname);
					if(strcasecmp(colname+strlen(colname)-3,"_IP")==0 && strchr(ptrf,'.')==NULL) {
						int ip=atoi(ptrs[i]);
						unsigned char *bip=(unsigned char*)&ip;
						//TODO : this only work on little-endian system
						sprintf(ipaddr,"%u.%u.%u.%u",bip[3],bip[2],bip[1],bip[0]);
						if(ip==0) ipaddr[0]=0;
						strcpy(mt.PtrStr(colspos[i],rows),ipaddr);
					}
				}
				else
				{
					//ascii��ת��
					if(asciicvt[colspos[i]]) {
						if(strlen(trimp)>3) {
							if(recordfmtcheck!=0) 
								ThrowWith("ASCII ת��ֻ֧��һλ���ļ�'%s'��%d�е�%d�ֶε�ֵ'%s'�޷�ת��",
								filename.c_str(),curoffset,i+1,trimp);
							    lgprintf  ("ASCII ת��ֻ֧��һλ���ļ�'%s'��%d�е�%d�ֶε�ֵ'%s'�޷�ת��",
								filename.c_str(),curoffset,i+1,trimp);
							errinfile++;
							WriteErrorData(linebak);skipline=true;
						}
						char chr[2];
						chr[1]=0;
						chr[0]=atoi(trimp);
						strcpy(mt.PtrStr(colspos[i],rows),chr);
					}
					else 
					{		
              // ���ȳ���Ŀ�����п�ȣ�����Ҫ�ضϴ����
              int _trimpLen = strlen(trimp);
              if(colsAdjustWidth[i])
              {
							   if( _trimpLen > mt.getColLen(colspos[i])-1)
	               {                 
	                  strncpy(mt.PtrStr(colspos[i],rows),trimp,(mt.getColLen(colspos[i])-1));
								    mt.PtrStr(colspos[i],rows)[mt.getColLen(colspos[i])-1] = 0;
								    lgprintf("�ļ�'%s'��%d�е�%d�ֶγ�������ĳ���(%d,%d),�ѽ�ȡ��Ч����(%d).",filename.c_str(),
								 	    curoffset,i+1,strlen(ptrs[i]),mt.getColLen(colspos[i])-1,mt.getColLen(colspos[i])-1);
								 	  ignore_rows++;
							   }
							   else{
							 	    strcpy(mt.PtrStr(colspos[i],rows),trimp);
							   }
				     	}
				     	else
				     	{
							  // ����Ҫ�����п�ȣ��ҳ��ȳ����п�ȵ����
							  //BUG? char(1),getColLen got 2
							  if( _trimpLen >mt.getColLen(colspos[i])-1)
						    { 
								  if(recordfmtcheck!=0) 
									ThrowWith("�ļ�'%s'��%d�е�%d�ֶγ�������ĳ���(%d,%d)��",filename.c_str(),
									curoffset,i+1,strlen(ptrs[i]),mt.getColLen(colspos[i])-1);
								  lgprintf("�ļ�'%s'��%d�е�%d�ֶγ�������ĳ���(%d,%d)��",filename.c_str(),
									curoffset,i+1,strlen(ptrs[i]),mt.getColLen(colspos[i])-1);
								  WriteErrorData(linebak);//skipline=true;	 
							  }
							  else {
								  strcpy(mt.PtrStr(colspos[i],rows),trimp);
							  }
						}
					}
				}
				break;
			case COLUMN_TYPE_FLOAT	:
				*mt.PtrDouble(colspos[i],rows)=*trimp==0?0:atof(trimp);
				break;
			case COLUMN_TYPE_INT	:
				*mt.PtrInt(colspos[i],rows)=*trimp==0?0:atoi(trimp);
				break;
			case COLUMN_TYPE_BIGINT	:
				/* HP-UX no atoll 
				*mt.PtrLong(colspos[i],rows)=atoll(ptrs[i]);
				*/
				*mt.PtrLong(colspos[i],rows)=*trimp==0?0:atol(trimp);
				break;
			case COLUMN_TYPE_DATE	: {
				if(*trimp==0) {
					wociSetDateTime(mt.PtrDate(colspos[i],rows),
					1900,1,1,0,0,0);
					break;
				}
				int of=0,y,m,d,hh,mm,ss;
				char *dptr=trimp;
				while(*dptr==' ') dptr++;
				if(*dptr<'0' || *dptr>'9') {
					if(recordfmtcheck!=0) 
						ThrowWith("�ļ�'%s'��%d�е�%d�ֶδ��������(%s)��",filename.c_str(),
						curoffset,i+1,dptr);
					lgprintf("�ļ�'%s'��%d�е�%d�ֶδ��������(%s)��",filename.c_str(),
						curoffset,i+1,dptr);
					errinfile++;
					WriteErrorData(linebak);skipline=true;
					break;
				}
				if(dptr[4]>='0' && dptr[4]<='9') {
					//û�зָ���,���磺2012-11-12 12:12:12,2012/12/12 12/12/12
					char r=dptr[4];
					dptr[4]=0;
					y=atoi(dptr);dptr+=4;
					*dptr=r;r=dptr[2];dptr[2]=0;
					m=atoi(dptr);dptr+=2;
					*dptr=r;r=dptr[2];dptr[2]=0;
					d=atoi(dptr);dptr+=2;
					*dptr=r;r=dptr[2];dptr[2]=0;
					hh=atoi(dptr);dptr+=2;
					*dptr=r;r=dptr[2];dptr[2]=0;
					mm=atoi(dptr);dptr+=2;
					*dptr=r;r=dptr[2];dptr[2]=0;
					ss=atoi(dptr);dptr+=2;
				}
				else {
					// ���ڷָ��������磺2012-11-12 12:12:12,2012/12/12 12/12/12
					y=atoi(dptr+of);of+=5;
					m=atoi(dptr+of);of+=(dptr[of+1]>='0' && dptr[of+1]<='9')?3:2;
					d=atoi(dptr+of);of+=(dptr[of+1]>='0' && dptr[of+1]<='9')?3:2;
					hh=atoi(dptr+of);of+=(dptr[of+1]>='0' && dptr[of+1]<='9')?3:2;
					mm=atoi(dptr+of);of+=(dptr[of+1]>='0' && dptr[of+1]<='9')?3:2;
					ss=atoi(dptr+of);
			  }
			 if(y<1900 || m<1 || m>12 || d<1 || d>31 ||hh<0 || hh>60 || mm<0 || mm>60 ||ss<0 || ss>60) {
						ThrowWith("�ļ�'%s'��%d�е�%d�ֶδ��������(%s)��",filename.c_str(),
						curoffset,i+1,dptr);
					break;
				}
				wociSetDateTime(mt.PtrDate(colspos[i],rows),
					y,m,d,hh,mm,ss);
			}
			break;
			case COLUMN_TYPE_NUM	:
			default :
				ThrowWith("Invalid column type :%d,id:%d",mt.getColType(colspos[i]),colspos[i]);
				break;
			}
			if(skipline) {rows--;break;}
		}  
		filerows++;
		rows++;
	}
	_wdbiSetMTRows(mt,rows);
	if(rows==maxrows) return 0;
	AutoStmt st(dbc);
	/***************�ļ����������¼*****************/
	//��ȡ�������ݴ洢λ��
	{
	  char errfilename[300];
	  strcpy(errfilename,pWrapper->getOption("files:errordatapath","/tmp"));
	  //δ���ô����ļ����·�����򲻱����������
	  if(errfilename[0]==0) return 0;
	  if(errfilename[strlen(errfilename)-1]!='/') strcat(errfilename,"/");
	  strcat(errfilename,basefilename.c_str());
	  strcat(errfilename,".err");
	  st.Prepare("update dp.dp_filelog set status=1,recordnum=%d,endproctime=now(),errrows=%d,errfname='%s',ignorerows=%d where tabid=%d and datapartid=%d and fileseq=%d",filerows,errinfile,errfilename,ignore_rows,tabid,datapartid,fileseq);
  }
	st.Execute(1);
	st.Wait();
	lgprintf("�ļ�'%s'�����������%d�м�¼���ļ�%d�У�����%d�С�",filename.c_str(),filerows,curoffset,errinfile);
	fclose(fp);fp=NULL;
	if(errinfile>0)
		sp.log(tabid,datapartid,106,"�ļ�'%s'�����������%d�м�¼���ļ�%d�У�����%d�С���־�ļ�: %s ",filename.c_str(),filerows,curoffset,errinfile,_wdbiGetLogFile());
	if(pWrapper->getOption("files:keeppickfile",0)!=1)
		unlink(filename.c_str());
		
	//>> Begin: delete src file ,after parsed src file,DM-216
	fm.DeleteSrcFile();
	//<< End:
	
	curoffset=0;
	return 1;
}
