#include "dt_common.h"
#include "AutoHandle.h"
#include "ztetext_parser.h"
#include "dumpfile.h"
#include <string.h> 

extern "C" uLongCRC crc32(uLongCRC crc, const Bytef * buf, uInt len);

#define zkb_revDouble(V)   { char def_temp[8];\
                          ((mbyte*) &def_temp)[0]=((mbyte*)(V))[7];\
                          ((mbyte*) &def_temp)[1]=((mbyte*)(V))[6];\
                          ((mbyte*) &def_temp)[2]=((mbyte*)(V))[5];\
                          ((mbyte*) &def_temp)[3]=((mbyte*)(V))[4];\
                          ((mbyte*) &def_temp)[4]=((mbyte*)(V))[3];\
                          ((mbyte*) &def_temp)[5]=((mbyte*)(V))[2];\
                          ((mbyte*) &def_temp)[6]=((mbyte*)(V))[1];\
                          ((mbyte*) &def_temp)[7]=((mbyte*)(V))[0];\
                          memcpy(V,def_temp,sizeof(double)); }
       
 #define zkb_revInt(V)   { char def_temp[4];\
                          ((mbyte*) &def_temp)[0]=((mbyte*)(V))[3];\
                          ((mbyte*) &def_temp)[1]=((mbyte*)(V))[2];\
                          ((mbyte*) &def_temp)[2]=((mbyte*)(V))[1];\
                          ((mbyte*) &def_temp)[3]=((mbyte*)(V))[0];\
			              memcpy(V,def_temp,sizeof(int)); }
			                       
 #define zkb_revShort(V)  { char def_temp[2];\
                          ((mbyte*) &def_temp)[0]=((mbyte*)(V))[1];\
                          ((mbyte*) &def_temp)[1]=((mbyte*)(V))[0];\
                          memcpy(V,def_temp,sizeof(short)); }



IFileParser *BuildParser(void) {
	return new ZteTxtParse();
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

int ZteTxtParse::ExtractNum(int offset,const char *info,int len) {
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
bool ZteTxtParse::SwitchOpt(char *deststr,char *opt,int val,int len)
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
bool ZteTxtParse::SwitchOpt(char *deststr,char *opt,char *value) {
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
int ZteTxtParse::GetFile(bool frombackup,SysAdmin &sp,int tabid,int datapartid,bool checkonly) {
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
	// ���ݿ����ļ���ȡ�����յ���Ϣ������yyyy,mm,dd,hour��
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
	fm.SetDeleteFileFlag(pWrapper->getOption("files:delsrcdata",1));
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
bool ZteTxtParse::ExtractEnd() {
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
	if(pWrapper->getOption("files:crcsequence",0)==0)
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
		mt.FetchAll( "select timestampdiff(MINUTE,now(),date_add(date_add('%04d-%02d-%02d',interval 1 day),interval '%d:%d' hour_minute))",
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
	}	
	lgprintf("�ļ���ȡ��������%d���ļ�(���к�%d-%d)��",filect,minseq,maxseq);
	return true;
}

int ZteTxtParse::GetFileSeq(const char *filename) {
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
int ZteTxtParse::parsercolumn(int memtab,const char *cols,int *colsflag) {
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
int ZteTxtParse::parserfixlen(int memtab,const char *cols,int *fixflag,int *collen){
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
int ZteTxtParse::WriteErrorData(const char *line) {
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


// ��ȡ�ַ��������ȣ�add by liujs ���磺 AAA:1,BBB:2,DDD:4,CCC:8,
// ��ȡ1,2,4,8,���ж��Ƿ���ȷ
int ZteTxtParse::GetColumnInfo(const char* pColumnWidthInfo,int memtab,/*char* pColumnsName,*/int * columnsId)
{
    std::string str;
    std::string strColName="";           //����
    std::string strNameTemp,strLenTemp;  //����������
    str = pColumnWidthInfo;              
    int iPos = 0;                           
    int temp = 0;                        
    int first=0;                    
    int colIndex = 0;                   // ������
    int colType =0;                     // ������             
    int colLen = 0;                     // �п��(�����ļ���ָ���Ŀ��)
    iPos = str.find_first_of(':',0);
    while(iPos>0)
    {
        // ��������
        if(first==0)
        {
            strNameTemp = str.substr(0,iPos);
            first=1;
        }
        else
        {
            strColName += ",";
            strNameTemp = str.substr(0,iPos);
        }
        strColName += strNameTemp;
        
        // �Ƴ��Ѿ���ȡ��������
        str = str.substr(iPos+1,str.size());
        
        // ���ҷָ�����,����λ��
        iPos = str.find_first_of(',',0);
        // ��ȡ����
        if(iPos <=0 )
        {
            temp = str.find_first_of(':',0);
            if(temp<=0)
            {
                strLenTemp = str.substr(0,str.size());
            }
            else
            {
                ThrowWith("���������ļ��Ƿ���ȷ�����ݣ�%s!",pColumnWidthInfo);
            }
        }
        else
        {
            strLenTemp = str.substr(0,iPos);
            str = str.substr(iPos+1,str.size());
        }	
        
        // ͨ��������ȡ���������п�ȣ�������
        colIndex = wociGetColumnPosByName(memtab,strNameTemp.c_str());
        colType = wociGetColumnType(memtab,colIndex);
        colLen = atoi(strLenTemp.c_str());
        columnsId[colIndex] = colLen;
           
        // �ж��е������Ƿ���ȷ
        if(COLUMN_TYPE_INT != colType && COLUMN_TYPE_BIGINT != colType)
        {    
            ThrowWith("ת�����������Ͳ��ԣ���ȷ��'%s'�е��������ʹ���!",strNameTemp.c_str());
        }
                
        // ���������ж����ݳ����Ƿ���ȷ
        if(colLen !=1 && colLen != 2 && colLen != 4 && colLen != 8)
        {
            ThrowWith("ת�����������Ϳ�Ȳ��ԣ���ȷ��'%s'�е����ݿ��Ϊ[1,2,4,8]!",strNameTemp.c_str()); 	
        }
        if(COLUMN_TYPE_INT == colType && (0>=colLen || 4<colLen))
        {
            ThrowWith("��ȡ�����ݳ��Ȳ��ԣ���ȷ��'%s'�е����ݳ���Ϊ[1-4]!",strNameTemp.c_str());
        }
        if(COLUMN_TYPE_BIGINT == colType && (4>=colLen || 8<colLen))
        {
            ThrowWith("��ȡ�����ݳ��Ȳ��ԣ���ȷ��'%s'�е����ݳ���Ϊ[1-4]!",strNameTemp.c_str());
        }
                
        // ���ҷָ�����:��
        iPos = str.find_first_of(':',0);
    }
    //strcpy(pColumnsName,strColName.c_str());
    return 0;
}

int ZteTxtParse::DoParse(int memtab,SysAdmin &sp,int tabid,int datapartid) {
	AutoMt mt(0,10);
	mt.SetHandle(memtab,true);
	char sepchar[10];
	errinfile=0;
	if(fp==NULL)
	{
		fp=fopen(filename.c_str(),"rb");
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
		filerows=0;		
		
    #if 1 // add by liujs		
    // begin: ��ȡ��ʵ���п��,�����Ӧ�е�������
    memset(realColLenAry,0x0,MAX_COLUMNS*sizeof(int));
    memset(fileColLenAry,0x0,MAX_COLUMNS*sizeof(int));
    fileRowLen = 0;
    
    // ��ȡ�ļ���ָ���еĿ��
    char * pColumnWidthInfo = NULL;
    pColumnWidthInfo = (char*)malloc(2048);
    strcpy(pColumnWidthInfo,pWrapper->getOption("files:column_width",""));
    GetColumnInfo(pColumnWidthInfo,memtab,fileColLenAry);
    if(NULL != pColumnWidthInfo)
    {
        free(pColumnWidthInfo);
        pColumnWidthInfo = NULL;
    }		
    
    int i,j;
    int colType = 0;
    for(i=0;i<colct;i++) 
    {
        // ��ȡ�����ͣ�������ַ������ͣ����п����Ҫ��1
        colType = wociGetColumnType(memtab,colspos[i]);
        
        // ��Ӧ�е���ʵ����
        if(fileColLenAry[colspos[i]] == 0)
        {
            // û��ָ�����ȵ��ֶΣ���mt�л�ȡ����ȡ��ʱ�򳤶���Ҫ��1
            if(COLUMN_TYPE_CHAR == colType)
            {
                realColLenAry[colspos[i]] = wociGetColumnDataLenByPos(mt,colspos[i]) - 1;
            }
            else if(COLUMN_TYPE_DATE == colType)
            {
            	  // ʱ�䳤��Ϊ14�ֽڵ��ı�---�����ݿ���Ϊ7�ֽڴ洢
            	  realColLenAry[colspos[i]] = 14;
            }
            else
            {
            	  // δָ������������
            	  realColLenAry[colspos[i]] = wociGetColumnDataLenByPos(mt,colspos[i]); 
            }
        }
        else
        {
            // �Ѿ�ָ���ֶεĳ��ȣ����ļ���ָ����
            realColLenAry[colspos[i]] = fileColLenAry[colspos[i]];
        }
    }	
    // ���ǵ�˳��ĵߵ���������������У���������ֶζ���ȡ���ݣ�������벻������
    for(i=0;i<colct;i++) 
    {
        // ��ȡ�����ͣ�������ַ������ͣ����п����Ҫ��1
        colType = wociGetColumnType(memtab,colspos[i]);
        
        // ��ȡ��Ӧ��֮ǰ�������еĳ������ݣ���ȡ���ݿ��д��ڣ��ļ��д��ڣ����ǲ���ȡ�����ݳ���
        for(int j=0;j<colspos[i];j++)
        {
            if(realColLenAry[j] == 0)
            {
               	// û��ָ�����ȵ��ֶΣ���mt�л�ȡ��CHAR��ȡ��ʱ�򳤶���Ҫ��1
                if(COLUMN_TYPE_CHAR == colType)
                {
                    realColLenAry[j] = wociGetColumnDataLenByPos(mt,colspos[j]) - 1;
                }
                else if(COLUMN_TYPE_DATE == colType)
                {
                	  // ʱ�䳤��Ϊ14�ֽڵ��ı�---�����ݿ���Ϊ7�ֽڴ洢
                	  realColLenAry[j] = 14;
                }
                else
                {
                	  // δָ������������
                	  realColLenAry[j] = wociGetColumnDataLenByPos(mt,colspos[j]); 
                }
            }
            else
            {
                // �Ѿ��洢�������еĿ�ȣ�����������ݳ���
                continue;
            }
        }
    }	
    // end����ȡ��ʵ���п��
    
    // begin: ��ȡÿ�ζ�ȡ�����ݳ���
    for(i=0;i<MAX_COLUMNS;i++)
    {
        fileRowLen += realColLenAry[i];
    }
    // end: ��ȡÿ�ζ�ȡ�����ݳ���		
    #endif
		
		lgprintf("�ļ�'%s'��ʼ����.",filename.c_str());
	}
	int rows=mt.GetRows();
	int maxrows=mt.GetMaxRows();
	char colname[300];
	char ipaddr[20];
	
	// colum start pos
	int colPos = 0;
	// colum width
	int colWidth = 0;
	bool ipcheck=pWrapper->getOption("files:ipconvert",0)==1;
	int recordfmtcheck=pWrapper->getOption("files:recordfmtcheck",1);
	
	// ��ȡ����ʱ�������ֶ���
    char temp1[5] = {0};
	char temp2[3] = {0};
	int _pos = 0;
	
    unsigned long value_8B = 0;
    unsigned int value_4B = 0;
    unsigned short value_2B = 0;

	curoffset = 0;
	while(rows<maxrows)
	{
	    curoffset ++;
		if(1!=fread(line,fileRowLen,1,fp))
		{
			 // read error or finish
			 break;
		}
		// ��ȡ��ʼ��ȡ��ƫ���� 
		colPos = 0;
		for(int i=0;i<colct;i++) 
		{   
			// ʵ�ʻ�ȡ���ݵ��п��
			colWidth = realColLenAry[colspos[i]];
			
			switch(mt.getColType(colspos[i])) 
			{
			case COLUMN_TYPE_CHAR	:
				if(ipcheck) 
				{
					wociGetColumnName(mt,colspos[i],colname);
					if(strcasecmp(colname+strlen(colname)-3,"_IP")==0 && strchr((line + colPos),'.')==NULL) {
						int ip=0;
						memcpy(&ip, line + colPos, 4);
						unsigned char *bip=(unsigned char*)&ip;
						//TODO : this only work on little-endian system
						char ipaddr[20] = {0};
						sprintf(ipaddr,"%u.%u.%u.%u",bip[3],bip[2],bip[1],bip[0]);
						if(ip==0) ipaddr[0]=0;
						strcpy(mt.PtrStr(colspos[i],rows),ipaddr);
					}
				}
				else
				{				
					memcpy(mt.PtrStr(colspos[i],rows),line+colPos,colWidth);
				}
				*(mt.PtrStr(colspos[i],rows)+colWidth) = '\0';
				break;
			case COLUMN_TYPE_FLOAT	:
				ThrowWith("Invalid column type :%d,id:%d",mt.getColType(colspos[i]),colspos[i]);
				break;
			case COLUMN_TYPE_INT	:
				if(1 == colWidth)
				{
					*(mt.PtrInt(colspos[i],rows)) = (unsigned char)*(line+colPos);
				}
				else if(2 == colWidth)
				{
					zkb_revShort(line+colPos); 
		            value_2B = 0;
					memcpy(&value_2B,line+colPos,colWidth);
  				   *mt.PtrInt(colspos[i],rows)= value_2B;			      
				}				 		
				else if(4 == colWidth)
				{
					zkb_revInt(line+colPos);
					value_4B = 0;		
					memcpy(&value_4B,line+colPos,colWidth);
					*mt.PtrInt(colspos[i],rows)= value_4B;						
				}
				else{
					ThrowWith("Invalid column type :%d,id:%d",mt.getColType(colspos[i]),colspos[i]);
				}
				break;
			case COLUMN_TYPE_BIGINT	:
				zkb_revDouble(line+colPos);
				value_8B = 0;
				memcpy(&value_8B,line+colPos,colWidth);
				*mt.PtrLong(colspos[i],rows)= value_8B;			
				break;
			case COLUMN_TYPE_DATE	: 
				int year;
				short mon,day,hour,min,sec;
				_pos = 0;
				strncpy(temp1,line+colPos,4);
				year = atoi(temp1);
				_pos+=4;
				
				strncpy(temp2,line+colPos+_pos,2);
				mon = atoi(temp2);
				_pos+=2;
				
				strncpy(temp2,line+colPos+_pos,2);
				day = atoi(temp2);
				_pos+=2;
				
				strncpy(temp2,line+colPos+_pos,2);
				hour = atoi(temp2);
				_pos+=2;
				
				strncpy(temp2,line+colPos+_pos,2);
				min = atoi(temp2);
				_pos+=2;
				
				strncpy(temp2,line+colPos+_pos,2);
				sec = atoi(temp2);				
				
				if(year != 0 && mon !=0 && day != 0)
				{ // ��������վ�Ϊ�գ�˵���������ǿյ�
				    if(year<1900 || mon<1 || mon>12 || day<1 || day>31 || hour<0 || hour>60 || min<0 || min>60 ||sec<0 || sec>60) 
				    {
  			    	    ThrowWith("�ļ�'%s'��%d�е�%d�ֶδ�������ڣ�",filename.c_str(),filerows+1,i+1);
				    	errinfile++;
				    	break;
				    }
			    }
				wociSetDateTime(mt.PtrDate(colspos[i],rows),year,mon,day,hour,min,sec);
				break;
			case COLUMN_TYPE_NUM :
			default :
				ThrowWith("Invalid column type :%d,id:%d",mt.getColType(colspos[i]),colspos[i]);
				break;
			}// end switch
			// ÿһ��λ���ƶ�
			colPos += realColLenAry[colspos[i]];
		}  // end for
		filerows++;
		rows++;
	}// end while
	_wdbiSetMTRows(mt,rows);
	if(rows==maxrows) return 0;
	AutoStmt st(dbc);
	/*
	st.Prepare("update dp.dp_filelog set status=1,recordnum=%d,endproctime=now() where tabid=%d and datapartid=%d and fileseq=%d",
		filerows,tabid,datapartid,fileseq);
	*/
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
	  st.Prepare("update dp.dp_filelog set status=1,recordnum=%d,endproctime=now(),errrows=%d,errfname='%s' where tabid=%d and datapartid=%d and fileseq=%d",filerows,errinfile,errfilename,tabid,datapartid,fileseq);
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
