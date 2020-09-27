//#include <cstring>
//#include <queue>
#include "dt_common.h"
#include "dt_svrlib.h"
#include <stdlib.h>
#include <ctype.h>
#include "zlib.h"
#include <lzo1x.h>
#include "lzo_asm.h"
#include <ucl.h>
#include <ucl_asm.h>
//#include <lzo1z.h> 
//#include <lzo1c.h>
//#include <lzo1.h>
//#include <zzlib.h>
#include <bzlib.h>
#include "dtio.h"
#include <math.h>
#include <my_global.h>
#include <m_ctype.h>
extern "C" { 
#include <decimal.h>
}
#ifdef __unix
#define thread_rt void *
#define thread_end return NULL
#else
#include <process.h>
#define thread_rt void
#define thread_end return
#endif


#include "ocidfn.h"  // add for SysAdmin::IgnoreBigSizeColumn


//#define dbgprintf printf
#define dbgprintf
//using namespace std;
SvrAdmin *SvrAdmin::psa=NULL;
AutoHandle *SvrAdmin::pdts=NULL;
int SvrAdmin::svrstarted=0;
int SvrAdmin::shutdown=0;
#ifdef __unix
pthread_t SvrAdmin::hd_pthread_t;
#endif
//int getMyRowLen(int *coltp,int *collen,int colct) ;

DTIOExport const char *GetDPLibVersion()
{
#ifdef MYSQL_VER_51
	return "5.6.4";
#else
	return "3.2.9";
#endif
}

void  Trim(char * Text)
{	
	//--------- trim right
	for (int i=strlen(Text)-1;i>0;i--)
	{
		if (Text[i] == ' ' || Text[i] == '\r' || Text[i] == '\n' || Text[i] == '\t' )  Text[i] = 0;
		else  break;
	}
	
	//--------- trim left
	int trimLeft = 0;
	int len = strlen(Text);
	while (*Text == ' ' || *Text == '\r' || *Text == '\n' || *Text == '\t')
	{
		Text++;
		trimLeft++;
	}
	strcpy(Text-trimLeft,Text);
}

thread_rt Handle_Read(void *ptr) {
	dt_file *p=(dt_file *)ptr;
	while(1)
	{
		//���ݿ��ò������ù�
		p->WaitBufEmpty(); //Lock buf empty,unlock buf empty by dt_file's main thread(ReadMT)
		//p->WaitBufReady(); //Lock buf ready
		if(p->Terminated()) {
			p->SetBufReady();
			break;
		}
		try {
			p->ReadMtThread();
		}
		catch(char *errstr) {
			p->SetReadError(errstr);
		}
		p->SetBufReady();
	}
	//p->ResetTerminated();
	thread_end; 
}

	dt_file::dt_file(bool _paral) {
		fp=NULL;
		openmode=0;
		fnid=0;
		file_version=FILE_DEF_VER;
		filename[0]=0;
		bflen=0;filesize=0;pwrkmem=NULL;
		cmprslen=0;terminate=false;isreading=false;
		readedrn=bufrn=-1;
		readedoffset=bufoffset=curoffset=(unsigned int)-1;
		readedblocklen=bufblocklen=(unsigned int)-1;
		readedfnid=buffnid=-1;
		offlinelen=-1;
		blockbf=NULL;
		cmprsbf=NULL;
		offlineblock=NULL;
		if (lzo_init() != LZO_E_OK)
		{
		   ThrowWith("lzo_init() failed !!!\n");
		}
		paral=false;
		contct=0;
		SetParalMode(_paral);
		memset(errstr,0,sizeof(errstr));
		pdtio=NULL;pdtfile=NULL;
		delmaskbf=new char[(MAX_BLOCKRN+7)/8];
		memset(delmaskbf,0,(MAX_BLOCKRN+7)/8);
	}


	int dt_file::dtfseek(long offset) {
		if(pdtfile) pdtfile->SeekAt(offset);
		else return fseek(fp,offset,SEEK_SET);
		return (int)offset;
	}
	size_t dt_file::dtfread(void *ptr,size_t size) {
		if(pdtfile) return pdtfile->ReadBuf((char *)ptr,(unsigned long)size);
		return fread(ptr,1,size,fp);
	}	

 dt_file::~dt_file() {
		SetParalMode(false);
		if(blockbf) delete[]blockbf;
		if(cmprsbf) delete[]cmprsbf;
		if(pwrkmem) delete[]pwrkmem;
		if(offlineblock) delete[]offlineblock;
		Close();
		if(pdtio) delete pdtio;
		if(pdtfile) delete pdtfile;
		if(delmaskbf) delete []delmaskbf;
	}

void dt_file::SetStreamName(const char *sfn)
{
	if(pdtio!=NULL) delete pdtio;
	if(pdtfile!=NULL) delete pdtfile;
	pdtio=new dtioStreamFile("./");
	pdtio->SetStreamName(sfn);
	pdtio->SetWrite(false);
	pdtio->StreamReadInit();
	if(pdtio->GetStreamType()!=FULL_BACKUP) 
		ThrowWith("ָ�����ļ�����ԭʼ�����ļ�!");
	pdtfile=new dtiofile(pdtio,true);
}

void dt_file::SetParalMode(bool val) {
	if(val) {
		if(paral) return;
		paral=val;
#ifdef __unix
	if(pthread_mutex_init(&buf_ready,NULL)!=0)
		ThrowWith("create mutex faid");
	if(pthread_mutex_init(&buf_empty,NULL)!=0)
		ThrowWith("create mutex faid");
	WaitBufEmpty();// Lock buf empty,in case thread reading.
	WaitBufReady();
	
	pthread_create(&hd_pthread_t,NULL,Handle_Read,(void *)this);
	pthread_detach(hd_pthread_t);
#else
	static int evtct=0;
	char evtname[100];
	sprintf(evtname,"dt_file_%d",evtct++);
	buf_ready=CreateEvent(NULL,false,false,evtname);
	sprintf(evtname,"dt_file_%d",evtct++);
	buf_empty=CreateEvent(NULL,false,false,evtname);
	WaitBufEmpty();// Lock buf empty,in case thread reading.
	WaitBufReady();
	_beginthread(Handle_Read,81920,(void *)this);
#endif
	}
	else {
	      if(paral) {
		if(isreading) 
		  WaitBufReady();
		terminate=true;
		SetBufEmpty();
		WaitBufReady();
		paral=false;
#ifdef __unix
		pthread_mutex_unlock(&buf_ready);
		pthread_mutex_destroy(&buf_empty);
#else
		CloseHandle(buf_ready);
		CloseHandle(buf_empty);
#endif
		}
		paral=false;
		isreading=false;
	}
}


	void dt_file::SetFileHeader(int rn,const char *nextfile) {
		if(!fp) ThrowWith("Write on open failed file at SetNextFile,filename:%s",filename);
		//file_hdr fh;
		//fread(&fh,sizeof(file_hdr),1,fp);
		if(nextfile==NULL) {
			fh.nextfile[0]=0;
			fh.islastfile=1;
		}
		else {
			strcpy(fh.nextfile,nextfile);
			fh.islastfile=0;
		}
		if(rn>0)
		 fh.rownum=rn;
		
		fseek(fp,0,SEEK_SET);
#ifdef MYSQL_VER_51
		if(file_version==FILE_VER40) fh.fileflag-=0x0100;
#endif
		fh.ReverseByteOrder();
                dp_fwrite(&fh,sizeof(file_hdr),1,fp);
                fh.ReverseByteOrder();
		curoffset=sizeof(file_hdr);
	}
	//openmode 0:rb read 1:w+b 2:wb
	void dt_file::Open(const char *filename,int openmode,int fnid) {
		//���������ļ��������ļ�������ͬ��fnid,openmodeҲ�п�����ͬ
		//if(fnid==this->fnid && fp && this->openmode==openmode) return;
		if(fp) fclose(fp);
		fp=NULL;
		//bk_lastrn=0;
		if(pdtio!=NULL && openmode!=0)
		    ThrowWith("Open file error!a stream packed file can only open in read mode.\n filename:%s,openmode:%d",
			filename,openmode);
		if(pdtio) pdtfile->OpenDtDataFile(filename);
		else {if(openmode==0) 
			fp=fopen(filename,"rb");
		      else if(openmode==1)
			fp=fopen(filename,"w+b");
		      else if(openmode==2) 
			fp=fopen(filename,"r+b");
		      if(!fp) ThrowWith("Open file error! filename:%s,openmode:%d",
			filename,openmode);
		}
		if(fnid==-1) this->fnid=0;
		else this->fnid=fnid;
		strcpy(this->filename,filename);
		this->openmode=openmode;
		//lastoffset=0;
		//if(openmode==1) WriteHeader(fnid);
		//else 
		curoffset=0;
		contct=0;
		bufoffset=0;
		//readedoffset=bufoffset=-1;readedblocklen=bufblocklen=-1;readedrn=
		if(openmode!=1) {
			if(pdtio) {
				filesize=pdtfile->GetFileLen();
			}
			else {
			 fseek(fp,0,SEEK_END);
			 filesize=ftell(fp);//_filelength(_fileno(fp));
			}
			dtfseek(0);
			ReadHeader();
			SetMySQLRowLen();
		}
		//if(openmode==0) {
		//	WaitBufEmpty();//Lock buf empty,in case thread reading.
		//}
		//touchedblock=false;
	}
    
	int dt_file::WriteHeader(int mt,int rn,int fid,const char *nextfilename) {
		if(!fp) ThrowWith("Write on open failed file,filename:%s",filename);
		fseek(fp,0,SEEK_SET);
		fh.fileflag=FILEFLAGEDIT;
		fh.fnid=fnid=fid;
		fh.islastfile=1;
		fh.rownum=rn;
	        void *pcd=NULL;
		if(!mt) {
			fh.cdlen=fh.cdnum=fh.rowlen=0;
		}
		else {
			wociReverseCD(mt);
			wociGetColumnDesc(mt,&pcd,fh.cdlen,fh.cdnum);
			char *pcd1=new char [fh.cdlen];
			memcpy(pcd1,pcd,fh.cdlen);
			pcd=pcd1;
			wociReverseCD(mt);
			fh.rowlen=wociGetRowLen(mt);
		}
		if(nextfilename) {
			fh.islastfile=false;
			strcpy(fh.nextfile,nextfilename);
		}
		else
		{
			fh.islastfile=true;
			fh.nextfile[0]=0;
		}
		//�ȱ��棬�����ֽ���ı任��
		int cdlen=fh.cdlen;
		int fileflag=fh.fileflag;
	        if(fileflag==FILEFLAGEDIT) {
		 memset(&fhe,0,sizeof(fhe));
		 fhe.dtp_sync=1;
		 fhe.ReverseByteOrder();
		}
		 fh.ReverseByteOrder();
		 if(dp_fwrite(&fh,sizeof(file_hdr),1,fp)!=1 ||
			(fileflag==FILEFLAGEDIT && dp_fwrite(&fhe,sizeof(fhe),1,fp)!=1) ||
			dp_fwrite(pcd,1,cdlen,fp)!=cdlen) 
			ThrowWith("Write file header failed! filename:%s,fnid:%d",
			   filename,fnid);
		fh.ReverseByteOrder();
	        if(fileflag==FILEFLAGEDIT) 
			fhe.ReverseByteOrder();
		if(pcd!=NULL) delete [] (char *)pcd;
		curoffset=sizeof(file_hdr)+fh.cdlen+sizeof(fhe);
		return curoffset;
	}

	int dt_file::AppendRecord(const char *rec,bool deleteable) {
		if(fh.fileflag!=FILEFLAGEDIT) {
			ThrowWith("AppendRecord have been called on a readonly file :%s%s .",
			   filename);
		}	
		int off=readedoffset;
		int rn=1;
		int noff=GetFileSize();
		int startrow=0;
		char *outbf=offlineblock;
		memcpy(outbf,rec,mysqlrowlen);
		if(fhe.lst_blk_off>0)
		{
			startrow=rn=ReadMySQLBlock(fhe.lst_blk_off,0,&outbf);
			if(rn<MAX_BLOCKRN) {
				memcpy(outbf+rn*mysqlrowlen,rec,mysqlrowlen);
				rn++;
				noff=fhe.lst_blk_off;
			}
			else rn=1;
		}
		if(noff>MAX_DATAFILELEN) return -1;
		Open(filename,2,fnid);
		fseek(fp,0,SEEK_SET);
		fh.rownum++;
		fh.ReverseByteOrder();
        	dp_fwrite(&fh,sizeof(file_hdr),1,fp);
        	fh.ReverseByteOrder();
		// modify file header extend area
		fhe.insertrn++;
		fhe.dtp_sync=0;
		fhe.ReverseByteOrder();
		dp_fwrite(&fhe,sizeof(fhe),1,fp);
		fhe.ReverseByteOrder();

		//ɾ�����󱣳�ԭ��ֵ
		delmaskbf[rn/8]^=(1<<(rn%8));
		fseek(fp,noff,SEEK_SET);
		WriteBlock(outbf,rn*mysqlrowlen,1,false,deleteable?BLOCKFLAGEDIT:BLOCKFLAG);
		//�ָ�WriteBlock�ж�fh.rownum������
		if(fh.rowlen>0) fh.rownum-=rn;
		return startrow;			
	}

	int dt_file::WriteBlock(char *bf,unsigned int len,int compress,bool packed,char bflag) {
		if(!fp) ThrowWith("Write on open failed file,filename:%s",filename);
		//���������ɱ༭�ļ��г��ֲ���ɾ�����ݿ�,�������������ļ�����׷�Ӽ�¼,������Ҫ��¼ɾ�����
		//if(fh.fileflag==FILEFLAGEDIT && !EditEnabled(bflag)) 
		//	ThrowWith("���ļ�'%s'���޸�ʹ���˲���ȷ�Ŀ�ʶ�����.",filename);
		if(packed) {
			block_hdr *pbh=(block_hdr *)bf;
                        pbh->ReverseByteOrder();
                        if(dp_fwrite(bf,1,len,fp)!=len)
				ThrowWith("Write file failed! filename:%s,blocklen:%d,offset:%d",
					filename,len,curoffset);
			curoffset+=len;
			pbh->ReverseByteOrder();
                        if(fh.rowlen>0) fh.rownum+=(len-sizeof(block_hdr))/fh.rowlen;
			return curoffset;
		}
		
		block_hdr bh;
		bh.blockflag=bflag;
		bh.compressflag=compress;
		bh.origlen=len;
		bh.storelen=len;
		char *dstbf=bf;
		if(compress>0) {
			unsigned int len2=max((int)(len*1.2),1024);
			if(cmprslen<len2) {
				if(cmprsbf) delete [] cmprsbf;
				cmprsbf= new char[len2];
				if(!cmprsbf) ThrowWith("MemAllocFailed on WriteBlock len:%d,len/3:%d",
					len,len2);
				cmprslen=len2;
			}
			int rt=0;
			uLong dstlen=len2;
			/**********bzip2 compress**************/
			if(compress==10) {
				unsigned int dstlen2=len2;
				rt=BZ2_bzBuffToBuffCompress(cmprsbf,&len2,bf,len,1,0,0);
				dstlen=len2;
				dstbf=cmprsbf;
			}			
			/****   UCL compress **********/
			else if(compress==8) {
				unsigned int dstlen2=len2;
				rt = ucl_nrv2d_99_compress((Bytef *)bf,len,(Bytef *)cmprsbf, &len2,NULL,5,NULL,NULL);
				dstlen=len2;
				dstbf=cmprsbf;
			}			
			/******* lzo compress ****/
			else if(compress==5) {
				if(!pwrkmem)  {
				 pwrkmem = //new char[LZO1X_999_MEM_COMPRESS];
					new char[LZO1X_MEM_COMPRESS+2048];
                 			memset(pwrkmem,0,LZO1X_MEM_COMPRESS+2048);
                		}
				lzo_uint dstlen2=len2;
				rt=lzo1x_1_compress((const unsigned char*)bf,len,(unsigned char *)cmprsbf,&dstlen2,pwrkmem);
				dstbf=cmprsbf;
				dstlen=dstlen2;
			}
		    /*** zlib compress ***/
			else if(compress==1) {
			 rt=compress2((Bytef *)cmprsbf,&dstlen,(Bytef *)bf,len,1);
			 dstbf=cmprsbf;
			}
			else 
				ThrowWith("Invalid compress flag %d",compress);
		    	if(rt!=Z_OK) {
				ThrowWith("Compress failed,datalen:%d,compress flag%d,errcode:%d",
					len,compress,rt);
			}
			else {
				bh.storelen=dstlen;
			}
		}
		int storelen=bh.storelen;
		unsigned int dml=0;
		if(EditEnabled(bflag)) {
		 if(len/GetBlockRowLen(bflag)>MAX_BLOCKRN) 
		 	ThrowWith("Build a editable block exceed maximum row limit,blockrn:%d,max :%d,filename:%s",
		 		len/GetBlockRowLen(bflag),MAX_BLOCKRN,filename);
		 dml=(len/GetBlockRowLen(bflag) +7)/8;
		 bh.storelen+=dml+sizeof(dmh);
		 dmh.rownum=len/GetBlockRowLen(bflag);
		 dmh.deletedrn=0;
		 dmh.ReverseByteOrder();
		 memset(delmaskbf,0,dml);
		}
		bh.ReverseByteOrder();
		if(dp_fwrite(&bh,sizeof(block_hdr),1,fp)!=1 || 
			(EditEnabled(bflag) && dp_fwrite(&dmh,sizeof(dmh),1,fp)!=1) ||
			(EditEnabled(bflag) && dp_fwrite(delmaskbf,1,dml,fp)!=dml) ||
			dp_fwrite(dstbf,1,storelen,fp)!=storelen)
			 ThrowWith("Write file failed! filename:%s,blocklen:%d,offset:%d",
			   filename,len,curoffset);
		curoffset+=sizeof(block_hdr)+storelen+(EditEnabled(bflag)?(dml+sizeof(dmh)):0);
		if(fh.rowlen>0) fh.rownum+=len/GetBlockRowLen(bflag);
		if(EditEnabled(bflag)) 
			dmh.ReverseByteOrder();
		return curoffset;
	}
	
	// rn>0,�������ð��д洢ʱ�ĸ�����ʼ��ַ(���baseaddr!=NULL)���п������ڼ����ֶ��ܳ���û��Ӱ�졣
	int getMyRowLen(int *coltp,int *collen,int *colscale,int colct,int rn=0,char **mycolptr=NULL,int *mycollen=NULL,char *baseaddr=NULL,int file_version=FILE_DEF_VER) {
		 int off=(colct+7)/8;
		 int colpos=0;
		 if(rn>0) {
		 	mycolptr[colpos]=baseaddr;
		 	mycollen[colpos++]=off;
		 	baseaddr+=off*rn;
		 }
		 for(int i=0;i<colct;i++) {
			int clen=collen[i];
//			int slen;
			switch(coltp[i]) {
			case COLUMN_TYPE_CHAR	:
#ifdef MYSQL_VER_51
				if(file_version==FILE_VER40) 
					clen--;
				else {
				// for mysql5.1 ,this decrease is not needed.
				if(clen>256) clen++; //������ȴ���255����Ҫ���ֽڵĳ���ָʾ(5.1)
				else if (clen<=4) clen--;
				}
#else
				clen--;
#endif
				off+=clen;
				break;
			case COLUMN_TYPE_FLOAT	:
				clen=sizeof(double);
				off+=clen;
				break;
			case COLUMN_TYPE_NUM	:
			      {
#ifdef MYSQL_VER_51
				if(file_version==FILE_VER40) {
				 clen=(collen[i]<=colscale[i])?(colscale[i]+1):collen[i];
				 clen+=2;
				 off+=clen;
				}
				else {
				 clen=decimal_bin_size(collen[i],colscale[i]);
				 off+=clen;
			  }
#else
			  clen=(collen[i]<=colscale[i])?(colscale[i]+1):collen[i];
				clen+=2;
				off+=clen;
#endif
				break;
			      }
			case COLUMN_TYPE_INT	:
				clen=sizeof(int);
				off+=clen;
				break;
			case COLUMN_TYPE_BIGINT	:
				clen=sizeof(LONG64);
				off+=clen;
				break;
			case COLUMN_TYPE_DATE	:
				clen=sizeof(LONG64);
				off+=clen;
				break;
			default :
  	    ThrowWith("Invalid column type :%d,id:%d",coltp[i],i);
		    break;
			}
		 	if(rn>0) {
		 		mycolptr[colpos]=baseaddr;
		 		baseaddr+=clen*rn;
		 		mycollen[colpos++]=clen;
		 	}
		}
		return off;
	}
		
	void setNullBit(char *buf, int colid)
	{
		static unsigned char marks[8]={1,2,4,8,16,32,64,128};
		buf[colid/8]|=(char )marks[colid%8];
	}

	int dt_file::WriteMySQLMt(int mt,int compress,bool corder)
	{	
		if(!mt) 
  		 ThrowWith("check a empty memory table in dt_file::WriteMySQLMt,mt=NULL.");
  		colct=wociGetColumnNumber(mt);
  		if(colct>MAX_COLS_IN_DT)
  		 ThrowWith("exceed maximun columns number, filename:%s,colnum:%d",
			   filename,colct);
		if(colct<1)
  		 ThrowWith("check a empty memory table in dt_file::WriteMySQLMt,colct:%d.",colct);
  		char *colptr[MAX_COLS_IN_DT];
  		int collen[MAX_COLS_IN_DT];
  		int colscale[MAX_COLS_IN_DT];
  		int coltp[MAX_COLS_IN_DT];
		wociAddrFresh(mt,colptr,collen,coltp);
		for(int lp=0;lp<colct;lp++) {
		 collen[lp]=wociGetColumnDataLenByPos(mt,lp);
		 colscale[lp]=wociGetColumnScale(mt,lp);
		 coltp[lp]=wociGetColumnType(mt,lp);
		}
  		char *mycolptr[MAX_COLS_IN_DT+1];//����mask��������Ϊʵ���ֶ�����1
  		int mycollen[MAX_COLS_IN_DT+1];
		int rl=getMyRowLen(coltp,collen,colscale,colct);
		int rn=wociGetMemtableRows(mt);
		mysqlrowlen=rl;
		unsigned int bfl1=rl*rn;
		if(bfl1>bflen || !blockbf) {
			bflen=(unsigned int)(bfl1*1.3);
			if(blockbf) delete[]blockbf;
			blockbf=new char[bflen];
			if(!blockbf) ThrowWith("Memory allocation faild in WriteMt of dt_file,size:%d",bfl1);
		}
  		if(corder)
  			getMyRowLen(coltp,collen,colscale,colct,rn,mycolptr,mycollen,blockbf,file_version);
		char *buf=blockbf;
#ifdef MYSQL_VER_51
		memset(buf,0,bfl1);
#else
		memset(buf,' ',bfl1);
#endif
		for(int pos=0;pos<rn;pos++) {
		 int i,j;
		 bool filled=false;
	  	 int off=(colct+7)/8;
	  	 if(corder) buf=mycolptr[0]+off*pos;
	 	 memset(buf,0,off);
		 int extralen=0;
		 for(i=0;i<colct;i++) {
			int clen=collen[i];
			int slen;
			char *pdst=(char*)buf+off;
			int chd=1;
			if(corder) pdst=mycolptr[i+1]+pos*mycollen[i+1];
			switch(coltp[i]) {
			case COLUMN_TYPE_CHAR	:
				{
				if(clen>256) chd++;
				clen--;
				char *src=(char *)(colptr[i]+(clen+1)*pos);
				if(*src!=0) {
#ifdef MYSQL_VER_51
				if(clen<4) {
					//FIX CHAR column type,JIRA DM-57
					memset(pdst,' ',clen);
					char *lst=(char *)memccpy(pdst,src,0,clen);
				  if(lst) *--lst=' ';
				}
				else {
					char *lst=(char *)memccpy(pdst+chd,src,0,clen);
					// in mysql 5.1 ,using 0 terminated string
					short int cl=clen;
					if(lst) cl=lst-(pdst+chd)-1;
					if(clen>255) {
					     revShort(&cl);
					     *((short *)pdst)=cl;
                                        }
					else *pdst=(char)cl;
					clen+=chd;
				}
#else
					char *lst=(char *)memccpy(pdst,src,0,clen);
					if(lst) *--lst=' ';
#endif
				}
				else {
#ifdef MYSQL_VER_51
					if(clen>3)
					clen+=chd;
#endif
					setNullBit(buf,i);
				}
				}
				off+=clen;
				break;
			case COLUMN_TYPE_FLOAT	:
				if(wociIsNull(mt,i,pos)) {
					setNullBit(buf,i);
					LONG64 mdt=0;
					memcpy(pdst,&mdt,sizeof(double));
				}
				else {
				 double v=((double *)colptr[i])[pos];
				 revDouble(&v);
				 memcpy(pdst,&v,sizeof(double));
				}
				clen=sizeof(double);
				off+=clen;
				break;
			case COLUMN_TYPE_NUM	:
#ifdef MYSQL_VER_51		
			{
				bool isnull=wociIsNull(mt,i,pos);
				int binlen=decimal_bin_size(clen,colscale[i]);
				decimal_digit_t dec_buf[9];
				decimal_t dec;
				decimal_digit_t dec_buf2[9];
				decimal_t dec2;
				dec.buf=dec_buf;
				dec.len= 9;
				dec2.buf=dec_buf2;
				dec2.len=9;
				double2decimal(isnull?(double)0:((double *)colptr[i])[pos],&dec2);
/* extract data from oracle to dm, cause lost significant part fraction...
      we missed roud process..
   example :
     create table testnum (telnum varchar2(10),localfee number(11,2))

     insert into testnum (telnum,localfee) values (131,8.20);
   field localfee became 8.19 at dm
   */
				decimal_round(&dec2,&dec,colscale[i],HALF_EVEN);
				decimal2bin(&dec,(uchar *)pdst,clen,colscale[i]);
				clen=binlen;
				off+=binlen;
				if(isnull) setNullBit(buf,i);
			}
#else
				clen=collen[i]<=colscale[i]?(colscale[i]+1):collen[i];
				clen+=2;
				if(wociIsNull(mt,i,pos)) {
					setNullBit(buf,i);
					memset(pdst,' ',clen);
				}
				else {
				if(((double *)colptr[i])[pos]>=pow(10.0,double(clen-2)) ||
					((double *)colptr[i])[pos]<=-pow(10.0,(double)(clen-2)) )
				     ThrowWith("��%d�У���%d�ֶε�ֵ������������.",pos+1,i+1);
				  sprintf(pdst,"%*.*f",clen,colscale[i],((double *)colptr[i])[pos]);
				}	//slen=(unsigned int) strlen(pdst);
				//if (slen > clen)
				//	return 1;
				//else
				//{
				//        //char *to=psrc(char *)buf+off;
				//	memmove(pdst+(clen-slen),pdst,slen);
				//	for (j=clen-slen ; j-- > 0 ;)
				//		*pdst++ = ' ' ;
				//}
				off+=clen;
#endif
				break;
			case COLUMN_TYPE_INT	:
			         //���������pa-riscƽ̨�����BUS-ERROR
				//*(int *)pdst=((int *)colptr[i])[pos];//*sizeof(int));
				//revInt(pdst);
				if(wociIsNull(mt,i,pos)) {
					setNullBit(buf,i);
					LONG64 mdt=0;
					memcpy(pdst,&mdt,sizeof(int));
				}
				else {
				int v=((int *)colptr[i])[pos];
				revInt(&v);
				memcpy(pdst,&v,sizeof(int));
				}
				slen=sizeof(int);
				off+=slen;
				break;
			case COLUMN_TYPE_BIGINT	:
			  //���������pa-riscƽ̨�����BUS-ERROR
				//*(int *)pdst=((int *)colptr[i])[pos];//*sizeof(int));
				//revInt(pdst);
				if(wociIsNull(mt,i,pos)) {
					setNullBit(buf,i);
					LONG64 mdt=0;
					memcpy(pdst,&mdt,sizeof(LONG64));
				}
				else {
				 LONG64 v=((LONG64 *)colptr[i])[pos];
				 revDouble(&v);
				 memcpy(pdst,&v,sizeof(LONG64));
				}
				slen=sizeof(LONG64);
				off+=slen;
				break;
		  	default:
  	    			ThrowWith("Invalid column type :%d,id:%d",coltp[i],i);
		    		break;
		  	case COLUMN_TYPE_DATE	:
				{
				 char *src=colptr[i]+7*pos;
				 if(*src==0 || (unsigned char )*src<101 || (unsigned char )*src>199) {//��Ԫ200-10000��Ϊ��Ч����������ֵ
					setNullBit(buf,i);
					LONG64 mdt=0;
					memcpy(pdst,&mdt,sizeof(LONG64));
					off+=sizeof(LONG64);
					//extralen+=sizeof(double);
				 }
				 else {
					LONG64 mdt=0;
					mdt=LL(LLNY)*(((unsigned char)src[0]-LL(100))*100+(unsigned char)src[1]-100);
					mdt+=LL(LLHMS)*src[2];
					mdt+=LL(1000000)*src[3];
					mdt+=LL(10000)*(src[4]-1);
					mdt+=100*(src[5]-1);
					mdt+=src[6]-1;
					memcpy(pdst,&mdt,sizeof(LONG64));
					rev8B(pdst);
					off+=sizeof(LONG64);
				  }
				}
				break;
			}
		}
		off+=extralen;
		buf+=off;
	    }
	    return WriteBlock(blockbf,bfl1,compress,false,fh.fileflag==FILEFLAGEDIT?(corder?MYCOLBLOCKFLAGEDIT:MYSQLBLOCKFLAGEDIT):MYSQLBLOCKFLAG);
	}

	int dt_file::WriteMt(int mt,int compress,int rn,bool deleteable) {
		rn=rn==0?wociGetMemtableRows(mt):rn;
		/* DM-55 check value of USER_ID */
		/*if(wociGetColumnType(mt,0)==COLUMN_TYPE_BIGINT) {
			printf("check dm-55 in dt_file::WriteMt\n");
			LONG64 *pchk;
		   wociGetLongAddrByName(mt,"USER_ID",0,&pchk) ;
		   int nullct=0;
		   for(int chkoff=0;chkoff<rn;chkoff++)
		   {
		   	if(wociIsNull(mt,0,chkoff)) nullct++;
		   }
		   printf("Check done(%d),null %d.\n",rn,nullct);
		}*/
		unsigned int bfl1=(wociGetRowLen(mt)+wociGetColumnNumber(mt)*sizeof(int))*rn;
		if(bfl1>bflen || !blockbf) {
			bflen=(unsigned int)(bfl1*1.3);
			if(blockbf) delete[]blockbf;
			blockbf=new char[bflen];
			if(!blockbf) ThrowWith("Memory allocation faild in WriteMt of dt_file,size:%d",bfl1);
		}
		wociExportSomeRowsWithNF(mt,blockbf,0,rn);
		return WriteBlock(blockbf,bfl1,compress,false,BLOCKNULLEDIT);
		   // backup before add NULL indicator 
		      //fh.fileflag==FILEFLAGEDIT?(deleteable?BLOCKFLAGEDIT:BLOCKFLAG):BLOCKFLAG);
	}

int dt_file::CreateMt(int maxrows)
{
	if(curoffset!=sizeof(file_hdr)+(fh.fileflag==FILEFLAGEDIT?sizeof(fhe):0) )
		ReadHeader();
	if(fh.cdlen<1) 
		ThrowWith("CreateMt on file '%s' which does not include column desc info",filename);
	char *pbf=new char[fh.cdlen];
	if(dtfread(pbf,fh.cdlen)!=fh.cdlen) {
		delete [] pbf;
		ThrowWith("CreateMt read column desc info failed on file '%s'",
		   filename);
	}
	int mt=wociCreateMemTable();
	curoffset=sizeof(file_hdr)+fh.cdlen+(fh.fileflag==FILEFLAGEDIT?sizeof(fhe):0);
	wociImport(mt,NULL,0,pbf,fh.cdnum,fh.cdlen,maxrows,0);
	delete []pbf;
	return mt;
}

int dt_file::GetFirstBlockOffset() {
	return sizeof(file_hdr)+fh.cdlen+(fh.fileflag==FILEFLAGEDIT?sizeof(fhe):0);
}

int dt_file::SetMySQLRowLen() {
	int mt=CreateMt(1);
	if(!mt) 
  	  ThrowWith("check a empty memory table in dt_file::ReadHeader");
  	colct=wociGetColumnNumber(mt);
  	if(colct>MAX_COLS_IN_DT)
  	 ThrowWith("exceed maximun columns number, filename:%s,colnum:%d",
			   filename,colct);
	if(colct<1)
  		 ThrowWith("check a empty memory table in dt_file::ReadHeader");
  	char *colptr[MAX_COLS_IN_DT+1]; //Ϊ����������������
  	int collen[MAX_COLS_IN_DT];
  	int colscale[MAX_COLS_IN_DT];
  	int coltp[MAX_COLS_IN_DT];
	wociAddrFresh(mt,colptr,collen,coltp);
	for(int lp=0;lp<colct;lp++) {
	 collen[lp]=wociGetColumnDataLenByPos(mt,lp);
	 colscale[lp]=wociGetColumnScale(mt,lp);
	 coltp[lp]=wociGetColumnType(mt,lp);
	}
	//ȡ��ƫ���������ڰ�����֯��mysql���ݿ���ȡ������
	char **tempptr=colptr; //��ʱ������ռλ
	mysqlrowlen=getMyRowLen(coltp,collen,colscale,colct,1,tempptr,mycollen,NULL,file_version);
	wocidestroy(mt);
	return 0;
}
	
int dt_file::ReadHeader()
{
	dtfseek(0);
	if(dtfread(&fh,sizeof(file_hdr))!=sizeof(file_hdr)) 
		ThrowWith("Read file header error on '%s'",filename);
        fh.ReverseByteOrder();
#ifdef MYSQL_VER_51
	if(fh.fileflag<FILE_VER51) {
		file_version=FILE_VER40;
		fh.fileflag+=0x0100;
	}
	else file_version=FILE_VER51;
#endif
	
	if(fh.fileflag==FILEFLAGEDIT && dtfread(&fhe,sizeof(fhe))!=sizeof(fhe)) 
		ThrowWith("Read file header error on '%s'",filename);
	curoffset=sizeof(file_hdr)+(fh.fileflag==FILEFLAGEDIT?sizeof(fhe):0);
	//oldoffset=lastoffset;
	//touchedblock=false;
	fnid=fh.fnid;
	return curoffset;
}

bool dt_file::OpenNext() {
 if(fh.islastfile!=0) return false;
 int oldfilesize=filesize;
 int oldcdlen=fh.cdlen;
 int oldfnid=fnid;
 Open(fh.nextfile,0);  
 if(oldcdlen!=fh.cdlen) 
	ThrowWith("ͬһ�������ݼ������������ļ��ֶθ�ʽ��һ��,fileid1:%d,cdlen1:%d,fileid2:%d,cdlen2:%d,filename2 '%s'",
		oldfnid,oldcdlen,fnid,fh.cdlen,filename);
 curoffset=GetFirstBlockOffset();
 dtfseek(curoffset);
 return true;
}

int dt_file::ReadMySQLBlock(int offset, int storesize,char **poutbf,int _singlefile)
{
	bool contread=false;
	int rn=ReadBlock(offset,storesize,contread,_singlefile);
	if(rn<0) return rn;
	block_hdr *pbh=(block_hdr *)cmprsbf;
	if(offlinelen<pbh->origlen) {
		if(offlineblock) delete []offlineblock;
		offlineblock=new char[int(pbh->origlen*1.2)+mysqlrowlen];
		offlinelen=int(pbh->origlen*1.2+mysqlrowlen);
	}
	memcpy(offlineblock,pblock,pbh->origlen);
	if(pbh->blockflag == MYSQLBLOCKFLAGEDIT || pbh->blockflag==MYCOLBLOCKFLAGEDIT) {
		char *pbf=cmprsbf+sizeof(block_hdr);
		memcpy(&dmh,pbf,sizeof(dmh));
		dmh.ReverseByteOrder();
		pbf+=sizeof(dmh);
		int dml=(pbh->origlen/mysqlrowlen+7)/8;
		memcpy(delmaskbf,pbf,dml);
	}

	if(contread && paral) {
	   if(curoffset<filesize ||(curoffset>=filesize && OpenNext())) {
	   	curblocklen=0;
	   	SetBufEmpty();// Start thread reading...
	   }
	}
	*poutbf=offlineblock;
	return rn;
}


int dt_file::GeneralRead(int offset,int storesize,AutoMt &mt,char **ptr,int clearfirst,int _singlefile,char *cacheBuf,int cachelen)
{
	bool contread=false;
	int rn=ReadBlock(offset,storesize,contread,_singlefile,cacheBuf,cachelen);
	if(rn<0) return rn;
	
	block_hdr *pbh=(block_hdr *)cmprsbf;
	if(pbh->blockflag==MYSQLBLOCKFLAG || pbh->blockflag==MYSQLBLOCKFLAGEDIT ||pbh->blockflag==MYCOLBLOCKFLAGEDIT) {
		if(offlinelen<pbh->origlen) {
			if(offlineblock) delete []offlineblock;
			offlineblock=new char[int(pbh->origlen*1.2)];
			offlinelen=int(pbh->origlen*1.2);
		}
		if(paral) {
			memcpy(offlineblock,pblock,pbh->origlen);
		    *ptr=offlineblock;
		}
		else {
			if(cacheBuf==NULL) 
				*ptr=pblock;
			else *ptr=cacheBuf;
		}
	}
	else {
		if(cacheBuf!=NULL) 
			ThrowWith("�ڴ�����ܶ��������,fileid:%d,offset:%d,filesize:%d,filename '%s'",
				fnid,offset,filesize,filename);
		if(clearfirst) {
			if(mt.GetMaxRows()<readedrn) {
				mt.SetMaxRows((int)(readedrn*1.3));
				mt.Build();
			}
			wociReset(mt);
		}
		wociAppendRows(mt,pblock,readedrn);
	}
	if(contread && paral) {
	   if(curoffset<filesize ||(curoffset>=filesize && OpenNext())) {
	   	curblocklen=0;
	   	SetBufEmpty();// Start thread reading...
	   }
	}
	return rn;
}


int dt_file::ReadBlock(int offset, int storesize,bool &contread,int _singlefile,char *cacheBuf,int cachelen)
{
	bool bufok=false;
	contread=false,
	dbgprintf("DBG(ReadMt): isreading:%d,bufoffset:%d,offset:%d.\n",isreading,bufoffset,offset);
	if(isreading) {
		WaitBufReady();
	}
	if(offset==0 || offset<-1) 
		offset=GetFirstBlockOffset();
	else if(offset==-1) {
		if(readedoffset<GetFirstBlockOffset()) offset=GetFirstBlockOffset();//��һ���ļ��Ŀ�ʼ
		else offset=readedoffset+readedblocklen;
		if(offset>curoffset) offset=GetFirstBlockOffset();//�Ѿ�������һ�ļ������ļ������¸�λ(���´�)
	}
	if(offset>filesize)
	 ThrowWith("������ʧ��,ָ��λ�ó����ļ���󳤶�,fileid:%d,offset:%d,filesize:%d,filename '%s'",
		fnid,offset,filesize,filename);
	//if(offset<GetFirstBlockOffset()) offset=GetFirstBlockOffset();
	dbgprintf("DBG(ReadMt):adj offset to%d,buffnid %d,fnid %d.\n",offset,buffnid,fnid);
	if(storesize<0) storesize=0;
	if(bufoffset!=offset || buffnid!=fnid || cacheBuf) {
	   //�����ļ����
	   curblocklen=storesize;
	   curoffset=offset;
	   if(curoffset>=filesize && (_singlefile==1 || !OpenNext())) {
	   	dbgprintf("DBG(ReadMt) come to eof,curoffset:%d,filesize:%d,bufoffset:%d,offset:%d.\n",
	   	   curoffset,filesize,bufoffset,offset);
	   	return -1;
	   }
	   offset=curoffset;
	   SetBufEmpty(cacheBuf,cachelen);// Start thread reading...
	   WaitBufReady();// Wait Reading complete.
	}
	else {
		if(curoffset<bufoffset+bufblocklen)
		 curoffset=bufoffset+bufblocklen;
	}
	if(bufoffset==-2) 
	       ThrowWith(errstr);
	if(bufoffset!=offset) {
	 ThrowWith("������ʧ��,fileid:%d,offset:%d,bufoffset:%d,filename '%s'",
		fnid,offset,bufoffset,filename);
	}
	
	if(readedoffset+readedblocklen==bufoffset) {
		if(contct>3) contread=true;
		else contct++;
	}
	else  {
		contct=0;
		dbgprintf("DBG(ReadMt) readedoff:%d,readedblocklen:%d,off+len %d,bufoffset:%d,break cont read.\n",
	          readedoffset,readedblocklen,readedoffset+readedblocklen,bufoffset);
	}
	readedrn=bufrn;
	readedoffset=bufoffset;

	readedblocklen=bufblocklen;
	readedfnid=buffnid;
	dbgprintf("DBG(ReadMt) Got %d rows from off %d,next should be %d.\n",readedrn,readedoffset,readedoffset+readedblocklen);
	return readedrn;
}
 
int dt_file::ReadMt(int offset, int storesize,AutoMt &mt,int clearfirst,int _singlefile,char *poutbf,BOOL forceparal,BOOL dm)
{
	bool contread=false;
	int rn=ReadBlock(offset,storesize,contread,_singlefile);
	if(rn<0) return rn;
	block_hdr *pbh=(block_hdr *)cmprsbf;
	if(poutbf) {
		if(dm) {
			memcpy(poutbf,cmprsbf,GetDataOffset(pbh));
			memcpy(poutbf+GetDataOffset(pbh),pblock,pbh->origlen);
		}
		else {
		 memcpy(poutbf,pbh,sizeof(block_hdr));
		 memcpy(poutbf+sizeof(block_hdr),pblock,pbh->origlen);
		}
	}
	else {
		if(pbh->blockflag!=BLOCKFLAG && pbh->blockflag!=BLOCKFLAGEDIT && pbh->blockflag!=BLOCKNULLEDIT) 
			ThrowWith("�ļ�'%s'��ʽΪMySQL,������dt_file::ReadMt����.",filename);
		if(clearfirst) {
			if(mt.GetMaxRows()<readedrn) {
				mt.SetMaxRows((int)(readedrn*1.3));
				mt.Build();
			}
			wociReset(mt);
		}
		if(pbh->blockflag==BLOCKNULLEDIT)
			wociAppendRowsWithNF(mt,pblock,readedrn);
		else wociAppendRows(mt,pblock,readedrn);
	}
	if(contread && paral) {
	   //if(curoffset>=filesize && (_singlefile==1 || !OpenNext())) return -1;
	   if(curoffset<filesize ||(curoffset>=filesize && OpenNext())) {
	   	curblocklen=0;
	   	SetBufEmpty();// Start thread reading...
	   }
	}
	return readedrn;
}
	int SysParam::GetMySQLLen(int mt)
	{
		if(!mt) 
  		 ThrowWith("check a empty memory table in SysParam::GetMySQLLen,mt=NULL.");
  		int colct=wociGetColumnNumber(mt);
		if(colct<1)
  		 ThrowWith("check a empty memory table in SysParam::GetMySQLLen,colct:%d.",colct);
  		int collen[MAX_COLS_IN_DT];
  		int colscale[MAX_COLS_IN_DT];
  		int coltp[MAX_COLS_IN_DT];
		for(int lp=0;lp<colct;lp++) {
		 collen[lp]=wociGetColumnDataLenByPos(mt,lp);
 		 colscale[lp]=wociGetColumnScale(mt,lp);
		 coltp[lp]=wociGetColumnType(mt,lp);
		}
		return getMyRowLen(coltp,collen,colscale,colct);
	}
// System parameter container
	int SysParam::BuildSrcDBC(int tabid,int datapartid) {
		AutoMt mt(dts,10);
		if(datapartid<1) 
		 mt.FetchAll("select sysid sid from dp.dp_table where tabid=%d",tabid);
		else
		 mt.FetchAll("select srcsysid sid from dp.dp_datapart where tabid=%d and datapartid=%d",tabid,datapartid);
		if(mt.Wait()<1) 
			ThrowWith("��%s���Ҳ�����Ӧ�ļ�¼: tabid:%d,datapartid:%d.",datapartid<1?"dp_table":"dp_datapart",tabid,datapartid);
		int srcid=mt.GetInt("sid",0);
		int srcidp=wociSearchIK(dt_srcsys,srcid);
		if(srcidp<0) ThrowWith("BuildSrcDBC has a invalid srcsysid in dp.dp_datasrc table:sysid=%d",srcid);
		//int tabid=dt_srctable.GetInt("tabid",srctabp);
		return BuildDBC(srcidp);
	};

        //datapartid������ȡ���ݵ�����ʱ·��
	int SysParam::GetSoledIndexParam(int datapartid,dumpparam *dp,int tabid) {
		int tabp=wociSearchIK(dt_table,tabid);
		if(tabp<0) ThrowWith("(GetSoledIndexParam),Դ��(id:%d)��dt_table���Ҳ���,����Ƿ������������д���",
			tabid);
		AutoMt idxmt(dts,MAX_DST_INDEX_NUM);
		idxmt.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>=1 order by indexgid limit %d",
			tabid,MAX_DST_INDEX_NUM);
		idxmt.Wait();
		int rn=idxmt.Wait();
		if(rn<1) ThrowWith("���Ϊ%d�ı�δ����������",tabid);
		dp->rowlen=dt_table.GetInt("recordlen",tabp);
		if(dp->rowlen<1)
		  ThrowWith( "Ŀ����ļ�¼�����쳣,�����¹���Ŀ����ṹ.�쳣��¼����Ϊ%d,tabid:%d.",dp->rowlen,tabid) ;
		//AutoMt cdf(dts);
		dp->psoledindex=0;
		for(int i=0;i<rn;i++) {
			dp->idxp[i].colnum=idxmt.GetInt("idxfieldnum",i);
			strcpy(dp->idxp[i].idxcolsname,idxmt.PtrStr("columnsname",i));
			strcpy(dp->idxp[i].idxreusecols,idxmt.PtrStr("reusecols",i));
			dp->idxp[i].idxid=idxmt.GetInt("indexgid",i);
			strcpy(dp->idxp[i].idxtbname,idxmt.PtrStr("indextabname",i));
			dp->idxp[i].idinidx=idxmt.GetInt("seqinidxtab",i);
			dp->idxp[i].idindat=idxmt.GetInt("seqindattab",i);
			if(idxmt.GetInt("issoledindex",i)==2) 
			 dp->psoledindex=i;
		}
		
		dp->dstpathid[0]=dt_table.GetInt("dstpathid",tabp);
		strcpy(dp->dstpath[0],GetMySQLPathName(dp->dstpathid[0]));
		
		if(datapartid>0)
		  idxmt.FetchAll("select * from dp.dp_datapart where tabid=%d and datapartid=%d",tabid,datapartid);
		else idxmt.FetchAll("select * from dp.dp_datapart where tabid=%d limit 3",tabid);
		if(idxmt.Wait()<1) 
			ThrowWith("�Ҳ���Դ�������(dp_datapart),tabid:%d; datapartid:%d.",tabid,datapartid);
		dp->tmppathid[0]=idxmt.GetInt("tmppathid",0);
		strcpy(dp->tmppath[0],GetMySQLPathName(dp->tmppathid[0]));

		dp->soledindexnum=rn;
		dp->maxrecnum=0;
		dp->tabid=tabid;
		return rn;
	}


	int SysParam::GetFirstTaskID(TASKSTATUS ts,int &tabid,int &datapartid) {
		AutoMt tasksch(dts);
		tasksch.FetchAll("select * from dp.dp_datapart where begintime<sysdate() and status=%d order by begintime limit 10",ts);
		int rn=tasksch.Wait();
		if(rn<1) return 0;
		datapartid=tasksch.GetInt("datapartid",0);
		tabid=tasksch.GetInt("tabid",0);
		return tabid;
	};

	int SysParam::UpdateTaskStatus(TASKSTATUS ts,int tabid,int datapartid) {
		AutoStmt tasksch(dts);
		//CMNET:���Ӷ������ֶ� touchtime,pid,hostname������
		char hostname[300];
		gethostname(hostname,300);

        if( DUMPING == ts){
            tasksch.Prepare("update dp.dp_datapart set status=%d,touchtime=now(),pid=%d,hostname='%s'  where tabid=%d and datapartid=%d and status<>%d and status not in(2,3,5)",
                        ts,getpid(),hostname,tabid,datapartid,ts);
        }else{
            tasksch.Prepare("update dp.dp_datapart set status=%d,touchtime=now(),pid=%d,hostname='%s'  where tabid=%d and datapartid=%d and status<>%d",
                        ts,getpid(),hostname,tabid,datapartid,ts);
        }

		//char dt[10];
		//wociGetCurDateTime(dt);
		//tasksch.BindDate(1,dt);
		tasksch.Execute(1);
		tasksch.Wait();
		int rn=wociGetFetchedRows(tasksch);
		if(rn<1) 
			ThrowWith("����״̬�޸�Ϊ%dʱʧ�ܣ����������������̳�ͻ��\n"
				" ��%d(%d).",ts,tabid,datapartid);
		wociCommit(dts);
		return rn;
	};

	int SysParam::GetDumpSQL(int tabid,int datapartid,char *sqlresult) {
		AutoMt tasksch(dts);
		tasksch.FetchAll("select * from dp.dp_datapart where tabid=%d and datapartid=%d ",tabid,datapartid);
		int rn=tasksch.Wait();
		if(rn<1) ThrowWith("���������ʧ��(GetDumpSQL) ,tabid:%d,datapartid%d",tabid,datapartid);
		//wociMTCompactPrint(tasksch,0,NULL);
		strcpy(sqlresult,tasksch.PtrStr("extsql",0));
		if(strlen(sqlresult)<6)
		  ThrowWith("SQL������ :%s, tabid %d,datapartid %d.",sqlresult,tabid,datapartid);
		return datapartid;
	}

	const char *SysParam::GetMySQLPathName(int pathid,char *pathtype)
	{
		const char *rt=internalGetPathName(pathid,pathtype);
		if(!rt) ThrowWith("��·�����ñ����Ҳ�����Ҫ��·��,����: '%s'/ID :%d",pathtype,pathid);
		if(xmkdir(rt)) ThrowWith("·�� '%s' �����ڻ���û��Ȩ�ޣ����Ҳ��ܽ�����%s/%d.",rt,pathtype,pathid);
		return rt;
	}
	
	const char * SysParam::internalGetPathName(int pathid, char *pathtype)
	{
		//����Ĵ�����Ǳ�ڵ�Σ��:ֱ���޸�MT�е��ַ��ֶ�,�и���Խ��
		if(pathtype==NULL) {
			int p=wociSearchIK(dt_path,pathid);
			if(p<0) return NULL;
			char *ptr=dt_path.PtrStr("pathval",p);
			if(ptr[strlen(ptr)-1]!='/' && ptr[strlen(ptr)-1]!='\\')
			 strcat(ptr,PATH_SEP);
			return ptr;
			//return dt_path.PtrStr("pathval",p);
		}
		int rn=wociGetMemtableRows(dt_path);
		int p=dt_path.GetPos("pathtype",COLUMN_TYPE_CHAR);
		for(int i=0;i<rn;i++) {
			if(STRICMP(dt_path.PtrStr(p,i),pathtype)==0) {
				char *ptr=dt_path.PtrStr("pathval",i);
				if(ptr[strlen(ptr)-1]!='/' && ptr[strlen(ptr)-1]!='\\')
			 		strcat(ptr,PATH_SEP);
				return ptr;
				//return dt_path.PtrStr("pathval",i);
			}
		}
		return NULL;
	}

	//�м���ʱ�����ļ���id��
	int SysParam::NextTmpFileID() {
		return GetSeqID("fileid");//"dt_fileid.nextval"
	}
	
	int SysParam::GetSeqID(const char *seqfield)
	{
		bool ec=wociIsEcho();
		wociSetEcho(FALSE);
		{
		 AutoStmt st(dts);
		 st.DirectExecute("lock tables dp.dp_seq write");
		 st.DirectExecute("update dp.dp_seq set id=id+1");
		}
		AutoMt seq(dts,10);
		seq.FetchAll("select id as fid from dp.dp_seq");
		seq.Wait();
		{
		 AutoStmt st(dts);
		 st.DirectExecute("unlock tables");
		}
		wociSetEcho(ec);
		if(seq.GetInt("fid",0)<1) {
			ThrowWith("dp.dp_seq���е�ֵ�������0 .");
		}
		return seq.GetInt("fid",0);
	}
	
	int SysParam::NextTableID() {
		return GetSeqID("tabid");//"dt_tableid.nextval"
	}

	int SysParam::NextDstFileID(int tabid) {
		bool ec=wociIsEcho();
		wociSetEcho(FALSE);
		{
		 AutoStmt st(dts);
		 st.DirectExecute("lock tables dp.dp_table write");
		 st.DirectExecute("update dp.dp_table set lstfid=lstfid+1 where tabid=%d",tabid);
		}
		AutoMt seq(dts,10);
		//seq.FetchAll("select dt_tableid.nextval as fid from dual");
		seq.FetchAll("select lstfid as fid from dp.dp_table where tabid=%d",tabid);
		seq.Wait();
		{
		 AutoStmt st(dts);
		 st.DirectExecute("unlock tables");
		}
		wociSetEcho(ec);
		return seq.GetInt("fid",0);
	}
	
#define DICT_CACHE_ROWS 200000
	void SysParam::Reload()
	{
	 dt_path.Clear();
	 dt_path.SetMaxRows(500);
	 dt_path.FetchAll("select * from dp.dp_path");
	 dt_path.Wait();
	 wociSetIKByName(dt_path,"pathid");
	 wociOrderByIK(dt_path);
/*****************************/

 	 dt_srcsys.Clear();
 	 dt_srcsys.SetMaxRows(200);
	 dt_srcsys.FetchAll("select * from dp.dp_datasrc");
	 dt_srcsys.Wait();
/*****************************/
	 wociSetIKByName(dt_srcsys,"sysid");
	 wociOrderByIK(dt_srcsys);

	 dt_table.Clear();
 	 dt_table.SetMaxRows(DICT_CACHE_ROWS);
	 dt_table.FetchAll("select * from dp.dp_table order by tabid");
	 dt_table.Wait();
	 wociSetIKByName(dt_table,"tabid");
	 wociOrderByIK(dt_table);
	 wociSetSortColumn(dt_table,"tabname");
	 wociSort(dt_table);

	 dt_index.Clear();
	 dt_index.SetMaxRows(DICT_CACHE_ROWS);
	 dt_index.FetchAll("select * from dp.dp_index order by tabid,indexgid,issoledindex desc");
	 dt_index.Wait();
	 wociSetSortColumn(dt_index,"tabid");
	 wociSort(dt_index);
/*****************************/
	}
	
	


	void SysParam::GetSrcSys(int sysid,char *sysname,char *svcname,char *username,char *pwd)
	{
		int res=wociSearchIK(dt_srcsys,sysid);
		if(res<0) ThrowWith("srcsys get error,sysid:%d",sysid);
		dt_srcsys.GetStr("sysname",res,sysname);
		dt_srcsys.GetStr("svcname",res,svcname);
		dt_srcsys.GetStr("username",res,username);
		dt_srcsys.GetStr("pswd",res,pwd);
	}


int SysParam::BuildDBC(int srcidp)
{
	char *pwd=dt_srcsys.PtrStr("pswd",srcidp);
	char pswd[200];
	if(!*pwd) {
		printf("SYSID:%d\nSVCNAME:%s\nUSERNAME:%s\n Input password:",
			dt_srcsys.GetInt("sysid",srcidp),dt_srcsys.PtrStr("svcname",srcidp),
			dt_srcsys.PtrStr("username",srcidp));
		scanf("%s",pswd);
		pwd=pswd;
	}
	else {
	  strcpy(pswd,pwd);
	  //decode(pswd);
	  pwd=pswd;
	}
	int dbc=0;
	int systype=dt_srcsys.GetInt("systype",srcidp);
	if(systype!=1 && systype!=2 && systype!=3 && systype!=4 && systype!=5)  ThrowWith("Դ���ݿ�ϵͳ�����ʹ���");
	if(systype==1)
	 dbc=wociCreateSession(dt_srcsys.PtrStr("username",srcidp),
			 pwd,dt_srcsys.PtrStr("jdbcdbn",srcidp),DTDBTYPE_ORACLE);//���jdbcdbn�ֶ�Ϊ�գ���Ҫִ��update dp_datasrc�޸�svcname,jdbcdbn��ֵ
	else dbc=wociCreateSession(dt_srcsys.PtrStr("username",srcidp),
			 pwd,dt_srcsys.PtrStr("svcname",srcidp),DTDBTYPE_ODBC);
	/* check db init process */
	 int sirn=0;
	 AutoMt dinit(dts);
	 try{
	  dinit.FetchAll("select * from dp.dp_datasrcinit where sysid=%d",dt_srcsys.GetInt("sysid",srcidp));
	  sirn=dinit.Wait();
	 }
	 catch(...) {
	 	lgprintf("��������Դ��ʼ��������ʧ�ܣ���������Ҫ������");
	 }
	 if(sirn>0) {
		AutoStmt st(dbc);
		st.DirectExecute(dinit.PtrStr("sqltext",0));
		lgprintf("����Դ��ʼ��ִ�У�%s.",dinit.PtrStr("sqltext",0));
	 }
	
	return dbc;
}

int SysParam::GetMiddleFileSet(int procstate)
{
	AutoMt mdfile(dts);
	//mdfile.FetchAll("select * from dt_middledatafile where procstate=%d and rownum<2 ",procstate);
	// Not use limit 1 ,which means desc columns only for DT mysqld.
	mdfile.FetchAll("select * from dt_middledatafile where procstate=%d limit 2 ",procstate);
	int rn=mdfile.Wait();
	if(rn<1) return 0;
	int mt;
	AutoStmt st(dts);
	st.Prepare("select * from dt_middledatefile where tabid=%d and datapartid=%d and indexgid=%d order by mdfid ",
	  mdfile.GetInt("tabid",0),mdfile.GetInt("datapartid",0),mdfile.GetInt("indexgid",0));
	mt=wociCreateMemTable();
	wociBuildStmt(mt,st,MAX_MIDDLE_FILE_NUM);
	wociFetchAll(mt);
	wociWaitLastReturn(mt);
	return mt;
}


int SysParam::GetMaxBlockRn(int tabid)
{
		int p=wociSearchIK(dt_table,tabid);
		if(p<0) ThrowWith("get maxrecinblock from table with tabid error,tabid:%d",tabid);
		return dt_table.GetInt("maxrecinblock",p);
}


thread_rt DT_CreateInstance(void *p) {
	SvrAdmin::CreateInstance();
	thread_end;
}

void SvrAdmin::CreateInstance()
{
	if(!pdts) return ;
	bool ec=wociIsEcho();
	wociSetEcho(FALSE);
	try {
	SvrAdmin *_psa=new SvrAdmin(*pdts);
	printf("reload dt parameters start.\n");
	_psa->Reload();
	psa=_psa;
	SetSvrStarted();
	ReleaseDTS();
	}
	catch(WDBIError &er) {
		int erc;
		char *buf;
		er.GetLastError(erc,&buf);
		fprintf(stderr,"Error code-%d: %s .\n",erc,buf);
		//throw buf;
	}
	catch(char *err) {
		fprintf(stderr," %s .\n",err);
		//throw err;
	}
        printf("reload end.\n");
        wociSetEcho(ec);
}

///* Database server extendted interface
	SvrAdmin *SvrAdmin::GetInstance() {
/*		if(psa==NULL ) {
                        void *pval=NULL;
			psa=(SvrAdmin *)0x0001;
#ifdef __unix
			pthread_create(&hd_pthread_t,NULL,DT_CreateInstance,(void *)pval);
			pthread_detach(hd_pthread_t);
#else
			_beginthread(DT_CreateInstance,81920,(void *)pval);
#endif
			//psa=new SvrAdmin(*pdts);
			//psa->Reload();
		}
*/		
		return svrstarted?(psa==(SvrAdmin *)0x1?NULL:psa):NULL;
	}
        
        SvrAdmin *SvrAdmin::RecreateInstance(const char *svcname,const char *usrnm,const char *pswd) {
          if(psa==(void *)0x1) return NULL;
          ReleaseInstance();
          void *pval=NULL;
          CreateDTS(svcname,usrnm,pswd);
#ifdef __unix
          pthread_create(&hd_pthread_t,NULL,DT_CreateInstance,(void *)pval);
          pthread_detach(hd_pthread_t);
#else
          _beginthread(DT_CreateInstance,81920,(void *)pval);
#endif
          return NULL;              //psa=new SvrAdmin(*pdts);
                        //psa->Reload();
        }

	void SvrAdmin::ReleaseInstance() {
		if(psa!=NULL && psa!=(void *)0x1) {
                  SvrAdmin *_psa=psa;
                  psa=(SvrAdmin *)0x1;
                  svrstarted=0;
                  delete _psa;
                }
	}
	
	int SvrAdmin::CreateDTS(const char *svcname,const char *usrnm,const char *pswd) {
		if(pdts==NULL) {
			pdts=new AutoHandle;
			try {
			pdts->SetHandle(wociCreateSession(usrnm,pswd,svcname,DTDBTYPE_ORACLE));
			}
			catch(WDBIError &er) {
				int erc;
				char *buf;
				er.GetLastError(erc,&buf);
				fprintf(stderr,"Error code-%d: %s .\n",erc,buf);
				throw buf;
			}
		}
		return *pdts;  
	}
	
	void SvrAdmin::ReleaseDTS() {
		if(pdts) 
			delete pdts;
		pdts=NULL;
	}
	
	int SvrAdmin::GetIndexStruct(int p) {
		int mt=wociCreateMemTable();
		wociAddColumn(mt,"idxidintab",NULL,COLUMN_TYPE_INT,0,0);
		wociAddColumn(mt,"idxidinidx",NULL,COLUMN_TYPE_INT,0,0);
		wociAddColumn(mt,"soledflag",NULL,COLUMN_TYPE_INT,0,0);
		wociAddColumn(mt,"tableoff",NULL,COLUMN_TYPE_INT,0,0);
		wociAddColumn(mt,"indexgid",NULL,COLUMN_TYPE_INT,0,0);
		wociAddColumn(mt,"tabname",NULL,COLUMN_TYPE_CHAR,30,0);
//		wociAddColumn(mt,"reuseindexid",NULL,COLUMN_TYPE_INT,0,0);
		wociAddColumn(mt,"idxfieldnum",NULL,COLUMN_TYPE_INT,0,0);
		wociAddColumn(mt,"databasename",NULL,COLUMN_TYPE_CHAR,20,0);
		wociBuild(mt,100);
		int tabid=dsttable.GetInt("tabid",p);
		char *pdbn=dsttable.PtrStr("databasename",p);
		int *ptab=dt_index.PtrInt("tabid",0);
		int *pidxtb=dt_index.PtrInt("seqindattab",0);
		int	*pidxidx=dt_index.PtrInt("seqinidxtab",0);
		int *pissoled=dt_index.PtrInt("issoledindex",0);
		int *pindexid=dt_index.PtrInt("indexgid",0);
		int idxtabnamep=dt_index.GetPos("indextabname",COLUMN_TYPE_CHAR);
		int rn=wociGetMemtableRows(dt_index);
		int solect=0;
		int i;
		for( i=0;i<rn;i++ ) {
			if(ptab[i]!=tabid) continue;
			else {
				void *ptr[20];
				ptr[0]=pidxtb+i;
				ptr[1]=pidxidx+i;
				ptr[2]=pissoled+i;
				int soledflag=-1;
				if(pissoled[i]==1) soledflag=solect++;
				ptr[3]=&soledflag;
				ptr[4]=pindexid+i;
				ptr[5]=dt_index.PtrStr(idxtabnamep,i);
				//ptr[6]=dt_index.PtrInt("reuse	",i);
				ptr[6]=dt_index.PtrInt("idxfieldnum",i);
				ptr[7]=pdbn;//dt_index.PtrInt("idxfieldnum",i);
				ptr[8]=NULL;
				wociInsertRows(mt,ptr,NULL,1);
			}
		}
		/*
		int totidxnum=GetTotIndexNum(p);
		//int *pissole=NULL;
		int *ptableoff=NULL;
		int *pidxid=NULL;
		int *preuseid;
		//wociGetIntAddrByName(mt,"soledflag",0,&pissole);
		wociGetIntAddrByName(mt,"tableoff",0,&ptableoff);
		wociGetIntAddrByName(mt,"reuseindexid",0,&preuseid);
		wociGetIntAddrByName(mt,"indexgid",0,&pidxid);
		for(i=0;i<totidxnum;i++) {
			if(preuseid[i]>0) {
				for(int j=0;j<totidxnum;j++) {
					if(pidxid[j]==preuseid[i]) {
						ptableoff[i]=ptableoff[j];
						break;
					}
				}
			}
		}*/
		
		return mt;
	}

	int SvrAdmin::Search(const char *pathval) {
		void *ptr[2];
		ptr[0]=(void *)pathval;
		ptr[1]=NULL;
		return wociSearch(dsttable,ptr);
	}
 
	const char *SvrAdmin::GetFileName(int tabid,int fileid) {
		void *ptr[3];
		ptr[0]=&tabid;ptr[1]=&fileid;ptr[2]=NULL;
		int i=wociSearch(filemap,ptr);
		if(i<0) {
			lgprintf("Invalid file id :%d",fileid);
			return NULL;
		}
		return filemap.PtrStr(filenamep,i);
	}

	void SvrAdmin::Reload() {
		SysParam::Reload();
		filemap.FetchAll("select * from dt_datafilemap where fileflag is null or fileflag=0");
		filemap.Wait();
		wociSetSortColumn(filemap,"tabid,fileid");
		wociSort(filemap);
		filenamep=filemap.GetPos("filename",COLUMN_TYPE_CHAR);

		wociClear(dsttable);
		wociAddColumn(dsttable,"tabid",NULL,COLUMN_TYPE_INT,0,0);
		wociAddColumn(dsttable,"soledindexnum",NULL,COLUMN_TYPE_INT,0,0);
		wociAddColumn(dsttable,"totindexnum",NULL,COLUMN_TYPE_INT,0,0);
		wociAddColumn(dsttable,"recordnum",NULL,COLUMN_TYPE_INT,0,0);
		wociAddColumn(dsttable,"pathval",NULL,COLUMN_TYPE_CHAR,300,0);
		wociAddColumn(dsttable,"firstfile",NULL,COLUMN_TYPE_CHAR,300,0);
		wociAddColumn(dsttable,"databasename",NULL,COLUMN_TYPE_CHAR,300,0);
		//wociAddColumn(dsttable,"colnum",NULL,COLUMN_TYPE_INT,0,0);
		int rn=wociGetMemtableRows(dt_table);
		dsttable.SetMaxRows(rn);
		dsttable.Build();
		for(int i=0;i<rn;i++) {
			int recnum=dt_table.GetInt("recordnum",i);
			if(recnum>0) {
				void *ptr[20];
				char pathval[300];
				ptr[0]=dt_table.PtrInt("tabid",i);
				ptr[1]=dt_table.PtrInt("soledindexnum",i);
				ptr[2]=dt_table.PtrInt("totindexnum",i);
				sprintf(pathval,"./%s/%s",dt_table.PtrStr("databasename",i),
					dt_table.PtrStr("tabname",i));
				ptr[3]=&recnum;
				ptr[4]=pathval;
				ptr[5]=(void *)GetFileName(dt_table.GetInt("tabid",i),dt_table.GetInt("firstdatafileid",i));
				ptr[6]=dt_table.PtrStr("databasename",i);
				if(ptr[5]==NULL) ptr[5]=(void *)"";//continue;
				ptr[7]=NULL;
				wociInsertRows(dsttable,ptr,NULL,1);
			}
		}
		wociSetSortColumn(dsttable,"pathval");
		wociSort(dsttable);
	}

const char * SvrAdmin::GetDbName(int p)
{
	return dsttable.PtrStr(6,p);
}

int dt_file::ReadMtThread(char *cacheBuf,int cachelen)
{
	//if(curoffset!=readedoffset+readedblocklen) {
		dtfseek(curoffset);
	//}
	if(curblocklen==0) {
		block_hdr bh;
		if(dtfread(&bh,sizeof(block_hdr))!=sizeof(block_hdr))
		  ThrowWith("File read failed on '%s',offset:%d,size:%d",
			filename,curoffset,curblocklen);
		bh.ReverseByteOrder();
		if(!CheckBlockFlag(bh.blockflag))
		 ThrowWith("Invalid block flag on '%s' ,offset :%d",
			filename,curoffset);
		curblocklen=bh.storelen+sizeof(block_hdr);
		if(cmprslen<curblocklen) {
			if(cmprsbf) delete []cmprsbf;
			cmprsbf=new char[int(curblocklen*1.3)];
			cmprslen=int(curblocklen*1.3);
		}
		memcpy(cmprsbf,&bh,sizeof(block_hdr));
		if(dtfread(cmprsbf+sizeof(block_hdr),bh.storelen)!=bh.storelen)
		  ThrowWith("File read failed on '%s',offset:%d,size:%d",
			filename,curoffset,curblocklen);
	}
	else {
		if(cmprslen<curblocklen) {
			if(cmprsbf) delete []cmprsbf;
			cmprsbf=new char[int(curblocklen*1.3)];
			cmprslen=int(curblocklen*1.3);
		}
		if(dtfread(cmprsbf,curblocklen)!=curblocklen) {
			ThrowWith("File read failed on '%s',offset:%d,size:%d",
				filename,curoffset,curblocklen);
		}
		//ֱ����cmprsbf��ַ����λ��任
		block_hdr *pbh1=(block_hdr *)cmprsbf;
		pbh1->ReverseByteOrder();
	}
	block_hdr *pbh=(block_hdr *)cmprsbf;
	int dml=0;
	int brn=0;
	if(!CheckBlockFlag(pbh->blockflag))
		ThrowWith("Invalid block flag on '%s' ,offset :%d",
			filename,curoffset);
	if(pbh->storelen+sizeof(block_hdr)!=curblocklen) 
		ThrowWith("Invalid block length on '%s' ,offset :%d,len:%d,should be:%d.",
			filename,curoffset,curblocklen,pbh->storelen+sizeof(block_hdr));
	bufoffset=curoffset;
	dbgprintf("DBG: ReadThread curoffset%d,bufoffset:%d.\n",curoffset,bufoffset);
	bufblocklen=curblocklen;
	buffnid=fnid;
	curoffset+=curblocklen;
	// very pool code,to avoid mixed del mask for datafile and indexfile
#ifdef  BAD_DEEPCOMPRESS_FORMAT	
	if(EditEnabled(pbh->blockflag) && (pbh->compressflag!=10 || pbh->blockflag==MYCOLBLOCKFLAGEDIT)) {
#else
	if(EditEnabled(pbh->blockflag)) { 
#endif		
		 pblock=cmprsbf+sizeof(block_hdr);
		 memcpy(&dmh,pblock,sizeof(dmh));
		 dmh.ReverseByteOrder();
		 brn=dmh.rownum;
		 pblock+=sizeof(dmh);
		 dml=(brn+7)/8;//pbh->origlen/GetBlockRowLen(pbh->blockflag)
		 if(brn>MAX_BLOCKRN) {
		  //ThrowWith("Read from block exceed maximum delete mask matrix limit,blockrn:%d,max :%d,filename:%s",
		  //		pbh->origlen/GetBlockRowLen(pbh->blockflag),MAX_BLOCKRN,filename);
		  errprintf("Read from block exceed maximum delete mask matrix limit,blockrn:%d,max :%d,filename:%s",
		  		brn,MAX_BLOCKRN,filename);
		 }
		 else {
			memcpy(delmaskbf,pblock,dml);
		 }
		 pblock+=dml;
		 dml+=sizeof(dmh);
	}
	else pblock=cmprsbf+sizeof(block_hdr);
	if(pbh->compressflag!=0) {
			int rt=0;
			uLong dstlen;
			char *destbuf;
			if(cacheBuf!=NULL) {
				dstlen=cachelen;
				destbuf=cacheBuf;
			}
			else {
			 if(bflen<pbh->origlen) {
				bflen=max(int(pbh->origlen*1.2),1024);
				if(blockbf) delete []blockbf;
				blockbf=new char[bflen];
			 }
			 destbuf=blockbf;
			 dstlen=bflen;
			}
			if(dstlen<pbh->origlen) {
				ThrowWith("���ݻ����̫С����Ҫ%d�ֽڣ���ֻ��%d�ֽ�.�ļ�'%s':%d.",
					pbh->origlen,dstlen,filename,bufoffset);
			}
			int rcl=pbh->storelen-dml;//dml already include delmask_hdr length.  -sizeof(delmask_hdr) ;
			//zzlib
		    	/***************************
			if(pbh->compressflag==11) {
				memcpy(blockbf,bk_psrc,pbh->storelen);
				dstlen = ZzUncompressBlock((unsigned char *)bk_psrc);
				if(dstlen<0) rt=dstlen;
			}
			else 
			****************************/
			//bzlib2
			if(pbh->compressflag==10) {
				unsigned int dstlen2=dstlen;
				rt=BZ2_bzBuffToBuffDecompress(destbuf,&dstlen2,pblock,rcl,0,0);
				dstlen=dstlen2;
				pblock=blockbf;
			}			
			/***********UCL decompress ***********/
			else if(pbh->compressflag==8) {
				unsigned int dstlen2=dstlen;
				#ifdef USE_ASM_8
				rt = ucl_nrv2d_decompress_asm_fast_8((Bytef *)pblock,rcl,(Bytef *)destbuf,(unsigned int *)&dstlen2,NULL);
				#else
				rt = ucl_nrv2d_decompress_8((Bytef *)pblock,rcl,(Bytef *)destbuf,(unsigned int *)&dstlen2,NULL);
				#endif
				dstlen=dstlen2;
				pblock=blockbf;
			}
			/******* lzo compress ****/
			else if(pbh->compressflag==5) {
				lzo_uint dstlen2=dstlen;
				#ifdef USE_ASM_5
				rt=lzo1x_decompress_asm_fast((unsigned char*)pblock,rcl,(unsigned char *)destbuf,&dstlen2,NULL);
				#else
				rt=lzo1x_decompress((unsigned char*)pblock,rcl,(unsigned char *)destbuf,&dstlen2,NULL);
				#endif
				dstlen=dstlen2;
				pblock=blockbf;
			}
		    	/*** zlib compress ***/
			else if(pbh->compressflag==1) {
			 rt=uncompress((Bytef *)destbuf,&dstlen,(Bytef *)pblock,rcl);
			 pblock=blockbf;
			}
			else 
				ThrowWith("Invalid uncompress flag %d",pbh->compressflag);
		  if(rt!=Z_OK) {
				ThrowWith("Decompress failed,fileid:%d,off:%d,datalen:%d,compress flag%d,errcode:%d,dml:%d,origlen:%d,rowlen:%d,blockflag:%d,rlen:%d,mrlen:%d,dmh.rownum%d,rcl:%d.",
					fnid,bufoffset,pbh->storelen,pbh->compressflag,rt,dml,pbh->origlen,GetBlockRowLen(pbh->blockflag),pbh->blockflag,fh.rowlen,mysqlrowlen,dmh.rownum,rcl);
			}
			else if(dstlen!=pbh->origlen) {
#ifdef  BAD_DEEPCOMPRESS_FORMAT	
			 if(EditEnabled(pbh->blockflag) && pbh->compressflag==10) {
		 		pblock=destbuf;
		 		memcpy(&dmh,pblock,sizeof(dmh));
		 		dmh.ReverseByteOrder();
		 		pblock+=sizeof(dmh);
		 		dml=(pbh->origlen/GetBlockRowLen(pbh->blockflag)+7)/8;
		 		if(pbh->origlen/GetBlockRowLen(pbh->blockflag)>MAX_BLOCKRN) {
		  		   errprintf("Read from block exceed maximum delete mask matrix limit,blockrn:%d,max :%d,filename:%s",
		  			pbh->origlen/GetBlockRowLen(pbh->blockflag),MAX_BLOCKRN,filename);
		 		}
		 		else {
					memcpy(delmaskbf,pblock,dml);
		 		}
		 		pblock+=dml;
		 		dml+=sizeof(dmh);
		 		dstlen-=dml;
		 		if(dstlen!=pbh->origlen)
				 ThrowWith("Decompress failed,fileid:%d,off:%d,datalen:%d,compress flag%d,decompress to len%d,should be %d.",
					fnid,bufoffset,pbh->storelen,pbh->compressflag,dstlen,pbh->origlen); 
				memmove(destbuf,destbuf+dml,dstlen);
			  }
#else			  
			 ThrowWith("Decompress failed,datalen %d should be %d.fileid:%d,off:%d,datalen:%d,compress flag%d,errcode:%d,rcl %d",
					dstlen,pbh->origlen,fnid,bufoffset,pbh->storelen,pbh->compressflag,rt,rcl);
#endif
			}
	}
	else if(cacheBuf!=NULL) 
			ThrowWith("��ѹ�����ݿ鲻��ʹ��Cacheģʽ");
	bufrn=pbh->origlen/GetBlockRowLen(pbh->blockflag);
	buforiglen=pbh->origlen;
	//ˢ��ƫ�������飬���ڰ�����֯��mysql���ݿ���������
	/*
	int offpos;
	int s_off=0;
	for(offpos=0;offpos<=colct;offpos++)
	{
		mycoloff[offpos]=s_off*bufrn;
		s_off+=mycoloff_s[0];
	}
	*/
	if(bufrn*GetBlockRowLen(pbh->blockflag)!=pbh->origlen) {
    FILE *fp=fopen("/tmp/dm_error.dat","wb");
    fwrite(pblock,pbh->origlen,1,fp);
    fclose(fp);
		ThrowWith("data block error on file '%s',offset :%d,original length %d,should be %d,block type:%d,rlen:%d,mrlen:%d.",
			filename,bufoffset,pbh->origlen,bufrn*GetBlockRowLen(pbh->blockflag),pbh->blockflag,fh.rowlen,mysqlrowlen);
  }		
	blockflag=pbh->blockflag;
	dbgprintf("DBG(ReadThread): off %d,coff %d,olen%d,len%d.\n",bufoffset,curoffset,buforiglen,bufblocklen);
	return bufrn;
}
/*
int dt_file::WaitEnd(int tm)
{
	#ifdef __unix
	int rt=pthread_mutex_lock(&hd_end);
	#else
	DWORD rt=WAIT_OBJECT_0;
	rt=WaitForSingleObject(hd_end,tm);
	#endif
	if(rt!=WAIT_OBJECT_0 && rt!=WAIT_TIMEOUT) {
		#ifndef __unix
		rt=GetLastError();
		#endif
		ThrowWith("Waitend failed ,code:%d",rt);
	}
	return rt;
}

void dt_file::Start()
{
	if(paral) {
#ifdef __unix
	 pthread_mutex_unlock(&hd_start);
#else
	SetEvent(hd_start);
#endif
	}
}
*/
int dt_file::WaitBufReady(int tm)
{
 dbgprintf("DBG:WaitBufReady.\n");
 if(paral) {
	#ifdef __unix
	int rt=pthread_mutex_lock(&buf_ready);
	#else
	DWORD rt=WAIT_OBJECT_0;
	rt=WaitForSingleObject(buf_ready,tm);
	#endif
	if(rt!=WAIT_OBJECT_0 && rt!=WAIT_TIMEOUT) {
		#ifndef __unix
		rt=GetLastError();
		#endif
		ThrowWith("Wait Buffer ready failed ,code:%d",rt);
	}
	dbgprintf("DBG:WaitBufReady(locked).\n");
 	isreading=false;
	return rt;
 }
 return 0;
}

int dt_file::WaitBufEmpty(int tm)
{
 dbgprintf("DBG:WaitBufEmpty.\n");
 if(paral) {
	#ifdef __unix
	int rt=pthread_mutex_lock(&buf_empty);
	#else
	DWORD rt=WAIT_OBJECT_0;
	rt=WaitForSingleObject(buf_empty,tm);
	#endif
	if(rt!=WAIT_OBJECT_0 && rt!=WAIT_TIMEOUT) {
		#ifndef __unix
		rt=GetLastError();
		#endif
		ThrowWith("Wait Buffer empty failed ,code:%d",rt);
	}
	dbgprintf("DBG:WaitBufEmpty(locked).\n");
	return rt;
 }
 return 0;
}

void dt_file::SetBufReady()
{
 dbgprintf("DBG:SetBufReady.\n");
 if(paral) {
	#ifdef __unix
	 pthread_mutex_unlock(&buf_ready);
	#else
	SetEvent(buf_ready);
	#endif
 }
}
 
void dt_file::SetBufEmpty(char *cacheBuf,int cachelen)
{
 dbgprintf("DBG:SetBufEmpty.\n");
 if(paral) {
	isreading=true;
	#ifdef __unix
	 pthread_mutex_unlock(&buf_empty);
	#else
	SetEvent(buf_empty);
	#endif
 }
 else ReadMtThread(cacheBuf,cachelen);
}

#define CHARENC(a)  ((a)>25?('0'+(a)-26):('A'+(a)))
#define CHARDEC(a)  ((a)<'A'?((a)-'0'+26):((a)-'A'))
	
DTIOExport void encode(char *str) 
{
	char str1[17];
	char cd[35];
	int i;
	int len=strlen(str);
	if(len<2 || len>15) {
		printf("����2-15���ַ�.\n");
		return;
	}
	static bool inited=false;
	if(!inited) {
	 srand( (unsigned)time( NULL ) );
	 inited=true;
	}
        for(i = len+1;   i < 17;i++ )
		 str[i]=rand()%127;
	int pos[]={13,8,9,14,7,0,10,1,4,12,5,15,6,2,11,3};
	for( i=0;i<16;i++) {
		str1[i]=str[pos[i]];
	}
	for(i=0;i<16;i++) {
		str1[i]^=str[16];//0x57;
		int off=(str[16]+i)%20;
		cd[2*i]=CHARENC((str1[i]>>4)+off);
		cd[2*i+1]=CHARENC((str1[i]&0x0f)+off);
	}
	cd[32]=CHARENC(str[16]>>4);
	cd[33]=CHARENC((str[16]&0x0f));
	cd[34]=0;
	printf("����Ϊ:%s\n",cd);
	//return ;
	//m_strEnc.SetWindowText(cd);
	
	/***Decode 
	str1[16]=(CHARDEC(cd[32])<<4)+CHARDEC(cd[33]);
	for(i=0;i<16;i++) {
		int off=(str1[16]+i)%20;
		str1[i]=((CHARDEC(cd[2*i])-off)<<4)+CHARDEC(cd[2*i+1])-off;
		str1[i]^=str1[16];
	}
	for(i=0;i<16;i++) {
		str[pos[i]]=str1[i];
	}
	str[16]=0;
	printf("����:%s\n",str);
	**************/
}

DTIOExport void decode(char *str) {
	char cd[40];
	char str1[20];
	int i;
	strcpy(cd,str);
	//Decode 
	str1[16]=(CHARDEC(cd[32])<<4)+CHARDEC(cd[33]);
	for(i=0;i<16;i++) {
		int off=(str1[16]+i)%20;
		str1[i]=((CHARDEC(cd[2*i])-off)<<4)+CHARDEC(cd[2*i+1])-off;
		str1[i]^=str1[16];
	}
	int pos[]={13,8,9,14,7,0,10,1,4,12,5,15,6,2,11,3};
	for(i=0;i<16;i++) {
		str[pos[i]]=str1[i];
	}
	str[16]=0;
}

int dt_file::SetLastWritePos(unsigned int off)
{
		if(!fp) ThrowWith("Write on open failed file at SetLastWritePos,filename:%s",filename);
		if(fh.fileflag!=FILEFLAGEDIT) ThrowWith("Call SetLastWritePos on readonly dt_file:%s",filename);
		fhe.lst_blk_off=off;
		fhe.ReverseByteOrder();
		fseek(fp,sizeof(fh)+sizeof(fhe),SEEK_SET);
		dp_fwrite(&fhe,sizeof(fhe),1,fp);
        	fhe.ReverseByteOrder();
		curoffset=sizeof(file_hdr)+sizeof(fhe);
		return curoffset;
}

int file_mt::AppendMt(int amt, int compress, int rn)
{
		int startrow=0;
	    if(fh.fileflag!=FILEFLAGEDIT) ThrowWith("AppendMt have been call on a readonly dt_file:%s",GetFileName());
	    if(fhe.lst_blk_off>0)
		{
			int fmt=ReadMtOrBlock(fhe.lst_blk_off,0,1,NULL);
			if(fmt==0) return -1;
			startrow=GetRowNum();
			if(startrow+rn<MAX_APPEND_BLOCKRN) {
				wociCopyRowsTo(amt,fmt,-1,0,rn);
				dtfseek(GetOldOffset());
				WriteMt(fmt,compress);
				return startrow;
			}
		}
		unsigned int off=GetFileSize();
		dtfseek(GetFileSize());
		WriteMt(amt,compress,rn);
		SetLastWritePos(off);
		return startrow;
}
#ifndef MYSQL_SERVER
bool SysAdmin::GetBlankTable(int &tabid)
{
	AutoMt mt(dts,100);
	mt.FetchFirst("select distinct dt.tabid tabid from dp.dp_table dt,dp.dp_datapart dp where dt.tabid=dp.tabid and dt.cdfileid=0 and ifnull(dp.blevel,0)%s100 ",
	   GetNormalTask()?"<":">=");
  if(mt.Wait()>0)
    tabid=mt.GetInt("tabid",0);
  else return false;
  return true;
}

bool SysAdmin::CreateDT(int tabid) 
{
	
	//AutoMt mt(dts,100);
	//mt.FetchAll("select * from dp.dp_datapart where tabid=%d %s",tabid,GetNormalTaskDesc());
	//if(mt.Wait()<1) return false;
	char sqlbf[MAX_STMT_LEN];
	// try to lock table for init structure.
	SetTrace("parsesource",tabid);
	Reload();
	lgprintf("׼������Դ��%d...",tabid);
	sprintf(sqlbf,"update dp.dp_table set recordlen=-1 where tabid=%d and recordlen!=-1",tabid); 
	int ern=DoQuery(sqlbf);
	if(ern!=1) {
		lgprintf("Դ��%d�������������н�����",tabid);
		return false;
	}
	// release lock while catch a error.
	try {
	lgprintf("����Դ��,�ع����ṹ,Ŀ������%d.",tabid);
	int tabp=wociSearchIK(dt_table,tabid);
	int srcid=dt_table.GetInt("sysid",tabp);
	int srcidp=wociSearchIK(dt_srcsys,srcid);
	if(srcidp<0)
	  ThrowWith( "�Ҳ���Դϵͳ��������,��%d,Դϵͳ��%d",tabid,srcid) ;
	//����������Դ������
	AutoHandle srcsys;
	srcsys.SetHandle(BuildDBC(srcidp));
	//����Դ���ݵ��ڴ���ṹ
	AutoMt srcmt(srcsys,0);
	srcmt.SetHandle(GetSrcTableStructMt(tabp,srcsys));
	if(wociGetRowLen(srcmt)<1) 
		ThrowWith( "Դ����������,��¼����Ϊ%d",wociGetRowLen(srcmt)) ;
	char tbname[150],idxname[150];
	GetTableName(tabid,-1,tbname,idxname,TBNAME_DEST);
	int colct=srcmt.GetColumnNum();
	#define WDBI_DEFAULT_NUMLEN 16
	#define WDBI_DEFAULT_SCALE  3
	for(int ci=0;ci<colct;ci++) {
	  if(wociGetColumnType(srcmt,ci)==COLUMN_TYPE_NUM) {
	   if(wociGetColumnDataLenByPos(srcmt,ci)==WDBI_DEFAULT_NUMLEN && wociGetColumnScale(srcmt,ci)==WDBI_DEFAULT_SCALE) {
	  	
#ifdef MYSQL_VER_51
		AutoMt mt(dts,100);
		char coln[200];
		wociGetColumnName(srcmt,ci,coln);
		mt.FetchAll("select prec,scale from dp.dp_columndef where tabid=%d and upper(columnname)=upper('%s')",tabid,coln);
		if(mt.Wait()>0) {
			wociSetColumnDisplay(srcmt,NULL,ci,coln,mt.GetInt("scale",0),mt.GetInt("prec",0));
           continue;
		}
#endif
		char sql_st[MAX_STMT_LEN];
	    wociGetCreateTableSQL(srcmt,sql_st,tbname,false);
	    char *psub;
	    while(psub=strstr(sql_st,"16,3")) memcpy(psub,"????",4);
	    log(tabid,0,132,"Դ����������,һЩ��ֵ�ֶ�ȱ����ȷ�ĳ��ȶ���,��ʹ�ø�ʽ����������.�ο��������(�޸�????�Ĳ���): \n %s .",sql_st) ;
	    sprintf(sqlbf,"update dp.dp_datapart set blevel=ifnull(blevel,0)+100  where tabid=%d",
		      tabid);
	    DoQuery(sqlbf);
	    ThrowWith( "Դ����������,һЩ��ֵ�ֶ�ȱ����ȷ�ĳ��ȶ���,��ʹ�ø�ʽ����������.\n�ο��������(�޸�????�Ĳ���): \n %s .",sql_st) ;
	   }
	  }
	}
		
	lgprintf("�ؽ�Ŀ����ṹ��CreateDP)��ǰ��Ҫ��ֹ�Ա��ķ���.�������ݻ������ṹ�����б仯,����ڽṹ�ؽ��������ݻָ�.");
	lgprintf("��¼������...");
	  CloseTable(tabid,tbname,true);
	  CreateTableOnMysql(srcmt,tbname,true);
	  CreateAllIndexTable(tabid,srcmt,TBNAME_DEST,true);
	sprintf(sqlbf,"update dp.dp_table set cdfileid=1 , recordlen=%d where tabid=%d",
		GetMySQLLen(srcmt)/*wociGetRowLen(srcmt)*/, tabid);
	DoQuery(sqlbf);
	log(tabid,0,100,"�ṹ�ؽ�:�ֶ���%d,��¼����%d�ֽ�.",colct,wociGetRowLen(srcmt));
          	}
  	catch(...) {
  	    log(tabid,0,132,"Դ����������.") ;
  	    sprintf(sqlbf,"update dp.dp_datapart set blevel=ifnull(blevel,0)+100  where tabid=%d",
		      tabid);
	    DoQuery(sqlbf);
  	    sprintf(sqlbf,"update dp.dp_table set recordlen=0,cdfileid=0 where tabid=%d",
		      tabid);
	    DoQuery(sqlbf);
	    throw;
	 }
	return true;
}

//�������ݱ����,���������ṹ�������ṹ
  bool SysAdmin::EmptyIndex(int tabid)
  {
	  AutoMt indexmt(dts,MAX_DST_INDEX_NUM);
	  indexmt.FetchAll("select databasename from dp.dp_table where tabid=%d",tabid);
	  int rn=indexmt.Wait();
	  char dbn[150];
	  char tbname[150],idxtbn[150];
	  strcpy(dbn,indexmt.PtrStr("databasename",0));
	  if(rn<1) ThrowWith("���������ʱ�Ҳ���%dĿ���.",tabid);
	  indexmt.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>0",tabid);
	  rn=indexmt.Wait();
	  if(rn<1) ThrowWith("���������ʱ�Ҳ�������������¼(%dĿ���).",tabid);
	  char sqlbf[MAX_STMT_LEN];
	  for(int i=0;i<rn;i++) {
	  	  int ilp=0;
	  	  while(GetTableName(tabid,indexmt.GetInt("indexgid",i),tbname,idxtbn,TBNAME_DEST,ilp++)) {
		   lgprintf("���������%s...",idxtbn);
		   sprintf(sqlbf,"truncate table %s",idxtbn);
		   DoQuery(sqlbf);
		  } 
	  }
	  return true;
  }
  
  //���ص�mtֻ�ܲ����ڴ��,���ܲ������ݿ�(fetch),��Ϊ�������ں����˳�ʱ�Ѿ��ͷ�
  int SysAdmin::BuildMtFromSrcTable(int srcsys,int tabid,AutoMt *mt) {
	  int tabp=wociSearchIK(dt_table,tabid);
	  const char *srctbn=dt_table.PtrStr("srctabname",tabp);

	  //>> Begin: fix dm230
      char sql[300];
	  sprintf(sql,"select * from %s.%s",dt_table.PtrStr("srcowner",tabp),srctbn);
      IgnoreBigSizeColumn(srcsys,sql);
	  //<< End:fix dm230 	
	  
  	  AutoStmt srcst(srcsys);
	  srcst.Prepare(sql);
	  wociBuildStmt(*mt,srcst,mt->GetMaxRows());
#ifdef MYSQL_VER_51
	int colct=mt->GetColumnNum();
	#define WDBI_DEFAULT_NUMLEN 16
	#define WDBI_DEFAULT_SCALE  3
	for(int ci=0;ci<colct;ci++) {
	  if(wociGetColumnType(*mt,ci)==COLUMN_TYPE_NUM) {
	   if(wociGetColumnDataLenByPos(*mt,ci)==WDBI_DEFAULT_NUMLEN && wociGetColumnScale(*mt,ci)==WDBI_DEFAULT_SCALE) {
		AutoMt mtc(dts,100);
		char coln[200];
		wociGetColumnName(*mt,ci,coln);
		mtc.FetchAll("select prec,scale from dp.dp_columndef where tabid=%d and upper(columnname)=upper('%s')",tabid,coln);
		if(mtc.Wait()>0) {
			wociSetColumnDisplay(*mt,NULL,ci,coln,mtc.GetInt("scale",0),mtc.GetInt("prec",0));
           continue;
		}
	   }
	  }
	}
#endif
	  return 0;
  }
  
  int SysAdmin::GetSrcTableStructMt(int tabp, int srcsys)
  {
	  AutoStmt srcst(srcsys);

	  //>> Begin: DM-230 ignore big column
	  char fetch_sql[4000];
	  sprintf(fetch_sql,"select * from %s.%s",dt_table.PtrStr("srcowner",tabp),dt_table.PtrStr("srctabname",tabp));
      IgnoreBigSizeColumn(srcsys,dt_table.PtrStr("srcowner",tabp),dt_table.PtrStr("srctabname",tabp),fetch_sql);
	  srcst.Prepare(fetch_sql);   
      //<< End:DM-230 ignore big column     
      
	  int mt=wociCreateMemTable();
	  wociBuildStmt(mt,srcst,10);

      //>> Begin: DM-230 change mysql key word column name
      char cfilename[256];
      strcpy(cfilename,getenv("DATAMERGER_HOME"));
	  strcat(cfilename,"/ctl/");
	  strcat(cfilename,MYSQL_KEYWORDS_REPLACE_LIST_FILE);
      ChangeMtSqlKeyWord(mt,cfilename);
	  //<< End: DM-230 change mysql key word column name

	  
#ifdef MYSQL_VER_51
	int colct=wociGetColumnNumber(mt);
	#define WDBI_DEFAULT_NUMLEN 16
	#define WDBI_DEFAULT_SCALE  3
	for(int ci=0;ci<colct;ci++) {
	  if(wociGetColumnType(mt,ci)==COLUMN_TYPE_NUM) {
	   if(wociGetColumnDataLenByPos(mt,ci)==WDBI_DEFAULT_NUMLEN && wociGetColumnScale(mt,ci)==WDBI_DEFAULT_SCALE) {
		AutoMt mtc(dts,100);
		char coln[200];
		wociGetColumnName(mt,ci,coln);
		mtc.FetchAll("select prec,scale from dp.dp_columndef where tabid=%d and upper(columnname)=upper('%s')",dt_table.GetInt("tabid",tabp),coln);
		if(mtc.Wait()>0) {
			wociSetColumnDisplay(mt,NULL,ci,coln,mtc.GetInt("scale",0),mtc.GetInt("prec",0));
           continue;
		}
	   }
	  }
	}
#endif	  
	   return mt;
  }

    void Str2Lower(char *str) {
  	    while(*str!=0) 
  	    {
  	    	*str=tolower(*str);
  	    	str++;
  	    }
	}

 //>> Begin: DM-230 ת��sql�ؼ��֣�����ת�����»����ֶ�,����:sql--->_sql
 // return 1: ��Ҫ����wdbi�ӿ���������
 // return 0: ����Ҫ����wdbi�ӿ���������
 int SysAdmin::ChangeColumns(char *columnName,char* MysqlKeyWordReplaceFile)
 {
     FILE *fp = NULL;
	 fp = fopen(MysqlKeyWordReplaceFile,"rt");
	 if(fp==NULL) 
	 {
		lgprintf("Mysql�滻�ؼ����б��ļ�[%s]�����ڣ��޷��滻�ؼ����б�.",MysqlKeyWordReplaceFile);
		return 0;
	 }
	 char lines[300];
	 while(fgets(lines,300,fp)!=NULL)
	 {
	    int sl = strlen(lines);
		if(lines[sl-1]=='\n') lines[sl-1]=0;
		if(strcasecmp(lines,columnName) == 0)
		{
		   // �滻�ؼ���
		   char _columnName[250];
		   sprintf(_columnName,"_%s",columnName);
		   strcpy(columnName,_columnName);
           
		
		   fclose(fp);
		   fp = NULL;
		   return 1;		  
		}
	 }
	 fclose(fp);
	 fp=NULL;
	 return 0;     
 }

 // �޸�Mt��sql�ؼ���
 bool SysAdmin::ChangeMtSqlKeyWord(int mt,char* MysqlKeyWordReplaceFile)
 { 
     char col_name[255];	
     for(int i=0;i<wociGetColumnNumber(mt);i++)
	 {
	     wociGetColumnName(mt,i,col_name);
         if(ChangeColumns(col_name,MysqlKeyWordReplaceFile))  
         {
             // ������Ҫ�滻����
             wociSetColumnName(mt,i,col_name);
         }
     }
     return true;
 } 
 //>> End: DM-230 ת��sql�ؼ��֣�����ת�����»����ֶ�,����:sql--->_sql
  
  	
 //>> Begin: DM-230 ���Դ��ֶ��У��ϳ��µĲɼ�sql���
 void SysAdmin::IgnoreBigSizeColumn(int dts,char* dp_datapart_extsql)
 {
	 // ��ȡԴ����������Ϣ
	  AutoStmt table_stmt(dts);
	  Str2Lower(dp_datapart_extsql);
	  table_stmt.Prepare(dp_datapart_extsql);
	  int colct = wociGetStmtColumnNum(table_stmt);
	  bool ExistBigCol = false;
	  int colType = 0;
	  for(int i=0;i<colct;i++)
	  {
		  colType = wociGetStmtColumnType(table_stmt,i);
		  if( (colType == SQLT_CLOB) || (colType == SQLT_BLOB) || 
			  (colType == SQLT_BFILEE) || (colType == SQLT_CFILEE) ||
			  (colType == SQLT_BIN) || (colType == SQLT_LVB) ||
			  (colType == SQLT_LBI))
		  {
			  ExistBigCol = true;
			  break;
		  }
	  }

      // ��ȡsql�е�һ��from������ַ���
      char sqlAfterFirstFrom[3000];
	  char *p = strstr(dp_datapart_extsql,"from");
      strcpy(sqlAfterFirstFrom,p);
	  
	  if(ExistBigCol)
	  { 			 
		  char colName[COLNAME_LEN] = {0};
		  char extsql[4000] = {0};
		  strcpy(extsql,"select ");
		  for(int i=0;i<colct;i++)		  
		  {
			  wociGetStmtColumnName(table_stmt,i,colName);			  
			  bool has_valid_column = false;		  
			  switch(wociGetStmtColumnType(table_stmt,i))
			  {   
				  case SQLT_CLOB:
				  case SQLT_BLOB:
				  case SQLT_BFILEE:   
				  case SQLT_CFILEE: 			  
				  case SQLT_BIN:			  
				  case SQLT_LVB:			
			      case SQLT_LBI:       // LONG RAW ,FIX DM-252	  
					  break;			  
				  default:				  
					  if(has_valid_column)					  
					  { 					  
						 strcat(extsql,",");			  
					  }
					  strcat(extsql,colName);				  
					  has_valid_column = true;					  
					  break;  
			  } 	  
		  } 	  
		  
		  sprintf(dp_datapart_extsql,"%s %s",extsql,sqlAfterFirstFrom);
	  } 
 }
 int SysAdmin::IgnoreBigSizeColumn(int dts,const char* dbname,const char* tbname,char* dp_datapart_extsql)
 {
    // ��ȡԴ����������Ϣ
    AutoStmt table_stmt(dts);
    table_stmt.Prepare("select * from %s.%s",dbname,tbname);
	int colct = wociGetStmtColumnNum(table_stmt);
	bool ExistBigCol = false;
	int colType = 0;
    for(int i=0;i<colct;i++)
    {
        colType = wociGetStmtColumnType(table_stmt,i);
		if( (colType == SQLT_CLOB) || (colType == SQLT_BLOB) || 
			(colType == SQLT_BFILEE) || (colType == SQLT_CFILEE) ||
			(colType == SQLT_BIN) || (colType == SQLT_LVB)||
            (colType == SQLT_LBI))
		{
		    ExistBigCol = true;
			break;
		}
    }

	if(ExistBigCol)
	{	    	   
        char colName[COLNAME_LEN] = {0};
		char extsql[4000] = {0};
		strcpy(extsql,"select ");
		for(int i=0;i<colct;i++)		
		{
		    wociGetStmtColumnName(table_stmt,i,colName);		    
			bool has_valid_column = false;			
			switch(wociGetStmtColumnType(table_stmt,i))
			{	
			    case SQLT_CLOB:
			    case SQLT_BLOB:
			    case SQLT_BFILEE:	
			   	case SQLT_CFILEE:				
				case SQLT_BIN:				
				case SQLT_LVB:		
				case SQLT_LBI:       // LONG RAW ,FIX DM-252
					break;				
				default:				
					if(has_valid_column)					
					{						
					   strcat(extsql,",");				
					}
					strcat(extsql,colName);					
					has_valid_column = true;					
					break;	
			}		
		}		
		
		sprintf(extsql+strlen(extsql)," from %s.%s ",dbname,tbname);
		strcpy(dp_datapart_extsql,extsql);
    } 

	return 0;
  }
	
 //>> End: DM-230 ���Դ��ֶ��У��ϳ��µĲɼ�sql���
  
  //���ȥ���ֶ������ļ���֧��,������ĺ��������
  bool SysAdmin::CreateTableOnMysql(int srcmt,const char *tabname,bool forcecreate)
  {
	  //���Ŀ����Ѵ��ڣ���ɾ��
	    char sqlbf[MAX_STMT_LEN];
			bool exist=conn.TouchTable(tabname,true);
			if(exist && !forcecreate) 
				ThrowWith("Create MySQL Table '%s' failed,table already exists.",tabname);
			//FIX JIRA DM-61:dead lock on DML	
	    connlock.DoQuery("lock tables dp.dp_lock low_priority write");
			if(exist) {
				printf("table %s has exist,dropped.\n",tabname);
				sprintf(sqlbf,"drop table %s",tabname);
				conn.DoQuery(sqlbf);
				mSleep(100);
			}
			//����Ŀ��꼰����ṹ�������ļ�

			//>> Begin: DM-230 change mysql key word column name
			char cfilename[256];
			strcpy(cfilename,getenv("DATAMERGER_HOME"));
			strcat(cfilename,"/ctl/");
			strcat(cfilename,MYSQL_KEYWORDS_REPLACE_LIST_FILE);
			ChangeMtSqlKeyWord(srcmt,cfilename);
			//<< End: DM-230 change mysql key word column name
			
			wociGetCreateTableSQL(srcmt,sqlbf,tabname,true);
			strcat(sqlbf," PACK_KEYS = 1");
			//printf("%s.\n",sqlbf);
			conn.DoQuery(sqlbf);
			mSleep(100);
			connlock.DoQuery("unlock tables");
			return true;
  }
  void SysAdmin::ReleaseTable() {
    	  if(lastlocktable[0]!=0) {
    	  	lgprintf("�ͷŶԱ�'%s'��������ˢ�±�",lastlocktable);
    	  	connlock.DoQuery("unlock tables");
    	  	connlock.FlushTables(lastlocktable);
    	  	memset(lastlocktable,0,sizeof(lastlocktable));
    	  }
  }  	
  
  void SysAdmin::CloseTable(int tabid,char *tbname,bool cleandt,bool withlock) {
	  char tabname[300];
	  char indextabname[300];
	  indextabname[0]=0;
	  //AutoStmt st(dts);
	  //if(cleandt)
	  //	  st.Prepare("update dp.dp_table set recordnum=0,firstdatafileid=0,totalbytes=0 where tabid=%d",tabid);
	  //else
	  //	  st.Prepare("update dp.dp_table set recordnum=0 where tabid=%d",tabid);
	  //st.Execute(1);
	  //st.Wait();
	  //wociCommit(dts);
	  if(tbname==NULL) {
	  	// 2010-07-18 ��MySQL 5.1�У���������Ǩ�ƻ������ݶ���ѹ��ʱ��
	  	// ���߹�����lock Ŀ��� write,����merge index table�е��ӱ�Ϊֻ����
	  	//���ܼ�write lock�Ĵ�����ɲ������ߡ���ϲ�����֤��MySQL�ĵ��ж������Ƶ�����
	  	// http://dev.mysql.com/doc/refman/5.1/en/lock-tables.html
	  	// ʹ��merge���������������Գɹ���lock write,����,����LOW_PRIORITYѡ�Ч����ͬ��
	  	// Ŀ���������
	  	//   1.����ж�������lock write�ȴ�
	  	//   2. ���lock write��������merge��)�ȴ���������Ŀ����Ķ���������������
	  	//   3. һ��lock write �ɹ���������Ŀ����Ķ�������������
	  AutoMt indexmt(dts,10);
	  indexmt.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>0 limit 10",tabid);
	  int rn=indexmt.Wait();
	  if(rn>0) //ThrowWith("�ر�Ŀ���ʱ�Ҳ�������������¼(Ŀ��� %d).",tabid);
		  GetTableName(tabid,indexmt.GetInt("indexgid",0),tabname,indextabname,TBNAME_DEST);
		else  GetTableName(tabid,-1,tabname,indextabname,TBNAME_DEST);
	  }
	  else strcpy(tabname,tbname);
	  lgprintf("�ر�'%s'��...",tabname);
	  if(withlock) {
	    char sql[MAX_STMT_LEN];
	    // In case of a dead lock for write.
	    //  A table has been locked and is not a same table ,then unlock it first
	   
    	if(lastlocktable[0]!=0 && strcmp(lastlocktable,indextabname)!=0)
    	     ReleaseTable();
	    //The second lock of same table,will be skipped!
	    if(strcmp(lastlocktable,indextabname)!=0) {
	       sprintf(sql,"lock tables %s LOW_PRIORITY write ",indextabname);
    	       lgprintf("��'%s'�����ͼ���--ֻ�иñ�û�ж��������ż������߹���...",tabname);
    	       connlock.DoQuery(sql);
    	       lgprintf("��'%s'�����ͼ���--�ɹ�.",tabname);
    	       strcpy(lastlocktable,indextabname);
    	       // TODO:
    	       // 20100718 ��Ŀ���������Ϊ��merge�����������󣨼�ǰ��),
    	       //���߹����в��ܶ���������flush table��������ʹ��ͬһ���ỰҲ���������
    	       // ���������---ȷ������������ʼǰ��û����ִ���еĶ�����
    	       ReleaseTable();
    	    }
    	  }
	  {
		  lgprintf("ɾ��DP�����ļ�.");
		  char basedir[300];
		  char streamPath[300];
		  char tbntmp[200];
		  strcpy(basedir,GetMySQLPathName(0,(char*)"msys"));
		  strcpy(tbntmp,tabname);
		  char *psep=strstr(tbntmp,".");
		  if(psep==NULL) 
			  ThrowWith("Invalid table name format'%s',should be dbname.tbname.",tbname);
		  *psep='/';
		  sprintf(streamPath,"%s%s.DTP",basedir,tbntmp);
#ifdef WIN32
			_chmod(streamPath,_S_IREAD | _S_IWRITE );
			_unlink(streamPath);
#else
		  unlink(streamPath);
#endif
	  }
    	  // table has been locked,so dont flush again!
    	  if(strcmp(lastlocktable,indextabname)!=0 && indextabname[0]!=0)
    	    connlock.FlushTables(indextabname);
    	    
	  lgprintf("��'%s'�����...",tabname);
	  // if withlock is true, not forgot to unlock tables!!!!!!
  }	
  
  double StatMySQLTable(const char *path,const char *fulltbn)
{
	char srcf[300],fn[300];
	strcpy(fn,fulltbn);
	char *psep=strstr(fn,".");
	if(psep==NULL) 
		  ThrowWith("Invalid table name format'%s',should be dbname.tbname.",fn);
	  *psep='/';
	struct stat st;
	double rt=0;
	// check destination directory
	sprintf(srcf,"%s%s%s",path,fn,".frm");
	stat(srcf,&st);
	rt+=st.st_size;
	sprintf(srcf,"%s%s%s",path,fn,".MYD");
	stat(srcf,&st);
	rt+=st.st_size;
	sprintf(srcf,"%s%s%s",path,fn,".MYI");
	stat(srcf,&st);
	rt+=st.st_size;
	return rt;
}

//�����������¸����ֶ�ֵ(dt_index.reusecols)
//�������������¼������
//1. �����������������ļ�,��Ҫ����ԭ�еı������ݼ�¼.
//2. ����ǰ����������,����Ҫ����ԭ�еı������ݼ�¼.
//3. ����ǰ��ʹ��ͬһ�������ļ�,��Ҫʹ���µı��������ṹ�滻ԭ����,��ʱҲ����Ҫ����ԭ�еı������ݼ�¼.
//
//   �п���ֻ�ǲ�����������(һ���򼸸�����)
//
//  ��������������ǰ��Ҫȷ�����ݵ�������:
//    1.������������,�������������Ѿ���.
//    2.ȫ����������,�����е����ݶ���ȱʧ.
void SysAdmin::DataOnLine(int tabid) {
	char tbname[150],idxname[150];
	char tbname_p[150],idxname_p[150];
	char sql[MAX_STMT_LEN];
	bool ec=wociIsEcho();
	wociSetEcho(FALSE);
	lgprintf("%d������...",tabid);
	
	AutoMt mtp(dts,1000);
//	mtp.FetchAll("select distinct datapartid,oldstatus from dp.dp_datapart where tabid=%d and status=21 and begintime<now() %s order by datapartid",tabid,GetNormalTaskDesc());
	mtp.FetchFirst("select distinct datapartid,oldstatus from dp.dp_datapart where tabid=%d and status=21 and begintime<now() order by datapartid",tabid);
	int rn=mtp.Wait();
	if(rn<1)
		return ;
	AutoMt mti(dts,1000);
	mti.FetchFirst("select * from dp.dp_index where tabid=%d and issoledindex>0",tabid);
	AutoStmt st(dts);
	int rni=mti.Wait();
	if(rni<1)
		ThrowWith("�Ҳ�����Ӧ��%d�����κ�������",tabid);
	
	double idxtbsize=0;
	int datapartid=-1;
	AutoMt mt(dts,1000);
	mt.FetchFirst("select * from dp.dp_datapart where tabid=%d and status!=21 and begintime<now() limit 10",tabid);
	bool onlypart=mt.Wait()>0;
	int pi;
	try {
		CloseTable(tabid,NULL,false,true);
		// close all index tables;
		for(int idi=0;idi<rni;idi++) {
			int indexgid=mti.GetInt("indexgid",idi);
			GetTableName(tabid,indexgid,tbname,idxname,TBNAME_DEST) ;
			sprintf(sql,"flush tables  %s",idxname);
			conn.DoQuery(sql);
			for(pi=0;pi<rn;pi++) {
				datapartid=mtp.GetInt("datapartid",pi);
				GetTableName(tabid,indexgid,tbname,idxname,TBNAME_DEST,-1,datapartid) ;
				sprintf(sql,"flush tables  %s",idxname);
				conn.DoQuery(sql);
			}
		}
		/* lock tables ... write ,then only these locked table allowed to query,and rename table command not support on 
		      locked tables or none locked tables.*/
		//GetTableName(tabid,-1,tbname,idxname,TBNAME_DEST) ;
		//sprintf(sql,"lock tables %s write",tbname);
		//lgprintf("׼������: %s.",sql);
		//conn.DoQuery(sql);
		for(pi=0;pi<rn;pi++) {
			//����Ƿ���Ҫ����ԭ�е�����,���������װ��,����ѹ�������,����ͬһ�������ϲ���,����Ҫ����.
			bool replace=(mtp.GetInt("oldstatus",pi)==4);
			datapartid=mtp.GetInt("datapartid",pi);
			if(replace) {
				st.DirectExecute("update dp.dp_datafilemap set fileflag=2 where tabid=%d and fileflag=0 and datapartid=%d",tabid,datapartid);
				st.DirectExecute("update dp.dp_datafilemap set fileflag=0 where tabid=%d and fileflag=1 and datapartid=%d",tabid,datapartid);
			}
			mt.FetchFirst("select sum(recordnum) rn  from dp.dp_datafilemap "
				" where tabid=%d and fileflag=0 and isfirstindex=1 ",tabid);
			mt.Wait();
			double sumrn=mt.GetDouble("rn",0);
			for(int idi=0;idi<rni;idi++) {
				int indexgid=mti.GetInt("indexgid",idi);
				GetTableName(tabid,indexgid,tbname,idxname,TBNAME_DEST,-1,datapartid) ;
				if(replace && conn.TouchTable(idxname,true)) {
					GetTableName(tabid,indexgid,tbname_p,idxname_p,TBNAME_FORDELETE,-1,datapartid);
					conn.RenameTable(idxname,idxname_p,true);
				}
				GetTableName(tabid,indexgid,tbname_p,idxname_p,TBNAME_PREPONL,-1,datapartid);
				//�����������,���ʾ�÷�������Ϊ��
				if(conn.TouchTable(idxname_p,true)) {
					conn.RenameTable(idxname_p,idxname,true);
					idxtbsize+=StatMySQLTable(GetMySQLPathName(0,(char*)"msys"),idxname);
				}
				else if(sumrn>0)
					ThrowWith("��%d�ķ���%d,ָʾ��¼��Ϊ%.0f,���Ҳ���������'%s'",tabid,datapartid,sumrn,idxname_p);
			}
		}
		
		 
		//���ֻ�ǲ��ַ���װ��,���޸�Ŀ���.
		
		//����Ĵ���������������ͻ������ִ��--�޸������������ú�����װ�룬���ܵ�����������ˣ�������Ҫ
		//  �ٶ�Ŀ��������Ѿ����ú��˸�ʽ�����Ҳ���Ҫ�ٸ��ġ�
		
		if(!onlypart) {
			connlock.DoQuery("unlock tables");
    	       		strcpy(lastlocktable,"");
			GetTableName(tabid,-1,tbname_p,idxname_p,TBNAME_FORDELETE);
			if(conn.TouchTable(tbname)) 
				conn.RenameTable(tbname,tbname_p,true);
			GetTableName(tabid,-1,tbname_p,idxname_p,TBNAME_PREPONL);
			conn.RenameTable(tbname_p,tbname,true);
		}
		
		// ��������Ĵ��벻ִ�У���Ҫ���Ԥ����Ŀ���
		//GetTableName(tabid,-1,tbname_p,idxname_p,TBNAME_PREPONL);
		//sprintf(sql,"drop table %s",tbname_p);
		//conn.DoQuery(sql);
		//�����ļ�����
		mt.FetchFirst("select * from dp.dp_datafilemap where tabid=%d and (fileflag=0 or fileflag is null) and isfirstindex=1 order by datapartid ,fileid",tabid);
		rn=mt.Wait();
		int k;
		for(k=0;k<rn;k++) {
		 //Build table data file link information.
		 if(k+1==rn) {
		  dt_file df;
		  df.Open(mt.PtrStr("filename",k),2,mt.GetInt("fileid",k));
		  df.SetFileHeader(0,NULL);
		  df.Open(mt.PtrStr("idxfname",k),2,mt.GetInt("fileid",k));
		  df.SetFileHeader(0,NULL);
		 }
		 else {
		  dt_file df;
		  df.Open(mt.PtrStr("filename",k),2,mt.GetInt("fileid",k));
		  df.SetFileHeader(0,mt.PtrStr("filename",k+1));
		  df.Open(mt.PtrStr("idxfname",k),2,mt.GetInt("fileid",k));
		  df.SetFileHeader(0,mt.PtrStr("idxfname",k+1));
		 }
		}

		mt.FetchFirst("select sum(recordnum) rn from dp.dp_datafilemap where tabid=%d and (fileflag=0 or fileflag is null) and isfirstindex=1",tabid);
		mt.Wait();
		double sumrn=mt.GetDouble("rn",0);
		mt.FetchFirst("select sum(recordnum) rn,sum(filesize) tsize,sum(idxfsize) itsize ,count(*) fnum from dp.dp_datafilemap where tabid=%d and (fileflag=0 or fileflag is null) ",tabid);
		mt.Wait();
		double tsize=mt.GetDouble("tsize",0);
		double itsize=mt.GetDouble("itsize",0);
		int fnum=mt.GetDouble("fnum",0);
		mt.FetchFirst("select fileid from dp.dp_datafilemap where tabid=%d and (fileflag=0 or fileflag is null) and isfirstindex=1 order by datapartid ,fileid ",tabid);
		int firstfid=0;
		if(mt.Wait()<1) {
			lgprintf("װ��һ���ձ� ���������ݳ�ȡ����������Ƿ�����.");
			firstfid=0;
			itsize=tsize=0;fnum=0;
			sumrn=0;
		}
		else firstfid=mt.GetInt("fileid",0);
		st.DirectExecute("update dp.dp_table set recordnum=%.0f,firstdatafileid=%d,totalbytes=%15.0f,"
			"datafilenum=%d,cdfileid=2 where tabid=%d",
			sumrn,firstfid,tsize,fnum,tabid);
		CreateMergeIndexTable(tabid);
	
	        st.DirectExecute("update dp.dp_datapart set status=5 where tabid=%d and status=21",
		  tabid);
	        st.DirectExecute("update dp.dp_datafilemap set procstatus=0 where tabid=%d and fileflag=0 ",
		   tabid);
	        lgprintf("״̬4(DestLoaded)-->5(Complete),tabid:%d.",tabid);
		
		BuildDTP(tbname);
		ReleaseTable();
		lgprintf("��'%s'�ɹ�����,��¼��%.0f,����%.0f,����%.0f. MySQLˢ��...",tbname,sumrn,tsize+itsize,idxtbsize);
		wociSetEcho(ec);
		log(tabid,0,115,"���ѳɹ�����,��¼��%.0f,����%.0f,����%.0f. ",sumrn,tsize+itsize,idxtbsize);
	}
	catch(...) {
		//�ָ������ļ���������
		//ֻ��������װ��ʱ�����Ҫ�ָ�����Ŀ,��װ������������Ҫ�ָ�.
		
		//�ͷŶԱ��������⽫ʹǰ̨�����ձ���������ͷţ������γ�������
		ReleaseTable();
		mt.FetchFirst("select distinct datapartid from dp.dp_datafilemap "
			" where fileflag=2 and tabid=%d",tabid);
		rn=mt.Wait();
    		st.DirectExecute("update dp.dp_datapart set blevel=ifnull(blevel,0)+100,status=%d where tabid=%d and status=21 ",rn<1?30:3,tabid);
		lgprintf("����״̬�ѻָ�Ϊ����װ��.");
		char deltbname[150];
		for(pi=0;pi<rn;pi++) {
			//�ָ�ԭ�е�����.
			datapartid=mt.GetInt(0,pi);
			st.DirectExecute("update dp.dp_datafilemap set fileflag=1 where tabid=%d and fileflag=0 and datapartid=%d",tabid,datapartid);
			st.DirectExecute("update dp.dp_datafilemap set fileflag=0 where tabid=%d and fileflag=2 and datapartid=%d",tabid,datapartid);
			for(int idi=0;idi<rni;idi++) {
				//�ָ�������
				int indexgid=mti.GetInt("indexgid",idi);
				GetTableName(tabid,indexgid,tbname_p,deltbname,TBNAME_FORDELETE,-1,datapartid);
				if(conn.TouchTable(deltbname)) {
					GetTableName(tabid,indexgid,tbname_p,idxname_p,TBNAME_PREPONL,-1,datapartid);
					GetTableName(tabid,indexgid,tbname,idxname,TBNAME_DEST,-1,datapartid) ;
					if(conn.TouchTable(idxname)) 
						conn.RenameTable(idxname,idxname_p,true);
					conn.RenameTable(deltbname,idxname,true);
				}
			}
		}
		
		if(!onlypart) {
			GetTableName(tabid,-1,deltbname,idxname_p,TBNAME_FORDELETE);
			if(conn.TouchTable(deltbname)) {
				GetTableName(tabid,-1,tbname_p,idxname_p,TBNAME_PREPONL);
				if(conn.TouchTable(tbname)) 
					conn.RenameTable(tbname,tbname_p,true);
				conn.RenameTable(deltbname,tbname,true);
			}
		}
		throw;
	}		
  }
  
  void SysAdmin::BuildDTP(const char *tbname)
  {
	  lgprintf("����DP�����ļ�.");
	  char basedir[300];
	  char streamPath[300];
	  char tbntmp[200];
	  
	  strcpy(basedir,GetMySQLPathName(0,(char*)"msys"));
	  
	  dtioStream *pdtio=new dtioStreamFile(basedir);
	  strcpy(tbntmp,tbname);
	  char *psep=strstr(tbntmp,".");
	  if(psep==NULL) 
		  ThrowWith("Invalid table name format'%s',should be dbname.tbname.",tbname);
	  *psep=0;
	  psep++;
	  sprintf(streamPath,"%s%s" PATH_SEP "%s.DTP",basedir,tbntmp,psep);
	  try {
	  pdtio->SetStreamName(streamPath);
	  pdtio->SetWrite(false);
	  pdtio->StreamWriteInit(DTP_BIND);
	  
	  pdtio->SetOutDir(basedir);
	  {
		  dtioDTTable dtt(tbntmp,psep,pdtio,false);
		  dtt.FetchParam(dts);
		  dtt.SerializeParam();
	  }
	  pdtio->SetWrite(true);
	  pdtio->StreamWriteInit(DTP_BIND);
	  pdtio->SetOutDir(basedir); 
	  {
		  dtioDTTable dtt(tbntmp,psep,pdtio,false);
		  dtt.FetchParam(dts);
		  dtt.SerializeParam();
	  }
	  delete pdtio;
	  }
	  catch(...) {
	  	unlink(streamPath);
	  	throw;
	  }
	  if(lastlocktable[0]!=0)
	   ReleaseTable();
	  else conn.FlushTables(tbname);
  }
  
  //����������ȫ��(�����ݿ���)
  void SysAdmin::GetPathName(char *path,const char *tbname,const char *surf) {
	  char dbname[150];
	  strcpy(dbname,tbname);
	  char *dot=strstr(dbname,".");
	  if(dot==NULL) 
		  ThrowWith("����������ȫ��('%s')",tbname);
	  char *mtbname=dot+1;
	  *dot=0;
	  const char *pathval=GetMySQLPathName(0,(char*)"msys");
	  sprintf(path,"%s" PATH_SEP "%s" PATH_SEP "%s.%s",pathval,dbname,mtbname,surf);
  }
  
  //type TBNAME_DEST: destination name
  //type TBNAME_PREPONL: prepare for online
  //type TBNAME_FORDELETE: fordelete 

    
  //ʹ�����datapartoff��datapartidָ��������,�������
  // �������ָ��(��Ϊ-1),�򷵻ز�������������������(�ϵĸ�ʽ).
  bool SysAdmin::GetTableName(int tabid,int indexid,char *tbname,char *idxname,int type,int datapartoff,int datapartid) {
	  char tbname1[150],idxname1[150],dbname[130];
	  idxname1[0]=0;
	  if(tabid==-1) 
		  ThrowWith("Invalid tabid parameter on call SysAdmin::GetTableName.");
	  AutoMt mt(dts,300);
	  mt.FetchAll("select * from dp.dp_table where tabid=%d",tabid);
	  int rn=mt.Wait();
	  if(rn<1)
		  ThrowWith("Tabid is invalid  :%d",tabid);
	  strcpy(tbname1,mt.PtrStr("tabname",0));
	  Trim(tbname1);
	  strcpy(dbname,mt.PtrStr("databasename",0));
	  Trim(dbname);
	  if(indexid!=-1) {
	  	  if(datapartoff>=0 || datapartid>0) {
	  	   if(datapartoff>=0) 
	  	    mt.FetchAll("select datapartid from dp.dp_datapart where tabid=%d order by datapartid",tabid);
	  	   else 
	  	    mt.FetchAll("select datapartid from dp.dp_datapart where tabid=%d and datapartid=%d",tabid,datapartid);
	  	   rn=mt.Wait();
	  	   if(rn<1)
	  	    ThrowWith("tabid:%d,indexid:%d,datapartoff:%d,datapartid:%d ��Ч.",tabid,indexid,datapartoff,datapartid);
	  	   if(datapartoff>=0) {
	  	    if(datapartoff>=rn) return false;
	  	     datapartid=mt.GetInt(0,datapartoff);
	  	   }
	  	  }
	  	  mt.FetchAll("select * from dp.dp_index where tabid=%d and indexgid=%d and issoledindex>0",tabid,indexid);
		  rn=mt.Wait();
		  if(rn<1)
		  	 ThrowWith("tabid:%d,indexid:%d ��Ч.",tabid,indexid);
		  
		  //���ش������򲻴�����(�ϸ�ʽ)������������
		  if(datapartid>=0) 
		     sprintf(idxname1,"%sidx%d_p_%d",tbname1,indexid,datapartid);
		  else if(strlen(mt.PtrStr("indextabname",0))>0)
		    strcpy(idxname1,mt.PtrStr("indextabname",0));
		  else sprintf(idxname1,"%sidx%d",tbname1,indexid);
	  }
//	  if(STRICMP(idxname1,tbname1)==0) 
//	   ThrowWith("��������'%s'��Ŀ�����ظ�:indexid:%d,tabid:%d!",indexid,tabid);
	  if(type!=TBNAME_DEST) {
		  if(indexid!=-1) {
			  strcat(idxname1,"_");
			  strcat(idxname1,dbname);
		  }
		  strcat(tbname1,"_");
		  strcat(tbname1,dbname);
		  if(type==TBNAME_PREPONL)
			  strcpy(dbname,PREPPARE_ONLINE_DBNAME);
		  else if(type==TBNAME_FORDELETE)
			  strcpy(dbname,FORDELETE_DBNAME);
		  else ThrowWith("Invalid table name type :%d.",type);
	  }
	  if(tbname) {
	  	sprintf(tbname,"%s.%s",dbname,tbname1);
        Str2Lower(tbname);
	  }
	  if(indexid!=-1 && idxname){
		  sprintf(idxname,"%s.%s",dbname,idxname1);
          Str2Lower(idxname);
	  }
	  return true;
  }
  
  void SysAdmin::CreateAllIndex(int tabid,int nametype,bool forcecreate,int ci_type,int datapartid)
  {
	  AutoMt mt(dts,MAX_DST_INDEX_NUM);
	  mt.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>0 order by seqindattab",tabid);
	  int rn=mt.Wait();
	  if(rn<1)
		  ThrowWith("�Ҳ���%d���Ķ���������",tabid);
	  for(int i=0;i<rn;i++)
		  CreateIndex(tabid,mt.GetInt("indexgid",i),nametype,forcecreate,ci_type,datapartid);
  }
  
  void SysAdmin::RepairAllIndex(int tabid,int nametype,int datapartid)
  {
	  AutoMt mt(dts,MAX_DST_INDEX_NUM);
	  mt.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>0 order by seqindattab",tabid);
	  int rn=mt.Wait();
	  if(rn<1)
		  ThrowWith("�Ҳ���%d���Ķ���������",tabid);
	  for(int i=0;i<rn;i++) {
		  int indexid=mt.GetInt("indexgid",i);
		  char tbname[100],idxname[100];
		  //int ilp=0;
		  if(GetTableName(tabid,indexid,tbname,idxname,nametype,-1,datapartid)) {
		   if(conn.TouchTable(idxname)) {
		    lgprintf("������ˢ��...");
		    FlushTables(idxname);
		    lgprintf("�������ؽ�:%s...",idxname);
		  
		    char fn[500],tpath[500];
		    GetPathName(fn,idxname,"MYI");
		    char cmdline[500];
		    // -n ѡ������ǿ��ʹ������ʽ�޸�
			//mysql5.1 : windows ƽ̨tmpdir����Ĳ��������õ����ţ�
			strcpy(tpath,GetMySQLPathName(0,(char*)"msys"));
#ifdef WIN32
			if(tpath[strlen(tpath)-1]=='\\')
				tpath[strlen(tpath)-1]=0;
#endif
		    sprintf(cmdline,"myisamchk -rqvn --tmpdir=\"%s\" %s ",tpath,fn);
			printf(cmdline);
		    int rt=system(cmdline);
		    //wait(&rt);
		    if(rt)
			  ThrowWith("�����ؽ�ʧ��!");
		    FlushTables(idxname);
		   }
		  }
		  //char sqlbf[MAX_STMT_LEN];
		  //sprintf(sqlbf,"repair table %s quick",idxname);
		  //conn.DoQuery(sqlbf);
	  }
  }
  
  void SysAdmin::CreateIndex(int tabid,int indexid,int nametype,bool forcecreate,int ci_type,int datapartoff,int datapartid)
  {
	  AutoMt mt(dts,10);
	  mt.FetchAll("select * from dp.dp_index where tabid=%d and indexgid=%d and issoledindex>0",tabid,indexid);
	  int rn=mt.Wait();
	  if(rn<1)
		  ThrowWith("Indexgid is invalid or not a soled :%d",indexid);
	  char colsname[300];
	  strcpy(colsname,mt.PtrStr("columnsname",0));
	  char tbname[100],idxname[100];
	  int ilp=0;
	  if(GetTableName(tabid,indexid,tbname,idxname,nametype,datapartoff,datapartid)) {
	   //��������������
	   if(ci_type==CI_ALL || ci_type==CI_IDX_ONLY)
		  CreateIndex(idxname,mt.GetInt("seqinidxtab",0),colsname,forcecreate);
	   if(ci_type==CI_ALL || ci_type==CI_DAT_ONLY)
		  CreateIndex(tbname,mt.GetInt("seqindattab",0),colsname,forcecreate);
	  
	  //��������������
	  //���Ҹö������������ķǶ������������Դ�Ϊ���ݽ���������
	   mt.FetchAll("select * from dp.dp_index where tabid=%d and indexgid=%d and issoledindex=0 order by seqinidxtab",
		  tabid,indexid);
	   int srn=mt.Wait();
	   for(int j=0;j<srn;j++) {
		  strcpy(colsname,mt.PtrStr("columnsname",j));
		  if(ci_type==CI_ALL || ci_type==CI_IDX_ONLY)
			  CreateIndex(idxname,mt.GetInt("seqinidxtab",j),colsname,forcecreate);
		  if(ci_type==CI_ALL || ci_type==CI_DAT_ONLY)
			  CreateIndex(tbname,mt.GetInt("seqindattab",j),colsname,forcecreate);
	   }
	}
  }
  
  void SysAdmin::CreateIndex(const char *dtname, int id, const char *colsname, bool forcecreate)
  {
	  //create index����е��������Ʋ����������ݿ�����
	  const char *tbname=strstr(dtname,".");
	  if(tbname==NULL) tbname=dtname;
	  else tbname++;
	  char sqlbf[MAX_STMT_LEN];
	  //FIX JIRA DM-61:dead lock on DML	
	  connlock.DoQuery("lock tables dp.dp_lock low_priority write");
	  if(forcecreate) {
		  try {
		  sprintf(sqlbf,"drop index %s_%d on %s",tbname,
			  id,dtname);
		  conn.DoQuery(sqlbf);
		  mSleep(100);
		}
		catch(...) {};
	  }
	  sprintf(sqlbf,"create index %s_%d on %s(%s)",
		  tbname,id,
		  dtname,colsname);
	  lgprintf("��������:%s.",sqlbf);
	  conn.DoQuery(sqlbf);
		mSleep(100);
	  connlock.DoQuery("unlock tables");
  }
  
  // �����ļ�״̬Ϊ2���ȴ�ɾ��������ȫ��������������ݳɹ����ߺ�ִ�С�
int SysAdmin::CleanData(bool prompt,int tabid)
{
	AutoMt mt(dts,100);
	AutoStmt st(dts);
	mt.FetchAll("select tabid,indexgid,sum(recordnum) recordnum from dp.dp_datafilemap where fileflag=2 group by tabid,indexgid");
	int rn=mt.Wait();
	if(rn<1) {
		printf("û��������Ҫ���!\n");
	}
	else {
	lgprintf("����ɾ������...");
	//for(int i=0;i<rn;i++) {
	tabid=mt.GetInt("tabid",0);
	//	int recordnum=mt.GetInt("recordnum",0);
	char tbname[150],idxname[150];
	GetTableName(tabid,-1,tbname,idxname,TBNAME_FORDELETE);
	AutoMt dtmt(dts,MAX_DST_DATAFILENUM);
	dtmt.FetchAll("select filename from dp.dp_datafilemap where tabid=%d  and fileflag=2",
		tabid);
	int frn=dtmt.Wait();
	AutoMt idxmt(dts,200);
	idxmt.FetchAll("select idxfname as filename from dp.dp_datafilemap where tabid=%d  and fileflag=2",
		tabid);
	idxmt.Wait();
	//int firstdatafileid=mt.GetInt("firstdatafileid",0);
	//int srctabid=mt.GetInt("srctabid",0);
	if(prompt)
	{
		while(true) {
			printf("\n�� '%s': ����ɾ�������:%d,����?(Y/N)?",
				tbname,tabid);
			char ans[100];
			fgets(ans,100,stdin);
			if(tolower(ans[0])=='n') {
				lgprintf("ȡ��ɾ���� ");
				return 0;
			}
			if(tolower(ans[0])=='y') break;
		}
	}
	
	for(int j=0;j<frn;j++) {
		lgprintf("ɾ��'%s'�͸��ӵ�depcp,dep5�ļ�",dtmt.PtrStr("filename",j));
		unlink(dtmt.PtrStr("filename",j));
		char tmp[300];
		sprintf(tmp,"%s.depcp",dtmt.PtrStr("filename",j));
		unlink(tmp);
		sprintf(tmp,"%s.dep5",dtmt.PtrStr("filename",j));
		unlink(tmp);
		lgprintf("ɾ��'%s'�͸��ӵ�depcp,dep5�ļ�",idxmt.PtrStr("filename",j));
		unlink(idxmt.PtrStr("filename",j));
		sprintf(tmp,"%s.depcp",idxmt.PtrStr("filename",j));
		unlink(tmp);
		sprintf(tmp,"%s.dep5",idxmt.PtrStr("filename",j));
		unlink(tmp);
	}
	st.Prepare(" delete from dp.dp_datafilemap where tabid=%d  and fileflag=2",tabid);
	st.Execute(1);
	st.Wait();
  }
	//ǧ����ɾ!!!!!!!!!!!
	//st.Prepare(" delete from dt_table where tabid=%d",tabid);
	//st.Execute(1);
	//st.Wait();
	DropDTTable(tabid,TBNAME_FORDELETE);
	lgprintf("��%d�������ݡ����������ļ���ɾ��.",tabid);
	return 1;
}

//�������Ͳ���������������ɾ��(ֻҪ����)
void SysAdmin::DropDTTable(int tabid,int nametype) {
	char sqlbf[MAX_STMT_LEN];
	AutoMt mt(dts,MAX_DST_INDEX_NUM);
	char tbname[150],idxname[150];
	mt.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>0",tabid);
	int rn=mt.Wait();
	if(rn<1) ThrowWith("�Ҳ���%d���Ķ�������.",tabid);
	for(int i=0;i<rn;i++) {
		GetTableName(tabid,mt.GetInt("indexgid",i),tbname,idxname,nametype);
		sprintf(sqlbf,"drop table %s",idxname);
		if(conn.TouchTable(idxname))
			DoQuery(sqlbf);
		int ilp=0;
		while(GetTableName(tabid,mt.GetInt("indexgid",i),tbname,idxname,nametype,ilp++)) {
		  sprintf(sqlbf,"drop table %s",idxname);
		  if(conn.TouchTable(idxname))
			DoQuery(sqlbf);
		}
	}
	sprintf(sqlbf,"drop table %s",tbname);
	if(conn.TouchTable(tbname))
		DoQuery(sqlbf);
}

//		
//���ز��������ֶε������ֶ���
//destmt:Ŀ������ڴ��,���ֶθ�ʽ��Ϣ
//indexid:�������
int SysAdmin::CreateIndexMT(AutoMt &idxtarget,int destmt,int tabid,int indexid,int *colidx,char *colsname,bool update_idxtb) {
	bool ec=wociIsEcho();
	wociSetEcho(FALSE);
	AutoMt idxsubmt(dts,10);
	idxsubmt.FetchAll("select * from dp.dp_index where tabid=%d and indexgid=%d and issoledindex>0",tabid,indexid);
	int rn=idxsubmt.Wait();
	if(rn<1) {
		ThrowWith("���������ṹ:��dp.dp_index����%d���������ļ�¼��",indexid);
	}
	wociClear(idxtarget);
	strcpy(colsname,idxsubmt.PtrStr("columnsname",0));
	wociCopyColumnDefine(idxtarget,destmt,colsname);
	//���Ҹö������������ķǶ������������Դ�Ϊ���ݽ���������
	idxsubmt.FetchAll("select * from dp.dp_index where tabid=%d and indexgid=%d and issoledindex=0 order by seqinidxtab",
		tabid,indexid);
	int srn=idxsubmt.Wait();
	for(int j=0;j<srn;j++) {
		//�ظ����ֶ��Զ��޳�
		wociCopyColumnDefine(idxtarget,destmt,idxsubmt.PtrStr("columnsname",j));
	}
	//�ع��������ö�
	char reusedcols[300];
	int cn1;
	cn1=wociConvertColStrToInt(destmt,colsname,colidx);
	reusedcols[0]=0;
	int tcn=wociGetMtColumnsNum(idxtarget);
	if(tcn>cn1) {
		for(int i=cn1;i<tcn;i++) {
			if(i!=cn1) strcat(reusedcols,",");
			wociGetColumnName(idxtarget,i,reusedcols+strlen(reusedcols));
		}
		strcat(colsname,",");
		strcat(colsname,reusedcols);
	}
	if(update_idxtb) {
		lgprintf("�޸�����%d�ĸ����ֶ�Ϊ'%s'.",indexid,reusedcols);
		AutoStmt st(dts);
		if(strlen(reusedcols)>0)
			st.Prepare("update dp.dp_index set reusecols='%s' where tabid=%d and indexgid=%d and issoledindex>0",
			reusedcols,tabid,indexid);
		else
			st.Prepare("update dp.dp_index set reusecols=null where tabid=%d and indexgid=%d and issoledindex>0",
			 tabid,indexid);
		st.Execute(1);
		st.Wait();
	}
	
	//�����������ֶ�
	wociAddColumn(idxtarget,"dtfid",NULL,COLUMN_TYPE_INT,10,0);
	wociAddColumn(idxtarget,"blockstart",NULL,COLUMN_TYPE_INT,10,0);
	wociAddColumn(idxtarget,"blocksize",NULL,COLUMN_TYPE_INT,10,0);
	wociAddColumn(idxtarget,"blockrownum",NULL,COLUMN_TYPE_INT,10,0);
	wociAddColumn(idxtarget,"startrow",NULL,COLUMN_TYPE_INT,10,0);
	wociAddColumn(idxtarget,"idx_rownum",NULL,COLUMN_TYPE_INT,10,0);
	idxtarget.Build();
	//ȡ���������͸���������blockmt(Ŀ�����ݿ��ڴ��)�ṹ�е�λ�ã�
	// ���ṹ�����ļ������������Ƿ��ϵͳ��������ָ�����ֶ�����ͬ��
	int bcn=cn1;//wociConvertColStrToInt(destmt,colsname,colidx);(colsname��reusedcols��������ͬ��
	bcn+=wociConvertColStrToInt(destmt,reusedcols,colidx+bcn);
	if(wociGetColumnNumber(idxtarget)!=bcn+6) {
		ThrowWith("Column number error,colnum:%d,deserved:%d",
			wociGetColumnNumber(idxtarget),bcn+6);
	}
	//����dt_index�е�idxfieldnum
	if(update_idxtb) {
		lgprintf("�޸�%d�����������������ֶ�����Ϊ%d.",indexid,bcn);
		AutoStmt st(dts);
		st.Prepare("update dp.dp_index set idxfieldnum=%d where tabid=%d and indexgid=%d ",
			bcn,tabid,indexid);
		st.Execute(1);
		st.Wait();
	}
	wociSetEcho(ec);
	return bcn;
}

//datapartid==-1:ȱʡֵ,��ʾ�����з����Ͻ���������
//  !=-1: ֻ��ָ�������Ͻ���������
void SysAdmin::CreateAllIndexTable(int tabid,int destmt,int nametype,bool createidx,int ci_type,int datapartid) {
	AutoMt mt(dts,100);
	mt.FetchAll("select * from dp.dp_index where tabid=%d and issoledindex>0 order by seqindattab",tabid);
	int rn=mt.Wait();
	for(int i=0;i<rn;i++) 
		CreateIndexTable(tabid,mt.GetInt("indexgid",i),-1,destmt,nametype,createidx,ci_type,datapartid);
}


void SysAdmin::CreateMergeIndexTable(int tabid) {
	AutoMt mtidx(dts,MAX_DST_DATAFILENUM);
	mtidx.FetchAll("select indexgid from dp.dp_index where tabid=%d and issoledindex>0 order by indexgid",tabid);
	int irn=mtidx.Wait();
	AutoMt mt(dts,MAX_DST_DATAFILENUM);
	mt.FetchAll("select * from dp.dp_datapart where tabid=%d and begintime<now() order by datapartid",tabid);
	int dpn=mt.Wait();
	char tbname[300],idxname[300],sqlbf[MAX_STMT_LEN];
	for(int idx=0;idx<irn;idx++) {
	 int indexid=mtidx.GetInt("indexgid",idx);
	 GetTableName(tabid,indexid,tbname,idxname,TBNAME_DEST,-1,mt.GetInt("datapartid",0));
	 if(!conn.TouchTable(idxname,true))
	  ThrowWith("����������%s������,�޷�����Ŀ��������. tabid:%d,indexid:%d,datapartid:%d",
	   idxname,tabid,indexid,mt.GetInt("datapartid",0));
	 AutoMt idxmt(dts,100);
	 idxmt.FetchAll("select * from %s limit 10",idxname);
	 idxmt.Wait();
	 GetTableName(tabid,indexid,tbname,idxname,TBNAME_DEST);
	 if(conn.TouchTable(idxname,true)) {
		sprintf(sqlbf,"drop table %s",idxname);
		conn.DoQuery(sqlbf);
	 }
	 wociGetCreateTableSQL(idxmt,sqlbf,idxname,true);
	 strcat(sqlbf,"  CHARSET=gbk TYPE=MERGE UNION=( ");
	 for(int part=0;part<dpn;part++) {
	   GetTableName(tabid,indexid,tbname,idxname,TBNAME_DEST,-1,mt.GetInt("datapartid",part));
	   //char *tbn=strstr(idxname,".");
	   //tbn++;
	   strcat(sqlbf,idxname);
	   strcat(sqlbf,(part+1)==dpn?") INSERT_METHOD=LAST":",");
	 }
	 printf("\nmerge syntax :\n%s.\n",sqlbf);
	 conn.DoQuery(sqlbf);
	 CreateIndex(tabid,indexid,TBNAME_DEST,true,CI_IDX_ONLY,-1,-1);
	}
}

//���indexmtΪ-1����destmt������Ч��
// datapartid==-1,�������з���������
//  datapartid!=-1,ֻ�����ƶ�����������
void SysAdmin::CreateIndexTable(int tabid,int indexid,int indexmt,int destmt,int nametype,bool createidx,int ci_type,int datapartid) {
	AutoMt targetidxmt(dts,10);
	if(indexmt==-1) {
		int colidx[50];
		char colsname[600];
		int cn=CreateIndexMT(targetidxmt,destmt,tabid,indexid,colidx,colsname,false);
		indexmt=targetidxmt;
	}
	char tbname[300],idxname[300];
	int ilp=0;
	while(true) {
	 if(datapartid==-1 && ! GetTableName(tabid,indexid,tbname,idxname,nametype,ilp++)) break;
	 if(datapartid>=0)
	   GetTableName(tabid,indexid,tbname,idxname,nametype,-1,datapartid);
	 
	 CreateTableOnMysql(indexmt,idxname,true);
	 if(createidx) {
		//2005/12/01�޸�,���Ӵ���������/Ŀ�������
		CreateIndex(tabid,indexid,nametype,true,ci_type,datapartid==-1?ilp-1:-1,datapartid);//tabname,idxtabname,0,conn,tabid,
		//indexid,true);
	 }
	 if(datapartid!=-1) break;
        }
}


//2006/07/03: �޸������ļ��Ĳ��ҷ�ʽ,����fileflag���ò���
int SysAdmin::CreateDataMtFromFile(AutoMt &destmt,int rownum,int tabid,int fileflag) {
	AutoMt mt(dts,10);
	mt.FetchAll("select filename from dp.dp_datafilemap where tabid=%d and fileflag in(0,1) limit 2",tabid);
	int rn=mt.Wait();
	if(rn<1) return 0;
	dt_file idf;
	idf.Open(mt.PtrStr("filename",0),0);
	destmt.SetHandle(idf.CreateMt(rownum));
	return wociGetColumnNumber(destmt);
}

int SysAdmin::CreateIndexMtFromFile(AutoMt &indexmt,int rownum,int tabid,int indexid) {
	AutoMt mt(dts,10);
	mt.FetchAll("select idxfname as filename from dp.dp_datafilemap where tabid=%d and indexgid=%d  and (fileflag=0 or fileflag is null) limit 2",tabid,indexid);
	int rn=mt.Wait();
	if(rn<1) ThrowWith("�����������ṹʱ�Ҳ��������ļ���");
	dt_file idf;
	idf.Open(mt.PtrStr("filename",0),0);
	indexmt.SetHandle(idf.CreateMt(rownum));
	return wociGetColumnNumber(indexmt);
}

void SysAdmin::OutTaskDesc(const char *prompt,int tabid,int datapartid,int indexid)
{
	AutoMt mt(dts,100);
	char tinfo[300];
	tinfo[0]=0;
	lgprintf(prompt);
	if(datapartid>0 && tabid>0) {
		mt.FetchAll("select * from dp.dp_datapart where tabid=%d and datapartid=%d",tabid,datapartid);
		if(mt.Wait()<1) ThrowWith("��������ݷ���:%d->%d.",tabid,datapartid);
		lgprintf("�������� : %s.",mt.PtrStr("partdesc",0));
		lgprintf("���ݷ��� : %d.",datapartid);
		lgprintf("Դϵͳ : %d.",mt.GetInt("srcsysid",0));
		lgprintf("���ݳ�ȡ : \n%s.",mt.PtrStr("extsql",0));
	}
	if(indexid>0 && tabid>0) {
	  mt.FetchAll("select * from dp.dp_index where tabid=%d and indexgid=%d and issoledindex>0",tabid,indexid);
	  if(mt.Wait()<1) ThrowWith("������������:%d->%d.",tabid,indexid);
	  lgprintf("�������%d,�����ֶ�: %s.",indexid,mt.PtrStr("columnsname",0));
	}		
	if(tabid>0) {
		mt.FetchAll("select concat(databasename,'.',tabname) as dstdesc,srcowner,srctabname from dp.dp_table where tabid=%d",tabid);
		if(mt.Wait()>0) 
{
printf("Ŀ��� :%s,Դ��:%s.%s\n.",mt.PtrStr("dstdesc",0),mt.PtrStr("srcowner",0),mt.PtrStr("srctabname",0));
			lgprintf("Ŀ��� :%s,Դ��:%s.%s.",mt.PtrStr("dstdesc",0),mt.PtrStr("srcowner",0),mt.PtrStr("srctabname",0));

}
	}
}

void SysAdmin::SetTrace(const char *type,int tabid)
{
	AutoMt mt(dts,100);
	char fn[300];
  mt.FetchAll("select concat(databasename,'.',tabname) as dstdesc,srcowner,srctabname from dp.dp_table where tabid=%d",tabid);
	if(mt.Wait()>0) {
		sprintf(fn,"%s/%s_%d.",type,mt.PtrStr("dstdesc",0),getpid());
		wociSetTraceFile(fn);
	}
}
#endif