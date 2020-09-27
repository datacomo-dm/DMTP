/* Copyright (C) 2005 Guosheng Wang
����DT��Ŀ�е����ݱ���/�ָ������һ���֡�
DT����/�ָ������DTϵͳ�еĶ��������/����Ĵ���.��������ݶ������:MyTableϵͳ��,
DTϵͳ��,DTĿ���(Ŀ������,�����ļ�,Ŀ���/������),�����п��ܳ��ֵĶ�����
Oracle(hs,dblink,synomyms)/MySQL/ODBC���ò�����.
��Ƶ�Ŀ�겻�����ڱ���/�ָ�,�����п��ܵ�ʹ��Ŀ�Ļ���:
1). DTϵͳ������mysqld�������Ĵ��ݣ�������MySQL��ÿ����������ļ�(.frm,.MYI,.MYD)֮��
����һ���ļ�������.dts�ļ����洢�ñ��DTϵͳ������mysqld��ȡDT��������ֱ���������ݿ���
sql����������ʹDT��ˢ�¸��ɿ�����mysqld��DTϵͳ�����Ĳ�ѯ����������������dtadmin)����
������ʱ��mysqld��Ӱ�죮
2). ���ݺ�����ݣ����߳�Ϊ���������ݣ����Բ������/�����������ݽ����ֱ�ӹ�mysqldʹ�ã�
3). ͨ��FTP��ʽֱ�Ӱ����ݴ����Զ�����������ȱ���ϵͳ����̱���ʹ�ã�
4). ͨ������ϵͳ������veritas������)...

  ���ǵ��Ŵ�/���̵�ý��Ȳ����ص㣬�����ݴ��ʱ�����'Ԥ����'���ܣ��������ɻ������ݡ�Ŀ¼��ȣ�����д���ļ�ͷ��
  �ڶ�дý��ʱ��������/���صõ���Щ��Ϣ��
*/

#ifndef DTIO_H
#define DTIO_H
#include "dtio_common.h" 
#include "AutoHandle.h"
#include "dtio_mt.h"
#include "dtio_mtchooser.h"
#include "dtiomap.h"
#include "dt_common.h"
#ifndef MYSQL_SERVER
#include "mysqlconn.h"
#endif
#include <stdio.h>
#include <stdlib.h>
#ifdef WIN32
#include <sys/types.h>
#include <sys/stat.h>
#include <io.h>
#include <stdio.h>
#include <stdlib.h>
#endif
enum DTIO_STREAM_TYPE {
	FULL_BACKUP,DTP_BIND,DTP_DETACH
};
struct dtioHeader {
	uint4 flag;
	uint4 hdsize; // size of this header structrue,include extended mt data.
	uint8B imagesize; // size of whole dumped image ,include this header.
	uint4 version;
	//���ݿ�����ѡ������� 
	//bool sysparmaexist;
	//bool dtparamexist;
	//bool dtdataexist;
	//uint8B dbsparamoffset,dbsparamlength; // database server(MySQL) system parameter table storeage area.
	//uint8B dtparamoffset,dtparamlength; // dt system parameter table storeage area.
	//uint8B dtdataoffset,dtdatalength;// dt destinateion table's data files and index files storeage area.
	enum DTIO_STREAM_TYPE stype;
	char exptime[8]; //  date and time op exporting.char array type for indep of platform.
	uint8B exportid; // identifier of export operation.
	uint8B dependon_exportid; //�����������������ݼ�.(Ϊ��������,�ر��Ǿ�ȷ����������׼��).
	uint4 internalfileid; // sequence of internal files.
	char detaildesc[160]; // detail description of backup objects.
	char basedir[PATH_LEN];
	uint4 dtiocontct;//�����ļ�����
	char basefilename[PATH_LEN];//����·��,��׺���ļ�������,�������ɺͲ��������ļ�.
	uint8B filelimit;// maximum length of a single file,data will be split to seperate file(s) while exceeds limits.
	uint8B filelength[MAXCONTFILE]; // size of all files,include this one.Ԥ���ļ���С�����ֵĹ���,����������ô洢��Դ.
	char attachto[PATH_LEN]; //���ĸ��ļ�Ϊ����.
	char blank[200];
	void ReverseByteOrder() {
		int i=0;
		rev8B(&exportid);
		rev8B(&dependon_exportid);
		rev8B(&filelimit);
		for(i=0;i<MAXCONTFILE;i++)
			rev8B(filelength+i);
		rev8B(&imagesize);
		revInt(&flag);revInt(&hdsize);revInt(&version);revInt(&stype);
		revInt(&internalfileid);revInt(&dtiocontct);
	}
};

#define CONTFILEFLAG 0xa533
struct dtioHeaderCont {
	uint4 flag; // �����ļ��ı�ʶ��ͬ
	uint8B hdsize;
	uint8B filelength; // size of this file,include this header.
	uint4 version;
	uint8B exportid; // һ�鵼�������ļ�ʹ����ͬ�ı�ʶ�롣
	uint4 internalfileid; // sequence of internal files.
	char blank[200];
	void ReverseByteOrder() {
	 rev8B(&hdsize);rev8B(&filelength);rev8B(&exportid);revInt(&flag);revInt(&version);revInt(&internalfileid);	
	}
};

// dtioStream����ص�:
//   1. ���ݱ���:��ʵ����Ŀ���ļ���д����,ֻ��������ռ�ÿռ�,ͨ��write=false,���Ժ�ʵ��д����ʱͬ��������������.
//   2. ����ض���:����ͨ��FTP��ͨ��,��Ŀ���ļ��Ĵ洢����λ��ת�Ƶ�����ƽ̨�򱸷��豸(ϵͳ).

class DTIOExport dtioStream {
protected :
	//uint8B length;//�ܳ���
	uint8B curoffset;//�ڵ�ǰ�ļ��е�ƫ�ơ�
	uint8B offset; //��ƫ��.
	bool write;
	int cursplitid;
	char streamopt[PATH_LEN];//����: file/ftp/tape/cdrom/backupsys
	char streamname[PATH_LEN];//��������(�ļ���)
	char streamlocation[PATH_LEN]; //Ŀ¼����Ŀ��ϵͳ��ַ
	char obasedir[PATH_LEN];
	uint8B splitlen;//�ļ���ֵ�
	uint4 splitedct;//���ݲ�ִ���
	dtioHeader dtiohdr;
	dtioUnitMap mtcontent;
	dtioHeaderCont *hdrcont; // size of all files,include this one.Ԥ���ļ���С�����ֵĹ���,����������ô洢��Դ.
	void SplitLenChanged() ;
	uint8B getStartOffset(int fid) ;
	uLongCRC crcv;
public :
	uint4 GetVersion() { return dtiohdr.version;}
	void SetOutDir(const char *_odir) {strcpy(dtiohdr.basedir,_odir);}
	const char *GetOutDir() {return dtiohdr.basedir;}
	const char *GetInBaseDir() {return obasedir;}
	dtioStream(const char *_basedir) ;
	DTIO_STREAM_TYPE GetStreamType() {return dtiohdr.stype;}
	const char *GetAttachFileName() {return dtiohdr.attachto;}
	int SetStream(const char *bname,const char *bopt,const char *path) ;
	virtual uint8B StreamWriteInit(DTIO_STREAM_TYPE _stype=FULL_BACKUP,const char *_attachto=NULL) ;
	virtual uint8B StreamReadInit() ;
	void initcrc() {crcv=0;}
	uLongCRC getcrc() {return crcv;}
	dtioUnitMap *GetContentMt() {return &mtcontent;}
	uint4 GetSplitCt() {return splitedct;}
	uint8B GetSPlitLen() {return splitlen;}
	void SetSplitLen(uint8B val) {splitlen=val;SplitLenChanged();}
	bool IsWrite() {return write;}
	void SetWrite(bool val) {write=val;}
	uint8B GetLength() {return dtiohdr.imagesize;}
	uint8B GetCurOffset() {return curoffset;}
	uint8B GetOffset() { return offset;}
	virtual uint8B Get(void *bf,uint8B len)=0;
	uint8B GetInt(void *bf) {
		int t=0;
		uint8B r=Get(&t,sizeof(int));
		revInt(&t);
		*(int *)bf=t;
		return r;
	}
	uint8B Get8B(void *bf) {
		uint8B t=0;
		uint8B r=Get(&t,sizeof(uint8B));
		rev8B(&t);
		*(uint8B *)bf=t;
		return r;
	}
	uint8B GetDouble(void *bf) {
		double t=0;
		uint8B r=Get(&t,sizeof(double));
		revDouble(&t);
		*(double *)bf=t;
		return r;
	}
	virtual uint8B Put(void *bf,uint8B len,bool splitable=true)=0;
	uint8B PutInt(void *bf) {
		int t=*(int *)bf;
		revInt(&t);
		return Put(&t,sizeof(int));
	}
	uint8B PutDouble(void *bf) {
		double t=*(double *)bf;
		revInt(&t);
		return Put(&t,sizeof(double));
	}
	uint8B Put8B(void *bf)
	{
		uint8B t=*(uint8B *)bf;
		rev8B(&t);
		return Put(&t,sizeof(uint8B));
	}
	
	virtual uint8B SeekAt(uint8B offset)=0;
	virtual uint8B SeekFrom(uint8B from,uint8B _offset)=0;
	virtual int SetStreamName(const char *fname)=0;
	virtual ~dtioStream() {delete []hdrcont;}
};


class DTIOExport dtioStreamFile :public dtioStream {
	FILE *fp;
protected :
	void OpenFile(int id,bool forwrite) ;
public :
	dtioStreamFile(const char *_basedir);
	virtual ~dtioStreamFile() {
		if(fp) fclose(fp);
		char fn[400];
		for(unsigned int id=0;id<=splitedct;id++)
		 {
		 	if(id!=0)
			 sprintf(fn,"%s%s.%04d",streamlocation,streamname,id);
			else sprintf(fn,"%s%s",streamlocation,streamname);
			//�ļ�����Ϊֻ��ģʽ
#ifdef WIN32
			_chmod(fn,_S_IREAD );
#else
			chmod(fn,S_IRUSR|S_IRGRP|S_IROTH);
#endif
		}
	}
	int SetStreamName(const char *fname) ;
	virtual uint8B Put(void *bf,uint8B len,bool splitable=true) ;
	virtual uint8B SeekAt(uint8B offset);
	virtual uint8B SeekFrom(uint8B from,uint8B _offset);
	virtual uint8B Get(void *bf,uint8B len) ;
	virtual uint8B StreamWriteInit(DTIO_STREAM_TYPE _stype=FULL_BACKUP,const char *_attachto=NULL) ;
	virtual uint8B StreamReadInit() ;
};

#define DTIOFILEFLAG 	  0x3167
#define DTIOFILEBLOCKFLAG 0x3169    //ѹ���ļ�����洢,�ṹΪFLAG(4bytes)+ORIGLEN(4bytes)+STORELEN(4bytes)+compressedblock.

struct dtioDataBlockHeader {
	uint4 flag;
	uint8B hdsize;
	uint8B blocksize; // size of whole dumped image size,include this header.
	char blank[20];
	void ReverseByteOrder() {
		rev8B(&hdsize);rev8B(&blocksize);revInt(&flag);
	}
};
//�ļ��ܵĴ洢�ռ�Ϊÿ�������ݿ�ĺ�(����������������ֽ�),ѹ�����͵�Ϊȫ�ֱ���,ֻ�洢һ��.
class DTIOExport  dtiofile {
	uint4 compressflag;
	uint8B filelen;
	uint8B storelen;//���ڱ�ѹ����Ŀ������,�洢���Ⱥ�ԭʼ���Ȳ�һ��.
	uint8B startoffset;
	char path[PATH_LEN];
	char filename[PATH_LEN];
	char *porigbuf;
	char *pzipbuf;
	int filetype;
	bool enablesplit;//�Ƿ��б�Ҫ����һЩ�ļ���Ŀ���в�������?
	uint4 blocklen;
	dtioStream *dtio;
	uint8B seekat;
	void GetFile(const char *fn,int type,const char *newdir=NULL);
public :
     void OpenDtDataFile(const char *fn) ;
     uint8B GetFileLen() { return filelen;}
     void SeekAt(unsigned long off) ;
     size_t ReadBuf(char *bf,size_t len) ;
	dtiofile(dtioStream *_dtio,bool _enablesplit,uint4 _blocklen=4096000) ;
	~dtiofile() ;
	uint8B Serialize(const char *fn,int type,const char *prefix=NULL,int _compressflag=0);
	uint8B DeserializeInit(const char *prefix,const char *fbasename,int type);
	uint4 GetCompresFlag() {return compressflag;}
	uint8B GetStartPos() {return startoffset;}
	int GetFileName(char *fn,int len) ;
	int GetPath(char *_path,int len) ;
	// offset ��ʾ�ļ��ڵ����ƫ��
	uint8B GetData(char *bf,uint8B offset,uint8B len);
	bool CheckCRC();
	uint8B Deserialize(const char *fn);//return scaned offset.
};

class DTIOExport dtioMyTable {
	char dbname[PATH_LEN];
	char tablename[PATH_LEN];
	dtioStream *dtio;
	int tbtype;
public :
	void SetTableByID(int id);
	int GetTableNum();
	void SetType(int type);
	dtioMyTable(dtioStream *_dtio);
	void GetDbName(char *dbn) ;
	void GetTableName(char *tn);
	void SetTable(const char *dbn,const char *tbn) ;
	uint8B Serialize(int compressflag=0);
	uint8B  Deserialize(const char *newtbn=NULL);
};

#ifndef MYSQL_SERVER
class DTIOExport dtioMyTableGroup {
	char desc[PATH_LEN];
	int type;
	int tabnum;
	dtioMyTable myio;
	dtioStream *dtio;
public :
	dtioMyTableGroup(dtioStream *_dtio,int _type,const char *_desc=NULL);
	void AddTable(const char *dbn,const char *tbn,int compressflag=0) ;
	int GetTableNum() ;
	int SearchTable(int rid,char *dbn,char *tbn);
	virtual void AfterEachRestore(MySQLConn &conn,const char *dbn,char *tbn) {
		char sql[200];
		sprintf(sql,"flush table %s.%s",dbn,tbn);
		conn.DoQuery(sql);
	}
	virtual void AfterRestoreAll(MySQLConn &conn) {
	}
	int Restore(MySQLConn &conn,int rid);
	int RestoreAll(MySQLConn &conn);
};

class DTIOExport dtioMySys :public dtioMyTableGroup {
public :
	dtioMySys(dtioStream *pdtio):dtioMyTableGroup(pdtio,DTIO_UNITTYPE_SYSDBFILE,"MySQLϵͳ��") {};
	void AddTables() {
		AddTable("mysql","columns_priv");
		AddTable("mysql","db");
		AddTable("mysql","func");
		AddTable("mysql","tables_priv");
		AddTable("mysql","user");
		AddTable("mysql","host");
	}
	void AfterRestoreAll(MySQLConn &conn) {
		conn.DoQuery("flush privileges");
	}
};


class DTIOExport dtioDTSys :public dtioMyTableGroup {
public :
	dtioDTSys(dtioStream *pdtio):dtioMyTableGroup(pdtio,DTIO_UNITTYPE_DTDBFILE,"DTϵͳ��") {
	}
	void AddTables() {
/*
		AddTable("dt","dt_cdfilemap");
		//AddTable("dt","dt_datachangelog");
		AddTable("dt","dt_datafilemap");
		//AddTable("dt","dt_filelink");
		AddTable("dt","dt_index");
		AddTable("dt","dt_indexfilemap");
		AddTable("dt","dt_middledatafile");
		AddTable("dt","dt_path");
		AddTable("dt","dt_srcpartinfo");
		AddTable("dt","dt_srcsys");
		AddTable("dt","dt_srctable");
		AddTable("dt","dt_table");
		AddTable("dt","dt_taskschedule");
*/
		AddTable("dp","dp_code");
		AddTable("dp","dp_codetype");
		AddTable("dp","dp_datafilemap");
		AddTable("dp","dp_datapart");
		AddTable("dp","dp_datasrc");
		AddTable("dp","dp_index");
		//AddTable("dp","dp_indexfilemap");
		AddTable("dp","dp_middledatafile");
		AddTable("dp","dp_path");
		AddTable("dp","dp_seq");
		AddTable("dp","dp_table");
		AddTable("dp","dp_user");
		AddTable("dp","dp_log");
	}
};
#endif

class DTIOExport dtioDTTable {
	char dbname[PATH_LEN];
	char tablename[PATH_LEN];
	dtioStream *dtio;
	dtparams_mt dtmts;
	bool psoleindexmode;
	void GetFile(const char *fn,int type,const char *newdir);
public:
	dtioDTTable(const char *dbn,const char *tbn,dtioStream *_dtio,bool _psoleindexmode=true);
	dtparams_mt *GetParamMt() {return &dtmts;}
	void RelinkFile();
	//Ԥ��ֱ�ӷ������ݹ��ܡ�
	uint8B ReadBlockData(int fid,int offset,int len ,char *bf);
	void FetchParam(int dts) ;//Fill mtparam from MySQL Server(dt_table,dt_index,dt_filemap,....)
	uint8B SerializeParam(dtioStream *alterDTIO=NULL,const char *newtab=NULL); //Write mtparam and register content of dtio. register dt table itself!
	uint8B DeserializeParam();//Read content,then fill mtparam,return 0 for no item found,others for offset.
	uint8B SerializeDestTab();//Write dest table (columns define ,no real data);
	uint8B DeserializeDestTab(const char *newtbn=NULL);//Read Dest table and restore to basedir relative.
	uint8B SerializeIndex();//Write index tables and register content of dtio.using data compression default.
	uint8B DeserializeIndex(const char *newtbn=NULL);//Read index tables and restore to basedir relative.
	uint8B SerializeFile();//Write data files and index files and register content of dtio.
	uint8B DeserializeFile(const char *newdir=NULL);//Read data files and index files and register content of dtio.
	// if newdir != NULL, need update parammt;
	//���µĳ�Ա�Ƕ����������/�������ӹ��̵�һ�����ɡ�
	uint8B Serialize();
	uint8B Deserialize(int dts,const char *newdir=NULL);
};

class DTIOExport dtioDTTableGroup {
	int type;
	char desc[PATH_LEN];//Group name ?
	int itemnum;
	dtioStream *dtio;
	bool psoledidxmode;
	dtioDTTable *pdtiodttab;
	int dts;
public :
	dtioDTTableGroup(int _dts,int _type,const char *_desc,dtioStream *_dtio,bool psoledidxonly) ;
	~dtioDTTableGroup( ) ;
	int AddTable(const char *dbn,const char *tbn,bool paramonly=false);
	int GetTableNum() ;
	dtioDTTable *GetDTTable() {return pdtiodttab;}
	int SearchTable(int rid,char *dbn,char *tbn);
	int SearchIDByName(const char *dbn,const char *tbn);
	int Restore(int rid,bool fullmode=true); //fullmode=false for parammt and index table,exclude data and index files.
	int RestoreAll(bool fullmode=true);
};

#ifndef MYSQL_SERVER
class dtioMain {
	bool myparam; //�Ƿ�MySQL������
	bool dtparam; //�Ƿ�DT������
	bool dtdata; //�Ƿ�DT����
	bool configfiles;//�Ƿ������ļ�:$ORACLE_HOME/network/admin/tnsnames.ora,
	//               $ORACLE_HOME/hs/admin/initXXXX.ora
	//               /etc/odbc.ini, /etc/odbcinst.ini
	//               $MYSQLBASEDIR/var/my.cnf
	bool psoledidxmode;//������������־.������������������.
	char basedir[PATH_LEN]; //MySQL base directory
	char streamPath[PATH_LEN]; //destination stream path and stream name.
	char destdir[PATH_LEN];// �ָ�����ʱ�����ļ�����ָ��·��,path for dt data(idx) file restore.���Stream���ж��DT��,�п���ÿ�����·����ͬ.
	dtioStream *pdtio;
	AutoHandle dts; // �������ݿ���������ӷ����������ݿ����������DT�������ͬ��ʡ�
	MTChooser dtmt;//Ŀ��DT��ѡ��
	MTChooser parammt; //MySQLϵͳ��������DTϵͳ���������ָ�����ʱ�������أ��ر����DT_PATH�ȱ�Ĳ���������Ӱ��DTϵͳ��
	MySQLConn conn;

protected :
	//int getOption(const char *prompt,int defaultval,const int lmin,const int lmax) ;
	//int getString(const char *prompt,const char *defaultval,char *val) ;
	//int getdbcstr(char *un,char *pwd,char *sn,const char *prompt) ;
	//bool GetYesNo(const char *prompt,bool defval) ;
	int CreateDataTableAndIndex(const char *dbname,const char *tbname,dtparams_mt *pdt);
	int SetSequence();
public :
	dtioMain() ;
	~dtioMain() ;
	void BackupPrepare() ;
	void Backup(const char *dbname=NULL,const char *tabname=NULL) ;
	void RunByParam(void *cp);
	void DoBackup(const char *dbname=NULL,const char *tabname=NULL) ;
	void BindPrepare();
	void PreviousBind();
	void CreateBind(const char *dbn,const char *tbn);
	void DoBind(const char *dbn,const char *tbn);
	void RestorePrepare();
	void List(const char *filename=NULL,bool withlog=false,const char *dbn=NULL,const char *tbn=NULL);
	void PreviousRestore(const char *dbn=NULL,const char *tbn=NULL,const char *ndtpath=NULL);
	void DoRestore();
   	void DetachPrepare();
   	void PreviousDetach(const char *dbn=NULL,const char *tbn=NULL,const char *newtbn=NULL);
};
#endif

#endif
