#include "dtio.h"
#include "dt_svrlib.h"
dtioDTTable::dtioDTTable(const char *dbn,const char *tbn,dtioStream *_dtio,bool _psoleindexmode):dtmts(dbn,tbn)
{
	strcpy(dbname,dbn);strcpy(tablename,tbn);dtio=_dtio;
	psoleindexmode=_psoleindexmode;
}

//Ԥ��ֱ�ӷ������ݹ��ܡ�
uint8B dtioDTTable::ReadBlockData(int fid,int offset,int len ,char *bf) {
	ThrowWith("�˹����ݲ�֧��!");	
	return 0;
}

void dtioDTTable::FetchParam(int dts)
{
	dtmts.FetchParam(dts,psoleindexmode,dtio);
}

uint8B dtioDTTable::SerializeParam(dtioStream *alterDTIO,const char *newtab) 
{
	dtioStream *ldtio=alterDTIO==NULL?dtio:alterDTIO;
	printf("����'%s.%s'--����...\n",dbname,tablename);
	uint8B st=ldtio->GetOffset();
	uint8B len=dtmts.Serialize(ldtio,0,true)-st;
        ldtio->GetContentMt()->SetUnit(DTIO_UNITTYPE_DTPARAM,dbname,newtab!=NULL?newtab:tablename,(double)st,(double)len);
	return ldtio->GetOffset();
}

uint8B dtioDTTable::DeserializeParam()
{
	//������Ϣ�������mysqld��־�����Ρ�
	//printf("�ָ�'%s.%s'--����...\n",dbname,tablename);
	double st=0,len=0;
	dtio->GetContentMt()->GetUnit(DTIO_UNITTYPE_DTPARAM,dbname,tablename,st,len);
	dtio->SeekAt((uint8B)st);
	dtmts.Deserialize(dtio);
	return dtio->GetOffset();
}

uint8B dtioDTTable::SerializeDestTab()
{
	printf("����'%s.%s'--Ŀ���...\n",dbname,tablename);
	dtioMyTable myio(dtio);
	myio.SetType(DTIO_UNITTYPE_DESTDBFILE);
	myio.SetTable(dbname,tablename);
	myio.Serialize();
	return dtio->GetOffset();
}

uint8B dtioDTTable::DeserializeDestTab(const char *newtbn)
{
	printf("�ָ�'%s.%s'--Ŀ���...\n",dbname,tablename);
	dtioMyTable myio(dtio);
	myio.SetType(DTIO_UNITTYPE_DESTDBFILE);
	myio.SetTable(dbname,tablename);
	myio.Deserialize(newtbn==NULL?tablename:newtbn);
	return dtio->GetOffset();
}

uint8B dtioDTTable::SerializeIndex()
{
	printf("����'%s.%s'--������...\n",dbname,tablename);
	dtioMyTable myio(dtio);
	myio.SetType(DTIO_UNITTYPE_IDXDBFILE);
	int idxnum=dtmts.GetSoledIndexNum();
	int datapartnum=dtmts.GetTotDataPartNum();
	for(int i=0;i<idxnum;i++)
	{
		char dbn[200],tbn[200];
		dtmts.GetIndexTable(i,dbn,tbn);
		myio.SetTable(dbn,tbn);
		myio.Serialize();
		if(dtmts.IsPartIndex(dtio->GetOutDir())) {
			for(int datapart=0;datapart<datapartnum;datapart++) {
				myio.SetTable(dbn,dtmts.PartIndexTbn(i,datapart));
				myio.Serialize();
			}
		}
	}
	return dtio->GetOffset();
}

//������������newtbn!=null,���޸�dt_table/dt_index�ڴ���е�tabname�ֶ�
//  ���������Ҫȷ���õ����ǳ���DTP��������������һ���ڴ������

//newtbn!=null,���Ǹ����ָ���Ĳ�����
uint8B dtioDTTable::DeserializeIndex(const char *newtbn)
{
	printf("�ָ�'%s.%s'--������...\n",dbname,tablename);
	dtioMyTable myio(dtio);
	myio.SetType(DTIO_UNITTYPE_IDXDBFILE);
	int idxnum=dtmts.GetSoledIndexNum();
	char dbn[PATH_LEN],tbn[PATH_LEN];
	dtmts.GetIndexTable(0,dbn,tbn);
	int datapartnum=dtmts.GetTotDataPartNum();
	for(int i=0;i<idxnum;i++)
	{
		dtmts.GetIndexTable(i,dbn,tbn);
		myio.SetTable(dbn,tbn);
		if(newtbn!=NULL) {
		 sprintf(tbn,"%sidx%d",newtbn,dtmts.GetInt("DP_INDEX","indexgid",i));//i+1);
		 //empty index tabname field.
		 dtmts.SetString(GetDPIndex(dtio),"indextabname","",i);
		}
		myio.Deserialize(tbn);
		char fn[PATH_LEN];
		//�������������Ƿ����(.frm).
		sprintf(fn,"%s.frm",dtmts.PartIndexTbn(i,0));
		if(dtio->GetContentMt()->CheckUnit(DTIO_UNITTYPE_MFRMFILE,dbn,fn))
		{
			sprintf(fn,"%s%s/%s.MRG",dtio->GetInBaseDir(),dbn,tbn);
			FILE *fp=fopen(fn,"w+t");
			for(int datapart=0;datapart<datapartnum;datapart++) {
			 myio.SetTable(dbn,dtmts.PartIndexTbn(i,datapart));
			 myio.Deserialize(dtmts.PartIndexTbn(i,datapart,newtbn));
			 fprintf(fp,"%s\n",dtmts.PartIndexTbn(i,datapart,newtbn));
			}
			fprintf(fp,"#INSERT_METHOD=LAST\n");
		}
	}
	//Ŀ����ѻָ���ΪDTP�����ļ�������׼��
	if(newtbn!=NULL) {
	  dtmts.SetString(GetDPTable(dtio),"tabname",newtbn,0);
	}
	return dtio->GetOffset();
}

uint8B dtioDTTable::SerializeFile()
{
	printf("����'%s.%s'--�����ļ�...\n",dbname,tablename);
	int filenum=dtmts.GetDataFileNum();
	dtiofile outf(dtio,true);
	char fn[200];
	for(int i=0;i<filenum;i++) {
		outf.Serialize((dtmts.GetDataFile(i,fn),fn),DTIO_UNITTYPE_DATFILE);
		outf.Serialize((dtmts.GetIndexFile(i,fn),fn),DTIO_UNITTYPE_IDXFILE);
	}
	return dtio->GetOffset();
}

void dtioDTTable::GetFile(const char *fn,int type,const char *newdir)
{
		  dtiofile inf(dtio,true);
		  char opath[200],obasename[200];
		  char dirc[200],basec[200];
		  strcpy(dirc ,fn);
		  strcpy(basec ,fn);
		  strcpy(opath, dirname(dirc));
		  strcpy(obasename,basename(basec));
		  inf.DeserializeInit(opath,obasename,type);
		  if(newdir!=NULL) strcpy(opath,newdir);
		  if(opath[strlen(opath)-1]!=PATH_SEPCHAR) {
			  strcat(opath,PATH_SEP);
		  }
		  strcat(opath,obasename);
		  inf.Deserialize(opath);
}

uint8B dtioDTTable::DeserializeFile(const char *newdir)
{
		printf("�ָ�'%s.%s'--�����ļ�...\n",dbname,tablename);
		  int filenum=dtmts.GetDataFileNum();
		  char fn[200];
		  for(int i=0;i<filenum;i++) {
			  dtmts.GetDataFile(i,fn);
			  GetFile(fn,DTIO_UNITTYPE_DATFILE,newdir);
			  dtmts.GetIndexFile(i,fn);
			  GetFile(fn,DTIO_UNITTYPE_IDXFILE,newdir);
		  }
	return dtio->GetOffset();
}

uint8B dtioDTTable::Serialize()
{
	printf("����'%s.%s'...\n",dbname,tablename);
	SerializeParam();
	SerializeDestTab();
	SerializeIndex();
	SerializeFile();
	return dtio->GetOffset();
}

void dtioDTTable::RelinkFile() {
	printf("�ؽ��ļ�����.\n");
	int rn=dtmts.GetDataFileNum();
	if(dtmts.GetInt(GetDFMName(dtio),"fileid",0)!=
	  dtmts.GetInt(GetDPTable(dtio),"firstdatafileid",0)) {
	  printf("�����ļ�ӳ�����Ŀ����¼�����ļ��Ų�һ�£��޷����ļ����ӣ���Ҫ����װ������.");
	  return;
	}
	
	for(int k=0;k<rn;k++) {
		//Build index & data file link information.
		if(k+1==rn) {
		  dt_file df;
		  df.Open(dtmts.GetString(GetDFMName(dtio),"filename",k),2,dtmts.GetInt(GetDFMName(dtio),"fileid",k));
		  df.SetFileHeader(0,NULL);
		  //����ر��ļ�,�����´δ�ʱ���fid��openmode��ͬʱ���ܱ��������.
		  df.Close();
		  df.Open(dtmts.GetString(GetIFMName(dtio),"idxfname",k),2,dtmts.GetInt(GetIFMName(dtio),"fileid",k));
		  df.SetFileHeader(0,NULL);
		  df.Close();
		  //lgprintf(
		}
		else {
		  dt_file df;
		  df.Open(dtmts.GetString(GetDFMName(dtio),"filename",k),2,dtmts.GetInt(GetDFMName(dtio),"fileid",k));
		  df.SetFileHeader(0,dtmts.GetString(GetDFMName(dtio),"filename",k+1));
		  //����ر��ļ�,�����´δ�ʱ���fid��openmode��ͬʱ���ܱ��������.
		  df.Close();
		  df.Open(dtmts.GetString(GetIFMName(dtio),"idxfname",k),2,dtmts.GetInt(GetIFMName(dtio),"fileid",k));
		  df.SetFileHeader(0,dtmts.GetString(GetIFMName(dtio),"idxfname",k+1));
		  df.Close();
		}
	}
}
			  
uint8B dtioDTTable::Deserialize(int dts,const char *newdir)
{
	printf("�ָ�'%s.%s'...\n",dbname,tablename);
	//DeserializeParam();
	char fn[500];
	dtmts.GetDataFile(0,fn);
#ifdef WIN32
	if(fn[1]!=':' && (newdir==NULL || newdir[1]!=':')) 
		ThrowWith("�����ļ�����Unixƽ̨(%s)������ָ��Windowsƽ̨�������ļ��ָ�����·��(-datapathѡ��)",fn);
#else
	if(fn[1]==':' && (newdir==NULL || newdir[1]==':')) 
		ThrowWith("�����ļ�����Windowsƽ̨(%s)������ָ��Unixƽ̨�������ļ��ָ�����·��(-datapathѡ��)",fn);
#endif
	/* JIRA DM-8 :�쳣�ָ�
	   ���Ȼָ�DP������ȷ����������������˳��ִ�� 
	   */
	dtmts.adjparam(newdir);
	dtmts.restore(dts,newdir);
	DeserializeDestTab();
	DeserializeIndex();
	DeserializeFile(newdir);
	RelinkFile();
	return dtio->GetOffset();
}


