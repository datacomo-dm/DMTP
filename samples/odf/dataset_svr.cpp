#include "odf.h"

   void dataset_svr::beginExt() {
	AutoStmt st(*pdb);
  	//�ڶ���ͻ��˹��õ�����£���ֻһ����¼����
  	st.DirectExecute("update tabt_dssn_client set status=2,load_status=1 where ds_sn=%d",ds_sn);//״̬2 �����������һ�����ݿ��ѿ���
  	st.DirectExecute("update tabt_dataset set ext_status=2,ext_lastrownum=0,ext_lstbegin=sysdate,ext_lstsn=%d where ds_id=%d",
  	        ds_sn,ds_id);//״̬2 �����������һ�����ݿ��ѿ���
   }

   void dataset_svr::endExt(int status,int rn) {
	AutoStmt st(*pdb);
  	//�ڶ���ͻ��˹��õ�����£���ֻһ����¼����
  	st.DirectExecute("update tabt_dssn_client set status=%d,ext_rownum=%d where ds_sn=%d",status,rn,ds_sn);//״̬2 �����������һ�����ݿ��ѿ���
  	st.DirectExecute("update tabt_dataset set ext_status=%d,ext_lastrownum=%d,ext_lstend=sysdate,ext_lstsn=%d where ds_id=%d",
  	        status,rn,ds_sn,ds_id);//״̬2 �����������һ�����ݿ��ѿ���
   }
   
   void dataset_svr::abortExt() {
	AutoStmt st(*pdb);
  	//�ڶ���ͻ��˹��õ�����£���ֻһ����¼����
  	st.DirectExecute("update tabt_dssn_client set status=4,load_status=0 where ds_sn=%d",ds_sn);//״̬4 �쳣��ֹ
  	st.DirectExecute("update tabt_dataset set ext_status=0,ext_lastrownum=0,ext_lstbegin=NULL where ds_id=%d",
  	        ds_id);//״̬2 �����������һ�����ݿ��ѿ���
   }
   
   //Lock and change status,if lock failed ,return false,other error throw exception;
   bool dataset_svr::lockForExt()
   {
   	AutoStmt stmt(*pdb);
   	int rn=stmt.DirectExecute("update tabt_dataset set ext_status=%d where ext_status=%d and ds_id=%d",1,0,ds_id);
   	if(rn!=1) return false;
   	return true;
   }
  
   //Lock and change status,if lock failed ,return false,other error throw exception;
   bool dataset_svr::lockForClear()
   {
   	AutoStmt stmt(*pdb);
   	int rn=stmt.DirectExecute("update tabt_dataset set ext_status=4 where ext_status=3 and ds_id=%d",ds_id);
   	//if(rn!=1) return false;
	AutoMt mt(*pdb,100);
	mt.FetchAll("select count(*) rn from tabt_dssn_client where ds_id=%d and ds_sn=%d",ds_id,ds_sn);
   	rn=mt.Wait();
	if(rn!=stmt.DirectExecute("update tabt_dssn_client set status=0 where status=3 and ds_id=%d and load_status>=4 and ds_sn=%d",ds_id,ds_sn))
   	if(rn<1) {
		stmt.DirectExecute("update tabt_dataset set ext_status=3 where ext_status=4 and ds_id=%d",ds_id);
		lgprintf("�������пͻ��˶����أ����ݼ�%d���ʧ��",ds_id);
		return false;
	}
	return true;
   }
   
   dataset_svr::dataset_svr(param_db *_pdb,datablockstore *_pdbs,int _ds_id)
   {
  	pdb=_pdb;pdbs=_pdbs;ds_id=_ds_id;ds_sn=0;
   }
   // return false for lock failed ,throw exception for other errors.
  bool dataset_svr::DoExtract()
  {
  	try {
  		AutoMt mt(*pdb,10);
  		if(!lockForExt()) {
  			errprintf("���ݳ�ȡʱ����ʧ�ܣ����ݼ����%d.",ds_id);
  			return false;
  		}
  		mt.FetchAll("select seq_dssn.nextval v from dual");
  		mt.Wait();
  		ds_sn=mt.GetInt("v",0);
  		pdbs->setdataset(ds_sn,ds_id,0);
  		mt.FetchAll("select * from tabt_dataset where ds_id=%d",ds_id);
  		mt.Wait();
  		lgprintf("���ݼ�%d -'%s' ��ʼ��ȡ���ݣ����κ�%d.",ds_id,mt.PtrStr("ds_name",0),ds_sn);
  		AutoStmt st(*pdb);
  		//���������ͬ���κż�¼����Ϊ��ȡ��״̬
  		//st.DirectExecute("update tabt_dssn_client set status=4 where ds_sn=%d",ds_sn);
  		lgprintf("�������ݳ�ȡ����%d,�޸ĳ�ȡ״̬...",ds_sn);
  		st.DirectExecute("delete from tabt_dssn_client where ds_sn=%d",ds_sn);
  		st.DirectExecute("insert into tabt_dssn_client "
  		     " select ds_id,%d,client_id,1,sysdate,sysdate,0,0 from tabt_dsbind "
  		     " where ds_id=%d order by client_id",
  		     ds_sn,ds_id);
  		AutoHandle extdbc;
  		lgprintf("������Դϵͳ������...");
   		extdbc.SetHandle(pdb->BuildSrcDBC(ds_id));
  		char extsql[SQL_LEN],paramsql[SQL_LEN];
  		int paramrl=pdb->GetParamSQL(ds_id,paramsql);
		pdb->GetExtSQL(ds_id,extsql);
  		int prn=0;
  		AutoMt parammt(*pdb,paramrl);
		lgprintf("���ݳ�ȡ�������������%d.",paramrl);
  		if(paramrl>0) {
  		  lgprintf("��ѯ���ݳ�ȡ����...");
		  parammt.FetchAll(paramsql);
  		  prn=parammt.Wait();
  		  if(prn<1)
  		   ThrowWith("����������Ч,���ݼ�%d !",ds_id);
  		  lgprintf("��ȡ�����洢...");
  		  pdbs->WriteBlock(parammt,2); //1:���ݿ飬2��������
		} 
  		int brn=pdb->GetBlockRows(ds_id);
		lgprintf("���ݳ�ȡÿ���ݿ�%d��,���Ʋ���%d��.",brn,prn);
  		AutoMt extmt(0,brn);
  		AutoStmt extstmt(extdbc);
  		extstmt.Prepare(extsql);
  		extmt.Build(extstmt);
  		int trn=0;
  		int rn=0;
  		int off=0;
  		bool firstblock=true;
		bool executed=false;
		if(paramrl>0) {
  		   for(int i=0;i<prn;i++){
  		   	wociBindToStatment(parammt,extstmt,NULL,i);
			if(!executed) {
				executed=true;
				wociExecute(extstmt,0);
				extstmt.Wait();
			}

  		   	wociFetchAt(extmt,brn-off,off);
  		   	rn=extmt.Wait();
  		   	while(rn==brn-off) {
			  trn+=off+rn;
			  lgprintf("���ݿ�洢����ȡ������%d�С�",trn);
			  pdbs->WriteBlock(extmt,1); //1:���ݿ飬2��������
			  if(firstblock) {
			  	firstblock=false;
			  	//����״̬
			  	beginExt();
			  }
			  off=0;
  		   	  wociFetchAt(extmt,brn-off,off);
  		   	  rn=extmt.Wait();
  		   	}
  		   	off+=rn;
		   }
		   if(off>0) {
		    pdbs->WriteBlock(extmt,1); //1:���ݿ飬2��������
		    if(firstblock) {
			  	firstblock=false;
			  	//����״̬
			  	beginExt();
		    }
		    trn+=off;
		    lgprintf("���ݿ�洢����ȡ������%d�С�",trn);
		   }
		}
		else {
		  extmt.FetchFirst();
		  rn=extmt.Wait();
		  while(rn>0) {
			  if(firstblock) {
			  	firstblock=false;
			  	//����״̬
			  	beginExt();
			  }
			  trn+=rn;
			  lgprintf("���ݿ�洢����ȡ������%d�С�",trn);
			  pdbs->WriteBlock(extmt,1); //1:���ݿ飬2��������
			  off=0;
  		   	  extmt.FetchNext();
  		   	  rn=extmt.Wait();
		  }
		}
		if(firstblock) //�ռ�¼��
		{
		  beginExt();
		  endExt(3,0);
		  return false;
		}
		endExt(3,trn);
  	}
  	catch(...) {
  		//�ָ�����״̬
  		abortExt();
  		throw;
  	}
	lgprintf("���ݳ�ȡ����������");
	return  true;
  }

    bool dataset_svr::DoClear(int _ds_sn)
	{
		ds_sn=_ds_sn;
		lgprintf("��ʼ���ݼ����,���%d,����%d.",ds_id,ds_sn);
		AutoStmt st(*pdb);
  		if(!lockForClear()) {
  			errprintf("�������ʱ����ʧ�ܣ����ݼ����%d,����%d.",ds_id,ds_id);
  			return false;
  		}
		try {
			AutoMt mt(*pdb,10000);
			mt.FetchAll("select * from tabt_extrec "
				" where ds_id=%d and ext_sn=%d",ds_id,ds_sn);
			int rn=mt.Wait();
			lgprintf("ɾ����ʱ��ȡ�ļ�...");
			for(int i=0;i<rn;i++) 
				unlink(mt.PtrStr("filename",i));
			lgprintf("��ɾ��%d���ļ�.",rn);
			st.DirectExecute("update tabt_extrec set status=3 where ds_id=%d and ext_sn=%d",
				ds_id,ds_sn);
			lgprintf("�ļ�״̬�޸�Ϊ��ɾ��.");
			mt.FetchAll("select * from tabt_dataset "
				" where ds_id=%d ",ds_id);
			mt.Wait();
			if(mt.GetInt("ext_repeatsec",0)>1000) {
			 lgprintf("���ݼ�Ϊ�ظ�ģʽ,�޸����ݳ�ȡ״̬Ϊ'����ȡ'.");
			 st.DirectExecute("update tabt_dataset set ext_status=0 where ds_id=%d ",ds_id);
			}
		}
		catch(...) {
			errprintf("���ݼ����ʱ�����쳣(id:%d,sn:%d)����Ҫ�˹���Ԥ��",
				ds_id,ds_sn);
			return false;
		}
		lgprintf("���ݼ������������.");
		return true;
	}