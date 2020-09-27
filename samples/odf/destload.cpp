#include "odf.h"   

   bool destload::LockForLoad() {
	AutoStmt stmt(*pdb);
   	if(stmt.DirectExecute("update tabt_dssn_client set load_status=2,lstactive_tm=sysdate where load_status=1 and client_id=%d and ds_sn=%d",client_id,ds_sn)<1) return false;
   	return true;
   }   	
   
   void destload::changeLoadStatus(int status) {
	AutoStmt stmt(*pdb);
   	stmt.DirectExecute("update tabt_dssn_client set load_status=%d,lstactive_tm=sysdate where client_id=%d and ds_sn=%d",status,client_id,ds_sn);
   }   	
   
   destload::destload(param_db *_pdb,datablockstore *_pdbs,int _client_id,int _ds_id, int _ds_sn)
   {
     pdb=_pdb;pdbs=_pdbs;client_id=_client_id;	ds_id=_ds_id;ds_sn=_ds_sn;
   }
   
   void destload::Load() {
   	try {
   		internalLoad();
   	}
   	catch(...) {
   		//�쳣�ָ�
   		lgprintf("�ָ�װ��״̬.");
   		changeLoadStatus(1);
   	}
   }
   
   void destload::internalLoad()
   {
	lgprintf("��ʼ����װ��,���κ�%d,���ݼ���%d��",ds_sn,ds_id);
 	char sql[SQL_LEN];
 	pdbs->setdataset(ds_sn,ds_id,client_id);
 	int pbn=pdbs->GetBlockNum(ds_sn,2);
	int bn=pdbs->GetBlockNum(ds_sn,1);
	if(bn<1)
	  ThrowWith("�Ҳ��������ļ���¼,���κ�%d,���ݼ���%d!",ds_sn,ds_id);
	if(!LockForLoad())
	  return ;
	lgprintf("����װ��:�����ļ�%d��,�����ļ�%d��.",pbn,bn);

	lgprintf("����װ��:�����ļ��͵�һ�������ļ�...",pbn,bn);
    if(pbn>0) pdbs->TransferBlock(2,0);
	pdbs->TransferBlock(1,0);
	int transfered=1;
	AutoMt blkmt(0,1);
	blkmt.SetHandle(pdbs->BuildBlockMT(1,0));

   	AutoMt mt(*pdb,10);
   	mt.FetchAll("select * from tabt_dsbind where ds_id=%d and client_id=%d",ds_id,client_id);
   	mt.Wait();
   	AutoHandle dstdbc;
   	dstdbc.SetHandle(pdb->BuildDstDBC(ds_id,client_id));
   	
   	//����Ƿ���Ҫ����Ŀ���
   	char *dest_tab=mt.PtrStr("dest_tab_name",0);
   	lgprintf("����װ��:Ŀ���'%s'.",dest_tab);
    if(!wociTestTable(dstdbc,dest_tab)) {
   	    lgprintf("����װ��:Ŀ������ڣ���Ҫ����...");
   		bool deny_create=false;
   		bool isdefined=pdb->GetCreateTabSQL(ds_id,client_id,deny_create,sql);
   		if(deny_create) 
   			ThrowWith("��Ŀ��ϵͳ�б�'%s'�����ڣ����������Զ�����!",dest_tab);
   		if(isdefined) {
   			AutoStmt stmt(dstdbc);
   			stmt.DirectExecute(sql);
   		}
   		else
   		  wociGeneTable(blkmt,dest_tab,dstdbc);
   		//Ŀǰ��֧�ֵ�����������
   		if(pdb->GetIndexSQL(ds_id,client_id,sql)) {
   	        lgprintf("����װ��:��������...");
   			AutoStmt stmt(dstdbc);
   			stmt.DirectExecute(sql);
   		}
   	}
   	
   	//����װ��ǰ������׼������
   	bool useparamblock=false;
   	bool repeat;
   	bool exists=pdb->GetClearSQL(ds_id,client_id,useparamblock,repeat,sql);
   	if(exists) {
   	    lgprintf("����װ��:��ʼװ��ǰ������������...");
   		if(useparamblock && pbn<1)
   		  ThrowWith("���������Ҫʹ�ò����ļ�����������δ���ɡ����ݼ�%d,�ͻ�%d.",ds_id,client_id);
   		if(useparamblock) {
   			AutoMt pmt(0,1);
   			pmt.SetHandle(pdbs->BuildBlockMT(2,0));
   			AutoStmt stmt(dstdbc);
   			stmt.Prepare(sql);
   			int prn=wociGetMemtableRows(pmt);
   			while(true) {
   			    wociBindToStatment(pmt,stmt,NULL,0);
   				stmt.Execute(prn);
				int rn=stmt.Wait();
				wociCommit(dstdbc);
   				if(rn<1 || !repeat) break;
   			}
	 		pdbs->EraseLocalBlock(2,0);
   	        lgprintf("����װ��:��������������ļ���ɾ��.");
   		}
   		else {
   	        lgprintf("����װ��:��ʼװ��ǰ������������(�޲���).");
   			AutoStmt stmt(dstdbc);
   			stmt.Prepare(sql);
   			while(true) {
   				stmt.Execute(1);
   				if(stmt.Wait()<1 || !repeat) break;
   			}
   	        lgprintf("����װ��:�������(�޲���).");
		}   			
	}
	
   	lgprintf("����װ��:��ʼ����װ��...");
	changeLoadStatus(3);
	//��ʼ����װ��
	int loadedrn=0;
	int toloadid=0;
	while(true) {
	 int est=pdb->GetExtStatus(ds_id);
	 if(est!=2 && est!=3)
   		ThrowWith("���������ݳ�ȡ�쳣��ֹ,��װ��%d�����ݡ����ݼ�%d,�ͻ�%d.",loadedrn,ds_id,client_id);
   	 bn=pdbs->GetBlockNum(ds_sn,1);
	 if(toloadid<transfered) {
		lgprintf("װ��...");
	 	wociAppendToDbTable(blkmt,dest_tab,dstdbc,true);//�Զ��ύ
		loadedrn+=wociGetMemtableRows(blkmt);
		lgprintf("��װ��%d���ļ�(%d��),�ѳ�ȡ�ļ���%d.",transfered,loadedrn,bn);
	 	pdbs->EraseLocalBlock(1,toloadid);
	 	toloadid++;
	 }
	 if(est==3 && toloadid==bn) {
	 	break;
	 }
	 if(toloadid<bn) {
		pdbs->TransferBlock(1,toloadid);
		transfered++;
		blkmt.SetHandle(pdbs->BuildBlockMT(1,toloadid));
	 }
	 else {
	 	printf("�ȴ��������������...");
	 	mSleep(120000);
	 }
	}
	changeLoadStatus(5); //4: װ����ɣ���ʱ������ɾ��
 	lgprintf("����װ����������.");
   }
