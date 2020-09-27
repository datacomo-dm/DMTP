#ifdef WIN32
#include <process.h>
#else
#include <unistd.h>
#include <malloc.h>
#include <sys/types.h>
#include <pthread.h>
#endif
#include <stdlib.h>
#include <stdio.h>
#include "AutoHandle.h"
#include "zlib.h"
#include "ucl.h"
#include "ucl_asm.h"
#include "lzo1x.h"
#include "lzo_asm.h"
#include "bzlib.h"
#include "dt_svrlib.h"
int Start(void *ptr);

int main(int argc,char *argv[]) {
    int nRetCode = 0;
    WOCIInit("samp/ziptest/");
    nRetCode=wociMainEntrance(Start,true,NULL,2);
    WOCIQuit(); 
    return nRetCode;
}

void TestCompress(AutoMt &mt,const char *fn,int cmpflag) ;

int Start(void *ptr) { 
    wociSetEcho(FALSE);
    wociSetOutputToConsole(TRUE);
    AutoHandle dts;
    printf("���ӵ����ݿ�...\n");
    dts.SetHandle(BuildConn(0));
    char sql[10000];
    while(true) {
     sql[0]=0;
     getString("ZIPTST_SQL",NULL,sql);
     if(sql[strlen(sql)-1]==';') sql[strlen(sql)-1]=0;
     if(strcmp(sql,"quit")==0 || strcmp(sql,"exit")==0) return 0;
     try {
     AutoMt mt(dts,2);
     mt.FetchFirst(sql);
     int rn=mt.Wait();
     int l=wociGetRowLen(mt);
     printf("���м�¼����%d�ֽ�.\n",l);
     int meml=getOption("�������ݿ��С(KB)<500>",500,20,2000);
     mt.SetMaxRows(meml*1024/l);
     mt.FetchFirst(sql);
     rn=mt.Wait();
     if(rn*l<20000) {
     	printf("������̫С,����Ҫ20KB!");
     	continue;
     }
     printf("��¼��:%d.\n",rn);
     int sel=getOption("\n1.��ͨѹ��(����1).\n2.����ѹ��(����5).\n3.����ѹ��(����8).\n4.���ѹ��(����10).\n5.ȫ��\n   ѡ��ѹ������(<0>)",5,1,5);
     printf("x: MB/s\n");
     printf("���� ��ʽ\t�ֽ���\t����\tѹ��x\t��ѹx\t����x\t�ۺ�x\tDOx\t����ʱ�����\n");
     switch(sel) {
     case 5:
     	TestCompress(mt,"test.dat",5);
        TestCompress(mt,"test.dat",1);
     	TestCompress(mt,"test.dat",8);
     	TestCompress(mt,"test.dat",10);
     	break;
     case 1:
     	TestCompress(mt,"test.dat",1);
	break;     
     case 2:
     	TestCompress(mt,"test.dat",5);
	break;     
     case 3:
     	TestCompress(mt,"test.dat",8);
	break;     
     case 4:
     	TestCompress(mt,"test.dat",10);
	break;     
     }		
	}
	catch(...) {}
   }     
    return 0;
}

//����	��ʽ	�ֽ���	����	ѹMB/s	��MB/s	����MB/s	�ۺ�MB/s	DiskOut(MB/s)	����ʱ�����

void TestCompress(AutoMt &mt,const char *fn,int cmpflag) 
{
	mytimer mtm;
	int reptm=10;
	//if(cmpflag!=10) reptm=100;
	//lgprintf("---------Test Compress ----------------");
	//lgprintf("maxrows:%d,cmpflag:%d,repeat %d times.",
	//   maxrows,cmpflag,reptm);
	//AutoMt mt(dts,maxrows);
	//mt.FetchAll(sqlst);
	int rl=wociGetRowLen(mt);
	int rn=mt.GetRows();
	int len=rn*rl;
	//printf("\n---ѹ������:%d---ԭʼ�ֽ���:%d---\n",cmpflag,len);
	//printf("\nѹ������\tѹ���ֽ���\t����\nѹ��(MB/s)\t---\n",cmpflag,len);
	//lgprintf("End of fetch,get %d rows,row len:%d,tot bytes:%d",
	//	rn,rl,rn*rl);
	double tm1=0;
        int mt1;
        char *resptr=NULL;
	file_mt df;
        df.Open("dt_file_my.dat",1);
        int off=df.WriteHeader(mt,wociGetMemtableRows(mt),100);
        mtm.Restart();
        off=df.WriteMt(mt,cmpflag)-off;
        mtm.Stop();
        tm1=mtm.GetTime();
        df.Close();
        df.Open("dt_file_my.dat",0);
        mtm.Restart();
        mt1=df.ReadMtOrBlock(-1,0,0,&resptr);
        mtm.Stop();
        printf("%2d  ԭʼ\t%d\t%.2f\t%.1f\t%.1f\n",cmpflag,off,(double)len/off,len/tm1/1024/1024,len/mtm.GetTime()/1024/1024);
        //printf("δת����ʽѹ��Ϊ%d�ֽ�,����%.2f,ѹ��%.1fMB/s,��ѹ��%.1fMB/s.\n",off,(double)len/off,len/tm1/1024/1024,len/mtm.GetTime()/1024/1024);
        df.Close();
        df.Open("dt_file_my.dat",1);
        off=df.WriteHeader(mt,wociGetMemtableRows(mt),100);
        mtm.Restart();
        off=df.WriteMySQLMt(mt,cmpflag)-off;
        mtm.Stop();
        tm1=mtm.GetTime();
        df.Close();
        df.Open("dt_file_my.dat",0);
        mtm.Restart();
        mt1=df.ReadMtOrBlock(-1,0,0,&resptr);
        char rowbuf[1024*2048];
        mtm.Stop();
        double tm2=mtm.GetTime();
        mtm.Start();
        int mlen=df.GetMySQLRowLen();
        int trn=df.GetBlockRowNum();
        //for(int pi=0;pi<trn;pi++)
        //	df.rebuildrow(resptr,rowbuf,trn,pi);
        df.rebuildblock(resptr,rowbuf,trn);
        //У���������һ�У����ڿ�ʼ��
        memcpy(rowbuf,rowbuf+trn*mlen-mlen,mlen);
        mtm.Stop();
        printf("%2d  ����\t%d\t%.2f\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f%%\n",cmpflag,off,(double)len/off,len/tm1/1024/1024,
        	len/tm2/1024/1024,len/(mtm.GetTime()-tm2)/1024/1024,len/mtm.GetTime()/1024/1024,off/mtm.GetTime()/1024/1024,(mtm.GetTime()-tm2)/mtm.GetTime()*100);

        //printf("���д洢ѹ��Ϊ%d�ֽ�,����%.2f.\n"
        //       "  ѹ��%.1fMB/s,��ѹ%.1fMB/s,����%.1fMB,�ۺ�%.1fMB(ѹ����%.1fMB) (����ʱ��ռ%%%.1f).\n",
       // 	off,(double)len/off,len/tm1/1024/1024,
       // 	len/tm2/1024/1024,len/(mtm.GetTime()-tm2)/1024/1024,len/mtm.GetTime()/1024/1024,off/mtm.GetTime()/1024/1024,(mtm.GetTime()-tm2)/mtm.GetTime()*100);
        df.Close();
        df.Open("dt_file_my.dat",1);
        off=df.WriteHeader(mt,wociGetMemtableRows(mt),100);
        mtm.Restart();
        off=df.WriteMySQLMt(mt,cmpflag,false)-off;
        mtm.Stop();
        tm1=mtm.GetTime();
        df.Close();
        df.Open("dt_file_my.dat",0);
        mtm.Restart();
        mt1=df.ReadMtOrBlock(-1,0,0,&resptr);
        mtm.Stop();
        tm2=mtm.GetTime();
        mtm.Start();
        char rowbuf2[30000];
        for(int pi=0;pi<trn;pi++,resptr+=mlen)
        	memcpy(rowbuf2,resptr,mlen);
        mtm.Stop();
        if(memcmp(rowbuf2,rowbuf,mlen)!=0) 
        	printf("\n\n block no match.\n\n");
        printf("%2d  ����\t%d\t%.2f\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f%%\n",cmpflag,off,(double)len/off,len/tm1/1024/1024,
        	len/tm2/1024/1024,len/(mtm.GetTime()-tm2)/1024/1024,len/mtm.GetTime()/1024/1024,off/mtm.GetTime()/1024/1024,(mtm.GetTime()-tm2)/mtm.GetTime()*100);
//        printf("���д洢ѹ��Ϊ%d�ֽ�,����%.2f.\n"
//               "  ѹ��%.1fMB/s,��ѹ%.1fMB/s,����%.1fMB,�ۺ�%.1fMB(ѹ����%.1fMB) (����ʱ��ռ%%%.1f).\n",
//        	off,(double)len/off,len/tm1/1024/1024,
//        	len/tm2/1024/1024,len/(mtm.GetTime()-tm2)/1024/1024,len/mtm.GetTime()/1024/1024,off/mtm.GetTime()/1024/1024,(mtm.GetTime()-tm2)/mtm.GetTime()*100);
//        printf("row order (%d),%.2f.\n",off,(double)len/off);
        return;
	char *psrc,*pdst;
	psrc=new char [len+1000];
	wociExportSomeRows(mt,psrc,0,rn);
	pdst=new char [len+1000];
	int fhl=0,fl=0;
	mtm.Clear();
	mtm.Start();
	int i;
	char *pwrkmem=NULL;
	int len2=len;
	for(i=0;i<reptm;i++)
	{
			int rt=0;
			uLong dstlen=len2;
			if(cmpflag==10) {
				unsigned int dstlen2=len2;
				rt=BZ2_bzBuffToBuffCompress(pdst,&dstlen2,psrc,len,1,0,0);
				dstlen=dstlen2;
			}			
			/******* lzo compress ****/
			else if(cmpflag==5) {
				if(!pwrkmem)  {
					pwrkmem = 
					new char[LZO1X_MEM_COMPRESS+204800];
                                        memset(pwrkmem,0,LZO1X_MEM_COMPRESS+204800);
                                }
                                unsigned long dstlen2=len2; 
				rt=lzo1x_1_compress((const unsigned char*)psrc,len,(unsigned char *)pdst,&dstlen2,pwrkmem);
				dstlen=dstlen2;
			}
			/*** zlib compress ***/
			else if(cmpflag==8) {
				rt = ucl_nrv2d_99_compress((Bytef *)psrc,len,(Bytef *)pdst, (unsigned int *)&dstlen,NULL,5,NULL,NULL);
			}
		    	/*** zlib compress ***/
			else if(cmpflag==1) {
			 rt=compress2((Bytef *)pdst,&dstlen,(Bytef *)psrc,len,1);
			}
			else 
				ThrowWith("Invalid compress flag %d",compress);
		    	if(rt!=Z_OK) {
				ThrowWith("Compress failed,datalen:%d,compress flag%d,errcode:%d",
					len,compress,rt);
			}
			fhl=0;
			fl=dstlen;
	}
	mtm.Stop();
	lgprintf("Write file,%d->%d,ratio %5.2f,%7.3f Sec,%7.1fK/s.",rl*rn,fl-fhl,(double)(rl*rn)/(fl-fhl),mtm.GetTime(),rl*rn*reptm/mtm.GetTime()/1024.0);
	//lgprintf("End of file writing, compress to %d bytes,file len:%d,file header len :%d.",fl-fhl,fl,fhl);
	mtm.Clear();
	mtm.Start();
	
	for(i=0;i<reptm;i++)
	{
			int rt=0;
			uLong dstlen=len;
			/****************************/
			//bzlib2
			if(cmpflag==10) {
				unsigned int dstlen2=dstlen;
				rt=BZ2_bzBuffToBuffDecompress(psrc,&dstlen2,pdst,fl,0,0);
				dstlen=dstlen2;
			}			
			/******* lzo compress ****/
			else if(cmpflag==5) {
				unsigned long dstlen2=dstlen;
				//rt=lzo1x_decompress_asm_fast((unsigned char*)pdst,fl,(unsigned char *)psrc,&dstlen2,NULL);
				rt=lzo1x_decompress((unsigned char*)pdst,fl,(unsigned char *)psrc,&dstlen2,NULL);
				dstlen=dstlen2;
			}
			/*** zlib compress ***/
			else if(cmpflag==8) {
				//rt = ucl_nrv2d_decompress_asm_fast_8((Bytef *)pdst,fl,(Bytef *)psrc,(unsigned int *)&dstlen,NULL);
				rt = ucl_nrv2d_decompress_8((Bytef *)pdst,fl,(Bytef *)psrc,(unsigned int *)&dstlen,NULL);
				//ucl_nrv2d_decompress_asm_fast_8
			}
		        /*** zlib compress ***/
			else if(cmpflag==1) {
			 rt=uncompress((Bytef *)psrc,&dstlen,(Bytef *)pdst,fl);
			}
			else 
				ThrowWith("Invalid uncompress flag %d",cmpflag);
		    	if(rt!=Z_OK) {
				ThrowWith("Decompress failed,datalen:%d,compress flag%d,errcode:%d",
					fl,cmpflag,rt);
			}
	}	
	mtm.Stop();
	lgprintf("Read file,%7.3f Sec,%7.1fK/s.",mtm.GetTime(),rl*rn*reptm/mtm.GetTime()/1024.0);
	//lgprintf("End of read file .");
	return;
}
	
