#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include "dtio_common.h"
#include "zlib/zlib.h"

#define CHARENC(a)  ((a)>25?('0'+(a)-26):('A'+(a)))
#define CHARDEC(a)  ((a)<'A'?((a)-'0'+26):((a)-'A'))
extern "C" uLongCRC crc32(uLongCRC crc, const Bytef * buf, uInt len);
#define C_decode  mystrcpy01
void C_decode(char *strx) {
	unsigned char *str=(unsigned char *)strx;
	unsigned char cd[129];
	unsigned char str1[129];
	int i;
	memcpy(cd,str,128);
	cd[128]=0;
	//Decode 
	str1[62]=(CHARDEC(cd[126])<<4)+CHARDEC(cd[127]);
	for(i=0;i<62;i++) {
		int off=(str1[62]+i)%20;
		str1[i]=((CHARDEC(cd[2*i])-off)<<4)+CHARDEC(cd[2*i+1])-off;
		str1[i]^=str1[62];
	}
	//���滹��һ��ͬ���Ķ���
	//ver1
	//int pos[]={44,37,47,14,7,0,10,1,4,12,5,15,16,26,17,20,28,21,31,22,18,27,19,45,40,41,46,39,32,51,38,34,33,35,61,56,57,55,48,58,6,2,11,3,29,24,25,30,23,13,8,9,42,43,36,59,52,60,53,54,50,49,
	//       };
	int pos[]={44,37,47,14,17,20,28,21,31,22,7,0,10,1,4,12,5,15,16,26,18,27,19,45,40,41,46,39,32,51,38,34,33,35,61,56,57,55,48,58,6,2,11,3,29,24,25,30,23,13,8,9,42,43,36,59,52,60,53,54,50,49,
	         };
	for(i=0;i<62;i++) {
		str[pos[i]]=str1[i];
	}
	str[62]=0;
}

int DecodeFromFile(const char *fn)
{
    char buf[81920];
    buf[0]=0;
    int crco=0,crcc=0;
    buf[0]=0;
    struct stat fs;
    stat(fn,&fs);
    int len=fs.st_size;
    FILE *fp=fopen(fn,"rb");
    if(fp==NULL) {
    	printf("�Ҳ�����Ȩ֤��.");
    	return -1;
    }
    fread(buf,256,1,fp);
    fread(&crco,sizeof(int),1,fp);
    revInt(&crco);
    fread(buf+256,len-256-sizeof(int),1,fp);
    fclose(fp);
    len-=sizeof(int);
    if(crco!=crc32(crcc,(const Bytef *)buf,len)){
     printf("��Ȩ֤�����.\n");
     return -2;
    }
    int codevalue=0x19710224;
    revInt(&codevalue);
    int *tp=(int *)buf;
    int *tpe=tp+len/sizeof(int);
    while(tp!=tpe) {*tp^=codevalue;tp++;}
    if(len%128!=0) {
       printf("��Ȩ֤�����.\n");
    	return -3;
    }
    for(int i=0;i<len;i+=128)
    {
    	C_decode(buf+i);
    	printf("%s\n",buf+i);
    }
    return len/128;
}

#define C_encode  mystrcpy
int C_encode(char *strx)
{
	unsigned char *str=(unsigned char *)strx;
	unsigned char str1[65];
	unsigned char cd[129];
	int i;
	int len=strlen(strx);
	if(len>62) {
		return -1;
	}
	static bool inited=false;
	if(!inited) {
	 srand( (unsigned)time( NULL ) );
	 inited=true;
	}
  	for(i = len+1;i < 64;i++ )
		str[i]=rand()%127;
	//ver1
	//int pos[]={44,37,47,14,7,0,10,1,4,12,5,15,16,26,17,20,28,21,31,22,18,27,19,45,40,41,46,39,32,51,38,34,33,35,61,56,57,55,48,58,6,2,11,3,29,24,25,30,23,13,8,9,42,43,36,59,52,60,53,54,50,49,
	//       };
	int pos[]={44,37,47,14,17,20,28,21,31,22,7,0,10,1,4,12,5,15,16,26,18,27,19,45,40,41,46,39,32,51,38,34,33,35,61,56,57,55,48,58,6,2,11,3,29,24,25,30,23,13,8,9,42,43,36,59,52,60,53,54,50,49,
	         };
	for( i=0;i<62;i++) {
		str1[i]=str[pos[i]];
	}
	str1[62]=str[62];
	for(i=0;i<62;i++) {
		str1[i]^=str[62];//0x57;
		int off=(str[62]+i)%20;
		cd[2*i]=CHARENC((str1[i]>>4)+off);
		cd[2*i+1]=CHARENC((str1[i]&0x0f)+off);
	}
	cd[124]=CHARENC(str[63]>>4);
	cd[125]=CHARENC((str[63]&0x0f));
	cd[126]=CHARENC(str[62]>>4);
	cd[127]=CHARENC((str[62]&0x0f));
	cd[128]=0;
	strcpy(strx,(char *)cd);
	return 0;
}

int EncodeToFile(const char *fn, char *userlogo)
{
//��Ŀǰ�󼰵Ⱥ�ǰ��Ҫ���ո�
char *strsx[]={ "usertitle=��ʷ�����������ڹ���ϵͳ DataMerger ", //���26������(52�ֽ�)
        "functioncode=FULL",//FULL:�����棬EVAL:���ð棬 STAND: ��׼��
                "logo=  ��Ȩ��Ϣ:�ӱ��ƶ� ",
                "copyright=  �����е��ͨͨ�ż����ɷ����޹�˾",
                "expiredate=20290201",
								"productid=20111222002", //��ͨ����
                "rowlimit=0",//����������
    		"core_version=5.6.4",//�汾
    		"bs_version=5.6.4",//�汾
    		"admin_version=5.6.4",//�汾
    		"lib_version=5.6.4",//�汾
//                "core_version=4.1.0",//�汾
//                "bs_version=4.1.0",//�汾
//                "admin_version=4.1.0",//�汾
//                "lib_version=4.1.0",//�汾

    		"hello=�ܶ���ɢ��������Ŷ���е��Ǽ�����һ��,�ټӳ�һ��.",
     		"hello1=Խ����ʼ�����󣬽�û����ʾ",
     		"certid=201112",
     		"teststr=12345678901234567890123456789012345678901234567890.",
    		NULL,
    	};
    int crco=0,crcc=0;
    char buf[81920];
    char newlogo[500];
    buf[0]=0;
    char **strs=strsx;
    if(userlogo!=NULL) {
	sprintf(newlogo,"logo= ��Ȩ��Ϣ: %s",userlogo);
	strs[2]=newlogo;
    }
    char substr[150];
    while(*strs) {
    	strcpy(substr,*strs);
    	C_encode(substr);
    	strcat(buf,substr);
    	//printf("encode '%s' to '%s'.\n",*strs,substr);
    	strs++;
    }
    //printf("encoded to :\n%s.\n",buf);
    int codevalue=0x19710224;
    revInt(&codevalue);
    int *tp=(int *)buf;	
    int len=strlen(buf)/sizeof(int);
    printf("strlen:%d,int len:%d.\n",strlen(buf),len);
    int *tpe=tp+len;
    while(tp!=tpe) {*tp^=codevalue;tp++;}
    crco=crc32(crco,(const Bytef*)buf,len*sizeof(int));
    revInt(&crco);
    FILE *fp=fopen(fn,"w+b");
    if(fp==NULL) {
    	printf("Write file '%s' failed.\n",fn);
    	return -1;
    }
    fwrite(buf,256,1,fp);
    fwrite(&crco,sizeof(int),1,fp);
    fwrite(buf+256,len*sizeof(int)-256,1,fp);
    fclose(fp);
    printf("file length:%d.\n",sizeof(int)*len+sizeof(int));
    return 0;
}

/***********************************************************************/
/*
 * Main (only for testing)
 */
#ifdef MAIN
int main( int argc, char **argv)
{
    EncodeToFile("cert.dat",argc==2?argv[1]:NULL);
    DecodeFromFile("cert.dat");
    return 0;
}

#endif
