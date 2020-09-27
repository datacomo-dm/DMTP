
#include <unistd.h>
#include <stdio.h>
#include <string.h>


typedef unsigned long long uint8B;
typedef long long int8B;
typedef unsigned int uint4;
typedef int int4;
#define DTIO_VERSION 0x1010
#define MAXCONTFILE 20
#define DTIO_STREAMFLAG 0xa511
#define PATH_LEN 250 //120
enum DTIO_STREAM_TYPE {
	FULL_BACKUP,DTP_BIND,DTP_DETATCH
};
struct tstst {
	uint8B a;
	uint4 b;
};
struct dtioHeaderCont {
	uint4 flag; // �����ļ��ı�ʶ��ͬ
	uint8B hdsize;
	uint8B filelength; // size of this file,include this header.
	uint4 version;
	uint8B exportid; // һ�鵼�������ļ�ʹ����ͬ�ı�ʶ�롣
	uint4 internalfileid; // sequence of internal files.
	char blank[200];

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
	uint4 internalfileid; // sequence of internal files. 52
	char detaildesc[160]; // detail description of backup objects.212
	
	char basedir[PATH_LEN]; //462
	uint4 dtiocontct;//�����ļ�����
	char basefilename[PATH_LEN];//����·��,��׺���ļ�������,�������ɺͲ��������ļ�.716
	uint8B filelimit;// maximum length of a single file,data will be split to seperate file(s) while exceeds limits.724
	uint8B filelength[MAXCONTFILE]; // size of all files,include this one.Ԥ���ļ���С�����ֵĹ���,����������ô洢��Դ.884
	char attachto[PATH_LEN]; //���ĸ��ļ�Ϊ����.1134
	char blank[195];//1334%8 =6 1334+6=1340
	
};
int main() {
 
 #ifdef __sunos
 printf("Symbol LINUX is defined.\n");
#endif
  double dv=12399.001;
  char buf[100];
  int val=1234;
//  *(int *)(buf+9)=val;
 unsigned char *pdv=(unsigned char *)&dv; 
 printf("sizeof off_t:%d\n",sizeof(off_t));
 printf("sizeof log log :%d\n",sizeof(long long));
 printf("sizeof size_t:%d\n",sizeof(size_t));
 printf("Float:%.*f\n",2,12399.001); 
 printf("str:%-10s,int %-6d\n","abcd",1234);
 printf("str:%-10s,strlen%d.\n","���Գ���",strlen("���Գ���"));
 printf("%-10d   %d%%.\r",12333,12);
printf("%-10d   %d%%.\r",22333,32);
printf("%-10d   %d%%.\n",32333,42);
printf("%02x,%02x,%02x,%02x,%02x,%02x,%02x,%02x.\n",pdv[0],pdv[1],pdv[2],pdv[3]
,pdv[4],pdv[5],pdv[6],pdv[7]);
 printf("sizeof enum :%d.\n",sizeof(DTIO_STREAM_TYPE)); 
 printf("sizeof dtio:%d(should be 1340).\n",sizeof(dtioHeader));
 printf("sizeof tst:%d.\n",sizeof(tstst));
}
