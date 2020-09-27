#include <stdio.h>

int main()
{
   int vali=0x1234;
   unsigned short int vals=0x1234;
   long vall=0x1234;
   printf("Test long size and bigendian.\n");
   printf("sizeof int :%d, sizeof long :%d,sizeof short %d \n",sizeof(vali),sizeof(vall),sizeof(vals));
   printf("bigendian:%s\n",((char *)&vali)[0]==0x34?"NO":"YES");
   return 0;
}
