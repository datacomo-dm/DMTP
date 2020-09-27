#include <stdio.h>

void testcatch() {
   throw "throw a string info here .";
}
void testcatchs();
int main()
{

  // call to a function with try ...
   try {
      // call function in a shared library(dynamic loading library)
      testcatchs();

   }
   catch(const char *err) {
      printf("Info %s catched.\n",err);
   }
   try {
	// call local function which throw a message.
      testcatch();

   }
   catch(const char *err) {
      printf("Info %s catched.\n",err);
   }
   return 0;
}
