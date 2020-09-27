#ifndef TAB_TRANSFER_SYS_HEADER
#define TAB_TRANSFER_SYS_HEADER

#ifdef WIN32
#include <windows.h>
#endif
#include <time.h>


//ȱʡ��  �Զ��׳� char * �쳣
//	  �󲿷ֱ�ʶ����0λ�Ƿ�ֵ����һ��ʼ���

#define CONNHOST_LEN 50

long gzipfile(const char *srcname,const char *dstname=NULL);
long gunzipfile(const char *srcname,const char *dstname=NULL);
int listfile(const char *parttern,const char *lstfile);
long localfilesize(const char *srcname);
long localcopyfile(const char *srcname,const char *dstname);
class FTPClient {

  bool connected;

  static void *envhdr;
  void *connhdr;
  static int refct;
  void *filehdr;
  char host[CONNHOST_LEN],user[CONNHOST_LEN],authcode[CONNHOST_LEN];
  int fstype; //refer to codetab .
public :
  long FileSizeAndMTime(const char *remotepath,time_t &mdtm);
  void	GetMTime(const char *const file, time_t & mdtm);
  long FileSize(const char *remotepath);
  // ref : http://www.ncftp.com/libncftp/doc/libncftp.html#FTPRemoteGlob
  int ListFile(const char *fileparttern,const char *lstfile);
  void Close();
  bool ConnectFTP();
  static void InitTransfer();
  static void QuitTransfer();
  FTPClient();
  void SetContext(const char *_host,const char *_user,const char *_password);
  void Delete(const char *remotefile);
  void PutFile(const char *srcpath,const char *destpath);
  void GetFile(const char *srcpath,const char *destpath);
  virtual ~FTPClient();
};


#endif
