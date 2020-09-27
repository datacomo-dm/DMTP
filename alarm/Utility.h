#ifndef UTILITY_DEF_H
#define UTILITY_DEF_H
/**************************************************************************************************
  Copyright (C), 2010-2020, DATACOMO COMMUNICATIONGS INC(http://www.datacomo.com/)
  File name   : UTILITY.H    
  Author      : Liujs(liu_shun_shun@126.com)     
  Version     : V1.00        
  DateTime    : 2013/1/14 22:39	
  Description : ����ͷ�ļ�
**************************************************************************************************/
#include "dt_common.h"
#include "AutoHandle.h"
#include <map>
#include <string>

//---- socket ---------
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <sys/time.h>
#include <sys/resource.h>
//#include <linux/tcp.h>
#include <linux/socket.h>
#include <arpa/inet.h>		// inet_addr 
//---- socket ---------

/**************************************************************************************************
  Object      : ConfigFile
  Author      : Liujs     
  Version     : V1.00        
  DateTime    : 2013/1/15 11:06
  Description : �����ļ���
**************************************************************************************************/
class ConfigFile
{
protected:
	typedef std::map<std::string,std::string>   STRING_MAP;
	STRING_MAP       m_options;
	int split(char *text,char **tokens,char delimter);
public :
	virtual ~ConfigFile() ;
	ConfigFile(const char *ctlfile);

public:
	const char*      getOption(const char *keyval,const char *defaults) ;
	const char       getOption(const char *keyval,const char defaults) ;
	int              getOption(const char *keyval,int defaults) ;
	double           getOption(const char *keyval,double defaults);
};

/**************************************************************************************************
  Object      : SockTcpClient
  Author      : Liujs     
  Version     : V1.00        
  DateTime    : 2013/1/15 11:07
  Description : tcp socket  �ͻ�����
                ͬ���������ݣ��첽�������ݻص��ϲ�֪ͨ
**************************************************************************************************/
class ThreadList;            // common thread
#define RECV_BUFF_LEN        16*1024     
#define MAX_EVENTS 200		// epoll_create(size) ��size �� event������

// �������ݵĻص�����
enum NetEvent
{
    Server_Disconnect,             // ����˶Ͽ�����
    Server_DataArrived,            // ���շ�������� 
};
typedef void(*pCallback_ProcNetEvent)(const NetEvent event,const unsigned char *rcvMsg,const unsigned int rcvMsgLen,void* pUserParam);
class SockTcpClient
{
protected:
    // ���ܷ������Ϣ�õģ�������Ϣ�������¼���ص�֪ͨ�ϲ�
    pthread_t          m_RecvMsgThread;
    pthread_t          m_ReconnectThread;
	pCallback_ProcNetEvent  m_cbProcNetEvent;                  // ������Ϣ�ص�
    void*              m_pUserParam;                           // �ϲ�Ӧ���Դ�����

	int	               m_localSock;			                   // socket
	struct sockaddr_in m_remote;                               // server remote
	bool               m_isConnected;                          // �Ƿ�����
    bool               m_isAutoReconnect;                      // �Ƿ��Զ�����
    char               m_serverIP[16];                         // Server IP
    unsigned short     m_serverPort;                           // Server Port
    bool               m_isRuning;                             // ��������

public:
	inline int&       GetLocalSock(){return m_localSock;};
	inline bool       IsAutoConnect(){return m_isAutoReconnect;};
    int               DoConnect();
    void              CloseConnect();
	inline pCallback_ProcNetEvent GetCbFun(){return m_cbProcNetEvent;};

public:
    // ������Ϣ��ͬ������
    int              Start();
    inline  void     Stop(){m_isRuning = false;m_isConnected = false;};
    int              SendMsg(const unsigned char* buff,const unsigned long buffLen);

protected:
    static void*     OnRecvMsgProc(void* param);     // ���������̴߳�����
    static void*     OnReconnect(void *param);       // �Զ������߳�

public:
    SockTcpClient(void(*pCallback_ProcNetEvent)(const NetEvent event,const unsigned char *rcvMsg,const unsigned int rcvMsgLen,void* pUserParam),
		const char* remoteServerIp,const unsigned short remoteServerPort,void* pUserParam=NULL,const bool isAutoReconnect = true);

    virtual ~SockTcpClient();
};
#endif
