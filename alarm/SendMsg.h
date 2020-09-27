#ifndef SEND_MESSAGE_DEF_H
#define SEND_MESSAGE_DEF_H
/**************************************************************************************************
  Copyright (C), 2010-2020, DATACOMO COMMUNICATIONGS INC(http://www.datacomo.com/)
  File name   : SENDMSG.H    
  Author      : Liujs(liu_shun_shun@126.com)     
  Version     : V1.00        
  DateTime    : 2013/1/14 21:55	
  Description : ���ⷢ����Ϣ�ӿ���ʵ����
**************************************************************************************************/
#include "Utility.h"
#include "AlarmMsg.h"

class AlarmMsgInfo;
class ConfigFile; 

// ������Ϣ�ӿ�
class ProcessRecvMsg
{
public:
	virtual int	ProcRecvMsg(const unsigned char *rcvMsg,const unsigned int rcvMsgLen) = 0;
};

class SendMessage  // ������Ϣ�ӿ�
{
public:
	SendMessage()
	{
        m_hasInited = false;
	}
	virtual ~SendMessage()
	{
	}

protected:
	bool                  m_hasInited;
    SendType              m_sendInterface;            // ������Ϣ�ӿ�����
	ProcessRecvMsg*       m_ProcessRecvMsg;           // ���շ����������ݽӿ� 

public:
	inline  void            SetSendInterface(SendType sendType){m_sendInterface = sendType;};
    inline  SendType        GetSendInterface(){return m_sendInterface;};
	inline  void            SetParentProcessRecvMsg(ProcessRecvMsg* obj){m_ProcessRecvMsg = obj;};
public:
    virtual int     Init(void* param) = 0;
    virtual int     SendMsg(AlarmMsgInfo* alarmMsgObj) = 0;
	virtual int     UnInit() = 0;
};

// tcp ����ӿ�
class SockTcpClient;
struct TcpServerItem
{
	unsigned short _port;
    char _ip[16];
	TcpServerItem()
	{
		_port = 0;
		memset(_ip,0,16);
	};
};
class SendMessageTcp:public SendMessage
{
public:
	SendMessageTcp();
	virtual ~SendMessageTcp();

protected:
	SockTcpClient   *m_sockTcpClient;

protected:
    // SockTcpClient ��Ĵ���ص�����
	static  void    Callback_ProcNetEvent(const NetEvent event,const unsigned char *rcvMsg,const unsigned int rcvMsgLen,void* pUserParam = NULL);

public:
    virtual int     Init(void* param /* TcpServerItem* */);
    virtual int     SendMsg(AlarmMsgInfo* alarmMsgObj);
	virtual int     UnInit();
};

// Db����ӿ�
struct DbServerItem
{
	short db_type;
	char  user[32];
	char  password[32];
	char  dsn[32];
	DbServerItem()
	{
		db_type = DTDBTYPE_ODBC;
		memset(user,0,32);
		memset(password,0,32);
		memset(dsn,0,32);
	};
};
class SendMessageDB:public SendMessage
{
public:
    SendMessageDB();
    virtual ~SendMessageDB();

protected:
    AutoHandle      m_Connection;

public:
    virtual int     Init(void* param/* DbServerItem* */);
    virtual int     SendMsg(AlarmMsgInfo* alarmMsgObj);
	virtual int     UnInit();
};

// ������Ϣ�򵥹�����
class SendMessageFactory
{
public:
    static   SendMessage* CreateSendMessageObj(const SendType sendMsgInterface)
	{
        if (SEND_DB == sendMsgInterface)
        {
            return new SendMessageDB();
        } 
        else if(SEND_SOCKET_TCP == sendMsgInterface)
        {
            return new SendMessageTcp();
        }
		else
		{
			return NULL;
		}
	}
};
#endif
