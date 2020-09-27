#include "SendMsg.h"

//===============================================================================
// SendMessageTcp ���ʵ���ļ�
SendMessageTcp::SendMessageTcp()
{
	m_ProcessRecvMsg = NULL;
	m_sockTcpClient = NULL;
}

SendMessageTcp::~SendMessageTcp()
{
	if(NULL != m_sockTcpClient)
	{
		m_sockTcpClient->Stop();
		delete m_sockTcpClient;
		m_sockTcpClient = NULL;
	}
}

// SockTcpClient ��Ĵ���ص�����
void  SendMessageTcp::Callback_ProcNetEvent(const NetEvent event,const unsigned char *rcvMsg,const unsigned int rcvMsgLen,void* pUserParam)
{
	SendMessageTcp* pObj = NULL;
	pObj = (SendMessageTcp*)pUserParam;
	if (NULL == pObj)
	{
		lgprintf("SendMessageTcp::Callback_ProcNetEvent error.");
	}

	switch(event)
	{
	case Server_Disconnect:                 // ����˶Ͽ�����
		{
			lgprintf("��⵽����˶Ͽ����ӣ�������������.");
			if (pObj->m_sockTcpClient->IsAutoConnect())
			{
				pObj->m_sockTcpClient->DoConnect();
			}

			// �ϲ�ҵ����
			if (pObj->m_ProcessRecvMsg != NULL)
			{
				pObj->m_ProcessRecvMsg->ProcRecvMsg(rcvMsg,rcvMsgLen);
			}
		}
		break;
	case Server_DataArrived:               // ���շ��������    
		{
			// �ϲ�ҵ����
			if (pObj->m_ProcessRecvMsg != NULL)
			{
				pObj->m_ProcessRecvMsg->ProcRecvMsg(rcvMsg,rcvMsgLen);
			}
		}
		break;
	}

}

int  SendMessageTcp::Init(void* param)
{
	if(m_hasInited)
	{
		lgprintf("SendMessageTcp::Init �Ѿ���ʼ����,�����ظ�ִ��.");
		return 0;
	}
	TcpServerItem* pObj = (TcpServerItem*)param;
	if(pObj == NULL)
	{
		return -1;
	}

	if (NULL == m_sockTcpClient)
	{
		m_sockTcpClient = new SockTcpClient(Callback_ProcNetEvent,pObj->_ip,pObj->_port,(void*)this,true);
	}

	if(NULL != m_sockTcpClient)
	{
		if(m_sockTcpClient->Start() < 0)
		{
			delete m_sockTcpClient;
			m_sockTcpClient = NULL;
			return -1;
		}
	}

	m_hasInited = true;
}
int  SendMessageTcp::SendMsg(AlarmMsgInfo* alarmMsgObj)
{
	if(NULL == alarmMsgObj || m_sockTcpClient == NULL || !m_hasInited )
	{
		lgprintf("����������������޷����͸澯��Ϣ.");
		return -1;
	}
    return m_sockTcpClient->SendMsg(alarmMsgObj->GetMsgInfo().msg_payload,alarmMsgObj->GetMsgInfo().msg_payload_len);
}

int  SendMessageTcp::UnInit()
{
	if(m_hasInited)    
	{
		if (NULL != m_sockTcpClient)
		{
			m_sockTcpClient->Stop();    
		}
	}
	return 0;
}


//===========================================================================
// SendMessageDB ��ʵ��
SendMessageDB::SendMessageDB()
{

}
SendMessageDB::~SendMessageDB()
{

}

int     SendMessageDB::Init(void* param)
{
    lgprintf("SendMessageDB ��ʱδʵ��.");
	return -1;
}
int     SendMessageDB::SendMsg(AlarmMsgInfo* alarmMsgObj)
{
    lgprintf("SendMessageDB ��ʱδʵ��.");
    return -1;
}
int     SendMessageDB::UnInit()
{
    lgprintf("SendMessageDB ��ʱδʵ��.");
    return -1;
}
