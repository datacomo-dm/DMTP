#ifndef  PROCESS_MSG_DEF_H
#define  PROCESS_MSG_DEF_H
/**************************************************************************************************
  Copyright (C), 2010-2020, DATACOMO COMMUNICATIONGS INC(http://www.datacomo.com/)
  File name   : PROCESSALARMMSG.H    
  Author      : Liujs(liu_shun_shun@126.com)     
  Version     : V1.00        
  DateTime    : 2013/1/14 22:50	
  Description : �澯�������ҵ��ʵ��
**************************************************************************************************/
#include "AlarmMsg.h"
#include "SendMsg.h"

// ����澯��Ϣ��,��ɾ���澯��ɾ����Ҫ�ֶ�ɾ��
class ProcessMsg:public ProcessRecvMsg 
{
protected:
    AlarmMsgQueue             m_AlarmMsgQueue;         // �ڴ�澯����
    int                       m_SendMsgNum;            // ���Ͷ������
	SendMessage**             m_SendMsgAry;            // ������Ϣ�ӿ���ָ�� new SendMessage*[x]
    bool                      m_HasStarted;            // �߳��Ƿ��Ѿ�����
	ConfigFile*               m_configFile;            // �����ļ�
	bool                      m_HasInited;             // �Ƿ��Ѿ���ʼ��

protected:
    pthread_t                 m_AlarmMsgProcThread;    // �澯��Ϣ�����߳�
    bool                      m_alarmProcRun;          // �����߳����б�־
	
	// ����澯��������ݿ�����
	AutoHandle                m_InsertConn,m_LoadConn,m_UpdateConn;
	my_timer                  m_HeartBeatTimer;        // �������ʱ��
	enum
    { 
        LogSendMsg_No = 0,      // �����������־���ļ�
		LogSendMsg_Yes = 1,     // ���������־���ļ�
    };
	enum
	{
        SEND_ALARM_YES = 1,     // ���͸澯
        SEND_ALARM_NO = 0,      // �����͸澯
	};

protected:
	// ��dp_log���л�ȡ�澯��Ϣ���뵽dp_alarm
	int                       InsertDbAlarmInfo();
	// �����������״̬����dp_alarm���еĸ澯��Ϣ�������澯�޸�Ϊ�ȴ�����澯
	int                       InsertDbClearAlarmInfo();
	// ���ظ澯��Ϣ���ڴ�
    int                       LoadDbSrcAlarmInfo(const bool bFirstLoad = false);
	// ���¸澯״̬��db���ݿ�
    int                       UpdateDbAlarmInfo(AlarmMsgInfo* alarminfo);    
	// ����澯��Ϣ���澯��־��
	int                       InsertAlarmInfo2AlarmLog(AlarmMsgInfo* alarminfo);
    // ��������Ĳ���
    int                       GenerateHeartBeat();
	// ����澯ҵ��
	int                       ProcAlarmInfo();
    // �������ݿ��е���Ϣ���ͽӿ����ͣ�������Ϣ���ͽӿڶ���
    int                       CreateSendMsgArray();

public:
	ProcessMsg();
	virtual ~ProcessMsg();

public:	// ������ֹͣ�澯����
	int                       OnInit(const char* ctlConfigFile);
    int                       OnStart();
    int                       OnStop();
	// ������˵�Է��ӿڵ���Ϣ
	virtual        int	      ProcRecvMsg(const unsigned char *rcvMsg,const unsigned int rcvMsgLen);
	

protected: // �澯��������߳�
	static        void*       OnAlarmMsgProc(void *params);      // ����澯��Ϣ�߳�
};

#endif
