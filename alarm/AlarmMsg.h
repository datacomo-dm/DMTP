#ifndef ALARM_MSG_DEF_H
#define	ALARM_MSG_DEF_H
/**************************************************************************************************
  Copyright (C), 2010-2020, DATACOMO(http://www.datacomo.com/)
  File name   : ALARMMSG.H      
  Author      : Liujs(liu_shun_shun@126.com)     
  Version     : V1.00        
  DateTime    : 2013/1/13 9:45
  Description : define alarm message info class
**************************************************************************************************/
#include "dt_common.h"
#include "dt_svrlib.h"
#include <stdlib.h>
#include <string.h>
#include <vector>
#include <iostream>
#include <pthread.h>

#define SEND_MAX_TIMES        5                // ������ʹ���
#define SEND_DEFAULT_INTERVAL 5*60             // ����ʱ����  
#define SEND_MIN_INTERVAL     10               // ��С����ʱ����
#define MAX_MESSAGE_LEN       8*1024           // ������Ϣ���֧��8K
#define DATETIME_LEN          20               // yyyy-mm-dd hh:mm:ss


enum AlarmStatus
{
    AlarmStatus_CLEAR,         // ����澯
    AlarmStatus_OCCUR,         // �澯��ʼ
    AlarmStatus_HEARTBEAT,     // ����������洢���ݿ⣬���ͼ�¼�洢��־��
    AlarmStatus_Delete,        // �ڴ���Ӧ�ñ�ɾ���ĸ澯��Ϣ
};
typedef AlarmEventType AlarmType;             // dt_svrlib.h �ж���澯����
typedef AlarmEventLevel AlarmLevel;           // dt_svrlib.h �ж���澯����

enum  ProcStatus  // �澯����״̬����Ӧdp_alarm.procstatus
{
    ProcStatus_Alarm_Wait=0,            // 0:�ȴ���������澯
    ProcStatus_Alarm_Doing,             // 1:�����澯������
    ProcStatus_Alarm_Done,              // 2:�����澯�������
	ProcStatus_Alarm_Error,             // 3:�����澯�������
    ProcStatus_Clear_Alarm_Wait=10,     // 10:�ȴ�����ָ��澯,
	ProcStatus_Clear_Alarm_Doing,       // 11:�����澯�������
	ProcStatus_Clear_Alarm_Done,        // 12:�����澯�������
    ProcStatus_Clear_Alarm_Error,       // 13:�ָ��澯�������
};

enum  SendType // code_type: 25 code_value : �澯��Ϣ���ͽӿ�����
{
   SEND_SOCKET_TCP = 100,   // 100-socket tcp(������ͨ-�����ƶ�) 
   SEND_SMS = 101,          // 101-sms 
   SEND_DB = 102,           // 102-database table 
   SEND_EMAIL = 103,        // 103-email 
   SEND_MMS = 104,          // 104-mms 
   SEND_WEB_SERVICE = 105,  // 105-web service
};

// ���� : yyyy-mm-dd hh:mm:ss
#define GetCurrentTime(datetime)\
{\
	char now[10];\
	wociGetCurDateTime(now);\
	_wdbiDateTimeToStr(now,datetime);\
}

// �澯״̬
extern const char* FunGetAlarmStatus(AlarmStatus& as);
// �澯����
extern const char* FunGetAlarmLevel(AlarmLevel& al);


/**************************************************************************************************
  Object      : my_timer
  Author      : Liujs     
  Version     : V1.00        
  DateTime    : 2013/1/25 18:07
  Description : ��ʱ���ļ�ʵ��
**************************************************************************************************/
class my_timer {
	struct timeval st;
public:
	my_timer() {
	    memset(&st,0,sizeof(timeval));
	}
	void Clear() {
		memset(&st,0,sizeof(timeval));
	}
	void Start() {
		gettimeofday(&st,NULL);
	}
	void Restart() {
		Clear();Start();
	}
	double GetTime() {
        struct timeval ct;
        gettimeofday(&ct,NULL);
		return (double)(ct.tv_sec-st.tv_sec) + (double)(ct.tv_usec/1000000);
	}
};

/**************************************************************************************************
  Object      : AlarmMsgInfo
  Author      : Liujs     
  Version     : V1.00        
  DateTime    : 2013/1/15 11:03
  Description : �澯��Ϣ��
**************************************************************************************************/
typedef struct StruMsgInfo
{
	unsigned int    msg_payload_len;                    // msg ��Ч����
	unsigned char   msg_payload[MAX_MESSAGE_LEN];       // ��Ϣ����Ϣ
}StruMsgInfo,*StruMsgInfoPtr;
class AlarmMsgInfo          // ��Ϣ������
{
protected:
	unsigned long        m_alarmId;                         // �澯��ˮ��:1699759
    char                 m_alarmTitle[128];                 // �澯����:�Ž�
    char                 m_alarmReason[1024];               // ����ԭ��:ĳ������̽ͷ̽�⵽��վ�ŷǷ���
	AlarmStatus          m_alarmStatus;                     // �澯״̬:����澯
	AlarmType            m_alarmType;                       // �澯���:�����Ž�
	AlarmLevel           m_alarmLevel;                      // ���ܸ澯����:�����澯
    char                 m_alarmOccurTime[DATETIME_LEN];    // �澯����ʱ��:2013-01-08 15:04:23
	char                 m_alarmClearTime[DATETIME_LEN];    // �澯�ָ�ʱ��:2013-01-08 15:04:22

protected:
	unsigned    char     m_PackHeader[32];                  // ��ͷ
	unsigned    char     m_PackTail[32];                    // ��β

protected:
	StruMsgInfo          m_stmsgInfo;                       // ������Ϣ��Ϣ    
                         
protected:	// ����ʹ��
	unsigned short       m_sendTimes;                       // �澯���ʹ���
	unsigned short       m_sendInterval;                    // �澯���ʹ�����ʱ����,��λΪs
	my_timer             m_alarmTimer;                      // �澯��ʱ��
	char                 m_lastSendTime[DATETIME_LEN];      // ���һ�η���ʱ��      
    ProcStatus           m_procStatus;                      // �澯����״̬��
    int                  m_ref;                             // �������ü����������л�ȡ�������ü���+1��ʹ����ɺ����ü���-1
    SendType             m_sendInterface;                   // ��Ϣ���ͽӿ�����  
	unsigned int         m_eventid;                         // �澯�¼�id����Ӧdp_log����

public:
	AlarmMsgInfo()
	{
        m_stmsgInfo.msg_payload[0] = 0;
		m_stmsgInfo.msg_payload_len = 0;
		memset(m_alarmOccurTime,0,DATETIME_LEN);
		memset(m_alarmClearTime,0,DATETIME_LEN);
		memset(m_lastSendTime,0,DATETIME_LEN);
		memset(m_alarmTitle,0,128);
		memset(m_alarmReason,0,1024);
		m_alarmTimer.Start();
		
		m_alarmId = -1;
        m_ref = 0;
	}
	virtual ~AlarmMsgInfo()
	{
        m_alarmTimer.Clear();
	}

public:
	// ��Ϣ��װ�࣬��ͬ��Ϣ���ͣ���������ʵ��
	virtual int             DoPackMessage(){ThrowWith("Call DoPackMessage Error");return -1;};
	// ��Ϣ����
	virtual int             DoUnPackMessage(const unsigned char *rcvMsg,const unsigned int rcvMsgLen){ThrowWith("call DoUnPackMessage Error");return -1;};

public:
	inline  unsigned  short GetSendTimes(){return m_sendTimes;};
	inline  void            AddSendTimes(){m_sendTimes++;};
	inline  void            SetSendTimes(const int sendTimes){m_sendTimes = sendTimes;};

	inline  unsigned  short GetSendInterval(){return m_sendInterval;};
	inline  void            SetSendInterval(const int sendInterval){m_sendInterval = sendInterval;};
	
	inline  char*           GetLastSendTime(){return m_lastSendTime;};
	inline  void            SetLastSendTime(const char* tm){strcpy(m_lastSendTime,tm);};
	inline  void            SetLastSendTime(){char tm[DATETIME_LEN] = {0};GetCurrentTime(tm);strcpy(m_lastSendTime,tm);};
	inline  StruMsgInfo&    GetMsgInfo(){return m_stmsgInfo;};

	inline  ProcStatus      GetProcStatus(){return m_procStatus;};
	inline  void            SetProcStatus(ProcStatus procStatus){m_procStatus = procStatus;};

	inline  void            SetSendInterface(SendType sendType){m_sendInterface = sendType;};
	inline  SendType        GetSendInterface(){return m_sendInterface;};

    inline  void            AddRef(){m_ref++;};
    inline  void            SubRef(){if(m_ref>0) m_ref--;};
    inline  int             GetRef(){return m_ref;};
	
	inline  unsigned  int   GetEventID(){return m_eventid;};
	inline  void            SetEventID(const int eventid){m_eventid = eventid;};

	inline  my_timer&        GetAlarmTimer(){return m_alarmTimer;};

public:
    inline  unsigned long   GetAlarmID(){return m_alarmId;};               // �澯��ˮ��:1699759
    inline  void            SetAlarmID(const unsigned long alarmId){m_alarmId = alarmId;};

	inline  char*           GetAlarmTitle(){return m_alarmTitle;};         // �澯����:�Ž�
    inline  void            SetAlarmTitle(const char* alarmTitle){strcpy(m_alarmTitle,alarmTitle);m_alarmTitle[strlen(alarmTitle)] = 0;};

	inline  char*           GetAlarmReason(){return m_alarmReason;};       // ����ԭ��:ĳ������̽ͷ̽�⵽��վ�ŷǷ���
	inline  void            SetAlarmReason(const char* alarmReason){ strcpy(m_alarmReason,alarmReason);m_alarmReason[strlen(alarmReason)] = 0;};

	inline  AlarmStatus     GetAlarmStatus(){return m_alarmStatus;};       // �澯״̬:����澯,�����澯
	inline  void            SetAlarmStatus(const AlarmStatus alarmStatus){m_alarmStatus = alarmStatus;};

	inline  AlarmType       GetAlarmType(){return m_alarmType;};           // �澯���:�����Ž�
	inline  void            SetAlarmType(const AlarmType alarmType){m_alarmType = alarmType;};
	
	inline  AlarmLevel      GetAlarmLevel(){return m_alarmLevel; };        // ���ܸ澯����:�����澯
	inline  void            SetAlarmLevel(const AlarmLevel alarmLevel){m_alarmLevel = alarmLevel;};

	inline  char*           GetAlarmOccurTime(){return m_alarmOccurTime;}; // �澯����ʱ��:2013-01-08 15:04:23
	inline  void            SetAlarmOccurTime(const char* aot){strcpy(m_alarmOccurTime,aot);m_alarmOccurTime[strlen(aot)]=0;};
	inline  char*           GetAlarmClearTime(){return m_alarmClearTime;}; // �澯�ָ�ʱ��:2013-01-08 15:04:22
	inline  void            SetAlarmClearTime(const char* act){strcpy(m_alarmClearTime,act);m_alarmClearTime[strlen(act)]=0;};
};


/**************************************************************************************************
  Object      : TextAlarmInfo
  Author      : Liujs     
  Version     : V1.00        
  DateTime    : 2013/1/15 11:02
  Description : �����ƶ��ƶ��ı��澯��
**************************************************************************************************/
class TextAlarmInfo:public AlarmMsgInfo
{
public:
	TextAlarmInfo():AlarmMsgInfo()
	{
        memset(m_PackHeader,0,32);
        strcpy((char*)m_PackHeader,"<+++>");
		memset(m_PackTail,0,32);
		strcpy((char*)m_PackTail,"<--->");
	}
	virtual ~TextAlarmInfo(){};

public:
	// �澯��Ϣ���
	virtual int    DoPackMessage();
	virtual int    DoUnPackMessage(const unsigned char *rcvMsg,const unsigned int rcvMsgLen);

};

enum AlarmProtocolType            // Э������
{
    Protocol_Text = 0,            // �����ı�Э��
    Protocol_Bin,                 // ������Э��
};

// ������Ϣ�򵥹�����
class AlarmMessageFactory
{
public:
	static   AlarmMsgInfo* CreateAlarmMessageObj(const AlarmProtocolType alarmProtocolType)
	{
		// Ŀǰ���ı���Э��
		if (Protocol_Text == alarmProtocolType)
		{
			return new TextAlarmInfo();
		} 
		else
		{
			// ��������Э��
			return NULL;
		}
	}
};

//================================================================================================
typedef pthread_mutex_t Lock;
class AutoLock 
{
private:
	Lock* m_lock;
public:
    AutoLock(Lock* lock):m_lock(lock)
	{
		pthread_mutex_lock(m_lock);
	}
	virtual ~AutoLock()
	{
		pthread_mutex_unlock(m_lock);
	}
};

/**************************************************************************************************
  Object      : AlarmMsgQueue
  Author      : Liujs     
  Version     : V1.00        
  DateTime    : 2013/1/15 11:00
  Description : �澯��Ϣ����
**************************************************************************************************/
class AlarmMsgQueue 
{
protected:
	typedef	std::vector<AlarmMsgInfo*>  AlarmMsgVector;
	AlarmMsgVector           m_msgVector;
	Lock                     m_lock;       
    unsigned long            m_currentIndex; // ��ǰ��ȡ�Ķ���������GetAlarmMsgʱ����

public:
    AlarmMsgQueue();
    virtual~ AlarmMsgQueue();

public:
    // �жϸ澯�洢���������澯id��ͬ���澯״̬��ͬ
	bool                    FindAlarmMsg(const unsigned long alarmId,const AlarmStatus as);
	int                     AddAlarmMsg(AlarmMsgInfo* msg);
	int                     DeleteAlarmMsg(AlarmMsgInfo* msg);
	int                     DeleteNullAlarmMsg();
	unsigned long           Size();

public:
    // ���к�������ɶ�ʹ��
	AlarmMsgInfo*           GetAlarmMsg();                         // �Ӷ����л�ȡһ������ʹ�õĸ澯��Ϣ                  
	int                     RetAlarmMsg(AlarmMsgInfo* msg);        // �澯��Ϣʹ����ɺ��䷵�ض����� 
};
#endif 
