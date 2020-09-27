#include "AlarmMsg.h"

const char* FunGetAlarmLevel(AlarmLevel& al)
{
    switch(al)
    {
    case  DUMP_LOG:
    	return "��������������־";
	case  MLOAD_LOG:
		return "��������������־";
	case  LOAD_LOG:
		return "װ������������־";
	case  DUMP_WARNING:
		return "��������";
	case  MLOAD_WARNING:
		return "������";
	case  LOAD_WARNING:
		return "װ�뾯��";
	case  DUMP_ERROR:
		return "��������";
	case  MLOAD_ERROR:
		return "�������";
	case  LOAD_ERROR:
		return "װ�����";
    default:
        break;  	
    }
    return NULL;
}


const  char* FunGetAlarmStatus(AlarmStatus& as)
{
	switch(as)
	{
	case AlarmStatus_CLEAR: 
		return "����澯";
    case AlarmStatus_OCCUR: 
    	return "�����澯";
	case AlarmStatus_HEARTBEAT: 
		return "HEARTBEAT";
	default:
		break;    
	}
	return NULL;
};
/**************************************************************************************************
  Function    : DoPackMessage()
  DateTime    : 2013/1/14 15:49
  Description : �ı��澯Э��Ĵ��
  Param       : null
  Return      : success : 0
                error : -1
  Note        : �����ƶ��澯��ʽ
  example:
  ------------------------------------------------------------------------------------------
  �澯��Ϣ:                                           �ָ���Ϣ:                                
  <BeginAlarm>                                        <BeginAlarm>                             
  �澯��ˮ��:1699759                                  �澯��ˮ��:1699759                       
  ��Ԫ����:XN785_�Ǻ�M-GSM900M��վ                    ��Ԫ����:XN785_�Ǻ�M-GSM900M��վ         
  ��λ��Ϣ:                                           ��λ��Ϣ:                                
  ����:����                                           ����:����                                
  ��Ԫ����:����                                       ��Ԫ����:����                            
  �澯����:�Ž�                                       �澯����:�Ž�                            
  ����ԭ��:ĳ������̽ͷ̽�⵽��վ�ŷǷ���           ����ԭ��:ĳ������̽ͷ̽�⵽��վ�ŷǷ���
  �澯״̬:��澯                                   �澯״̬:����澯                        
  �澯���:�����Ž�                                   �澯���:�����Ž�                        
  �澯����:�����澯                                   ���ܸ澯����:�����澯                    
  �澯����ʱ��:2013-01-08 15:04:23                    �澯����ʱ��:2013-01-08 15:04:23         
  <EndAlarm>                                          �澯�ָ�ʱ��:2013-01-08 15:04:22         
                                                      <EndAlarm>                      
  -----------------------------------------------------------------------------------------
**************************************************************************************************/
#define  alarm_occur_format "%s\n\
�澯��ˮ��:%d\n\
��Ԫ����:\n\
��λ��Ϣ:\n\
����:\n\
��Ԫ����:\n\
�澯����:%s\n\
����ԭ��:%s\n\
�澯״̬:%s\n\
�澯���:%d\n\
�澯����:%s\n\
�澯����ʱ��:%s\n\
%s"    

#define  alarm_clear_format "%s\n\
�澯��ˮ��:%d\n\
��Ԫ����:\n\
��λ��Ϣ:\n\
����:\n\
��Ԫ����:\n\
�澯����:%s\n\
����ԭ��:%s\n\
�澯״̬:%s\n\
�澯���:%d\n\
�澯����:%s\n\
�澯����ʱ��:%s\n\
�澯�ָ�ʱ��:%s\n\
%s"

#define  alarm_heartbeat_format "%s\n%s %s\n%s"

int TextAlarmInfo::DoPackMessage()
{
    memset(&m_stmsgInfo,0,sizeof(StruMsgInfo));
    if (AlarmStatus_OCCUR == m_alarmStatus)
    {
        sprintf((char*)m_stmsgInfo.msg_payload,alarm_occur_format,
            m_PackHeader,
            m_alarmId,
            m_alarmTitle,
            m_alarmReason,
            FunGetAlarmStatus(m_alarmStatus),
            m_alarmType,
            FunGetAlarmLevel(m_alarmLevel),
            m_alarmOccurTime,
            m_PackTail);
    } 
    else if(AlarmStatus_CLEAR == m_alarmStatus)
    {
		sprintf((char*)m_stmsgInfo.msg_payload,alarm_clear_format,
            m_PackHeader,
            m_alarmId,
            m_alarmTitle,
            m_alarmReason,
            FunGetAlarmStatus(m_alarmStatus),
            m_alarmType,
            FunGetAlarmLevel(m_alarmLevel),
            m_alarmOccurTime,
			m_alarmClearTime,
            m_PackTail);
	}
    else if(AlarmStatus_HEARTBEAT == m_alarmStatus)
	{
		char current_dt[DATETIME_LEN] = {0};
        GetCurrentTime(current_dt);

        sprintf((char*)m_stmsgInfo.msg_payload,alarm_heartbeat_format,
			m_PackHeader,
			FunGetAlarmStatus(m_alarmStatus),
			current_dt,
			m_PackTail);
	}
	else
	{
		return -1;
	}	
	m_stmsgInfo.msg_payload_len = strlen((char*)m_stmsgInfo.msg_payload);
	return 0;
}


// �ı�Э�飬�����ƶ�[������ͨ]�����Э��
int TextAlarmInfo::DoUnPackMessage(const unsigned char *rcvMsg,const unsigned int rcvMsgLen)
{ 
	// ��ʱû�з��ص�Э��
    lgprintf("TextAlarmInfo::DoUnPackMessage ��δ֧�ֽ�����Э��.����[%s],����[%d].",(char*)rcvMsg,rcvMsgLen);
    return -1;
}

//=======================================================================================
// AlarmMsgQueue
AlarmMsgQueue::AlarmMsgQueue()
{
	pthread_mutex_init(&m_lock,NULL);
	m_msgVector.clear();
    m_currentIndex = -1;
}

AlarmMsgQueue::~AlarmMsgQueue()
{
    pthread_mutex_lock(&m_lock);
    for (int i=0;i<m_msgVector.size();i++)
    {
        AlarmMsgInfo *pObj = NULL;
        pObj = m_msgVector[i];
        if(NULL != pObj)
		{
            delete pObj;
            pObj = NULL;
        }
    }
	m_msgVector.clear();
	pthread_mutex_unlock(&m_lock);
    pthread_mutex_destroy(&m_lock);
    m_currentIndex = -1;
}

// ��Ӹ澯ǰ���жϸ澯��¼�Ƿ�����ڴ���
int AlarmMsgQueue::AddAlarmMsg(AlarmMsgInfo* msg)
{
    if(NULL != msg)
	{
        AutoLock alk(&m_lock);
        m_msgVector.push_back(msg);
        return 0;
	}
	return -1;
}

// �жϸ澯�洢���������澯id��ͬ���澯״̬��ͬ����Ϊ:ͬһ���澯����2��״̬[�澯�������澯�ָ�]
bool AlarmMsgQueue::FindAlarmMsg(const unsigned long alarmId,const AlarmStatus as)
{
    AutoLock alk(&m_lock);
    for (int i=0;i<m_msgVector.size();i++)
    {
        if ((m_msgVector[i] != NULL ) && (m_msgVector[i]->GetAlarmID() == alarmId && m_msgVector[i]->GetAlarmStatus() == as))
        {
			return true;
        }
    }

	return false;
}

/**************************************************************************************************
  Function    : GetAlarmMsg(),RetAlarmMsg(AlarmMsgInfo* msg)
  DateTime    : 2013/1/15 10:07
  Description : ��������������ɶԳ���ʹ��
**************************************************************************************************/
AlarmMsgInfo*  AlarmMsgQueue::GetAlarmMsg()
{
    AutoLock alk(&m_lock);
    if(m_msgVector.size() >0 && (m_currentIndex > m_msgVector.size() -1))
	{
        m_currentIndex = 0;
	}
    AlarmMsgInfo* pObj = NULL;
    int   indexAdd = 0;
	int   i = 0;

    // �ӵ�ǰλ��������	
    for (i=m_currentIndex;i<m_msgVector.size();i++)
    {
        indexAdd++;
        if(m_msgVector[i] != NULL && m_msgVector[i]->GetRef() ==0 && m_msgVector[i]->GetAlarmStatus() != AlarmStatus_Delete)
		{
             pObj = (AlarmMsgInfo*)m_msgVector[i];
			 break;
        }
    }
	m_currentIndex+= indexAdd;
	if(m_currentIndex > m_msgVector.size() -1)
	{
		m_currentIndex = 0;
	}
	if (NULL != pObj)
	{
		pObj->AddRef();
		return pObj;
	}

	// ��ͷ����ʼ������
    indexAdd=0;
	for (i=0;i<=m_currentIndex;i++)
	{
        indexAdd++;
		if(m_msgVector[i] != NULL && m_msgVector[i]->GetRef() ==0 && m_msgVector[i]->GetAlarmStatus() != AlarmStatus_Delete)
		{
			pObj = (AlarmMsgInfo*)m_msgVector[i];
	        break;
		}
	}
	m_currentIndex+= indexAdd;
	if(m_currentIndex > m_msgVector.size() -1)
	{
		m_currentIndex = 0;
	}
	if (NULL != pObj)
	{
		pObj->AddRef();
		return pObj;
	}

	// û���ҵ����������ķ���NULL
	return NULL;
}
int AlarmMsgQueue::RetAlarmMsg(AlarmMsgInfo* msg)       // �澯��Ϣʹ����ɺ��䷵�ض����� 
{
    if(NULL == msg)
	{
        return -1;
	}
	msg->SubRef();
    return 0;
}

// ɾ���յĶ���
int AlarmMsgQueue::DeleteNullAlarmMsg()
{
	AutoLock alk(&m_lock);
	AlarmMsgVector::iterator iter;
	int deleteNum = 0;
retry:
	for (iter = m_msgVector.begin();iter!=m_msgVector.end();iter++)
	{
		AlarmMsgInfo* obj = *iter;
		if ( obj != NULL && obj->GetAlarmStatus() == AlarmStatus_Delete)
		{
			m_msgVector.erase(iter);
			delete obj;
			obj = NULL;
			deleteNum++;
			goto retry;
		}		
	}	
	return deleteNum;
}

int  AlarmMsgQueue::DeleteAlarmMsg(AlarmMsgInfo* msg)
{	
	AutoLock alk(&m_lock);
	AlarmMsgVector::iterator iter;
	for (iter = m_msgVector.begin();iter!=m_msgVector.end();iter++)
	{
		AlarmMsgInfo* obj = *iter;
		if((obj == NULL) && (obj->GetAlarmID() == msg->GetAlarmID() && obj->GetAlarmStatus() == msg->GetAlarmStatus()))
		{
			m_msgVector.erase(iter);
			delete obj;
			obj = NULL;
		}
	}	
}

unsigned long AlarmMsgQueue::Size()
{
    AutoLock alk(&m_lock);
    return m_msgVector.size();
}
