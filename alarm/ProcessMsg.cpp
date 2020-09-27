#include "ProcessMsg.h"

ProcessMsg::ProcessMsg()
{
	m_SendMsgAry = NULL;
	m_configFile = NULL;
	m_alarmProcRun = false;
	m_HasInited = false;
}

ProcessMsg::~ProcessMsg()
{
	if (m_SendMsgAry != NULL)
	{
		for (int i=0;i<m_SendMsgNum;i++)
		{
			if(NULL != m_SendMsgAry[i])
			{
				m_SendMsgAry[i]->UnInit();
				delete m_SendMsgAry[i];
				m_SendMsgAry[i] = NULL;
			}
		}

		delete [] m_SendMsgAry;
		m_SendMsgAry = NULL;

		m_SendMsgNum = 0;
	}

	m_alarmProcRun = false;

	if (m_configFile != NULL)
	{
		delete m_configFile;
		m_configFile = NULL;
	}
}


// ��dp_log���л�ȡ�澯��Ϣ���뵽dp_alarm�У�������dp_log����֪ͨ��¼
int ProcessMsg::InsertDbAlarmInfo()
{
	if(!m_HasStarted)
	{
		return -1;
	}

	// ȡ���澯�¼�������dp_alarm��
	AutoMt  mt(m_InsertConn,1000);
	mt.FetchFirst("select a.eventid,a.tabid,a.datapartid,a.evt_tm,a.evt_tp,a.event,a.notifystatus,b.eventtypeid,b.eventtype,b.eventlevel,c.sendinterface "
		" from dp.dp_log a, dp.dp_eventtype b, dp.dp_eventsendctl c where a.evt_tp = b.eventtypeid and  b.eventtypeid = c.eventtype and "
		" b.eventlevel = c.eventlevel and a.notifystatus = %d",WAIT_TO_NOTIFY);

	int InsertNum = mt.Wait();
	if(InsertNum>0)
	{
		for (int i=0;i<InsertNum;i++)
		{
			AutoStmt mt2(m_InsertConn);  
			// �澯����dp_alarm��
			mt2.Prepare("INSERT INTO `dp`.`dp_alarm`(`eventid`,`procstatus`,`sendtimes`,`sendinterface`)  values (%d,%d,%d,%d)",
				mt.GetInt("eventid",i),ProcStatus_Alarm_Wait,0,mt.GetInt("sendinterface",i));
			mt2.Execute(1);
			mt2.Wait();

			// ����dp_log�еļ�¼
			mt2.Prepare("update dp.dp_log set notifystatus = %d where eventid = %d",HAS_NOTIFYED,mt.GetInt("eventid",i));
			mt2.Execute(1);
			mt2.Wait();
		}		 
	}
	return InsertNum;
}

// �����������״̬����dp_alarm���еĸ澯��Ϣ�������澯�޸�Ϊ�ȴ�����澯	
// �澯�ָ�������
// 1>. �ɼ����̣��ɼ������еĲ����ĸ澯��Ϣ���ȴ��ñ�Ķ�Ӧ�������ݲɼ���ɺ󣬾ͻָ��澯����Ӧdp_datapart.status = 2
// 2>. �����װ����̣������װ������в����ĸ澯��Ϣ���ȴ��ñ�Ķ�Ӧ��������װ����ɺ�ָ��澯����Ӧdp_datapart.status = 5
int  ProcessMsg::InsertDbClearAlarmInfo()
{
	if(!m_HasStarted)
	{
		return -1;
	}
	AutoStmt mt(m_UpdateConn);  

	/*
	-- ���������еĻָ��澯����
	update dp.dp_alarm set procstatus = 10 ,sendtimes = 0 where (procstatus = 2 or (procstatus = 3 and sendtimes >= 5)) and eventid in (
	select a.eventid from dp.dp_log a,dp.dp_datapart b,dp.dp_eventtype c 
	where a.tabid = b.tabid and a.datapartid = b.datapartid and 
	a.evt_tp = c.eventtypeid and c.eventtypeid < 250 and b.status = 2
	*/
	mt.Prepare("update dp.dp_alarm set procstatus = %d ,sendtimes = 0 where (procstatus = %d or (procstatus = %d and sendtimes >= %d)) and eventid in ( "
				" select a.eventid from dp.dp_log a,dp.dp_datapart b,dp.dp_eventtype c "
				" where a.tabid = b.tabid and a.datapartid = b.datapartid and "
				" a.evt_tp = c.eventtypeid and c.eventtypeid < %d and b.status = 2 )",
				ProcStatus_Clear_Alarm_Wait,
				ProcStatus_Alarm_Done,
                ProcStatus_Alarm_Error,
				m_configFile->getOption("send_msg_ctl:send_times",SEND_MAX_TIMES),
				DUMP_BEGIN_DUMPING_NOTIFY);

	mt.Execute(1);
	mt.Wait();

	/*
	-- ����ת������е�����澯
	update dp.dp_alarm set procstatus = 10 ,sendtimes = 0 where (procstatus = 2 or (procstatus = 3 and sendtimes >= 5)) and eventid in (
	select a.eventid from dp.dp_log a,dp.dp_datapart b,dp.dp_eventtype c 
	where a.tabid = b.tabid and a.datapartid = b.datapartid and 
	a.evt_tp = c.eventtypeid and c.eventtypeid > 300 and b.status = 5
	*/
	mt.Prepare("update dp.dp_alarm set procstatus = %d ,sendtimes = 0 where (procstatus = %d or (procstatus = %d and sendtimes >= %d)) and eventid in ( "
		" select a.eventid from dp.dp_log a,dp.dp_datapart b,dp.dp_eventtype c "
		" where a.tabid = b.tabid and a.datapartid = b.datapartid and "
		" a.evt_tp = c.eventtypeid and c.eventtypeid > %d and b.status = 5 )",
		ProcStatus_Clear_Alarm_Wait,
		ProcStatus_Alarm_Done,
		ProcStatus_Alarm_Error,
		m_configFile->getOption("send_msg_ctl:send_times",SEND_MAX_TIMES),
		MLOAD_CAN_NOT_FIND_MIDDLEDATA_ERROR);

	mt.Execute(1);
	mt.Wait();

	return 0;
}

// ���ظ澯��Ϣ���ڴ�(����Ŀǰֻ֧��һ����������)
int ProcessMsg::LoadDbSrcAlarmInfo(const bool bFirstLoad)
{
    if(!m_HasStarted)
	{
		return -1;
	}

	// ȡ���澯�¼�������dp_alarm��
    AutoMt  mt(m_LoadConn,1000);
	if (bFirstLoad)
	{
		// ��һ�μ��أ�ֻ���ش�������кʹ���ʧ���ҷ��ʹ���С������ʹ����ĸ澯
		mt.FetchFirst("select a.eventid,a.tabid,a.datapartid,a.evt_tm,a.evt_tp,a.event,a.notifystatus,b.eventtypeid,b.eventtype,b.eventlevel,"
			"c.alarmid,c.procstatus,c.sendtimes,c.sendinterface,c.lastproctime "
			"from dp.dp_log a, dp.dp_eventtype b, dp.dp_alarm c where a.evt_tp = b.eventtypeid "
			" and c.eventid = a.eventid and ( sendtimes < %d and c.procstatus in(%d,%d,%d,%d))",
			m_configFile->getOption("send_msg_ctl:send_times",SEND_MAX_TIMES),
			ProcStatus_Alarm_Doing,ProcStatus_Alarm_Error,ProcStatus_Clear_Alarm_Doing,ProcStatus_Clear_Alarm_Error);
	}
	else
	{
		// ���ǵ�һ�μ��أ�ֻ���صȴ�����ĸ澯���ͻָ�����ĸ澯
		mt.FetchFirst("select a.eventid,a.tabid,a.datapartid,a.evt_tm,a.evt_tp,a.event,a.notifystatus,b.eventtypeid,b.eventtype,b.eventlevel,"
			"c.alarmid,c.procstatus,c.sendtimes,c.sendinterface,c.lastproctime "
			"from dp.dp_log a, dp.dp_eventtype b, dp.dp_alarm c where a.evt_tp = b.eventtypeid "
			"and c.eventid = a.eventid and (c.procstatus = %d or c.procstatus = %d)",
			ProcStatus_Alarm_Wait,ProcStatus_Clear_Alarm_Wait);
	}
	int InsertNum = mt.Wait();
	char dt1[DATETIME_LEN],dt2[10];
    if(InsertNum>0)
	{
		for (int i=0;i<InsertNum;i++)
		{
			// �澯�����ڴ����
            AlarmMsgInfo* pAlarmMsg = NULL;
			pAlarmMsg = AlarmMessageFactory::CreateAlarmMessageObj(Protocol_Text);
			if (pAlarmMsg == NULL)
			{
				lgprintf("��ʱ��֧�ָ�����%d�ĸ澯Э��",Protocol_Text);
				continue;
			}

			// ���ø澯����
			pAlarmMsg->SetAlarmID(mt.GetInt("alarmid",i));
			pAlarmMsg->SetAlarmTitle(mt.PtrStr("eventtype",i));
			pAlarmMsg->SetSendInterface((SendType)mt.GetInt("sendinterface",i));
			pAlarmMsg->SetAlarmType((AlarmType)mt.GetInt("eventtypeid",i));
			pAlarmMsg->SetAlarmLevel((AlarmLevel)mt.GetInt("eventlevel",i));
			pAlarmMsg->SetAlarmReason(mt.PtrStr("event",i));
			if (mt.GetInt("procstatus",i) < ProcStatus_Clear_Alarm_Wait)
			{
                pAlarmMsg->SetAlarmStatus(AlarmStatus_OCCUR);
				pAlarmMsg->SetProcStatus(ProcStatus_Alarm_Doing);
			}
			else
			{
				pAlarmMsg->SetProcStatus(ProcStatus_Clear_Alarm_Doing);
				pAlarmMsg->SetAlarmStatus(AlarmStatus_CLEAR);
				char tm[DATETIME_LEN] = {0};
				GetCurrentTime(tm);
				pAlarmMsg->SetAlarmClearTime(tm);
			}
			
			// �澯����ʱ��
			memset(dt2,0,10);
			memset(dt1,0,DATETIME_LEN);
			mt.GetDate("evt_tm",i,dt2);
			_wdbiDateTimeToStr(dt2,dt1);
			pAlarmMsg->SetAlarmOccurTime(dt1);
			
			int sendInterval = m_configFile->getOption("send_msg_ctl:send_interval",SEND_DEFAULT_INTERVAL);
			if (sendInterval>SEND_MIN_INTERVAL)
			{
				sendInterval = SEND_MIN_INTERVAL;
				pAlarmMsg->SetSendInterval(sendInterval);
			}
			pAlarmMsg->SetSendTimes(mt.GetInt("sendtimes",i));
			pAlarmMsg->SetEventID(mt.GetInt("eventid",i));       // ���ڲ���dp_alarmlog��ʱ���ѯ��
			
			// ������ʱ��
			memset(dt2,0,10);
			memset(dt1,0,DATETIME_LEN);
			mt.GetDate("lastproctime",i,dt2);
			_wdbiDateTimeToStr(dt2,dt1);
			pAlarmMsg->SetLastSendTime(dt1);
			
			// ������ʱ��
			pAlarmMsg->GetAlarmTimer().Restart();
			
			// �������
			m_AlarmMsgQueue.AddAlarmMsg(pAlarmMsg);

			// �������ݿ��и澯״̬
			UpdateDbAlarmInfo(pAlarmMsg);
		}		 
	}
	return InsertNum;
}

// ���¸澯״̬��db���ݿ�
int ProcessMsg::UpdateDbAlarmInfo(AlarmMsgInfo* alarminfo)
{
	if (NULL == alarminfo)
	{
		return -1;
	}
	if (!m_HasStarted)
	{
		return -1;
	}
	// �����澯���������ݿ���
	if (alarminfo->GetAlarmStatus() == AlarmStatus_HEARTBEAT)
	{
		return 0;
	}

	AutoStmt mt2(m_UpdateConn);  
	// ����dp_log�еļ�¼
	mt2.Prepare("UPDATE `dp`.`dp_alarm` SET `procstatus` = %d ,`sendtimes` = %d,`lastproctime` = '%s'  WHERE alarmid = %d",
		alarminfo->GetProcStatus(),alarminfo->GetSendTimes(),alarminfo->GetLastSendTime(),
		alarminfo->GetAlarmID());
	mt2.Execute(1);
	mt2.Wait();

	return 0;
}
// ����澯��Ϣ���澯��־��
int ProcessMsg::InsertAlarmInfo2AlarmLog(AlarmMsgInfo* alarminfo)
{
	if (alarminfo == NULL)
	{
		return -1;
	}

	// �����澯���������ݿ���
	if (alarminfo->GetAlarmStatus() == AlarmStatus_HEARTBEAT)
	{
		return 0;
	}

	// �澯����dp_alarmlog��
	AutoStmt mt2(m_InsertConn);  
	// �澯����dp_alarm��
	mt2.Prepare("INSERT INTO `dp`.`dp_alarmlog`(`alarmid`,`tabid`,`datapartid`,`alarmtype`,`alarmlevel`,`alarmtime`,`alarmtitle`,`alarmreason`,`alarmstatus`,"
		" `procstatus`,`sendtimes`,`alarmprocesstime`,`sendinterface`) "
		" select '%d',tabid,datapartid,'%d','%d','%s','%s','%s','%d','%d','%d','%s','%d' from dp.dp_log where eventid = %d",
		alarminfo->GetAlarmID(),
		alarminfo->GetAlarmType(),
		alarminfo->GetAlarmLevel(),
		// ���ݸ澯������д�澯ʱ�������澯ʱ��
		alarminfo->GetAlarmStatus() == AlarmStatus_OCCUR ? alarminfo->GetAlarmOccurTime():alarminfo->GetAlarmClearTime(),
		alarminfo->GetAlarmTitle(),
		alarminfo->GetAlarmReason(),
		alarminfo->GetAlarmStatus(),
		alarminfo->GetProcStatus(),
		alarminfo->GetSendTimes(),
		alarminfo->GetLastSendTime(),
		alarminfo->GetSendInterface(),
		alarminfo->GetEventID());

	mt2.Execute(1);
	mt2.Wait();
    
	return 0;
}

// ��������Ĳ���
int ProcessMsg::GenerateHeartBeat()
{
	if (m_SendMsgAry == NULL)
	{
		return -1;
	}
	int send_interval = m_configFile->getOption("heart_beat:send_interval",5);
	if (m_HeartBeatTimer.GetTime()>send_interval)
	{
		for (int i=0;i<m_SendMsgNum;i++)
		{
			if (m_SendMsgAry[i] == NULL)
			{
				continue;
			}

			AlarmMsgInfo* pAlarmMsgObj = NULL;
			switch(m_SendMsgAry[i]->GetSendInterface())
			{
			case SEND_SOCKET_TCP:
                pAlarmMsgObj = AlarmMessageFactory::CreateAlarmMessageObj(Protocol_Text);
				break;

			default:
				// ����������ʱֻ��tcp��
				break;
			}

			if (NULL != pAlarmMsgObj)
			{
				// �����������нӿڶ����ͣ�����ʱ��ͷ���
				pAlarmMsgObj->SetAlarmStatus(AlarmStatus_HEARTBEAT);
				// ��Ӷ���
				m_AlarmMsgQueue.AddAlarmMsg(pAlarmMsgObj);
			}
		}

		m_HeartBeatTimer.Restart();
		return 0;
	}
	return -1;
}
// �������ݿ��е���Ϣ���ͽӿ����ͣ�������Ϣ���ͽӿڶ���
int ProcessMsg::CreateSendMsgArray()
{
	if(m_SendMsgAry != NULL)
	{
		lgprintf("�澯�ӿڶ����Ѿ���ʼ�����޷��������Ͷ���.");
		return -1;
	}
	AutoMt  mt(m_LoadConn,1000);
	mt.FetchFirst("select distinct codevalue from dp.dp_code where codetype = 25");
	int SendMsgTypeNum = mt.Wait();
	m_SendMsgNum = SendMsgTypeNum;
	if(SendMsgTypeNum>0)
	{
		m_SendMsgAry = new SendMessage*[SendMsgTypeNum];
		for (int i=0;i<SendMsgTypeNum;i++)
		{		    
			m_SendMsgAry[i] = NULL;	
		}
		// ��������
		for (int i=0;i<SendMsgTypeNum;i++)
		{
			// �澯�����ڴ����
			SendType send_type;
			try
			{
				send_type = (SendType)atoi(mt.PtrStr("codevalue",i));
			}
			catch(...)
			{
			   lgprintf("���ݿ��д洢��codevalue����ֵ����.");
			   return -1;	
			}
			m_SendMsgAry[i] = SendMessageFactory::CreateSendMessageObj(send_type);
			if (m_SendMsgAry[i] == NULL)
			{
				lgprintf("��ʱ��֧�ָ�����%d�ķ�����Ϣ�ӿ�.",send_type);
				continue;
			}
			m_SendMsgAry[i]->SetParentProcessRecvMsg(this);
            m_SendMsgAry[i]->SetSendInterface(send_type); // ���÷�����������
			
			int initFlag = -1;
			TcpServerItem st_tcp;
			DbServerItem st_db;
			switch(send_type)
			{
			case SEND_SOCKET_TCP:
				strcpy(st_tcp._ip,m_configFile->getOption("send_interface_tcp:serverip",""));
				st_tcp._port = m_configFile->getOption("send_interface_tcp:serverport",6666);
		        initFlag = m_SendMsgAry[i]->Init((void*)&st_tcp);
				break;
			case SEND_DB:
				// ��ʼ��
				//initFlag = m_SendMsgAry[i]->Init((void*)&st_db);
				initFlag =0;  // �Ժ��޸�ʵ��
				break;
			default:
				// ��֧��
				initFlag =0;
				break;
			}

			if (initFlag < 0)
			{
				lgprintf("��Ϣ���ͽӿ�%d��ʼ��ʧ��.",send_type);
				return -1;
			}

		}// end for

		return 0;

	}// end if
    
	lgprintf("û�и澯�ӿڿ��Գ�ʼ��.");
	return -1;
}

int  ProcessMsg::OnInit(const char* ctlConfigFile)
{
    if(m_HasInited)
    {
        lgprintf("ProcessMsg �Ѿ���ʼ�����������ظ�����.");
        return 0;	
    }
    if (NULL == m_configFile)
    {
		m_configFile = new ConfigFile(ctlConfigFile);
    }

	// ��������,�����ļ���ȡ����
	const char* dns = m_configFile->getOption("dp_server:dsn","dp");
	const char* user = m_configFile->getOption("dp_server:user","root");
	const char* password = m_configFile->getOption("dp_server:password","dbplus03");
	m_LoadConn.SetHandle(wociCreateSession(user,password,dns,DTDBTYPE_ODBC));
	m_UpdateConn.SetHandle(wociCreateSession(user,password,dns,DTDBTYPE_ODBC));
	m_InsertConn.SetHandle(wociCreateSession(user,password,dns,DTDBTYPE_ODBC));

	// �������Ͷ���
	if(CreateSendMsgArray() != 0)
	{
		return -1;
	}

	m_HeartBeatTimer.Clear();
	m_HeartBeatTimer.Start();

    m_HasInited = true;
    return 0;
}
int  ProcessMsg::OnStart()
{
    if(!m_HasInited)
    {
        if (-1 == OnInit("SendAlarm.ctl"))
        {
        	lgprintf(" OnInit(SendAlarm.ctl) ʧ�ܣ��޷�����澯��Ϣ.");
        	return -1;
        }
    }      
    
	// �����澯�����߳�
	if(pthread_create(&m_AlarmMsgProcThread,NULL,OnAlarmMsgProc,(void*)this)!=0)
    {
		lgprintf("can't create thread.\n");
		return -1;    	
    }
	m_alarmProcRun = true;
	m_HasStarted = true;
    m_HeartBeatTimer.Start();
    
	return 0;
}

int  ProcessMsg::OnStop()
{
	m_HasStarted = false;
	m_alarmProcRun = false;
	sleep(1);
	
	// ֹͣ���ͽӿ�
	if (m_SendMsgAry != NULL)
	{
		for (int i=0;i<m_SendMsgNum;i++)
		{
			if(NULL != m_SendMsgAry[i])
			{
				m_SendMsgAry[i]->UnInit();
				delete m_SendMsgAry[i];
				m_SendMsgAry[i] = NULL;
			}
		}

		delete [] m_SendMsgAry;
		m_SendMsgAry = NULL;

		m_SendMsgNum = 0;
	}
	sleep(1);

	// �ڴ��еĸ澯״̬������£��´���������س���	

	return 0;
}


// ����澯ҵ��
int ProcessMsg::ProcAlarmInfo()
{
    unsigned long alarmSize = m_AlarmMsgQueue.Size();
	// ÿ�δ���1��ĸ澯
    for (int i=0;i<alarmSize;i++)
    {
		// ��ȡ�澯��Ϣ
		AlarmMsgInfo* pAlarmObj = m_AlarmMsgQueue.GetAlarmMsg();
		if (pAlarmObj == NULL)
		{  
		    lgprintf("û���ҵ����������ĸ澯��¼��");
			continue;
		}

		// �ж�ʱ���Ƿ���Ч,���������ֱ�Ӵ���
		if(pAlarmObj->GetSendTimes()>0 && pAlarmObj->GetAlarmStatus() != AlarmStatus_HEARTBEAT)
		{
			if(pAlarmObj->GetAlarmTimer().GetTime() < pAlarmObj->GetSendInterval())
			{
				// �����㷢������
				m_AlarmMsgQueue.RetAlarmMsg(pAlarmObj);
				continue;
			}
		}

		// ֱ�ӿ��Է��͵�
		int retFlag = -1;
		for (int indexInterface=0;indexInterface<m_SendMsgNum;indexInterface++)
		{
			if (m_SendMsgAry[indexInterface] != NULL && 
				(m_SendMsgAry[indexInterface]->GetSendInterface() == pAlarmObj->GetSendInterface()|| 
				pAlarmObj->GetAlarmStatus() == AlarmStatus_HEARTBEAT))
			{
				// ������Ϣ
				pAlarmObj->DoPackMessage();

				if( pAlarmObj->GetAlarmStatus() == AlarmStatus_HEARTBEAT )
				{   // �������ֱ�ӷ��͸澯
				    retFlag = m_SendMsgAry[indexInterface]->SendMsg(pAlarmObj);
				}
				else
				{   // �Ƿ����θ澯�����ξͲ�����
				    if(SEND_ALARM_YES == m_configFile->getOption("send_msg_ctl:send_alarm_switch",SEND_ALARM_YES)) 
				    {
    				    retFlag = m_SendMsgAry[indexInterface]->SendMsg(pAlarmObj);               
				    }
					else
					{
					    lgprintf("���͸澯����send_msg_ctl:send_alarm_switch = 0,δ���͸澯(ֻ����������).");
                        retFlag = 1;
					}
				}
				pAlarmObj->GetAlarmTimer().Restart();
				if(LogSendMsg_Yes == m_configFile->getOption("send_msg_ctl:log_send_msg",LogSendMsg_Yes))
				{
					lgprintf("������Ϣ:\n%s\n %s.",pAlarmObj->GetMsgInfo().msg_payload,retFlag < 0 ? "ʧ��":"�ɹ�");
                }
				// ���������������������ݿ���ش���
				if (pAlarmObj->GetAlarmStatus()== AlarmStatus_HEARTBEAT)
				{
					lgprintf("������������:%s , ����%s.",pAlarmObj->GetMsgInfo().msg_payload,retFlag < 0 ? "ʧ��":"�ɹ�");
					break;
				}

				// ���Ĵ���״̬
				if (pAlarmObj->GetProcStatus()<ProcStatus_Clear_Alarm_Wait)
				{
					pAlarmObj->SetProcStatus(retFlag < 0 ? ProcStatus_Alarm_Error : ProcStatus_Alarm_Done);
				}
				else
				{
					pAlarmObj->SetProcStatus(retFlag < 0 ? ProcStatus_Clear_Alarm_Error : ProcStatus_Clear_Alarm_Done);
				}
				pAlarmObj->SetLastSendTime();
				pAlarmObj->AddSendTimes();

				// ���¸澯״̬��Ϣ����dp_alarm����
				UpdateDbAlarmInfo(pAlarmObj);
				
				// ����澯��־��Ϣ��dp_alarmlog����
				InsertAlarmInfo2AlarmLog(pAlarmObj);
				
				break;
			} // end if 
		}// end for
        m_AlarmMsgQueue.RetAlarmMsg(pAlarmObj);	

		// ɾ���ڴ��еĸ澯
		// 1. ����
		// 2. �Ѿ�������ɵĸ澯��Ϣ
		// 3. ���ʹ�����������ʹ����ĸ澯
		if (pAlarmObj->GetAlarmStatus() == AlarmStatus_HEARTBEAT || 
			(pAlarmObj->GetProcStatus() == ProcStatus_Alarm_Done) || 
			(pAlarmObj->GetProcStatus() == ProcStatus_Clear_Alarm_Done)|| 
			(pAlarmObj->GetSendTimes() > m_configFile->getOption("send_msg_ctl::send_times",SEND_MAX_TIMES)) )
		{
			// ���ø澯Ϊɾ��״̬,��DeleteNullAlarmMsg��ɾ��
			pAlarmObj->SetAlarmStatus(AlarmStatus_Delete);
		}

    } // for (int i=0;i<alarmSize;i++) 

	// ��ո澯�б���ΪNULL�ĸ澯��Ϣ
	return m_AlarmMsgQueue.DeleteNullAlarmMsg();
}

// �澯�����߳�
void* ProcessMsg::OnAlarmMsgProc(void *params)
{
    ProcessMsg* _this = NULL;
	_this = (ProcessMsg*)params;
	if (_this == NULL)
	{
		lgprintf("�̲߳�������ȷ��ProcessMsg::OnAlarmMsgProc�߳�����ʧ��.");
		return NULL;
	}

	//  ��һ�δ����ݿ��м�����һ��û�д�����ɵĸ澯
	_this->LoadDbSrcAlarmInfo(true);

    while (_this->m_alarmProcRun)
    {
		// ���ظ澯��Ϣ���ڴ���
		_this->LoadDbSrcAlarmInfo(false);
		sleep(1);

		// ���������Ϣ�澯��Ϣ���ڴ�
		if(-1 == _this->GenerateHeartBeat())
		{			
			sleep(4);          // û������
		}

		// ��dp_log�еĸ澯��Ϣ�����dp_alarm��
		_this->InsertDbAlarmInfo();
		
		// ����澯ҵ����Ϣ
		int processNum = _this->ProcAlarmInfo();
		int sleepRate = processNum / 50;
		if (sleepRate == 0)
		{
			sleep(5);
		}
		else if (sleepRate >=1 && sleepRate<= 5)
		{
			sleep(3);
		}
		else
		{
			sleep(1);
		}
			
		// �ָ��澯��״̬����
		_this->InsertDbClearAlarmInfo();
    }
    
    return NULL;
}



// ������˵�Է��ӿڵ���Ϣ
int	ProcessMsg::ProcRecvMsg(const unsigned char *rcvMsg,const unsigned int rcvMsgLen)
{
	// �Ժ������ԶԷ�����Ϣ
	lgprintf("recv msg : len = %d,msg = %s.",rcvMsgLen,(char*)rcvMsg);
	return 0;
}
