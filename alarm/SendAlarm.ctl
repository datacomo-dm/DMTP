# ��#�ſ�ʼ��ʾ��һ����ע��
[heart_beat] 
# ��������ʱ��������λΪ��
send_interval=30

#���Ϳ���
[send_msg_ctl]
# ͬһ�澯������2�η���ʱ��������λ���ӣ�Ĭ��ֵ300�룬��СֵΪ10��
send_interval = 30
# ����ʧ���ظ�������Ĭ��ֵΪ5�Σ���СֵΪ1��
send_times = 6
# �Ƿ����������־,1:��־�ļ������������־��Ϣ��0:��־�ļ��в����������־��Ϣ,Ĭ��ֵΪ:1
log_send_msg = 1


#�澯���ݿ�������Ϣ,����dp�����ݿ���Ϣ
[dp_server]
# dsn ����Դ
dsn = dp
# �û���
user = root
# ����
password = dbplus03

#�澯���ͽӿ����Ͳ���
[send_interface_tcp]
#�����ip
serverip=192.168.1.107
#����˶˿�
serverport=6800

[send_interface_db]
#���ݿ�ӿ�����
