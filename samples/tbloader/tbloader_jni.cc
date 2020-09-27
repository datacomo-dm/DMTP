#include <string.h>
#include <stdio.h>
#include "tbloader_jni.h"
#include "tbloader_helper.h"
#include "tbexport_helper.h"

#ifdef DEBUG
#define Dbg_printf printf
#else
#define Dbg_printf 
#endif

#define MAX_LOADER_NUM   50

// �������ݶ���
pthread_mutex_t     g_loaderlock;         // ��
bool                g_loader_init_flag = false;
tbloader_helper     g_loader[MAX_LOADER_NUM];  // loader



/*
    ����: onInit
    ����: ��ʼ���⣬������Դ(�����Ƶ�)
    ����: 
          env[in]   : jni �����ӿ�ָ��
          obj[in]   : jni �������������Ҫ�е�
    ����ֵ:
         0: �ɹ�
         -1:ʧ��
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeStorer_onInit(JNIEnv *env,jobject obj)
{
    printf("NativeStorer_onInit...\n");
    if(g_loader_init_flag == true){
    	return (jint)0;
    }
    pthread_mutex_init(&g_loaderlock,NULL);
    if(g_loader_init_flag == true){
        pthread_mutex_unlock(&g_loaderlock);
    	return (jint)0;
    }
    pthread_mutex_lock(&g_loaderlock);
    for(int i=0;i<MAX_LOADER_NUM;i++)
    {
       g_loader[i].setId(-1);
       g_loader[i].setRunFlag(false);
    }
    g_loader_init_flag = true;
    pthread_mutex_unlock(&g_loaderlock);
    printf("NativeStorer_onInit ok.\n");
    return (jint)0;
}

/*
    ����: onUninit
    ����: �ͷ����������Դ(�����Ƶ�)
    ����: 
          env[in]   : jni �����ӿ�ָ��
          obj[in]   : jni �������������Ҫ�е�
    ����ֵ:
         0: �ɹ�
         -1:ʧ��
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeStorer_onUninit(JNIEnv *env,jobject obj)
{
    printf("NativeStorer_onUninit...\n");
    int retcode = 0;
    if(g_loader_init_flag == false){
       return (jint)0;
    }  	
    pthread_mutex_lock(&g_loaderlock);
    if(g_loader_init_flag == false){
       pthread_mutex_unlock(&g_loaderlock);
       return (jint)0;
    } 
    for(int i=0;i<MAX_LOADER_NUM;i++)
    {
       if(g_loader[i].getRunFlag()){
          g_loader[i].stop();
          g_loader[i].setRunFlag(false);
       }
       g_loader[i].setId(-1);
    }
    pthread_mutex_unlock(&g_loaderlock);
    pthread_mutex_destroy(&g_loaderlock);
    g_loader_init_flag = false;
    printf("NativeStorer_onUninit ok.\n");
    return (jint)retcode;   		
}

/*
    ����: getStorerID
    ����: ��ȡ���еĴ洢ID
    ����: 
          env[in]   : jni �����ӿ�ָ��
          obj[in]   : jni �������������Ҫ�е�
    ����ֵ:
         -1: ʧ��û�п��п��õ�ID
         [0-50]: ���õ�ID
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeStorer_getStorerID(JNIEnv *env,jobject obj)
{
    int storerid=-1;
    pthread_mutex_lock(&g_loaderlock);
    for(int i=0;i<MAX_LOADER_NUM;i++)
    {
        if(!g_loader[i].getRunFlag())  
        {
            storerid = i;
            g_loader[i].setRunFlag(true);
            g_loader[i].setId(i);
            break;
        }
    }
    pthread_mutex_unlock(&g_loaderlock);
    
    printf("id=[%d],getStorerID storerid = %d\n",g_loader[storerid].getId(),storerid);
    return (jint)storerid;
}

/*
    ����: initStorerForDB
    ����: ��ʼ���ݴ洢����
    ����: 
          env[in]   : jni �����ӿ�ָ��
          obj[in]   : jni �������������Ҫ�е�
          storerid[in] : storerID
          dsn[in]   : odbc �������ݿ�����Դ
          user[in]  : odbc ����Դ�������ݿ���û�����
          pwd[in]   : odbc ����Դ�������ݿ���û�����
          dbname[in]: ���ݿ�ʵ������
          tbname[in]: ���ݿ������
          logpth[in]: ��־���·��
          engine[in]: ���ݱ�����<1:MyISAM> <2:brighthouse>
    ����ֵ:
          0: �ɹ�
          -1: ���ݿ�����ʧ�� 
          1: �Ѿ���ʼ�����������ظ�����
          2: ��Ч��storerid
    ǰ������:
          1. odbc ����Դ��������
          2. dbname.tbname Ҫ�����
          3. getStorerID
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeStorer_initStorerForDB(JNIEnv *env,jobject obj,jint storerid,
                          jstring dsn,jstring user,jstring pwd,jstring dbname,jstring tbname,
                          jint engine,jstring logpth)
{
    int _storerid = storerid;
    if(_storerid > MAX_LOADER_NUM || _storerid < 0){
        printf("initStorerForDB storerid = %d error.\n",_storerid);
        return 3;
    }
    
    if (g_loader[_storerid].getStatus()!= status_unknown &&  g_loader[_storerid].getStatus() != hasstoped)
    {
        printf("initStorerForDB getStatus() = %d error.\n",g_loader[_storerid].getStatus());
        return 1;
    }
               
    _InitInfo st;
    strcpy(st.dsn,(char *)env->GetStringUTFChars(dsn, 0));
    strcpy(st.user,(char *)env->GetStringUTFChars(user, 0));
    strcpy(st.pwd,(char *)env->GetStringUTFChars(pwd, 0));
    strcpy(st.dbname,(char *)env->GetStringUTFChars(dbname, 0));
    strcpy(st.tbname,(char *)env->GetStringUTFChars(tbname, 0));
    int _engine = (int)engine;
    st.tbEngine = (_EngineType)_engine;
    const char* logpath = (char *)env->GetStringUTFChars(logpth, 0);   
    if(st.tbEngine != MyISAM)
    {
       printf("table %s.%s engine %d error,engine<1:MyISAM>.\n",st.dbname,st.tbname,st.tbEngine);
       return 5;
    }
    
    printf("id=[%d],initStorerForDB params [storerid = %d,dsn = %s,user = %s pwd = %s,dbname = %s,tbname = %s,engine = %d,logpth=%s]\n",
           g_loader[storerid].getId(),storerid,st.dsn,st.user,st.pwd,st.dbname,st.tbname,st.tbEngine,logpath);
    
    int ret = 0;
    pthread_mutex_lock(&g_loaderlock);
    ret = g_loader[_storerid].start(&st,insert_db,logpath);	
    pthread_mutex_unlock(&g_loaderlock);
    return ret;    
}

/*
    ����: initStorerForFile
    ����: ��ʼ���ݴ洢����
    ����: 
          env[in]   : jni �����ӿ�ָ��
          obj[in]   : jni �������������Ҫ�е�
          storerid[in] : storerID
          logpth[in]: ��־���·��
          fname[in]: ����·���ļ�����
    ����ֵ:
          0: �ɹ�
          -1: ���ݿ�����ʧ�� 
          1: �Ѿ���ʼ�����������ظ�����
          2: �ļ���ʧ��
          3: ��Ч��storerid
    ǰ������:
          1. odbc ����Դ��������
          2. dbname.tbname Ҫ�����
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeStorer_initStorerForFile(JNIEnv *env,jobject obj,jint storerid,
                           jstring fname,jstring logpth)
{
    int _storerid = storerid;
    if(_storerid > MAX_LOADER_NUM || _storerid < 0){
        printf("initStorerForFile storerid = %d error.\n",_storerid);
        return 3;
    }
    
    if (g_loader[_storerid].getStatus()!= status_unknown && g_loader[_storerid].getStatus() != hasstoped)
    {
        printf("initStorerForFile getStatus() = %d error.\n",g_loader[_storerid].getStatus());
        return 1;
    }
    
    _InitInfo st;
    strcpy(st.fn_ext,(char *)env->GetStringUTFChars(fname, 0));
    const char* logpath = (char *)env->GetStringUTFChars(logpth, 0);
    
    printf("id=[%d],initStorerForDB params [storerid = %d,filename = %s,logpth=%s]\n",
           g_loader[storerid].getId(),storerid,st.fn_ext,logpath);
           
    return g_loader[_storerid].start(&st,write_files,logpath);	
}

/*
    ����: writeHeadInfo
    ����: д����ͷ����
    ����: 
          env[in]   : jni �����ӿ�ָ��
          obj[in]   : jni �������������Ҫ�е�
          storerid[in] : storerID
          headerlen[in]   : �ļ���ͷ�����ݳ���,4�ֽ�
          header[in]: �ļ���ͷ������
    ����ֵ:
          0: �ɹ�
          -1: �ļ�д��ʧ��
          1: �ļ�ͷ�����ظ�����д��
          2: ���ݳ���У��ʧ��
          3: ��Ч��storerid
          4: δ��ʼ�� initStorerForFile ���� initStorerForDB
		  5: ����ͷ���ݽ�������
		  6: ����ͷ��Ŀ���ṹ��һ��
    ǰ������:
          1. initStorerForFile ���óɹ� ���� initStorerForDB ���óɹ�
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeStorer_writeHeadInfo(JNIEnv *env,jobject obj,jint storerid,
                          jint headerlen,jbyteArray header)
{
    int _storerid = storerid;
    if(_storerid > MAX_LOADER_NUM || _storerid < 0){
        printf("writeHeadInfo storerid = %d error.\n",_storerid);
        return 3;
    }
    
    if (g_loader[_storerid].getStatus()>= parserheader){
        printf("writeHeadInfo getStatus() = %d error.\n",g_loader[_storerid].getStatus());
        return 1;
    }
    if(g_loader[_storerid].getStatus()< hasstarted){
        printf("writeHeadInfo getStatus() = %d error.\n",g_loader[_storerid].getStatus());
        return 4;
    }
    printf("id=[%d],writeHeadInfo storerid = %d,headerlen = %d\n",g_loader[_storerid].getId(),_storerid,headerlen);
    
    jbyte * olddata = (jbyte*)env->GetByteArrayElements(header, 0);
    jint  oldsize = env->GetArrayLength(header);
    unsigned char* bytearr = (unsigned char*)olddata;
    int len = (int)oldsize;
    int _headerlen = (int)headerlen;
    if(len != (int)_headerlen){
    	
        #ifdef FREE_MEM       
        env->ReleaseByteArrayElements(header,(jbyte*)olddata,0);
        //env->DeleteLocalRef((jobject)olddata);
        #endif 
        printf("writeHeadInfo len [ %d != %d] error \n",_headerlen,len);
        return 2;
    }
      
    int ret = g_loader[_storerid].parserColumnInfo((const HerderPtr)bytearr,_headerlen);
    
    #ifdef FREE_MEM       
    env->ReleaseByteArrayElements(header,(jbyte*)olddata,0);
    //env->DeleteLocalRef((jobject)olddata);
    #endif
    
    return (jint)ret;
}

/*
    ����: writeDataBlock
    ����: д����ͷ����
    ����: 
          env[in]   : jni �����ӿ�ָ��
          obj[in]   : jni �������������Ҫ�е�
          storerid[in] : storerID
          blocklen[in]   : �������ݳ���
          datablock[in]: ��������
    ����ֵ:
          0: �ɹ�
          -1: �ļ�д��ʧ��
          1: �ļ�ͷδд���,tl_writeHeadInfoδ���óɹ���
          2: ���ݳ���У��ʧ��
          3: ��Ч��storerid
          4: �Ѿ��ر�״̬��������д������
		  5: ���ݰ�����
    ǰ������:
          1. initStorerForFile ���óɹ� ���� initStorerForDB ���óɹ�
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeStorer_writeDataBlock(JNIEnv *env,jobject obj,jint storerid,
                          jint blocklen,jbyteArray datablock,jint rowNum)
{
    int _storerid = storerid;
    if(_storerid > MAX_LOADER_NUM || _storerid < 0){
        printf("writeDataBlock storerid = %d\n error.",_storerid);
        return 3;
    }
    
    if (g_loader[_storerid].getStatus()< parserheader){
        printf("writeDataBlock getStatus() = %d error.\n",g_loader[_storerid].getStatus());
        return 1;
    }
    if(g_loader[_storerid].getStatus() == hasstoped){
        printf("writeDataBlock getStatus() = %d error.\n",g_loader[_storerid].getStatus());
        return 4;
    }
    if(g_loader[_storerid].getOperType() == write_files && rowNum<=0)
	{
        printf("writeDataBlock getOperType() == write_files && rowNum<=0.\n");
        return 6;
    }
    Dbg_printf("id=[%d],writeDataBlock storerid = %d,blocklen = %d,rowNum = %d.\n",g_loader[_storerid].getId(),_storerid,blocklen,rowNum);
    
    jbyte * olddata = (jbyte*)env->GetByteArrayElements(datablock, 0);
    jint  oldsize = env->GetArrayLength(datablock);
    unsigned char* bytearr = (unsigned char*)olddata;
    int len = (int)oldsize;
    int _blocklen = (int)blocklen;
    if(len != (int)_blocklen){
        printf("closeStorer block len [%d != %d] error = %d \n",_blocklen,blocklen);
        
        #ifdef FREE_MEM       
        env->ReleaseByteArrayElements(datablock,(jbyte*)olddata,0);
        //env->DeleteLocalRef((jobject)olddata);
        #endif
        
        return 2;
    }
    
    int ret = g_loader[_storerid].writePackData((const char*)bytearr,_blocklen,rowNum);
    
    #ifdef FREE_MEM       
    env->ReleaseByteArrayElements(datablock,(jbyte*)olddata,0);
    //env->DeleteLocalRef((jobject)olddata);
    #endif
    
    return (jint)ret;
}

/*
    ����: getDataMD5
    ����: ��ȡ���ݵ�md5У��ֵ
    ����: 
          env[in]   : jni �����ӿ�ָ��
          obj[in]   : jni �������������Ҫ�е�
          storerid[in] : storerID
          hasdata[in]   : �Ƿ�д�������
    ����ֵ:
          32���ַ�������md5У��ֵ
    ˵��:
          �ڵ���closeStorer֮ǰ����
*/
JNIEXPORT jstring JNICALL Java_com_datamerger_file_NativeStorer_getDataMD5(JNIEnv *env,jobject obj,jint storerid)
{
    int retcode = 0;
    int _storerid = storerid;
    char md5sum[conMd5StrLen] = {0};
    
    jstring rtn;
    printf("getDataMD5 storeid = %d\n",_storerid);
    // ��Чid��0000000
    if(_storerid > MAX_LOADER_NUM || _storerid < 0){
        memset(md5sum,0,conMd5StrLen);
        rtn = env->NewStringUTF(md5sum);
        printf("getDataMD5 md5sum = %s \n",md5sum);
        return rtn;
    }
    
    // ״̬����,111111
    if(g_loader[_storerid].getStatus() != writedata)
    {
        memset(md5sum,1,conMd5StrLen);
        rtn = env->NewStringUTF(md5sum);
        printf("getDataMD5 md5sum = %s \n",md5sum);
        return rtn;
    }
    
    g_loader[_storerid].getMd5Sum(md5sum);
    printf("id=[%d],getDataMD5 md5sum = %s \n",g_loader[storerid].getId(),md5sum);
    rtn = env->NewStringUTF(md5sum);
    return rtn;
}

/*
    ����: closeStorer
    ����: �ͷ�tbloader�ӿ�
    ����: 
          env[in]   : jni �����ӿ�ָ��
          obj[in]   : jni �������������Ҫ�е�
          storerid[in] : storerID
          hasdata[in]   : �Ƿ�д�������
    ����ֵ:
          code_md5sum<32�ֽڣ�����DataBlock��md5����У��ֵ(writeDataBlock����)>
          codeֵ:
          0: �ɹ�     
          1: ���ݿ��ļ�д��ʧ��
          2: �����ļ�д��ʧ��
          3: δ����initStorerForFile����initStorerForDB
          4: storerid����
    ǰ������:
          1. initStorerForFile ���óɹ� ���� initStorerForDB ���óɹ�
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeStorer_closeStorer(JNIEnv *env,jobject obj,jint storerid)
{
    int retcode = 0;
    int _storerid = storerid;
    if(_storerid > MAX_LOADER_NUM || _storerid < 0){
        printf("closeStorer storerid = %d \n",_storerid);
        return 4;
    }
   
    printf("id=[%d],closeStorer storerid = %d \n",g_loader[storerid].getId(),_storerid);
    retcode = g_loader[_storerid].stop();
    g_loader[_storerid].setRunFlag(false);
    g_loader[_storerid].setId(-1);    
    return (jint)retcode;
}



//--------------------------------------------------------------------------------------------------------
pthread_mutex_t     g_exporterlock;         // ��
bool                g_exporter_init_flag = false;
tbexport            g_exporter[MAX_LOADER_NUM];  // exporter

/*
    ����: onInit
    ����: ��ʼ���⣬������Դ(�����Ƶ�)
    ����: 
          env[in]   : jni �����ӿ�ָ��
          obj[in]   : jni �������������Ҫ�е�
    ����ֵ:
         0: �ɹ�
         -1:ʧ��
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeCSV_onInit(JNIEnv *env,jobject obj)
{
    printf("NativeCSV_onInit...\n");
    if(g_exporter_init_flag == true){
    	return (jint)0;
    }
    pthread_mutex_init(&g_exporterlock,NULL);
    pthread_mutex_lock(&g_exporterlock);
    if(g_exporter_init_flag == true){
        pthread_mutex_unlock(&g_exporterlock);
    	return (jint)0;
    }
    for(int i=0;i<MAX_LOADER_NUM;i++)
    {
       g_exporter[i].setId(-1);
       g_exporter[i].setRunFlag(false);
    }
    pthread_mutex_unlock(&g_exporterlock);
    g_exporter_init_flag = true;    
    printf("NativeCSV_onInit ok.\n");
    return (jint)0;
}

/*
    ����: onUninit
    ����: �ͷ����������Դ(�����Ƶ�)
    ����: 
          env[in]   : jni �����ӿ�ָ��
          obj[in]   : jni �������������Ҫ�е�
    ����ֵ:
         0: �ɹ�
         -1:ʧ��
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeCSV_onUninit(JNIEnv *env,jobject obj)
{
    printf("NativeCSV_onUninit...\n");
	if(g_exporter_init_flag == false){
    	return (jint)0;
    }
    pthread_mutex_lock(&g_exporterlock);
	if(g_exporter_init_flag == false){
        pthread_mutex_unlock(&g_exporterlock);
    	return (jint)0;
    }
    int retcode = 0;
    for(int i=0;i<MAX_LOADER_NUM;i++)
    {
       if(g_exporter[i].getRunFlag()){
          g_exporter[i].stop();
          g_exporter[i].setRunFlag(false);
       }
       g_exporter[i].setId(-1);
    }       
    pthread_mutex_unlock(&g_exporterlock);
    pthread_mutex_destroy(&g_exporterlock);
    g_exporter_init_flag = false;
    printf("NativeCSV_onUninit ok.\n");

    return (jint)retcode;   
}

/*
    ����: getCsvID
    ����: ��ȡ���е�CSV����ID
    ����: 
          evn[in]   : jni �����ӿ�ָ��
          obj[in]   : jni �������������Ҫ�е�
    ����ֵ:
         -1: ʧ��û�п��п��õ�ID
         [0-50]: ���õ�ID
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeCSV_getCsvID(JNIEnv *env,jobject obj)
{    
    int storerid ;
    pthread_mutex_lock(&g_exporterlock);
    for(int i=0;i<MAX_LOADER_NUM;i++)
    {
        if(!g_exporter[i].getRunFlag())  
        {
            storerid = i;
            g_exporter[i].setRunFlag(true);
            g_exporter[i].setId(i);
            break;
        }
    }
    pthread_mutex_unlock(&g_exporterlock);
    printf("getCsvID storerid = %d\n",storerid);
    return (jint)storerid;

}

/*
    ����: initCsv
    ����: ��ʼ����csv�ļ����ݽӿ�
    ����: 
          env[in]   : jni �����ӿ�ָ��
          obj[in]   : jni �������������Ҫ�е�
          csvID[in] : csvID
          filepath[in]:csv �ļ�����
          dsn[in]   : odbc �������ݿ�����Դ
          user[in]  : odbc ����Դ�������ݿ���û�����
          pwd[in]   : odbc ����Դ�������ݿ���û�����
          SQL[in]   : sql���
          logpth[in]: ��־���·��
    ����ֵ:
          0: �ɹ�
          -1: ���ݿ�����ʧ�� 
          1: �Ѿ���ʼ�����������ظ�����
          2: ��ЧID
    ǰ������:
          1. odbc ����Դ��������
          2. getCsvID
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeCSV_initCsv(JNIEnv *env,jobject obj,jint csvID,
                          jstring filepath,jstring dsn ,jstring user,jstring pwd,jstring SQL,jstring logpth)
{
    int ret = 0;
    
    int _csvID = (int)csvID;
    if(_csvID > MAX_LOADER_NUM || _csvID < 0){  	  
        printf("initCsv csvID = %d error.\n",_csvID);
        return 2;
    }
    
    if (g_exporter[_csvID].getStatus()!= exp_unstart && g_exporter[_csvID].getStatus() != exp_hasstoped)
    {
        printf("initCsv getStatus() = %d\n",g_exporter[_csvID].getStatus());
        return 1;
    }
               
    _InitInfo st;
    strcpy(st.dsn,(char *)env->GetStringUTFChars(dsn, 0));
    strcpy(st.user,(char *)env->GetStringUTFChars(user, 0));
    strcpy(st.pwd,(char *)env->GetStringUTFChars(pwd, 0));
    strcpy(st.fn_ext,(char *)env->GetStringUTFChars( pwd, 0));
    const char* logpath = (char *)env->GetStringUTFChars(filepath, 0);
    const char* strsql = (char*)env->GetStringUTFChars(SQL, 0);
    
    printf("initStorerForDB params [storerid = %d,dsn = %s,user = %s pwd = %s,dbname = %s,logpth=%s]\n",
           _csvID,st.dsn,st.user,st.pwd,logpath);
           
    pthread_mutex_lock(&g_exporterlock);
    ret = g_exporter[_csvID].start(&st,strsql,logpath);
    pthread_mutex_unlock(&g_exporterlock);
    
    return (jint)ret;
}

/*
    ����: writeCSV
    ����: дcsv�ļ��ӿ�
    ����: 
          env[in]   : jni �����ӿ�ָ��
          obj[in]   : jni �������������Ҫ�е�
          csvID[in] : csvID
    ����ֵ:
          true : �ɹ�
          false: ʧ��
    ǰ������:
          1. initCsv
    ˵��:�ñ�������첽ִ�еģ��ڲ������߳�ִ��
*/
JNIEXPORT jboolean JNICALL Java_com_datamerger_file_NativeCSV_writeCSV(JNIEnv *env,jobject obj,jint csvID)
{
    int ret = 0;
    
    int _csvID = (int)csvID;
    if(_csvID > MAX_LOADER_NUM || _csvID < 0){	  
        printf("initCsv csvID = %d error.\n",_csvID);
        return false;
    }
    printf("writeCSV csvid = %d.",csvID);  
    ret = g_exporter[_csvID].doStart();
    return (jboolean)(ret == 0 ? true:false);  
}

/*
    ����: stateCSV
    ����: дcsv�ļ��ӿ�
    ����: 
          env[in]   : jni �����ӿ�ָ��
          obj[in]   : jni �������������Ҫ�е�
          csvID[in] : csvID
    ����ֵ: ������_��ǰд���¼��_�����ܼ�¼��
        ������ȡֵ:
        0: �ɹ�  <0_��ǰд���¼��_�����ܼ�¼��>
        1: ʧ�ܣ�д������ʧ�� <1_��ǰд���¼��_�����ܼ�¼��>              
    ǰ������:
          1. initCsv
    ˵��:�ñ�������첽ִ�еģ��ڲ������߳�ִ��
*/
JNIEXPORT jstring  JNICALL Java_com_datamerger_file_NativeCSV_stateCSV(JNIEnv *env,jobject obj,jint csvID)
{
    char rtnValue[128] = {0};
    jstring rtn;
    
    int _csvID = (int)csvID;
    
    if(_csvID > MAX_LOADER_NUM || _csvID < 0){	  
        printf("stateCSV csvID = %d error.\n",_csvID);
        strcpy(rtnValue,"1_0_0");        
        rtn = env->NewStringUTF(rtnValue);
        return rtn;
    }    
    sprintf(rtnValue,"0_%d_%d",g_exporter[_csvID].getCurrentRows(),g_exporter[_csvID].getRowSum());
    Dbg_printf("stateCSV return : %s.\n",rtnValue);
    rtn = env->NewStringUTF(rtnValue);
    return rtn;
}

/*
    ����: closeCsv
    ����: �ͷ�csv�ӿ�
    ����: 
          env[in]   : jni �����ӿ�ָ��
          obj[in]   : jni �������������Ҫ�е�
          csvID[in] : csvID
    ����ֵ:
          0: �ɹ�  
          1: ��Ч��ID
    ǰ������:
          1. getCsvID
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeCSV_closeCsv(JNIEnv *env,jobject obj,jint csvID)
{
    int ret = 1;
    int _csvID = (int)csvID;
    if(_csvID > MAX_LOADER_NUM || _csvID < 0){
        printf("closeCsv csvID = %d error.\n",_csvID);
        return (jint)ret;
    }
    ret = g_exporter[_csvID].stop();
    g_exporter[_csvID].setId(-1);
    printf("closeCsv csvid = %d.\n",csvID);
   
    return (jint)0;
}


