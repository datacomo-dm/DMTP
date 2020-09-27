#ifndef TBLOADER_JIN_DEF_H
#define TBLOADER_JIN_DEF_H

/********************************************************************
  file : tbloader_jni.h
  desc : define the tbloader java call interface
  author: liujianshun,201304
  note: support multiple thread use
  libname:libtbloader.so
********************************************************************/
#include <jni.h>

#ifdef __cplusplus  
extern "C" {  
#endif 

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
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeStorer_onInit(JNIEnv *env,jobject obj);

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
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeStorer_onUninit(JNIEnv *env,jobject obj);


/*
    ����: getStorerID
    ����: ��ȡ���еĴ洢ID
    ����: 
          env[in]   : jni �����ӿ�ָ��
          obj[in]   : jni �������������Ҫ�е�
    ����ֵ:
         -1: ʧ��û�п��п��õ�ID
         �ɹ���[0-50]: ���õ�ID
    ǰ������:onInit���óɹ�
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeStorer_getStorerID(JNIEnv *env,jobject obj);

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
    ǰ������:
          1. odbc ����Դ��������
          2. dbname.tbname Ҫ�����
          3. getStorerID
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeStorer_initStorerForDB(JNIEnv *env,jobject obj,jint storerid,
                          jstring dsn,jstring user,jstring pwd,jstring dbname,jstring tbname,
                          jint engine,jstring logpth); 

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
          1: �Ѿ���ʼ�����������ظ�����
          2: �ļ���ʧ��
    ǰ������:
          1. odbc ����Դ��������
          2. dbname.tbname Ҫ�����
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeStorer_initStorerForFile(JNIEnv *env,jobject obj,jint storerid,
                           jstring fname,jstring logpth); 

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
          3: δ��ʼ�� initStorerForFile ���� initStorerForDB
		  4: ����ͷ���ݽ�������
		  5: ����ͷ��Ŀ���ṹ��һ��
    ǰ������:
          1. initStorerForFile ���óɹ� ���� initStorerForDB ���óɹ�
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeStorer_writeHeadInfo(JNIEnv *env,jobject obj,jint storerid,
                          jint headerlen,jbyteArray header);


/*
    ����: writeDataBlock
    ����: д����ͷ����
    ����: 
          env[in]   : jni �����ӿ�ָ��
          obj[in]   : jni �������������Ҫ�е�
          storerid[in] : storerID
          blocklen[in]   : �������ݳ���
          datablock[in]: ��������
          rowNum[in]: ���ݰ��м�¼����
    ����ֵ:
          0: �ɹ�
          -1: �ļ�д��ʧ��
          1: �ļ�ͷδд���,writeHeadInfoδ���óɹ���
          2: ���ݳ���У��ʧ��
          3: δ��ʼ�� initStorerForFile ���� initStorerForDB
		  4: ���ݰ�����
    ǰ������:
          1. initStorerForFile ���óɹ� ���� initStorerForDB ���óɹ�
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeStorer_writeDataBlock(JNIEnv *env,jobject obj,jint storerid,
                          jint blocklen,jbyteArray datablock,jint rowNum);

/*
    ����: getDataMD5
    ����: ��ȡ���ݵ�md5У��ֵ
    ����: 
          env[in]   : jni �����ӿ�ָ��
          obj[in]   : jni �������������Ҫ�е�
          storerid[in] : storerID
    ����ֵ:
          32���ַ�������md5У��ֵ
    ˵��:
          �ڵ���closeStorer֮ǰ����
*/
JNIEXPORT jstring JNICALL Java_com_datamerger_file_NativeStorer_getDataMD5(JNIEnv *env,jobject obj,jint storerid);


/*
    ����: closeStorer
    ����: �ͷ�tbloader�ӿ�
    ����: 
          env[in]   : jni �����ӿ�ָ��
          obj[in]   : jni �������������Ҫ�е�
          storerid[in] : storerID
    ����ֵ:
          0: �ɹ�     
          1: ���ݿ��ļ�д��ʧ��
          2: �����ļ�д��ʧ��
          3: δ����initStorerForFile����initStorerForDB
    ǰ������:
          1. initStorerForFile ���óɹ� ���� initStorerForDB ���óɹ�
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeStorer_closeStorer(JNIEnv *env,jobject obj,jint storerid);



//--------------------------------------------------------------------------------------------------------
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
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeCSV_onInit(JNIEnv *env,jobject obj);

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
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeCSV_onUninit(JNIEnv *env,jobject obj);

/*
    ����: getCsvID
    ����: ��ȡ���е�CSV����ID
    ����: 
          env[in]   : jni �����ӿ�ָ��
          obj[in]   : jni �������������Ҫ�е�
    ����ֵ:
         -1: ʧ��û�п��п��õ�ID
         �ɹ���[0-50]: ���õ�ID
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeCSV_getCsvID(JNIEnv *env,jobject obj);

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
                          jstring filepath,jstring dsn ,jstring user,jstring pwd,jstring SQL,jstring logpth); 

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
JNIEXPORT jboolean JNICALL Java_com_datamerger_file_NativeCSV_writeCSV(JNIEnv *env,jobject obj,jint csvID);

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
JNIEXPORT jstring  JNICALL Java_com_datamerger_file_NativeCSV_stateCSV(JNIEnv *env,jobject obj,jint csvID);

/*
    ����: closeCsv
    ����: �ͷ�csv�ӿ�
    ����: 
          env[in]   : jni �����ӿ�ָ��
          obj[in]   : jni �������������Ҫ�е�
          csvID[in] : csvID
    ����ֵ:
          0: �ɹ�  
          -1: ��Ч��ID
    ǰ������:
          1. getCsvID
*/
JNIEXPORT jint JNICALL Java_com_datamerger_file_NativeCSV_closeCsv(JNIEnv *env,jobject obj,jint csvID);


#ifdef __cplusplus  
}
#endif 

#endif
