#ifndef _WDBI_INC_H
#define _WDBI_INC_H
#include "wdbi_common.h"
#ifdef __unix
#define getch getchar
#endif
#include <stdio.h>
#include "wdbierr.h"

//////////////////////////////////////////////////////////////////
// Common function (5). 
//
//
//////////////////////////////////////////////////////////////////

#ifndef WOCI_DEBUG
#define wociAppendRowsWithNF(mt,pData,rnnum)					_wdbiAppendRowsWithNF(mt,pData,rnnum)
#define wociExportSomeRowsWithNF(mt,pData,startrn,rnnum)					_wdbiExportSomeRowsWithNF(mt,pData,startrn,rnnum)
#define wociSetNull(mt,col,rown,setnull)						_wdbiSetNull(mt,col,rown,setnull)
#define wociIsNull(mt,col,rown)							  _wdbiIsNull(mt,col,rown)
#define wociIsFixedMySQLBlock(memtab)                                             _wdbiIsFixedMySQLBlock(memtab)
#define wociDefineLongByPos(stmt,pos,ptr)                                             _wdbiDefineLongByPos(stmt,pos,ptr)
#define wociBindLongByPos(stmt,pos,ptr)                                             _wdbiBindLongByPos(stmt,pos,ptr)
#define wociSetDefaultPrec(prec,scale)                                             _wdbiSetDefaultPrec(prec,scale)
#define wociMainEntrance(func,asyncmode,ptr,catchLevel)				   _wdbiMainEntrance(func,asyncmode,ptr,catchLevel)				  
#define wociMainEntranceEx(func,asyncmode,ptr,catchLevel,opt)				   _wdbiMainEntranceEx(func,asyncmode,ptr,catchLevel,opt)				  
#define wocidestroy(handle)                                                        _wdbidestroy(handle)                                                      
#define wociDestroyAll()                                                           _wdbiDestroyAll()                                                         
#define wociBreakAll()                                                             _wdbiBreakAll()                                                           
#define wociCreateSession(username,password,svcname,dbtype)                        _wdbiCreateSession(username,password,svcname,dbtype)                             
#define wociCommit(sess)                                                           _wdbiCommit(sess)                                                         
#define wociRollback(sess)                                                         _wdbiRollback(sess)                                                       
#define wociSetTerminate(sess,val)                                                 _wdbiSetTerminate(sess,val)                                               
#define wociIsTerminate(sess)                                                      _wdbiIsTerminate(sess)                                                    
#define wociSetNonBlockMode(sess)                                                  _wdbiSetNonBlockMode(sess)                                                
#define wociCreateStatment(sess)                                                   _wdbiCreateStatment(sess)                                                 
#define wociBindStrByPos(stmt,pos,ptr,len)                                         _wdbiBindStrByPos(stmt,pos,ptr,len)                                       
#define wociBindDoubleByPos(stmt,pos,ptr)                                          _wdbiBindDoubleByPos(stmt,pos,ptr)                                        
#define wociBindIntByPos(stmt,pos,ptr)                                             _wdbiBindIntByPos(stmt,pos,ptr)                                           
#define wociBindDateByPos(stmt,pos,ptr)                                            _wdbiBindDateByPos(stmt,pos,ptr)                                          
#define wociDefineStrByPos(stmt,pos,ptr,len)                                       _wdbiDefineStrByPos(stmt,pos,ptr,len)                                     
#define wociDefineDoubleByPos(stmt,pos,ptr)                                        _wdbiDefineDoubleByPos(stmt,pos,ptr)                                      
#define wociDefineIntByPos(stmt,pos,ptr)                                           _wdbiDefineIntByPos(stmt,pos,ptr)                                         
#define wociDefineDateByPos(stmt,pos,ptr)                                          _wdbiDefineDateByPos(stmt,pos,ptr)                                        
#define wociBreakAndReset(stmt)                                                    _wdbiBreakAndReset(stmt)                                                  
#define wociExecute(stmt,times)                                                    _wdbiExecute(stmt,times)                                                  
#define wociExecuteAt(stmt,times,offset)                                           _wdbiExecuteAt(stmt,times,offset)                                         
#define wociFetch(stmt,rows)                                                       _wdbiFetch(stmt,rows)                                                     
#define wociGetStmtColumnsNum(stmt)                                                _wdbiGetStmtColumnsNum(stmt)                                              
#define wociGetMtColumnsNum(mt)                                                    _wdbiGetMtColumnsNum(mt)                                                  
#define wociGetFetchedRows(stmt)                                                   _wdbiGetFetchedRows(stmt)                                                 
#define wociGetSession(stmt)                                                       _wdbiGetSession(stmt)                                                     
#define wociGetStmtType(stmt)                                                      _wdbiGetStmtType(stmt)                                                    
#define wociPrepareStmt(stmt,sqlstmt)                                              _wdbiPrepareStmt(stmt,sqlstmt)                                            
#define wociGetStmtColumnNum(stmt)                                                 _wdbiGetStmtColumnNum(stmt)
#define wociGetStmtColumnName(stmt,col,colname)                                    _wdbiGetStmtColumnName(stmt,col,colname)
#define wociGetStmtColumnType(stmt,col)                                            _wdbiGetStmtColumnType(stmt,col)
#define wociGetCreateTableSQL(memtab,buf,tabname,ismysql)                          _wdbiGetCreateTableSQL(memtab,buf,tabname,ismysql)                        
#define wociCreateMemTable()                                                       _wdbiCreateMemTable()                                                     
#define wociAddColumn(memtab,name,dspname,ctype,length,scale)                      _wdbiAddColumn(memtab,name,dspname,ctype,length,scale)                    
#define wociBuildStmt(memtab,stmt,rows)                                            _wdbiBuildStmt(memtab,stmt,rows)                                          
#define wociBuild(memtab,rows)                                                     _wdbiBuild(memtab,rows)                                                   
#define wociClear(memtab)                                                          _wdbiClear(memtab)                                                        
#define wociAppendToDbTable(memtab,tablename,sess,autocommit)                      _wdbiAppendToDbTable(memtab,tablename,sess,autocommit)                    
#define wociAppendToDbTableWithColName(memtab,tablename,sess,autocommit)           _wdbiAppendToDbTableWithColName(memtab,tablename,sess,autocommit)                    
#define wociFetchAll(memtab)                                                       _wdbiFetchAll(memtab)                                                     
#define wociFetchFirst(memtab,rows)                                                _wdbiFetchFirst(memtab,rows)                                              
#define wociFetchAt(memtab,rows,st)                                                _wdbiFetchAt(memtab,rows,st)                                              
#define wociFetchNext(memtab,rows)                                                 _wdbiFetchNext(memtab,rows)                                               
#define wociGeneTable(memtab,tablename,sess)                                       _wdbiGeneTable(memtab,tablename,sess)                                     
#define wociSetMTName(memtab,mtname)                                               _wdbiSetMTName(memtab,mtname)                                             
#define wociGetStrAddrByName(memtab,col,rowst,pstr,celllen)                        _wdbiGetStrAddrByName(memtab,col,rowst,pstr,celllen)                      
#define wociGetDateAddrByName(memtab,col,rowst,pstr)                               _wdbiGetDateAddrByName(memtab,col,rowst,pstr)                             
#define wociGetDoubleAddrByName(memtab,col,rowst,pstr)                             _wdbiGetDoubleAddrByName(memtab,col,rowst,pstr)                           
#define wociGetIntAddrByName(memtab,col,rowst,pstr)                                _wdbiGetIntAddrByName(memtab,col,rowst,pstr)                              
#define wociGetLongAddrByName(memtab,col,rowst,pstr)                                _wdbiGetLongAddrByName(memtab,col,rowst,pstr)                              
#define wociGetStrValByName(memtab,col,rowst,pstr)                                 _wdbiGetStrValByName(memtab,col,rowst,pstr)                               
#define wociGetDateValByName(memtab,col,rowst,pstr)                                _wdbiGetDateValByName(memtab,col,rowst,pstr)                              
#define wociGetDoubleValByName(memtab,col,rowst)                                   _wdbiGetDoubleValByName(memtab,col,rowst)                                 
#define wociGetIntValByName(memtab,col,rowst)                                      _wdbiGetIntValByName(memtab,col,rowst)                                    
#define wociGetLongValByName(memtab,col,rowst)                                      _wdbiGetLongValByName(memtab,col,rowst)                                    
#define wociGetStrAddrByPos(memtab,col,rowst,pstr,celllen)                         _wdbiGetStrAddrByPos(memtab,col,rowst,pstr,celllen)                       
#define wociGetDateAddrByPos(memtab,col,rowst,pstr)                                _wdbiGetDateAddrByPos(memtab,col,rowst,pstr)                              
#define wociGetDoubleAddrByPos(memtab,col,rowst,pstr)                              _wdbiGetDoubleAddrByPos(memtab,col,rowst,pstr)                            
#define wociGetIntAddrByPos(memtab,col,rowst,pstr)                                 _wdbiGetIntAddrByPos(memtab,col,rowst,pstr)                               
#define wociGetLongAddrByPos(memtab,col,rowst,pstr)                                 _wdbiGetLongAddrByPos(memtab,col,rowst,pstr)                               
#define wociGetBufferLen(memtab)                                                   _wdbiGetBufferLen(memtab)                                                 
#define wociGetCell(memtab,row,col,str,rawpos)                                     _wdbiGetCell(memtab,row,col,str,rawpos)                                   
#define wociGetColumnDisplayWidth(memtab,col)                                      _wdbiGetColumnDisplayWidth(memtab,col)                                    
#define wociGetColumnPosByName(memtab,colname)                                     _wdbiGetColumnPosByName(memtab,colname)                                   
#define wociGetColumnName(memtab,id,colname)                                       _wdbiGetColumnName(memtab,id,colname)                                     
#define wociSetColumnName(memtab,id,colname)                                       _wdbiSetColumnName(memtab,id,colname)                                     
#define wociGetColumnDataLenByPos(memtab,colid)                                    _wdbiGetColumnDataLenByPos(memtab,colid)                                  
#define wociGetColumnTypeByName(memtab,colsnm,ctype)                               _wdbiGetColumnTypeByName(memtab,colsnm,ctype)                                          
#define wociGetSortColType(memtab,ctype)                                           _wdbiGetSortColType(memtab,ctype)                                          
#define wociGetColumnNumber(memtab)                                                _wdbiGetColumnNumber(memtab)                                              
#define wociGetColumnScale(memtab,colid)                                           _wdbiGetColumnScale(memtab,colid)                                         
#define wociGetColumnTitle(memtab,colid,str,len)                                   _wdbiGetColumnTitle(memtab,colid,str,len)                                 
#define wociGetColumnType(memtab,colid)                                            _wdbiGetColumnType(memtab,colid)                                          
#define wociGetLine(memtab,row,str,rawpos,colsnm)                             _wdbiGetLine(memtab,row,str,rawpos,colsnm,NULL)                                
// ��ȡ�������ݣ������ַ���add by lirawposujs
#define wociGetLineStr(memtab,row,str,rawpos,colsnm)                               _wdbiGetLineStr(memtab,row,str,rawpos,colsnm,NULL)    
#define wociGetCompactLen(memtab,rowstart,rownum,colsnm,clen)			   _wdbiGetCompactLen(memtab,rowstart,rownum,colsnm,clen)
#define wociGetCompactLine(memtab,row,str,rawpos,colsnm,clen)                      _wdbiGetLine(memtab,row,str,rawpos,colsnm,clen)                               
#define wociGetCompactTitle(memtab,str,len,colsnm,clen)                            _wdbiGetTitle(memtab,str,len,colsnm,clen)                                      
#define wociGetMemtableRows(memtab)                                                _wdbiGetMemtableRows(memtab)                                              
#define wociGetTitle(memtab,str,len,colsnm)                                        _wdbiGetTitle(memtab,str,len,colsnm,NULL)                                      
#define wociIsIKSet(memtab)                                                        _wdbiIsIKSet(memtab)                                                      
#define wociOrderByIK(memtab)                                                      _wdbiOrderByIK(memtab)                                                    
#define wociSearchIK(memtab,key)                                                   _wdbiSearchIK(memtab,key)                                                 
#define wociSearchIKLE(memtab,key)                                                 _wdbiSearchIKLE(memtab,key)                                               
#define wociSetColumnDisplayName(memtab,colnm,str)                                 _wdbiSetColumnDisplayName(memtab,colnm,str)                               
#define wociSetIKByName(memtab,str)                                                _wdbiSetIKByName(memtab,str)                                              
#define wociSetSortColumn(memtab,colsnm)                                           _wdbiSetSortColumn(memtab,colsnm)                                         
#define wociReInOrder(memtab)                                                      _wdbiReInOrder(memtab)                                                    
#define wociSetStrValues(memtab,colid,rowstart,rownum,bf)                          _wdbiSetStrValues(memtab,colid,rowstart,rownum,bf)                        
#define wociSetDateValues(memtab,colid,rowstart,rownum,bf)                         _wdbiSetDateValues(memtab,colid,rowstart,rownum,bf)                       
#define wociSetDoubleValues(memtab,colid,rowstart,rownum,bf)                       _wdbiSetDoubleValues(memtab,colid,rowstart,rownum,bf)                     
#define wociSetIntValues(memtab,colid,rowstart,rownum,bf)                          _wdbiSetIntValues(memtab,colid,rowstart,rownum,bf)                       
#define wociSetLongValues(memtab,colid,rowstart,rownum,bf)                         _wdbiSetLongValues(memtab,colid,rowstart,rownum,bf)                       
#define wociSort(memtab)                                                           _wdbiSort(memtab)                                                         
#define wociSortHeap(memtab)                                                       _wdbiSortHeap(memtab)                                                     
#define wociSetGroupSrc(memtab,src)                                                _wdbiSetGroupSrc(memtab,src)                                              
#define wociSetIKGroupRef(memtab,ref,colnm)                                        _wdbiSetIKGroupRef(memtab,ref,colnm)                                      
#define wociSetGroupSrcCol(memtab,colsnm)                                          _wdbiSetGroupSrcCol(memtab,colsnm)                                        
#define wociSetGroupRefCol(memtab,colsnm)                                          _wdbiSetGroupRefCol(memtab,colsnm)                                        
#define wociSetSrcSumCol(memtab,colsnm)                                            _wdbiSetSrcSumCol(memtab,colsnm)                                          
#define wociGroup(memtab,rowstart,rownum)                                          _wdbiGroup(memtab,rowstart,rownum)                                        
#define wociCopyRowsTo(memtab,memtabto,toStart,start,rowsnum)                      _wdbiCopyRowsTo(memtab,memtabto,toStart,start,rowsnum)                    
#define wociCopyRowsToNoCut(memtab,memtabto,toStart,start,rowsnum)                 _wdbiCopyRowsToNoCut(memtab,memtabto,toStart,start,rowsnum)                    
#define wociGetColumnDesc(memtab,pColDesc,cdlen,_colnm)                            _wdbiGetColumnDesc(memtab,pColDesc,cdlen,_colnm)                          
#define wociCopyColumnDefine(memtab,memtabfrom,colsname)                           _wdbiCopyColumnDefine(memtab,memtabfrom,colsname)                         
#define wociExportSomeRows(memtab,pData,startrn,rnnum)                             _wdbiExportSomeRows(memtab,pData,startrn,rnnum)                           
#define wociExport(memtab,pData,_len,_maxrows,_rowct)                              _wdbiExport(memtab,pData,_len,_maxrows,_rowct)                            
#define wociImport(memtab,pData,_len,pColDesc,_colnm,_cdlen,_maxrows,_rowct)       _wdbiImport(memtab,pData,_len,pColDesc,_colnm,_cdlen,_maxrows,_rowct)     
#define wociAppendRows(memtab,pData,rnnum)                                         _wdbiAppendRows(memtab,pData,rnnum)                                       
#define wociReset(memtab)                                                          _wdbiReset(memtab)                                                        
#define wociClearIK(memtab)                                                        _wdbiClearIK(memtab)                                                      
#define wociClearSort(memtab)                                                      _wdbiClearSort(memtab)                                                    
#define wociGetMaxRows(memtab)                                                     _wdbiGetMaxRows(memtab)                                                   
#define wociCompact(memtab)                                                        _wdbiCompact(memtab)                                                      
#define wociGetRowLen(memta)                                                       _wdbiGetRowLen(memta)                                                     
#define wociCreateExcelEnv()                                                       _wdbiCreateExcelEnv()                                                     
#define wociSetDir(excel,strTemplate,strReport)                                    _wdbiSetDir(excel,strTemplate,strReport)                                  
#define wociSetMemTable(excel,memtab)                                              _wdbiSetMemTable(excel,memtab)                                            
#define wociLoadTemplate(excel,tempname)                                           _wdbiLoadTemplate(excel,tempname)                                         
#define wociSelectSheet(excel,sheetname)                                           _wdbiSelectSheet(excel,sheetname)                                         
#define wociFillData(excel,tocol, torow, fromcol, fromrow, colnum, rownum,rawpos)  _wdbiFillData(excel,tocol, torow, fromcol, fromrow, colnum, rownum,rawpos)
#define wociFillTitle(excel,tocol,torow,fromcol,colnum)                            _wdbiFillTitle(excel,tocol,torow,fromcol,colnum)                          
#define wociSaveAs(excel,filename)                                                 _wdbiSaveAs(excel,filename)                                               
#define wociGetMemUsed(memtab)                                                     _wdbiGetMemUsed(memtab)                                                   
#define wociSetSortedGroupRef(memtab,mtref, colssrc)                               _wdbiSetSortedGroupRef(memtab,mtref, colssrc)                             
#define wociSearch(memtab,ptr)                                                     _wdbiSearch(memtab,ptr)                                                   
#define wociSearchLE(memtab,ptr)                                                   _wdbiSearchLE(memtab,ptr)                                                 
#define wociValuesSet(memtab,mtsrc,colto,colfrom,usedestKey,op)                    _wdbiValuesSet(memtab,mtsrc,colto,colfrom,usedestKey,op)                  
#define wociDeleteRow(memtab,rown)                                                 _wdbiDeleteRow(memtab,rown)                                                  
#define wociInsertRows(memtab,ptr,colsname,num)                                    _wdbiInsertRows(memtab,ptr,colsname,num)                                     
#define wociBindToStatment(memtab,stmt,colsname,rowst)                             _wdbiBindToStatment(memtab,stmt,colsname,rowst)                              
#define wociTestTable(sess,tablename)                                              _wdbiTestTable(sess,tablename)                                               
#define wociGetRawrnBySort(memtab,ind)                                             _wdbiGetRawrnBySort(memtab,ind)                                              
#define wociGetRawrnByIK(memtab,ind)                                               _wdbiGetRawrnByIK(memtab,ind)                                                
#define wociCompressBf(memtab)                                                     _wdbiCompressBf(memtab)                                                      
#define wociSearchQDel(memtab,key,schopt)                                          _wdbiSearchQDel(memtab,key,schopt)                                           
#define wociIsQDelete(memtab,rownm)                                                _wdbiIsQDelete(memtab,rownm)                                                 
#define wociQDeletedRows(memtab)                                                   _wdbiQDeletedRows(memtab)                                                    
#define wociQDeleteRow(memtab,rownm)                                               _wdbiQDeleteRow(memtab,rownm)                                                
#define wociWaitStmt(stmt)                                                         _wdbiWaitStmt(stmt,INFINITE)                                                          
#define wociWaitMemtable(memtab)                                                   _wdbiWaitMemtable(memtab,INFINITE)                                                    
#define wociWaitLastReturn(handle)                                                 _wdbiWaitLastReturn(handle)                                                  
#define wociWaitTime(handle,time)                                                  _wdbiWaitTime(handle,time)                                                   
#define wociFreshRowNum(memtab)                                                    _wdbiFreshRowNum(memtab)                                                     
#define wociBatchSelect(result,param,colsnm)                                       _wdbiBatchSelect(result,param,colsnm)                                        
#define wociMTPrint(memtab,rownm,colsnm)                                           _wdbiMTPrint(memtab,rownm,colsnm,false)                                            
#define wociMTCompactPrint(memtab,rownm,colsnm)                                    _wdbiMTPrint(memtab,rownm,colsnm,true)                                            
#define wociGetCurDateTime(date)                                                   _wdbiGetCurDateTime(date)                                                    
#define wociSetDateTime(date,year,mon,day,hour,min,sec)                            _wdbiSetDateTime(date,year,mon,day,hour,min,sec)                             
#define wociSetDate(date,year,mon,day)                                             _wdbiSetDate(date,year,mon,day)                                              
#define wociDateTimeToStr(date,str)                                                _wdbiDateTimeToStr(date,str)                                                 
#define wociGetYear(date)                                                          _wdbiGetYear(date)                                                           
#define wociGetMonth(date)                                                         _wdbiGetMonth(date)                                                          
#define wociGetDay(date)                                                           _wdbiGetDay(date)                                                            
#define wociGetHour(date)                                                          _wdbiGetHour(date)                                                           
#define wociGetMin(date)                                                           _wdbiGetMin(date)                                                            
#define wociGetSec(date)                                                           _wdbiGetSec(date)                                                            
#define wociSetEcho(val)                                                           _wdbiSetEcho(val)                                                            
#define wociIsEcho()                                                           	   _wdbiIsEcho()                                                            
// �����ı��ļ�*.cvs,add by liujs 
#define wociMTToTextFileStr(memtab,fn,rownm,colsnm)                                _wdbiMTToTextFileStr(memtab,fn,rownm,colsnm)    
#define wociMTToTextFile(memtab,fn,rownm,colsnm)                                   _wdbiMTToTextFile(memtab,fn,rownm,colsnm)                                    
#define wociGetMTName(memtab,bf)                                                   _wdbiGetMTName(memtab,bf)                                                    
#define wociReadFromTextFile(memtab,fn,rowst,rownm)                                _wdbiReadFromTextFile(memtab,fn,rowst,rownm)                                 
#define wociFetchAllAt(memtab,st)                                                  _wdbiFetchAllAt(memtab,st)                                                   
#define wociDateDiff(d1,d2)                                                        _wdbiDateDiff(d1,d2)                                                         
#define wociSetOutputToConsole(val)                                                _wdbiSetOutputToConsole(val)                                                 
#define wociSetTraceFile(fn)                                             	   _wdbiSetTraceFile(fn)                                              
#define wociSetErrorFile(fn)                                                       _wdbiSetErrorFile(fn)                                                        
#define wociSetLogFile(fn)                                                         _wdbiSetLogFile(fn)                                                          
#define wociGetErrorFile()                                                         _wdbiGetErrorFile()                                                        
#define wociGetLogFile()                                                           _wdbiGetLogFile()                                                          
#define WOCIInit(appname)                                                          _WDBIInit(appname)                                                           
#define WOCIQuit()                                                                 _WDBIQuit()                                                                  
#define wociReplaceStmt(memtab,stmt)                                               _wdbiReplaceStmt(memtab,stmt)                                                
#define wociConvertColStrToInt(memtab,colsn,pcol)                                  _wdbiConvertColStrToInt(memtab,colsn,pcol)                                   
#define wociSaveSort(memtab,fp)                                                    _wdbiSaveSort(memtab,fp)                                                     
#define wociCalculate(memtab,colnm,op)                                             _wdbiCalculate(memtab,colnm,op)                                                     
#define wociLoadSort(memtab,fp)                                                    _wdbiLoadSort(memtab,fp)                                                     
#define wociCopyToMySQL(memtab,startrow,rownum,fp)                                 _wdbiCopyToMySQL(memtab,startrow,rownum,fp)                                  
#define wociCompareSortRow(memtab,row1, row2)                                      _wdbiCompareSortRow(memtab,row1, row2)                                       
#define wociList(pout)                                                             _wdbiList(pout)                                                              
#define wociGetLastError(handle)                                                   _wdbiGetLastError(handle)                                                    
#define wociAddrFresh(memtab,colval,collen,tp)                                     _wdbiAddrFresh(memtab,colval,collen,tp)                                      
#define wociGetColumnInfo(mt,crttab,ismysql)                               	   _wdbiGetColumnInfo(mt,crttab,ismysql)                                      
#define wociGetFetchSize(stmt)							   _wdbiGetFetchSize(stmt)
#define wociSetFetchSize(stmt,fsize)						   _wdbiSetFetchSize(stmt,fsize)
#define wociReverseByteOrder(mt)						   _wdbiReverseByteOrder(mt)
#define wociReverseCD(mt)						   	   _wdbiReverseCD(mt)
#define wociSetColumnDisplay(mt,cols,colid,dspname,scale,prec)		   	   _wdbiSetColumnDisplay(mt,cols,colid,dspname,scale,prec)
#else
#define LOGWHERE _wdbilogwhere(__FILE__,__LINE__)
#define wociAppendRowsWithNF(mt,pData,rnnum)					(LOGWHERE,_wdbiAppendRowsWithNF(mt,pData,rnnum))
#define wociExportSomeRowsWithNF(mt,pData,startrn,rnnum)				(LOGWHERE,_wdbiExportSomeRowsWithNF(mt,pData,startrn,rnnum))
#define wociSetNull(mt,col,rown,setnull)					(LOGWHERE,_wdbiSetNull(mt,col,rown,setnull))
#define wociIsNull(mt,col,rown)							  (LOGWHERE,  _wdbiIsNull(mt,col,rown))
#define wociDefineLongByPos(stmt,pos,ptr)                                          (LOGWHERE,   _wdbiDefineLongByPos(stmt,pos,ptr)       )
#define wociBindLongByPos(stmt,pos,ptr)                                          (LOGWHERE,   _wdbiBindLongByPos(stmt,pos,ptr)       )
#define wociIsFixedMySQLBlock(memtab)                                             (LOGWHERE,_wdbiIsFixedMySQLBlock(memtab)																	)
#define wociSetDefaultPrec(prec,scale)                                             (LOGWHERE,_wdbiSetDefaultPrec(prec,scale)						  )
#define wociMainEntrance(func,asyncmode,ptr,catchLevel)				   (LOGWHERE,_wdbiMainEntrance(func,asyncmode,ptr,catchLevel)				  )
#define wociMainEntranceEx(func,asyncmode,ptr,catchLevel,opt)				   (LOGWHERE,_wdbiMainEntranceEx(func,asyncmode,ptr,catchLevel,opt)				  )
#define wocidestroy(handle)                                                        (LOGWHERE,_wdbidestroy(handle)                                                         )                            
#define wociDestroyAll()                                                           (LOGWHERE,_wdbiDestroyAll()                                                            )
#define wociBreakAll()                                                             (LOGWHERE,_wdbiBreakAll()                                                              )
#define wociCreateSession(username,password,svcname,dbtype)                        (LOGWHERE,_wdbiCreateSession(username,password,svcname,dbtype)                         )
#define wociCommit(sess)                                                           (LOGWHERE,_wdbiCommit(sess)                                                            )
#define wociRollback(sess)                                                         (LOGWHERE,_wdbiRollback(sess)                                                          )
#define wociSetTerminate(sess,val)                                                 (LOGWHERE,_wdbiSetTerminate(sess,val)                                                  )
#define wociIsTerminate(sess)                                                      (LOGWHERE,_wdbiIsTerminate(sess)                                                       )
#define wociSetNonBlockMode(sess)                                                  (LOGWHERE,_wdbiSetNonBlockMode(sess)                                                   )
#define wociCreateStatment(sess)                                                   (LOGWHERE,_wdbiCreateStatment(sess)                                                    )
#define wociBindStrByPos(stmt,pos,ptr,len)                                         (LOGWHERE,_wdbiBindStrByPos(stmt,pos,ptr,len)                                          )
#define wociBindDoubleByPos(stmt,pos,ptr)                                          (LOGWHERE,_wdbiBindDoubleByPos(stmt,pos,ptr)                                           )
#define wociBindIntByPos(stmt,pos,ptr)                                             (LOGWHERE,_wdbiBindIntByPos(stmt,pos,ptr)                                              )
#define wociBindDateByPos(stmt,pos,ptr)                                            (LOGWHERE,_wdbiBindDateByPos(stmt,pos,ptr)                                             )
#define wociDefineStrByPos(stmt,pos,ptr,len)                                       (LOGWHERE,_wdbiDefineStrByPos(stmt,pos,ptr,len)                                        )
#define wociDefineDoubleByPos(stmt,pos,ptr)                                        (LOGWHERE,_wdbiDefineDoubleByPos(stmt,pos,ptr)                                         )
#define wociDefineIntByPos(stmt,pos,ptr)                                           (LOGWHERE,_wdbiDefineIntByPos(stmt,pos,ptr)                                            )
#define wociDefineDateByPos(stmt,pos,ptr)                                          (LOGWHERE,_wdbiDefineDateByPos(stmt,pos,ptr)                                           )
#define wociBreakAndReset(stmt)                                                    (LOGWHERE,_wdbiBreakAndReset(stmt)                                                     )
#define wociExecute(stmt,times)                                                    (LOGWHERE,_wdbiExecute(stmt,times)                                                     )
#define wociExecuteAt(stmt,times,offset)                                           (LOGWHERE,_wdbiExecuteAt(stmt,times,offset)                                            )
#define wociFetch(stmt,rows)                                                       (LOGWHERE,_wdbiFetch(stmt,rows)                                                        )
#define wociGetStmtColumnsNum(stmt)                                                (LOGWHERE,_wdbiGetStmtColumnsNum(stmt)                                                 )
#define wociGetMtColumnsNum(mt)                                                    (LOGWHERE,_wdbiGetMtColumnsNum(mt)                                                     )
#define wociGetFetchedRows(stmt)                                                   (LOGWHERE,_wdbiGetFetchedRows(stmt)                                                    )
#define wociGetSession(stmt)                                                       (LOGWHERE,_wdbiGetSession(stmt)                                                        )
#define wociGetStmtType(stmt)                                                      (LOGWHERE,_wdbiGetStmtType(stmt)                                                       )
#define wociPrepareStmt(stmt,sqlstmt)                                              (LOGWHERE,_wdbiPrepareStmt(stmt,sqlstmt)                                               )
#define wociGetStmtColumnNum(stmt)                                                 (LOGWHERE,_wdbiGetStmtColumnNum(stmt)                                                  )
#define wociGetStmtColumnName(stmt,col,colname)                                    (LOGWHERE,_wdbiGetStmtColumnName(stmt,col,colname)                                     )
#define wociGetStmtColumnType(stmt,col)                                            (LOGWHERE,_wdbiGetStmtColumnType(stmt,col)                                             )
#define wociGetCreateTableSQL(memtab,buf,tabname,ismysql)                          (LOGWHERE,_wdbiGetCreateTableSQL(memtab,buf,tabname,ismysql)                           )
#define wociCreateMemTable()                                                       (LOGWHERE,_wdbiCreateMemTable()                                                        )
#define wociAddColumn(memtab,name,dspname,ctype,length,scale)                      (LOGWHERE,_wdbiAddColumn(memtab,name,dspname,ctype,length,scale)                       )
#define wociBuildStmt(memtab,stmt,rows)                                            (LOGWHERE,_wdbiBuildStmt(memtab,stmt,rows)                                             )
#define wociBuild(memtab,rows)                                                     (LOGWHERE,_wdbiBuild(memtab,rows)                                                      )
#define wociClear(memtab)                                                          (LOGWHERE,_wdbiClear(memtab)                                                           )
#define wociAppendToDbTable(memtab,tablename,sess,autocommit)                      (LOGWHERE,_wdbiAppendToDbTable(memtab,tablename,sess,autocommit)                       )
#define wociAppendToDbTableWithColName(memtab,tablename,sess,autocommit)                      (LOGWHERE,_wdbiAppendToDbTableWithColName(memtab,tablename,sess,autocommit)                       )
#define wociFetchAll(memtab)                                                       (LOGWHERE,_wdbiFetchAll(memtab)                                                        )
#define wociFetchFirst(memtab,rows)                                                (LOGWHERE,_wdbiFetchFirst(memtab,rows)                                                 )
#define wociFetchAt(memtab,rows,st)                                                (LOGWHERE,_wdbiFetchAt(memtab,rows,st)                                                 )
#define wociFetchNext(memtab,rows)                                                 (LOGWHERE,_wdbiFetchNext(memtab,rows)                                                  )
#define wociGeneTable(memtab,tablename,sess)                                       (LOGWHERE,_wdbiGeneTable(memtab,tablename,sess)                                        )
#define wociSetMTName(memtab,mtname)                                               (LOGWHERE,_wdbiSetMTName(memtab,mtname)                                                )
#define wociGetStrAddrByName(memtab,col,rowst,pstr,celllen)                        (LOGWHERE,_wdbiGetStrAddrByName(memtab,col,rowst,pstr,celllen)                         )
#define wociGetDateAddrByName(memtab,col,rowst,pstr)                               (LOGWHERE,_wdbiGetDateAddrByName(memtab,col,rowst,pstr)                                )
#define wociGetDoubleAddrByName(memtab,col,rowst,pstr)                             (LOGWHERE,_wdbiGetDoubleAddrByName(memtab,col,rowst,pstr)                              )
#define wociGetIntAddrByName(memtab,col,rowst,pstr)                                (LOGWHERE,_wdbiGetIntAddrByName(memtab,col,rowst,pstr)                                 )
#define wociGetLongAddrByName(memtab,col,rowst,pstr)                                (LOGWHERE,_wdbiGetLongAddrByName(memtab,col,rowst,pstr)                                 )
#define wociGetStrValByName(memtab,col,rowst,pstr)                                 (LOGWHERE,_wdbiGetStrValByName(memtab,col,rowst,pstr)                                  )
#define wociGetDateValByName(memtab,col,rowst,pstr)                                (LOGWHERE,_wdbiGetDateValByName(memtab,col,rowst,pstr)                                 )
#define wociGetDoubleValByName(memtab,col,rowst)                                   (LOGWHERE,_wdbiGetDoubleValByName(memtab,col,rowst)                                    )
#define wociGetIntValByName(memtab,col,rowst)                                      (LOGWHERE,_wdbiGetIntValByName(memtab,col,rowst)                                       )
#define wociGetLongValByName(memtab,col,rowst)                                      (LOGWHERE,_wdbiGetLongValByName(memtab,col,rowst)                                     )
#define wociGetStrAddrByPos(memtab,col,rowst,pstr,celllen)                         (LOGWHERE,_wdbiGetStrAddrByPos(memtab,col,rowst,pstr,celllen)                          )
#define wociGetDateAddrByPos(memtab,col,rowst,pstr)                                (LOGWHERE,_wdbiGetDateAddrByPos(memtab,col,rowst,pstr)                                 )
#define wociGetDoubleAddrByPos(memtab,col,rowst,pstr)                              (LOGWHERE,_wdbiGetDoubleAddrByPos(memtab,col,rowst,pstr)                               )
#define wociGetIntAddrByPos(memtab,col,rowst,pstr)                                 (LOGWHERE,_wdbiGetIntAddrByPos(memtab,col,rowst,pstr)                                  )
#define wociGetLongAddrByPos(memtab,col,rowst,pstr)                                 (LOGWHERE,_wdbiGetLongAddrByPos(memtab,col,rowst,pstr)                                )
#define wociGetBufferLen(memtab)                                                   (LOGWHERE,_wdbiGetBufferLen(memtab)                                                    )
#define wociGetCell(memtab,row,col,str,rawpos)                                     (LOGWHERE,_wdbiGetCell(memtab,row,col,str,rawpos)                                      )
#define wociGetColumnDisplayWidth(memtab,col)                                      (LOGWHERE,_wdbiGetColumnDisplayWidth(memtab,col)                                       )
#define wociGetColumnPosByName(memtab,colname)                                     (LOGWHERE,_wdbiGetColumnPosByName(memtab,colname)                                      )
#define wociGetColumnTypeByName(memtab,colsnm,ctype)                               (LOGWHERE,_wdbiGetColumnTypeByName(memtab,colsnm,ctype))                                          
#define wociGetSortColType(memtab,ctype)                                           (LOGWHERE,_wdbiGetSortColType(memtab,ctype))                                          
#define wociGetColumnName(memtab,id,colname)                                       (LOGWHERE,_wdbiGetColumnName(memtab,id,colname)                                        )
#define wociSetColumnName(memtab,id,colname)                                       (LOGWHERE,_wdbiSetColumnName(memtab,id,colname)                                        )
#define wociGetColumnDataLenByPos(memtab,colid)                                    (LOGWHERE,_wdbiGetColumnDataLenByPos(memtab,colid)                                     )
#define wociGetColumnNumber(memtab)                                                (LOGWHERE,_wdbiGetColumnNumber(memtab)                                                 )
#define wociGetColumnScale(memtab,colid)                                           (LOGWHERE,_wdbiGetColumnScale(memtab,colid)                                            )
#define wociGetColumnTitle(memtab,colid,str,len)                                   (LOGWHERE,_wdbiGetColumnTitle(memtab,colid,str,len)                                    )
#define wociGetColumnType(memtab,colid)                                            (LOGWHERE,_wdbiGetColumnType(memtab,colid)                                             )
#define wociGetLine(memtab,row,str,rawpos,colsnm)                             (LOGWHERE,_wdbiGetLine(memtab,row,str,rawpos,colsnm,NULL)                              )
// ��ȡ�������ݣ������ַ�����add by liujs
#define wociGetLineStr(memtab,row,str,rawpos,colsnm)                               (LOGWHERE,_wdbiGetLineStr(memtab,row,str,rawpos,colsnm,NULL)                           )      
#define wociGetCompactLine(memtab,row,str,rawpos,colsnm,clen)                      (LOGWHERE,_wdbiGetLine(memtab,row,str,rawpos,colsnm,clen)                       ) 
#define wociGetCompactLen(memtab,row,str,rawpos,colsnm,clen)                       (LOGWHERE,_wdbiGetCompactLen(memtab,row,str,rawpos,colsnm,clen)                        )       
#define wociGetCompactTitle(memtab,str,len,colsnm,clen)                            (LOGWHERE,_wdbiGetTitle(memtab,str,len,colsnm,clen)                                    )  
#define wociGetMemtableRows(memtab)                                                (LOGWHERE,_wdbiGetMemtableRows(memtab)                                                 )
#define wociGetTitle(memtab,str,len,colsnm)                                        (LOGWHERE,_wdbiGetTitle(memtab,str,len,colsnm,NULL)                                         )
#define wociIsIKSet(memtab)                                                        (LOGWHERE,_wdbiIsIKSet(memtab)                                                         )
#define wociOrderByIK(memtab)                                                      (LOGWHERE,_wdbiOrderByIK(memtab)                                                       )
#define wociSearchIK(memtab,key)                                                   (LOGWHERE,_wdbiSearchIK(memtab,key)                                                    )
#define wociSearchIKLE(memtab,key)                                                 (LOGWHERE,_wdbiSearchIKLE(memtab,key)                                                  )
#define wociSetColumnDisplayName(memtab,colnm,str)                                 (LOGWHERE,_wdbiSetColumnDisplayName(memtab,colnm,str)                                  )
#define wociSetIKByName(memtab,str)                                                (LOGWHERE,_wdbiSetIKByName(memtab,str)                                                 )
#define wociSetSortColumn(memtab,colsnm)                                           (LOGWHERE,_wdbiSetSortColumn(memtab,colsnm)                                            )
#define wociReInOrder(memtab)                                                      (LOGWHERE,_wdbiReInOrder(memtab)                                                       )
#define wociSetStrValues(memtab,colid,rowstart,rownum,bf)                          (LOGWHERE,_wdbiSetStrValues(memtab,colid,rowstart,rownum,bf)                           )
#define wociSetDateValues(memtab,colid,rowstart,rownum,bf)                         (LOGWHERE,_wdbiSetDateValues(memtab,colid,rowstart,rownum,bf)                          )
#define wociSetDoubleValues(memtab,colid,rowstart,rownum,bf)                       (LOGWHERE,_wdbiSetDoubleValues(memtab,colid,rowstart,rownum,bf)                        )
#define wociSetIntValues(memtab,colid,rowstart,rownum,bf)                         (LOGWHERE,_wdbiSetIntValues(memtab,colid,rowstart,rownum,bf)                            )
#define wociSetLongValues(memtab,colid,rowstart,rownum,bf)                         (LOGWHERE,_wdbiSetLongValues(memtab,colid,rowstart,rownum,bf)                          )
#define wociSort(memtab)                                                           (LOGWHERE,_wdbiSort(memtab)                                                            )
#define wociSortHeap(memtab)                                                       (LOGWHERE,_wdbiSortHeap(memtab)                                                        )
#define wociSetGroupSrc(memtab,src)                                                (LOGWHERE,_wdbiSetGroupSrc(memtab,src)                                                 )
#define wociSetIKGroupRef(memtab,ref,colnm)                                        (LOGWHERE,_wdbiSetIKGroupRef(memtab,ref,colnm)                                         )
#define wociSetGroupSrcCol(memtab,colsnm)                                          (LOGWHERE,_wdbiSetGroupSrcCol(memtab,colsnm)                                           )
#define wociSetGroupRefCol(memtab,colsnm)                                          (LOGWHERE,_wdbiSetGroupRefCol(memtab,colsnm)                                           )
#define wociSetSrcSumCol(memtab,colsnm)                                            (LOGWHERE,_wdbiSetSrcSumCol(memtab,colsnm)                                             )
#define wociGroup(memtab,rowstart,rownum)                                          (LOGWHERE,_wdbiGroup(memtab,rowstart,rownum)                                           )
#define wociCopyRowsTo(memtab,memtabto,toStart,start,rowsnum)                      (LOGWHERE,_wdbiCopyRowsTo(memtab,memtabto,toStart,start,rowsnum)                       )
#define wociCopyRowsToNoCut(memtab,memtabto,toStart,start,rowsnum)                 (LOGWHERE,_wdbiCopyRowsToNoCut(memtab,memtabto,toStart,start,rowsnum)                       )
#define wociGetColumnDesc(memtab,pColDesc,cdlen,_colnm)                            (LOGWHERE,_wdbiGetColumnDesc(memtab,pColDesc,cdlen,_colnm)                             )
#define wociCopyColumnDefine(memtab,memtabfrom,colsname)                           (LOGWHERE,_wdbiCopyColumnDefine(memtab,memtabfrom,colsname)                            )
#define wociExportSomeRows(memtab,pData,startrn,rnnum)                             (LOGWHERE,_wdbiExportSomeRows(memtab,pData,startrn,rnnum)                              )
#define wociExport(memtab,pData,_len,_maxrows,_rowct)                              (LOGWHERE,_wdbiExport(memtab,pData,_len,_maxrows,_rowct)                               )
#define wociImport(memtab,pData,_len,pColDesc,_colnm,_cdlen,_maxrows,_rowct)       (LOGWHERE,_wdbiImport(memtab,pData,_len,pColDesc,_colnm,_cdlen,_maxrows,_rowct)        )
#define wociAppendRows(memtab,pData,rnnum)                                         (LOGWHERE,_wdbiAppendRows(memtab,pData,rnnum)                                          )
#define wociReset(memtab)                                                          (LOGWHERE,_wdbiReset(memtab)                                                           )
#define wociClearIK(memtab)                                                        (LOGWHERE,_wdbiClearIK(memtab)                                                         )
#define wociClearSort(memtab)                                                      (LOGWHERE,_wdbiClearSort(memtab)                                                       )
#define wociGetMaxRows(memtab)                                                     (LOGWHERE,_wdbiGetMaxRows(memtab)                                                      )
#define wociCompact(memtab)                                                        (LOGWHERE,_wdbiCompact(memtab)                                                         )
#define wociGetRowLen(memta)                                                       (LOGWHERE,_wdbiGetRowLen(memta)                                                        )
#define wociCreateExcelEnv()                                                       (LOGWHERE,_wdbiCreateExcelEnv()                                                        )
#define wociSetDir(excel,strTemplate,strReport)                                    (LOGWHERE,_wdbiSetDir(excel,strTemplate,strReport)                                     )
#define wociSetMemTable(excel,memtab)                                              (LOGWHERE,_wdbiSetMemTable(excel,memtab)                                               )
#define wociLoadTemplate(excel,tempname)                                           (LOGWHERE,_wdbiLoadTemplate(excel,tempname)                                            )
#define wociSelectSheet(excel,sheetname)                                           (LOGWHERE,_wdbiSelectSheet(excel,sheetname)                                            )
#define wociFillData(excel,tocol, torow, fromcol, fromrow, colnum, rownum,rawpos)  (LOGWHERE,_wdbiFillData(excel,tocol, torow, fromcol, fromrow, colnum, rownum,rawpos)   )
#define wociFillTitle(excel,tocol,torow,fromcol,colnum)                            (LOGWHERE,_wdbiFillTitle(excel,tocol,torow,fromcol,colnum)                             )
#define wociSaveAs(excel,filename)                                                 (LOGWHERE,_wdbiSaveAs(excel,filename)                                                  )
#define wociGetMemUsed(memtab)                                                     (LOGWHERE,_wdbiGetMemUsed(memtab)                                                      )
#define wociSetSortedGroupRef(memtab,mtref, colssrc)                               (LOGWHERE,_wdbiSetSortedGroupRef(memtab,mtref, colssrc)                                )
#define wociSearch(memtab,ptr)                                                     (LOGWHERE,_wdbiSearch(memtab,ptr)                                                      )
#define wociSearchLE(memtab,ptr)                                                   (LOGWHERE,_wdbiSearchLE(memtab,ptr)                                                    )
#define wociValuesSet(memtab,mtsrc,colto,colfrom,usedestKey,op)                    (LOGWHERE,_wdbiValuesSet(memtab,mtsrc,colto,colfrom,usedestKey,op)                     )
#define wociDeleteRow(memtab,rown)                                                 (LOGWHERE,_wdbiDeleteRow(memtab,rown)                                                  )
#define wociInsertRows(memtab,ptr,colsname,num)                                    (LOGWHERE,_wdbiInsertRows(memtab,ptr,colsname,num)                                     )
#define wociBindToStatment(memtab,stmt,colsname,rowst)                             (LOGWHERE,_wdbiBindToStatment(memtab,stmt,colsname,rowst)                              )
#define wociTestTable(sess,tablename)                                              (LOGWHERE,_wdbiTestTable(sess,tablename)                                               )
#define wociGetRawrnBySort(memtab,ind)                                             (LOGWHERE,_wdbiGetRawrnBySort(memtab,ind)                                              )
#define wociGetRawrnByIK(memtab,ind)                                               (LOGWHERE,_wdbiGetRawrnByIK(memtab,ind)                                                )
#define wociCompressBf(memtab)                                                     (LOGWHERE,_wdbiCompressBf(memtab)                                                      )
#define wociSearchQDel(memtab,key,schopt)                                          (LOGWHERE,_wdbiSearchQDel(memtab,key,schopt)                                           )
#define wociIsQDelete(memtab,rownm)                                                (LOGWHERE,_wdbiIsQDelete(memtab,rownm)                                                 )
#define wociQDeletedRows(memtab)                                                   (LOGWHERE,_wdbiQDeletedRows(memtab)                                                    )
#define wociQDeleteRow(memtab,rownm)                                               (LOGWHERE,_wdbiQDeleteRow(memtab,rownm)                                                )
#define wociWaitStmt(stmt)                                                         (LOGWHERE,_wdbiWaitStmt(stmt,INFINITE)                                                          )
#define wociWaitMemtable(memtab)                                                   (LOGWHERE,_wdbiWaitMemtable(memtab,INFINITE)                                                    )
#define wociWaitLastReturn(handle)                                                 (LOGWHERE,_wdbiWaitLastReturn(handle)                                                  )
#define wociWaitTime(handle,time)                                                  (LOGWHERE,_wdbiWaitTime(handle,time)                                                   )
#define wociFreshRowNum(memtab)                                                    (LOGWHERE,_wdbiFreshRowNum(memtab)                                                     )
#define wociBatchSelect(result,param,colsnm)                                       (LOGWHERE,_wdbiBatchSelect(result,param,colsnm)                                        )
#define wociMTPrint(memtab,rownm,colsnm)                                           (LOGWHERE,_wdbiMTPrint(memtab,rownm,colsnm,false)                                      )
#define wociMTCompactPrint(memtab,rownm,colsnm)                                    (LOGWHERE,_wdbiMTPrint(memtab,rownm,colsnm,true)                                       )    
#define wociGetCurDateTime(date)                                                   (LOGWHERE,_wdbiGetCurDateTime(date)                                                    )
#define wociSetDateTime(date,year,mon,day,hour,min,sec)                            (LOGWHERE,_wdbiSetDateTime(date,year,mon,day,hour,min,sec)                             )
#define wociSetDate(date,year,mon,day)                                             (LOGWHERE,_wdbiSetDate(date,year,mon,day)                                              )
#define wociDateTimeToStr(date,str)                                                (LOGWHERE,_wdbiDateTimeToStr(date,str)                                                 )
#define wociGetYear(date)                                                          (LOGWHERE,_wdbiGetYear(date)                                                           )
#define wociGetMonth(date)                                                         (LOGWHERE,_wdbiGetMonth(date)                                                          )
#define wociGetDay(date)                                                           (LOGWHERE,_wdbiGetDay(date)                                                            )
#define wociGetHour(date)                                                          (LOGWHERE,_wdbiGetHour(date)                                                           )
#define wociGetMin(date)                                                           (LOGWHERE,_wdbiGetMin(date)                                                            )
#define wociGetSec(date)                                                           (LOGWHERE,_wdbiGetSec(date)                                                            )
#define wociSetEcho(val)                                                           (LOGWHERE,_wdbiSetEcho(val)                                                            )
#define wociIsEcho()                                                               (LOGWHERE,_wdbiIsEcho()                                                                )
#define wociMTToTextFile(memtab,fn,rownm,colsnm)                                   (LOGWHERE,_wdbiMTToTextFile(memtab,fn,rownm,colsnm)                                    )
// �����ı��ļ�*.cvs ,add by liujs     
#define wociMTToTextFileStr(memtab,fn,rownm,colsnm)                                (LOGWHERE,_wdbiMTToTextFileStr(memtab,fn,rownm,colsnm)                                 )                        
#define wociGetMTName(memtab,bf)                                                   (LOGWHERE,_wdbiGetMTName(memtab,bf)                                                    )
#define wociReadFromTextFile(memtab,fn,rowst,rownm)                                (LOGWHERE,_wdbiReadFromTextFile(memtab,fn,rowst,rownm)                                 )
#define wociFetchAllAt(memtab,st)                                                  (LOGWHERE,_wdbiFetchAllAt(memtab,st)                                                   )
#define wociDateDiff(d1,d2)                                                        (LOGWHERE,_wdbiDateDiff(d1,d2)                                                         )
#define wociSetOutputToConsole(val)                                                (LOGWHERE,_wdbiSetOutputToConsole(val)                                                 )
#define wociSetTraceFile(fn)                                             	   (LOGWHERE,_wdbiSetTraceFile(fn)                                              	  )
#define wociSetErrorFile(fn)                                                       (LOGWHERE,_wdbiSetErrorFile(fn)                                                        )
#define wociSetLogFile(fn)                                                         (LOGWHERE,_wdbiSetLogFile(fn)                                                          )
#define wociGetErrorFile()                                                         (LOGWHERE,_wdbiGetErrorFile()                                                        )
#define wociGetLogFile()                                                           (LOGWHERE,_wdbiGetLogFile()                                                          )
#define WOCIInit(appname)                                                          (LOGWHERE,_WDBIInit(appname)                                                           )
#define WOCIQuit()                                                                 (LOGWHERE,_WDBIQuit()                                                                  )
#define wociReplaceStmt(memtab,stmt)                                               (LOGWHERE,_wdbiReplaceStmt(memtab,stmt)                                                )
#define wociConvertColStrToInt(memtab,colsn,pcol)                                  (LOGWHERE,_wdbiConvertColStrToInt(memtab,colsn,pcol)                                   )
#define wociSaveSort(memtab,fp)                                                    (LOGWHERE,_wdbiSaveSort(memtab,fp)                                                     )
#define wociCalculate(memtab,colnm,op)                                             (LOGWHERE,_wdbiCalculate(memtab,colnm,op)                                              )       
#define wociLoadSort(memtab,fp)                                                    (LOGWHERE,_wdbiLoadSort(memtab,fp)                                                     )
#define wociCopyToMySQL(memtab,startrow,rownum,fp)                                 (LOGWHERE,_wdbiCopyToMySQL(memtab,startrow,rownum,fp)                                  )
#define wociCompareSortRow(memtab,row1, row2)                                      (LOGWHERE,_wdbiCompareSortRow(memtab,row1, row2)                                       )
#define wociList(pout)                                                             (LOGWHERE,_wdbiList(pout)                                                              )
#define wociGetLastError(handle)                                                   (LOGWHERE,_wdbiGetLastError(handle)                                                    )
#define wociAddrFresh(memtab,colval,collen,tp)                                     (LOGWHERE,_wdbiAddrFresh(memtab,colval,collen,tp)                                      )
#define wociGetColumnInfo(mt,crttab,ismysql)                               	   (LOGWHERE,_wdbiGetColumnInfo(mt,crttab,ismysql)                                     	  )
#define wociGetFetchSize(stmt)							   (LOGWHERE,_wdbiGetFetchSize(stmt)							  )
#define wociSetFetchSize(stmt,fsize)						   (LOGWHERE,_wdbiSetFetchSize(stmt,fsize)						  )
#define wociReverseByteOrder(mt)						   (LOGWHERE,_wdbiReverseByteOrder(mt)							  )
#define wociReverseCD(mt)						   	   (LOGWHERE,_wdbiReverseCD(mt)							  	  )
#define wociSetColumnDisplay(mt,cols,colid,dspname,scale,prec)		   	   (LOGWHERE,_wdbiSetColumnDisplay(mt,cols,colid,dspname,scale,prec)			  )
#endif

DllExport void _wdbilogwhere(const char *fn,int ln);
//Catch level : 0 -- no catch 1--catch WDBIError 2--catch 'WDBIError' and 'int'
//  and 'char *' exceptions
DllExport int _wdbiMainEntrance(int (*func)(void *),bool asyncmode,void *ptr,int catchLevel);
DllExport int _wdbiMainEntranceEx(int (*func)(void *),bool asyncmode,void *ptr,int catchLevel,int opt);
DllExport  int _wdbidestroy(int handle) ;
DllExport int _wdbiDestroyAll();
DllExport void _wdbiBreakAll();
//////////////////////////////////////////////////////////////////
// Session function(6) . 
//
//
//////////////////////////////////////////////////////////////////
DllExport  int _wdbiCreateSession(const char *username,const char *password,const char *svcname,int dbtype) ;
DllExport  int _wdbiCommit(int sess) ;
DllExport  int _wdbiRollback(int sess) ;
DllExport  int _wdbiSetTerminate(int sess,bool val) ;
DllExport  bool _wdbiIsTerminate(int sess) ;
DllExport  bool _wdbiSetNonBlockMode(int sess) ;

//////////////////////////////////////////////////////////////////
// Statment function (18). 
//
//
//////////////////////////////////////////////////////////////////
DllExport  int _wdbiCreateStatment(int sess) ;
DllExport  bool _wdbiBindStrByPos(int stmt,int pos,char *ptr,int len) ;
DllExport  bool _wdbiBindDoubleByPos(int stmt,int pos,double *ptr) ;
DllExport  bool _wdbiBindIntByPos(int stmt,int pos,int *ptr) ;
DllExport  bool _wdbiBindLongByPos(int stmt,int pos,LONG64 *ptr) ;
DllExport  bool _wdbiBindDateByPos(int stmt,int pos,char *ptr) ;
DllExport  bool _wdbiDefineStrByPos(int stmt,int pos,char *ptr,int len) ;
DllExport  bool _wdbiDefineDoubleByPos(int stmt,int pos,double *ptr) ;
DllExport  bool _wdbiDefineIntByPos(int stmt,int pos,int *ptr) ;
DllExport  bool _wdbiDefineLongByPos(int stmt,int pos,LONG64 *ptr) ;
DllExport  bool _wdbiDefineDateByPos(int stmt,int pos,char *ptr);
DllExport  int _wdbiBreakAndReset(int stmt) ;
DllExport  int _wdbiExecute(int stmt,int times) ;
DllExport  int _wdbiExecuteAt(int stmt,int times,int offset) ;
DllExport  int _wdbiFetch(int stmt,int rows) ;
DllExport  int _wdbiGetStmtColumnsNum(int stmt) ;
DllExport  int _wdbiGetMtColumnsNum(int mt) ;
 //int _wdbiGetColumnsNum(int stmt) ;
DllExport  int _wdbiGetFetchedRows(int stmt) ;
DllExport  int _wdbiGetSession(int stmt) ;
DllExport  unsigned short _wdbiGetStmtType(int stmt) ;
DllExport  bool _wdbiPrepareStmt(int stmt,const char *sqlstmt) ;
DllExport  int _wdbiGetCreateTableSQL(int memtab,char *buf,const char *tabname,bool ismysql) ;
DllExport  int _wdbiGetStmtColumnNum(int stmt);
DllExport  bool _wdbiGetStmtColumnName(int stmt,int col,char *colname);
DllExport  int _wdbiGetStmtColumnType(int stmt,int col);
//////////////////////////////////////////////////////////////////
// MemTable function (43). 
//
//
//////////////////////////////////////////////////////////////////
DllExport  int _wdbiCreateMemTable() ;
DllExport  bool _wdbiAddColumn(int memtab,const char *name,const char *dspname,int ctype,int length,int scale) ;
DllExport  bool _wdbiBuildStmt(int memtab,int stmt,unsigned int rows);
DllExport  bool _wdbiBuild(int memtab,unsigned int rows);
DllExport  void _wdbiClear(int memtab) ;
DllExport  bool _wdbiAppendToDbTable(int memtab,const char *tablename,int sess,bool autocommit) ;
DllExport  bool _wdbiAppendToDbTableWithColName(int memtab,const char *tablename,int sess,bool autocommit) ;
DllExport  unsigned int _wdbiFetchAll(int memtab) ;
DllExport  unsigned int _wdbiFetchFirst(int memtab,unsigned int rows) ;
DllExport  unsigned int _wdbiFetchNext(int memtab,unsigned int rows) ;
DllExport unsigned int _wdbiFetchAt(int memtab,unsigned int rows,int st) ;
DllExport  bool _wdbiGeneTable(int memtab,const char *tablename,int sess) ;
DllExport  void _wdbiSetMTName(int memtab,const char *mtname) ;
DllExport  int _wdbiGetStrAddrByName(int memtab,const char *col,unsigned int rowst,char **pstr,int *celllen) ;
DllExport  int _wdbiGetDateAddrByName(int memtab,const char *col,unsigned int rowst,char **pstr) ;
DllExport  int _wdbiGetDoubleAddrByName(int memtab,const char *col,unsigned int rowst,double **pstr);
DllExport  int _wdbiGetIntAddrByName(int memtab,const char *col,unsigned int rowst,int **pstr) ;
DllExport  int _wdbiGetLongAddrByName(int memtab,const char *col,unsigned int rowst,LONG64 **pstr) ;
 
DllExport  int _wdbiGetStrValByName(int memtab,const char *col,unsigned int rowst,char *pstr) ;
DllExport  int _wdbiGetDateValByName(int memtab,const char *col,unsigned int rowst,char *pstr) ;
DllExport  double _wdbiGetDoubleValByName(int memtab,const char *col,unsigned int rowst);
DllExport  int _wdbiGetIntValByName(int memtab,const char *col,unsigned int rowst) ;
DllExport  LONG64 _wdbiGetLongValByName(int memtab,const char *col,unsigned int rowst) ;

DllExport  int _wdbiGetStrAddrByPos(int memtab,int col,unsigned int rowst,char **pstr,int *celllen) ;
DllExport  int _wdbiGetDateAddrByPos(int memtab,int col,unsigned int rowst,char **pstr) ;
DllExport  int _wdbiGetDoubleAddrByPos(int memtab,int col,unsigned int rowst,double **pstr);
DllExport  int _wdbiGetIntAddrByPos(int memtab,int col,unsigned int rowst,int **pstr) ;
DllExport  int _wdbiGetLongAddrByPos(int memtab,int col,unsigned int rowst,LONG64 **pstr) ;
 
DllExport  int _wdbiGetBufferLen(int memtab) ;
DllExport  bool _wdbiGetCell(int memtab,unsigned int row,int col,char *str,bool rawpos) ;
DllExport  int _wdbiGetColumnDisplayWidth(int memtab,int col) ;
DllExport  int _wdbiGetColumnPosByName(int memtab,const char *colname);
DllExport  int _wdbiGetColumnName(int memtab,int id,char *colname);
DllExport  int _wdbiSetColumnName(int memtab,int id,char *colname);
DllExport   int _wdbiGetColumnTypeByName(int memtab,const char *colsnm,int *ctype);
DllExport int _wdbiGetSortColType(int memtab,int *ctype);
DllExport  int _wdbiGetColumnDataLenByPos(int memtab,int colid);
DllExport  int _wdbiGetColumnNumber(int memtab);
DllExport  int _wdbiGetColumnScale(int memtab,int colid);
DllExport  void _wdbiGetColumnTitle(int memtab,int colid,char *str,int len);
DllExport  short _wdbiGetColumnType(int memtab,int colid);
DllExport  bool _wdbiGetLine(int memtab,unsigned int row,char *str,bool rawpos,const char *colsnm,int *clen);
DllExport  int _wdbiGetMemtableRows(int memtab);
DllExport  void _wdbiGetTitle(int memtab,char *str,int len,const char *colsnm,int *clen);
DllExport  bool _wdbiIsIKSet(int memtab) ;
DllExport  bool _wdbiOrderByIK(int memtab);
DllExport  int _wdbiSearchIK(int memtab,int key);
DllExport  int _wdbiSearchIKLE(int memtab,int key);
DllExport  void _wdbiSetColumnDisplayName(int memtab,const char *colnm,char *str);
DllExport  bool _wdbiSetIKByName(int memtab,const char *str) ;
DllExport  bool _wdbiSetSortColumn(int memtab,const char *colsnm) ;
DllExport  void _wdbiReInOrder(int memtab);
DllExport  int _wdbiSetStrValues(int memtab,int colid,unsigned int rowstart,unsigned int rownum,const char *bf) ;
DllExport  int _wdbiSetDateValues(int memtab,int colid,unsigned int rowstart,unsigned int rownum,const char *bf) ;
DllExport  int _wdbiSetDoubleValues(int memtab,int colid,unsigned int rowstart,unsigned int rownum,const double *bf);
DllExport  int _wdbiSetIntValues(int memtab,int colid,unsigned int rowstart,unsigned int rownum,const int *bf) ;
DllExport  int _wdbiSetLongValues(int memtab,int colid,unsigned int rowstart,unsigned int rownum,const LONG64 *bf) ;
DllExport  bool _wdbiSort(int memtab) ;
DllExport  bool _wdbiSortHeap(int memtab) ;
//*  Group Functions (6)***************************************/
DllExport  bool _wdbiSetGroupSrc(int memtab,int src) ;
DllExport  bool _wdbiSetIKGroupRef(int memtab,int ref,const char *colnm) ;
DllExport  bool _wdbiSetGroupSrcCol(int memtab,const char *colsnm) ;
DllExport  bool _wdbiSetGroupRefCol(int memtab,const char *colsnm) ;
DllExport  bool _wdbiSetSrcSumCol(int memtab,const char *colsnm) ;
DllExport  bool _wdbiGroup(int memtab,int rowstart,int rownum);
DllExport  bool _wdbiCopyRowsTo(int memtab,int memtabto,int toStart,int start,int rowsnum);
DllExport  bool _wdbiCopyRowsToNoCut(int memtab,int memtabto,int toStart,int start,int rowsnum);
DllExport  bool _wdbiGetColumnDesc(int memtab,void **pColDesc,int &cdlen,int &_colnm) ;
DllExport bool _wdbiCopyColumnDefine(int memtab,int memtabfrom,const char *colsname);
DllExport  bool _wdbiExportSomeRows(int memtab,char *pData,int startrn,int rnnum);
DllExport  bool _wdbiExport(int memtab,void *pData,int &_len,int &_maxrows,int &_rowct) ;
DllExport bool _wdbiImport(int memtab,void *pData,int _len,void *pColDesc,int _colnm,int _cdlen,int _maxrows,int _rowct);
DllExport  bool _wdbiAppendRows(int memtab,char *pData,int rnnum); 
 	//������ṹ����������ṹ��������������
DllExport  void _wdbiReset(int memtab) ;
DllExport  void _wdbiClearIK(int memtab) ;
DllExport  void _wdbiClearSort(int memtab) ;
DllExport int _wdbiGetMaxRows(int memtab); 
DllExport  bool _wdbiCompact(int memtab);
DllExport  int _wdbiGetRowLen(int memta);
//////////////////////////////////////////////////////////////////
// ExcelEnv function(7) . 
//
//
//////////////////////////////////////////////////////////////////
DllExport  int _wdbiCreateExcelEnv() ;
DllExport  void _wdbiSetDir(int excel,const char *strTemplate,const char *strReport);
DllExport  void _wdbiSetMemTable(int excel,int memtab);
DllExport  bool _wdbiLoadTemplate(int excel,const char *tempname);
DllExport  bool _wdbiSelectSheet(int excel,const char *sheetname) ;
DllExport  bool _wdbiFillData(int excel,unsigned int tocol, unsigned int torow, unsigned int fromcol, unsigned int fromrow, unsigned int colnum, unsigned int rownum,bool rawpos) ;
DllExport  bool _wdbiFillTitle(int excel,unsigned int tocol,unsigned int torow,unsigned int fromcol,unsigned int colnum);
DllExport  bool _wdbiSaveAs(int excel,const char *filename) ;

DllExport  int _wdbiGetMemUsed(int memtab);
DllExport  bool _wdbiSetSortedGroupRef(int memtab,int mtref, const char *colssrc);
DllExport  int  _wdbiSearch(int memtab,void **ptr);
DllExport  int  _wdbiSearchLE(int memtab,void **ptr);
DllExport  bool _wdbiValuesSet(int memtab,int mtsrc,const char *colto,const char *colfrom,bool usedestKey,int op);
DllExport  bool _wdbiDeleteRow(int memtab,int rown) ;
DllExport  bool _wdbiInsertRows(int memtab,void **ptr,const char *colsname,int num);
DllExport  bool _wdbiBindToStatment(int memtab,int stmt,const char *colsname,int rowst);
DllExport  int _wdbiTestTable(int sess,const char *tablename);
DllExport  int _wdbiGetRawrnBySort(int memtab,int ind);
DllExport  int _wdbiGetRawrnByIK(int memtab,int ind);
DllExport  bool _wdbiCompressBf(int memtab);
DllExport  int _wdbiSearchQDel(int memtab,int key,int schopt);
DllExport  bool _wdbiIsQDelete(int memtab,int rownm);
DllExport  int _wdbiQDeletedRows(int memtab);
DllExport  bool _wdbiQDeleteRow(int memtab,int rownm);
DllExport  int _wdbiWaitStmt(int stmt,int tm);
DllExport  int _wdbiWaitMemtable(int memtab,int tm);
DllExport  int _wdbiWaitLastReturn(int handle);
DllExport  int _wdbiWaitTime(int handle,int time);
DllExport  bool _wdbiFreshRowNum(int memtab);
DllExport  int _wdbiBatchSelect(int result,int param,const char *colsnm);
//DllExport void _wdbiMTPrint(int memtab,int rownm,char *colsnm) ;
DllExport void _wdbiMTPrint(int memtab,int rownm,const char *colsnm,bool compact) ;
DllExport bool _wdbiGetCompactLen(int memtab,unsigned int rowstart,unsigned int rownum,const char *colnm,int *clen);

DllExport void _wdbiGetCurDateTime(char *date) ;
//Parametes: year(four digits),month(1-12),day(1-31),hour(0-23),minute(0-59),second(0-59).
// return a new datatime handle
DllExport void _wdbiSetDateTime(char *date,int year,int mon,int day,int hour,int min,int sec) ;
//Parameters:year,month,day ; hour,minute,second will be set to zero.
// return a new datetime handle
DllExport void _wdbiSetDate(char *date,int year,int mon,int day);
//Format : yyyy/mm/dd hh24:mi:ss
DllExport void _wdbiDateTimeToStr(const char *date,char *str) ;
DllExport int _wdbiGetYear(const char *date) ;
DllExport int _wdbiGetMonth(const char *date);
DllExport int _wdbiGetDay(const char *date) ;
DllExport int _wdbiGetHour(const char *date) ;
DllExport int _wdbiGetMin(const char *date) ;
DllExport int _wdbiGetSec(const char *date) ;
DllExport void _wdbiSetEcho(bool val);
DllExport bool _wdbiIsEcho();
DllExport void _wdbiMTToTextFile(int memtab,const char *fn,int rownm,const char *colsnm) ;
//  �����ı��ļ�*.cvs,add by liujs
DllExport void _wdbiMTToTextFileStr(int memtab,const char *fn,int rownm,const char *colsnm);
// ��ȡ�������ݣ�add by liujs
DllExport  bool _wdbiGetLineStr(int memtab,unsigned int row,char *str,bool rawpos,const char *colsnm,int *clen);
DllExport void _wdbiGetMTName(int memtab,char *bf);
DllExport int _wdbiReadFromTextFile(int memtab,const char *fn,int rowst,int rownm);
DllExport unsigned int _wdbiFetchAllAt(int memtab,int st) ;
DllExport int _wdbiDateDiff(const char *d1,const char *d2);
DllExport void _wdbiSetOutputToConsole(bool val);
DllExport void _wdbiSetTraceFile(const char *fn); 
DllExport double _wdbiCalculate(int memtab,const char *colnm,int op);
DllExport void _wdbiSetErrorFile(const char *fn);
DllExport void _wdbiSetLogFile(const char *fn);
DllExport const char * _wdbiGetErrorFile();
DllExport const char * _wdbiGetLogFile();
DllExport int xmkdir(const char *dir);
DllExport int xmkdir_withfile(const char *path);
DllExport int errprintf(const char *format,...) ;
DllExport int lgprintf(const char *format,...) ;
DllExport void _WDBIInit(const char *appname);
DllExport void _WDBIQuit();
DllExport bool _wdbiReplaceStmt(int memtab,int stmt);
DllExport int _wdbiConvertColStrToInt(int memtab,const char *colsn,int *pcol);
DllExport int _wdbiSaveSort(int memtab,FILE *fp);
DllExport int _wdbiLoadSort(int memtab,FILE *fp);
DllExport void _wdbiCopyToMySQL(int memtab,unsigned int startrow,
			unsigned int rownum,FILE *fp);
DllExport int _wdbiCompareSortRow(int memtab,unsigned int row1, unsigned int row2);
DllExport void _wdbiList(char *pout);
DllExport int _wdbiGetLastError(int handle);
DllExport void _wdbiAddrFresh(int memtab,char **colval,int *collen,int *tp);
DllExport int _wdbiGetColumnInfo(int mt,char *crttab,bool ismysql);
DllExport unsigned int _wdbiGetFetchSize(int stmt);
DllExport void _wdbiSetFetchSize(int stmt,unsigned int fsize);
DllExport bool _wdbiIsFixedMySQLBlock(int memtab);
// cols set to null to use colid,scale/prec set to -1 to ignore.
DllExport void _wdbiSetColumnDisplay(int mt,const char *cols,int colid,const char *dspname,int scale,int prec);
DllExport void _wdbiReverseByteOrder(int mt);
DllExport void _wdbiReverseCD(int mt);
DllExport void _wdbiSetDefaultPrec(int prec,int scale);
DllExport void _wdbiSetMTRows(int mt,int rown);
DllExport bool _wdbiIsNull(int mt,int col,unsigned rown);
DllExport void _wdbiSetNull(int mt,int col,unsigned rown,bool setnull);
DllExport bool _wdbiExportSomeRowsWithNF(int mt,char *pData,int startrn,int rnnum);
DllExport bool _wdbiAppendRowsWithNF(int mt,char *pData,int rnnum);
// End of file _wdbi8intface.h

#endif
