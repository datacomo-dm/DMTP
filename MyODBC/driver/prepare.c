/*
  Copyright (C) 1995-2006 MySQL AB

  This program is free software; you can redistribute it and/or modify
  it under the terms of version 2 of the GNU General Public License as
  published by the Free Software Foundation.

  There are special exceptions to the terms and conditions of the GPL
  as it is applied to this software. View the full text of the exception
  in file LICENSE.exceptions in the top-level directory of this software
  distribution.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

/**
  @file  prepare.c
  @brief Prepared statement functions.
*/

/***************************************************************************
 * The following ODBC APIs are implemented in this file:		   *
 *									   *
 *   SQLPrepare		 (ISO 92)					   *
 *   SQLBindParameter	 (ODBC)						   *
 *   SQLDescribeParam	 (ODBC)						   *
 *   SQLParamOptions	 (ODBC, Deprecated)				   *
 *   SQLNumParams	 (ISO 92)					   *
 *   SQLSetScrollOptions (ODBC, Deprecated)				   *
 *									   *
 ****************************************************************************/

#include "myodbc3.h"
#ifndef _UNIX_
# include <dos.h>
#endif /* !_UNIX_ */
#include <my_list.h>
#include <m_ctype.h>

/**
  Prepare a statement for later execution.

  @param[in] hStmt     Handle of the statement
  @param[in] szSqlStr  The statement to prepare
  @param[in] cbSqlStr  The length of the statement (or @c SQL_NTS if it is
                       NUL-terminated)
*/
SQLRETURN SQL_API SQLPrepare(SQLHSTMT hStmt, SQLCHAR *szSqlStr,
                             SQLINTEGER cbSqlStr)
{
  STMT *stmt= (STMT *)hStmt;
  /*
    We free orig_query here, instead of my_SQLPrepare, because
    my_SQLPrepare is used by my_pos_update() when a statement requires
    additional parameters.
  */
  if (stmt->orig_query)
    my_free(stmt->orig_query,MYF(0));

  return my_SQLPrepare(hStmt, szSqlStr, cbSqlStr);
}


/*
  @type    : myodbc3 internal
  @purpose : prepares an SQL string for execution
*/

SQLRETURN my_SQLPrepare( SQLHSTMT hstmt, SQLCHAR FAR *szSqlStr, SQLINTEGER cbSqlStr )
{
    STMT FAR *stmt= (STMT FAR*) hstmt;
    char in_string,*pos;
    uint param_count;
#ifdef USE_MB
    char *end;
#endif
    CHARSET_INFO *charset_info= stmt->dbc->mysql.charset;
    int bPerhapsEmbraced = 1;
    int bEmbraced = 0;
    char *pcLastCloseBrace = 0;

#ifdef USE_MB
    LINT_INIT(end);
#endif

    CLEAR_STMT_ERROR(stmt);
    if (stmt->query)
        my_free(stmt->query,MYF(0));
#ifdef NOT_NEEDED  
    SQLFreeStmt(hstmt,SQL_UNBIND);    /* Not needed according to VB 5.0 */
#endif

    if (!(stmt->query= dupp_str((char*) szSqlStr, cbSqlStr)))
        return set_error(stmt, MYERR_S1001, NULL, 4001);

    /* Count number of parameters and save position for each parameter */
    in_string= 0;
    param_count= 0;

#ifdef USE_MB
    if (use_mb(charset_info))
        end= strend(stmt->query);
#endif

    for (pos= stmt->query; *pos ; pos++)
    {
#ifdef USE_MB
        if (use_mb(charset_info))
        {
            int l;
            if ((l= my_ismbchar(charset_info, pos , end)))
            {
                pos+= l-1;
                continue;
            }
        }
#endif

        /* handle case where we have statement within {} - in this case we want to replace'em with ' ' */
        if ( bPerhapsEmbraced )
        {
            if ( *pos == '{' )
            {
                bPerhapsEmbraced = 0;
                bEmbraced = 1;
                *pos =  ' ';
                pos++;
                continue;
            }
            else if ( !isspace( *pos ) )
                bPerhapsEmbraced = 0;
        }
        else if ( bEmbraced && *pos == '}' )
            pcLastCloseBrace = pos;

        /* escape char? */
        if (*pos == '\\' && pos[1])     /* Next char is escaped */
        {
            pos++;
            continue;
        }

        /* in a string? */
        if (*pos == in_string)
        {
            if (pos[1] == in_string)      /* Two quotes is ok */
                pos++;
            else
                in_string= 0;
            continue;
        }

        /* parameter marker? */
        if (!in_string)
        {
            if (*pos == '\'' || *pos == '"' || *pos == '`') /* start of string? */
            {
                in_string= *pos;
                continue;
            }
            if (*pos == '?')
            {
                PARAM_BIND *param;

                if (param_count >= stmt->params.elements)
                {
                    PARAM_BIND tmp_param;
                    bzero(&tmp_param,sizeof(tmp_param));
                    if (push_dynamic(&stmt->params,(DYNAMIC_ELEMENT)&tmp_param))
                    {
                        return set_error(stmt, MYERR_S1001, NULL, 4001);
                    }
                }
                param= dynamic_element(&stmt->params,param_count,PARAM_BIND*);
                param->pos_in_query= pos;
                param_count++;
            }
        }
    }

    /* remove closing brace if we have one */
    if ( pcLastCloseBrace )
        *pcLastCloseBrace = ' ';

    stmt->param_count= param_count;
    /* Reset current_param so that SQLParamData starts fresh. */
    stmt->current_param= 0;
    stmt->query_end= pos;
    stmt->state= ST_PREPARED;
    return SQL_SUCCESS;
}


/*
  @type    : myodbc3 internal
  @purpose : binds a buffer to a parameter marker in an SQL statement.
*/

SQLRETURN SQL_API my_SQLBindParameter( SQLHSTMT     hstmt,
                                       SQLUSMALLINT ipar,
                                       SQLSMALLINT  fParamType __attribute__((unused)),
                                       SQLSMALLINT  fCType,
                                       SQLSMALLINT  fSqlType,
                                       SQLULEN      cbColDef __attribute__((unused)),
                                       SQLSMALLINT  ibScale __attribute__((unused)),
                                       SQLPOINTER   rgbValue,
                                       SQLLEN       cbValueMax,
                                       SQLLEN *     pcbValue )
{
    STMT FAR *stmt= (STMT FAR*) hstmt;
    PARAM_BIND param;

    CLEAR_STMT_ERROR(stmt);
  /* MODIFIED BY DT PROJECT BEGIN*/
  // Manually convert date expression used in parameters.
  if( rgbValue && ((char*)rgbValue)[23]==0 && ((char*)rgbValue)[2]=='-' &&
	  ((char*)rgbValue)[6]=='-') {
	  char *dt=(char *)rgbValue;
	  int mon=-1;  
	  if(strncmp(dt+3,"JAN",3)==0) mon=1; 
	  else if(strncmp(dt+3,"FEB",3)==0) mon=2;
	  else if(strncmp(dt+3,"MAR",3)==0) mon=3;
	  else if(strncmp(dt+3,"APR",3)==0) mon=4;
	  else if(strncmp(dt+3,"MAY",3)==0) mon=5;
	  else if(strncmp(dt+3,"JUN",3)==0) mon=6;
	  else if(strncmp(dt+3,"JUL",3)==0) mon=7;
	  else if(strncmp(dt+3,"AUG",3)==0) mon=8;
	  else if(strncmp(dt+3,"SEP",3)==0) mon=9;
	  else if(strncmp(dt+3,"OCT",3)==0) mon=10;
	  else if(strncmp(dt+3,"NOV",3)==0) mon=11;
	  else if(strncmp(dt+3,"DEC",3)==0) mon=12;
	  if(mon>0) {
		  char y[5],d[3],t[15];
		  strcpy(t,dt+12);
		  strncpy(y,dt+7,4);y[4]=0;
		  strncpy(d,dt,2);d[2]=0;
		  //Reassigning parameter length.
		  *pcbValue=19;
		  sprintf(dt,"%s-%02d-%s %s",y,mon,d,t);
		  DBUG_PRINT("convert timestamp",
			("value :%s",dt));
	  }
  }
  /* MODIFIED BY DT PROJECT END*/
    if (ipar-- < 1)
    {
        set_error(stmt,MYERR_S1093,NULL,0);
        return SQL_ERROR;
    }
    if (fCType == SQL_C_NUMERIC) /* We don't support this now */
    {
        set_error(stmt,MYERR_07006,
                  "Restricted data type attribute violation(SQL_C_NUMERIC)",0);
        return SQL_ERROR;
    }
    if (stmt->params.elements > ipar)
    {
        /* Change old bind parameter */
        PARAM_BIND *old= dynamic_element(&stmt->params,ipar,PARAM_BIND*);
        if (old->alloced)
        {
            old->alloced= 0;
            my_free(old->value,MYF(0));
        }
        memcpy(&param, old, sizeof(param));
    }
    else
        bzero(&param, sizeof(param));
    /* Simply record the values. These are used later (SQLExecute) */
    param.used= 1;
    param.SqlType= fSqlType;
    param.CType= (fCType == SQL_C_DEFAULT ? default_c_type(fSqlType) : fCType);
    param.buffer= rgbValue;
    param.ValueMax= cbValueMax;
    param.actual_len= pcbValue;
    param.real_param_done= TRUE;

    if (set_dynamic(&stmt->params, (DYNAMIC_ELEMENT)&param, ipar))
    {
        set_error(stmt,MYERR_S1001,NULL,4001);
        return SQL_ERROR;
    }
    return SQL_SUCCESS;
}


/**
  Deprecated function, for more details see SQLBindParamater. 

  @param[in] stmt           Handle to statement
  @param[in] ipar           Parameter number
  @param[in] fCType         Value type
  @param[in] fSqlType       Parameter type
  @param[in] cbColDef       Column size
  @param[in] ibScale        Decimal digits
  @param[in] rgbValue       Parameter value pointer
  @param[in] pcbValue       String length or index pointer

  @return SQL_SUCCESS or SQL_ERROR (and diag is set)

*/

SQLRETURN SQL_API SQLSetParam(SQLHSTMT        hstmt,
                              SQLUSMALLINT    ipar, 
                              SQLSMALLINT     fCType, 
                              SQLSMALLINT     fSqlType,
                              SQLULEN         cbColDef, 
                              SQLSMALLINT     ibScale,
                              SQLPOINTER      rgbValue, 
                              SQLLEN *        pcbValue)
{
  return my_SQLBindParameter(hstmt, ipar, SQL_PARAM_INPUT_OUTPUT, fCType, 
                             fSqlType, cbColDef, ibScale, rgbValue, 
                             SQL_SETPARAM_VALUE_MAX, pcbValue);
}


/*
  @type    : ODBC 2.0 API
  @purpose : binds a buffer to a parameter marker in an SQL statement.
*/

SQLRETURN SQL_API SQLBindParameter( SQLHSTMT        hstmt,
                                    SQLUSMALLINT    ipar, 
                                    SQLSMALLINT     fParamType,
                                    SQLSMALLINT     fCType, 
                                    SQLSMALLINT     fSqlType,
                                    SQLULEN         cbColDef, 
                                    SQLSMALLINT     ibScale,
                                    SQLPOINTER      rgbValue, 
                                    SQLLEN          cbValueMax,
                                    SQLLEN *        pcbValue )
{
  return my_SQLBindParameter(hstmt, ipar, fParamType, fCType, fSqlType,
                             cbColDef, ibScale, rgbValue, cbValueMax, pcbValue);
}


/*
  @type    : ODBC 1.0 API
  @purpose : returns the description of a parameter marker associated
  with a prepared SQL statement
*/

SQLRETURN SQL_API SQLDescribeParam( SQLHSTMT        hstmt,
                                    SQLUSMALLINT    ipar __attribute__((unused)),
                                    SQLSMALLINT FAR *pfSqlType,
                                    SQLULEN *       pcbColDef,
                                    SQLSMALLINT FAR *pibScale __attribute__((unused)),
                                    SQLSMALLINT FAR *pfNullable )
{
    STMT FAR *stmt= (STMT FAR*) hstmt;

    if (pfSqlType)
        *pfSqlType= SQL_VARCHAR;
    if (pcbColDef)
        *pcbColDef= (stmt->dbc->flag & FLAG_BIG_PACKETS ? 24*1024*1024L : 255);
    if (pfNullable)
        *pfNullable= SQL_NULLABLE_UNKNOWN;

    return SQL_SUCCESS;
}


/*
  @type    : ODBC 1.0 API
  @purpose : sets multiple values (arrays) for the set of parameter markers
*/

#ifdef USE_SQLPARAMOPTIONS_SQLULEN_PTR
SQLRETURN SQL_API SQLParamOptions( SQLHSTMT     hstmt, 
                                   SQLULEN      crow,
                                   SQLULEN      *pirow __attribute__((unused)) )
#else
SQLRETURN SQL_API SQLParamOptions( SQLHSTMT     hstmt, 
                                   SQLUINTEGER  crow,
                                   SQLUINTEGER *pirow __attribute__((unused)) )
#endif
{
    if (crow != 1)
    {
        /*
          Currently return warning for batch processing request,
          but need to handle in the future..
        */
        return set_error(hstmt, MYERR_01S02,
                         "Option value changed to default parameter size", 0);
    }
    return SQL_SUCCESS;
}


/*
  @type    : ODBC 1.0 API
  @purpose : returns the number of parameter markers.
*/

SQLRETURN SQL_API SQLNumParams(SQLHSTMT hstmt, SQLSMALLINT *pcpar)
{
  STMT *stmt= (STMT *)hstmt;

  if (pcpar)
    *pcpar= stmt->param_count;

  return SQL_SUCCESS;
}


/*
  @type    : ODBC 1.0 API
  @purpose : sets options that control the behavior of cursors.
*/

SQLRETURN SQL_API SQLSetScrollOptions(  SQLHSTMT        hstmt,
                                        SQLUSMALLINT    fConcurrency __attribute__((unused)),
                                        SQLLEN          crowKeyset __attribute__((unused)),
                                        SQLUSMALLINT    crowRowset )
{
    STMT FAR *stmt= (STMT FAR*) hstmt;

    stmt->stmt_options.rows_in_set= crowRowset;

    return SQL_SUCCESS;
}
