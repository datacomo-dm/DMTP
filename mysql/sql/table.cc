/* Copyright 2000-2008 MySQL AB, 2008 Sun Microsystems, Inc.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */


/* Some general useful functions */

#include "mysql_priv.h"
#include "sql_trigger.h"
#include <m_ctype.h>
#include "my_md5.h"
#define ER_ERROR_MESSAGES 239
/* INFORMATION_SCHEMA name */
LEX_STRING INFORMATION_SCHEMA_NAME= {C_STRING_WITH_LEN("information_schema")};

/* MYSQL_SCHEMA name */
LEX_STRING MYSQL_SCHEMA_NAME= {C_STRING_WITH_LEN("mysql")};

/* GENERAL_LOG name */
LEX_STRING GENERAL_LOG_NAME= {C_STRING_WITH_LEN("general_log")};

/* SLOW_LOG name */
LEX_STRING SLOW_LOG_NAME= {C_STRING_WITH_LEN("slow_log")};

	/* Functions defined in this file */

void open_table_error(TABLE_SHARE *share, int error, int db_errno,
                      myf errortype, int errarg);
static int open_binary_frm(THD *thd, TABLE_SHARE *share,
                           uchar *head, File file);
static void fix_type_pointers(const char ***array, TYPELIB *point_to_type,
			      uint types, char **names);
static uint find_field(Field **fields, uchar *record, uint start, uint length);

inline bool is_system_table_name(const char *name, uint length);

/**************************************************************************
  Object_creation_ctx implementation.
**************************************************************************/

Object_creation_ctx *Object_creation_ctx::set_n_backup(THD *thd)
{
  Object_creation_ctx *backup_ctx;
  DBUG_ENTER("Object_creation_ctx::set_n_backup");

  backup_ctx= create_backup_ctx(thd);
  change_env(thd);

  DBUG_RETURN(backup_ctx);
}

void Object_creation_ctx::restore_env(THD *thd, Object_creation_ctx *backup_ctx)
{
  if (!backup_ctx)
    return;

  backup_ctx->change_env(thd);

  delete backup_ctx;
}

/**************************************************************************
  Default_object_creation_ctx implementation.
**************************************************************************/

Default_object_creation_ctx::Default_object_creation_ctx(THD *thd)
  : m_client_cs(thd->variables.character_set_client),
    m_connection_cl(thd->variables.collation_connection)
{ }

Default_object_creation_ctx::Default_object_creation_ctx(
  CHARSET_INFO *client_cs, CHARSET_INFO *connection_cl)
  : m_client_cs(client_cs),
    m_connection_cl(connection_cl)
{ }

Object_creation_ctx *
Default_object_creation_ctx::create_backup_ctx(THD *thd) const
{
  return new Default_object_creation_ctx(thd);
}

void Default_object_creation_ctx::change_env(THD *thd) const
{
  thd->variables.character_set_client= m_client_cs;
  thd->variables.collation_connection= m_connection_cl;

  thd->update_charset();
}

/**************************************************************************
  View_creation_ctx implementation.
**************************************************************************/

View_creation_ctx *View_creation_ctx::create(THD *thd)
{
  View_creation_ctx *ctx= new (thd->mem_root) View_creation_ctx(thd);

  return ctx;
}

/*************************************************************************/

View_creation_ctx * View_creation_ctx::create(THD *thd,
                                              TABLE_LIST *view)
{
  View_creation_ctx *ctx= new (thd->mem_root) View_creation_ctx(thd);

  /* Throw a warning if there is NULL cs name. */

  if (!view->view_client_cs_name.str ||
      !view->view_connection_cl_name.str)
  {
    push_warning_printf(thd, MYSQL_ERROR::WARN_LEVEL_NOTE,
                        ER_VIEW_NO_CREATION_CTX,
                        ER(ER_VIEW_NO_CREATION_CTX),
                        (const char *) view->db,
                        (const char *) view->table_name);

    ctx->m_client_cs= system_charset_info;
    ctx->m_connection_cl= system_charset_info;

    return ctx;
  }

  /* Resolve cs names. Throw a warning if there is unknown cs name. */

  bool invalid_creation_ctx;

  invalid_creation_ctx= resolve_charset(view->view_client_cs_name.str,
                                        system_charset_info,
                                        &ctx->m_client_cs);

  invalid_creation_ctx= resolve_collation(view->view_connection_cl_name.str,
                                          system_charset_info,
                                          &ctx->m_connection_cl) ||
                        invalid_creation_ctx;

  if (invalid_creation_ctx)
  {
    sql_print_warning("View '%s'.'%s': there is unknown charset/collation "
                      "names (client: '%s'; connection: '%s').",
                      (const char *) view->db,
                      (const char *) view->table_name,
                      (const char *) view->view_client_cs_name.str,
                      (const char *) view->view_connection_cl_name.str);

    push_warning_printf(thd, MYSQL_ERROR::WARN_LEVEL_NOTE,
                        ER_VIEW_INVALID_CREATION_CTX,
                        ER(ER_VIEW_INVALID_CREATION_CTX),
                        (const char *) view->db,
                        (const char *) view->table_name);
  }

  return ctx;
}

/*************************************************************************/

/* Get column name from column hash */

static uchar *get_field_name(Field **buff, size_t *length,
                             my_bool not_used __attribute__((unused)))
{
  *length= (uint) strlen((*buff)->field_name);
  return (uchar*) (*buff)->field_name;
}


/*
  Returns pointer to '.frm' extension of the file name.

  SYNOPSIS
    fn_rext()
    name       file name

  DESCRIPTION
    Checks file name part starting with the rightmost '.' character,
    and returns it if it is equal to '.frm'. 

  TODO
    It is a good idea to get rid of this function modifying the code
    to garantee that the functions presently calling fn_rext() always
    get arguments in the same format: either with '.frm' or without '.frm'.

  RETURN VALUES
    Pointer to the '.frm' extension. If there is no extension,
    or extension is not '.frm', pointer at the end of file name.
*/

char *fn_rext(char *name)
{
  char *res= strrchr(name, '.');
  if (res && !strcmp(res, reg_ext))
    return res;
  return name + strlen(name);
}

TABLE_CATEGORY get_table_category(const LEX_STRING *db, const LEX_STRING *name)
{
  DBUG_ASSERT(db != NULL);
  DBUG_ASSERT(name != NULL);

  if ((db->length == INFORMATION_SCHEMA_NAME.length) &&
      (my_strcasecmp(system_charset_info,
                    INFORMATION_SCHEMA_NAME.str,
                    db->str) == 0))
  {
    return TABLE_CATEGORY_INFORMATION;
  }

  if ((db->length == MYSQL_SCHEMA_NAME.length) &&
      (my_strcasecmp(system_charset_info,
                    MYSQL_SCHEMA_NAME.str,
                    db->str) == 0))
  {
    if (is_system_table_name(name->str, name->length))
    {
      return TABLE_CATEGORY_SYSTEM;
    }

    if ((name->length == GENERAL_LOG_NAME.length) &&
        (my_strcasecmp(system_charset_info,
                      GENERAL_LOG_NAME.str,
                      name->str) == 0))
    {
      return TABLE_CATEGORY_PERFORMANCE;
    }

    if ((name->length == SLOW_LOG_NAME.length) &&
        (my_strcasecmp(system_charset_info,
                      SLOW_LOG_NAME.str,
                      name->str) == 0))
    {
      return TABLE_CATEGORY_PERFORMANCE;
    }
  }

  return TABLE_CATEGORY_USER;
}


/*
  Allocate a setup TABLE_SHARE structure

  SYNOPSIS
    alloc_table_share()
    TABLE_LIST		Take database and table name from there
    key			Table cache key (db \0 table_name \0...)
    key_length		Length of key

  RETURN
    0  Error (out of memory)
    #  Share
*/

TABLE_SHARE *alloc_table_share(TABLE_LIST *table_list, char *key,
                               uint key_length)
{
  MEM_ROOT mem_root;
  TABLE_SHARE *share;
  char *key_buff, *path_buff;
  char path[FN_REFLEN];
  uint path_length;
  DBUG_ENTER("alloc_table_share");
  DBUG_PRINT("enter", ("table: '%s'.'%s'",
                       table_list->db, table_list->table_name));

  path_length= build_table_filename(path, sizeof(path) - 1,
                                    table_list->db,
                                    table_list->table_name, "", 0);
  init_sql_alloc(&mem_root, TABLE_ALLOC_BLOCK_SIZE, 0);
  if (multi_alloc_root(&mem_root,
                       &share, sizeof(*share),
                       &key_buff, key_length,
                       &path_buff, path_length + 1,
                       NULL))
  {
    bzero((char*) share, sizeof(*share));

    share->set_table_cache_key(key_buff, key, key_length);

    share->path.str= path_buff;
    share->path.length= path_length;
    strmov(share->path.str, path);
    share->normalized_path.str=    share->path.str;
    share->normalized_path.length= path_length;

    share->version=       refresh_version;

    /*
      This constant is used to mark that no table map version has been
      assigned.  No arithmetic is done on the value: it will be
      overwritten with a value taken from MYSQL_BIN_LOG.
    */
    share->table_map_version= ~(ulonglong)0;

    /*
      Since alloc_table_share() can be called without any locking (for
      example, ha_create_table... functions), we do not assign a table
      map id here.  Instead we assign a value that is not used
      elsewhere, and then assign a table map id inside open_table()
      under the protection of the LOCK_open mutex.
    */
    share->table_map_id= ~0UL;
    share->cached_row_logging_check= -1;

    memcpy((char*) &share->mem_root, (char*) &mem_root, sizeof(mem_root));
    pthread_mutex_init(&share->mutex, MY_MUTEX_INIT_FAST);
    pthread_cond_init(&share->cond, NULL);
  }
  DBUG_RETURN(share);
}


/*
  Initialize share for temporary tables

  SYNOPSIS
    init_tmp_table_share()
    thd         thread handle
    share	Share to fill
    key		Table_cache_key, as generated from create_table_def_key.
		must start with db name.    
    key_length	Length of key
    table_name	Table name
    path	Path to file (possible in lower case) without .frm

  NOTES
    This is different from alloc_table_share() because temporary tables
    don't have to be shared between threads or put into the table def
    cache, so we can do some things notable simpler and faster

    If table is not put in thd->temporary_tables (happens only when
    one uses OPEN TEMPORARY) then one can specify 'db' as key and
    use key_length= 0 as neither table_cache_key or key_length will be used).
*/

void init_tmp_table_share(THD *thd, TABLE_SHARE *share, const char *key,
                          uint key_length, const char *table_name,
                          const char *path)
{
  DBUG_ENTER("init_tmp_table_share");
  DBUG_PRINT("enter", ("table: '%s'.'%s'", key, table_name));

  bzero((char*) share, sizeof(*share));
  init_sql_alloc(&share->mem_root, TABLE_ALLOC_BLOCK_SIZE, 0);
  share->table_category=         TABLE_CATEGORY_TEMPORARY;
  share->tmp_table=              INTERNAL_TMP_TABLE;
  share->db.str=                 (char*) key;
  share->db.length=		 strlen(key);
  share->table_cache_key.str=    (char*) key;
  share->table_cache_key.length= key_length;
  share->table_name.str=         (char*) table_name;
  share->table_name.length=      strlen(table_name);
  share->path.str=               (char*) path;
  share->normalized_path.str=    (char*) path;
  share->path.length= share->normalized_path.length= strlen(path);
  share->frm_version= 		 FRM_VER_TRUE_VARCHAR;

  /*
    Temporary tables are not replicated, but we set up these fields
    anyway to be able to catch errors.
   */
  share->table_map_version= ~(ulonglong)0;
  share->cached_row_logging_check= -1;

  /*
    table_map_id is also used for MERGE tables to suppress repeated
    compatibility checks.
  */
  share->table_map_id= (ulong) thd->query_id;

  DBUG_VOID_RETURN;
}


/*
  Free table share and memory used by it

  SYNOPSIS
    free_table_share()
    share		Table share

  NOTES
    share->mutex must be locked when we come here if it's not a temp table
*/

void free_table_share(TABLE_SHARE *share)
{
  MEM_ROOT mem_root;
  DBUG_ENTER("free_table_share");
  DBUG_PRINT("enter", ("table: %s.%s", share->db.str, share->table_name.str));
  DBUG_ASSERT(share->ref_count == 0);

  /*
    If someone is waiting for this to be deleted, inform it about this.
    Don't do a delete until we know that no one is refering to this anymore.
  */
  if (share->tmp_table == NO_TMP_TABLE)
  {
    /* share->mutex is locked in release_table_share() */
    while (share->waiting_on_cond)
    {
      pthread_cond_broadcast(&share->cond);
      pthread_cond_wait(&share->cond, &share->mutex);
    }
    /* No thread refers to this anymore */
    pthread_mutex_unlock(&share->mutex);
    pthread_mutex_destroy(&share->mutex);
    pthread_cond_destroy(&share->cond);
  }
  hash_free(&share->name_hash);
  
  plugin_unlock(NULL, share->db_plugin);
  share->db_plugin= NULL;

  /* We must copy mem_root from share because share is allocated through it */
  memcpy((char*) &mem_root, (char*) &share->mem_root, sizeof(mem_root));
  free_root(&mem_root, MYF(0));                 // Free's share
  DBUG_VOID_RETURN;
}


/**
  Return TRUE if a table name matches one of the system table names.
  Currently these are:

  help_category, help_keyword, help_relation, help_topic,
  proc, event
  time_zone, time_zone_leap_second, time_zone_name, time_zone_transition,
  time_zone_transition_type

  This function trades accuracy for speed, so may return false
  positives. Presumably mysql.* database is for internal purposes only
  and should not contain user tables.
*/

inline bool is_system_table_name(const char *name, uint length)
{
  CHARSET_INFO *ci= system_charset_info;

  return (
          /* mysql.proc table */
          length == 4 &&
          my_tolower(ci, name[0]) == 'p' && 
          my_tolower(ci, name[1]) == 'r' &&
          my_tolower(ci, name[2]) == 'o' &&
          my_tolower(ci, name[3]) == 'c' ||

          length > 4 &&
          (
           /* one of mysql.help* tables */
           my_tolower(ci, name[0]) == 'h' &&
           my_tolower(ci, name[1]) == 'e' &&
           my_tolower(ci, name[2]) == 'l' &&
           my_tolower(ci, name[3]) == 'p' ||

           /* one of mysql.time_zone* tables */
           my_tolower(ci, name[0]) == 't' &&
           my_tolower(ci, name[1]) == 'i' &&
           my_tolower(ci, name[2]) == 'm' &&
           my_tolower(ci, name[3]) == 'e' ||

           /* mysql.event table */
           my_tolower(ci, name[0]) == 'e' &&
           my_tolower(ci, name[1]) == 'v' &&
           my_tolower(ci, name[2]) == 'e' &&
           my_tolower(ci, name[3]) == 'n' &&
           my_tolower(ci, name[4]) == 't'
          )
         );
}


/*
  Read table definition from a binary / text based .frm file
  
  SYNOPSIS
  open_table_def()
  thd		Thread handler
  share		Fill this with table definition
  db_flags	Bit mask of the following flags: OPEN_VIEW

  NOTES
    This function is called when the table definition is not cached in
    table_def_cache
    The data is returned in 'share', which is alloced by
    alloc_table_share().. The code assumes that share is initialized.

  RETURN VALUES
   0	ok
   1	Error (see open_table_error)
   2    Error (see open_table_error)
   3    Wrong data in .frm file
   4    Error (see open_table_error)
   5    Error (see open_table_error: charset unavailable)
   6    Unknown .frm version
*/

int open_table_def(THD *thd, TABLE_SHARE *share, uint db_flags)
{
  int error, table_type;
  bool error_given;
  File file;
  uchar head[288], *disk_buff;
  char	path[FN_REFLEN];
  MEM_ROOT **root_ptr, *old_root;
  DBUG_ENTER("open_table_def");
  DBUG_PRINT("enter", ("table: '%s'.'%s'  path: '%s'", share->db.str,
                       share->table_name.str, share->normalized_path.str));

  error= 1;
  error_given= 0;
  disk_buff= NULL;

  strxmov(path, share->normalized_path.str, reg_ext, NullS);
  if ((file= my_open(path, O_RDONLY | O_SHARE, MYF(0))) < 0)
  {
    /*
      We don't try to open 5.0 unencoded name, if
      - non-encoded name contains '@' signs, 
        because '@' can be misinterpreted.
        It is not clear if '@' is escape character in 5.1,
        or a normal character in 5.0.
        
      - non-encoded db or table name contain "#mysql50#" prefix.
        This kind of tables must have been opened only by the
        my_open() above.
    */
    if (strchr(share->table_name.str, '@') ||
        !strncmp(share->db.str, MYSQL50_TABLE_NAME_PREFIX,
                 MYSQL50_TABLE_NAME_PREFIX_LENGTH) ||
        !strncmp(share->table_name.str, MYSQL50_TABLE_NAME_PREFIX,
                 MYSQL50_TABLE_NAME_PREFIX_LENGTH))
      goto err_not_open;

    /* Try unencoded 5.0 name */
    uint length;
    strxnmov(path, sizeof(path)-1,
             mysql_data_home, "/", share->db.str, "/",
             share->table_name.str, reg_ext, NullS);
    length= unpack_filename(path, path) - reg_ext_length;
    /*
      The following is a safety test and should never fail
      as the old file name should never be longer than the new one.
    */
    DBUG_ASSERT(length <= share->normalized_path.length);
    /*
      If the old and the new names have the same length,
      then table name does not have tricky characters,
      so no need to check the old file name.
    */
    if (length == share->normalized_path.length ||
        ((file= my_open(path, O_RDONLY | O_SHARE, MYF(0))) < 0))
      goto err_not_open;

    /* Unencoded 5.0 table name found */
    path[length]= '\0'; // Remove .frm extension
    strmov(share->normalized_path.str, path);
    share->normalized_path.length= length;
  }

  error= 4;
  if (my_read(file, head, 64, MYF(MY_NABP)))
    goto err;

/* Modified by DT project--BEGIN--*/
  if (head[0] == (uchar) 254 && (head[1] == 1 || head[1]==2))
/* Modified by DT project--END--*/
//修改前： 
//  if (head[0] == (uchar) 254 && head[1] == 1 )
  {
    if (head[2] == FRM_VER || head[2] == FRM_VER+1 ||
        (head[2] >= FRM_VER+3 && head[2] <= FRM_VER+4))
    {
      /* Open view only */
      if (db_flags & OPEN_VIEW_ONLY)
      {
        error_given= 1;
        goto err;
      }
      table_type= 1;
    }
    else
    {
      error= 6;                                 // Unkown .frm version
      goto err;
    }
  }
  else if (memcmp(head, STRING_WITH_LEN("TYPE=")) == 0)
  {
    error= 5;
    if (memcmp(head+5,"VIEW",4) == 0)
    {
      share->is_view= 1;
      if (db_flags & OPEN_VIEW)
        error= 0;
    }
    goto err;
  }
  else
    goto err;

  /* No handling of text based files yet */
  if (table_type == 1)
  {
    root_ptr= my_pthread_getspecific_ptr(MEM_ROOT**, THR_MALLOC);
    old_root= *root_ptr;
    *root_ptr= &share->mem_root;
    error= open_binary_frm(thd, share, head, file);
    *root_ptr= old_root;
    error_given= 1;
  }

  share->table_category= get_table_category(& share->db, & share->table_name);

  if (!error)
    thd->status_var.opened_shares++;

err:
  my_close(file, MYF(MY_WME));

err_not_open:
  if (error && !error_given)
  {
    share->error= error;
    open_table_error(share, error, (share->open_errno= my_errno), 0);
  }

  DBUG_RETURN(error);
}


/*
  Read data from a binary .frm file from MySQL 3.23 - 5.0 into TABLE_SHARE
*/

static int open_binary_frm(THD *thd, TABLE_SHARE *share, uchar *head,
                           File file)
{
  int error, errarg= 0;
  uint new_frm_ver, field_pack_length, new_field_pack_flag;
  uint interval_count, interval_parts, read_length, int_length;
  uint db_create_options, keys, key_parts, n_length;
  uint key_info_length, com_length, null_bit_pos;
  uint extra_rec_buf_length;
  uint i,j;
  bool use_hash;
  char *keynames, *names, *comment_pos;
  uchar *record;
  uchar *disk_buff, *strpos, *null_flags, *null_pos;
  ulong pos, record_offset, *rec_per_key, rec_buff_length;
  handler *handler_file= 0;
  KEY	*keyinfo;
  KEY_PART_INFO *key_part;
  SQL_CRYPT *crypted=0;
  Field  **field_ptr, *reg_field;
  const char **interval_array;
  enum legacy_db_type legacy_db_type;
  my_bitmap_map *bitmaps;
  DBUG_ENTER("open_binary_frm");
/* Modified by DT project--BEGIN--*/
// add a local variable
  bool use_dtformat=false;
/* Modified by DT project--END--*/
  new_field_pack_flag= head[27];
  new_frm_ver= (head[2] - FRM_VER);
/* Modified by DT project--BEGIN--*/
// add lines.
  if(head[1]==2) {
	  use_dtformat=true;head[1]=1;
      field_pack_length= new_frm_ver < 2 ? 12 : 17;
  }
  else
/* Modified by DT project--END--*/  
  field_pack_length= new_frm_ver < 2 ? 11 : 17;
  disk_buff= 0;

  error= 3;
  if (!(pos=get_form_pos(file,head,(TYPELIB*) 0)))
    goto err;                                   /* purecov: inspected */

  share->frm_version= head[2];
  /*
    Check if .frm file created by MySQL 5.0. In this case we want to
    display CHAR fields as CHAR and not as VARCHAR.
    We do it this way as we want to keep the old frm version to enable
    MySQL 4.1 to read these files.
  */
  if (share->frm_version == FRM_VER_TRUE_VARCHAR -1 && head[33] == 5)
    share->frm_version= FRM_VER_TRUE_VARCHAR;

#ifdef WITH_PARTITION_STORAGE_ENGINE
  if (*(head+61) &&
      !(share->default_part_db_type= 
        ha_checktype(thd, (enum legacy_db_type) (uint) *(head+61), 1, 0)))
    goto err;
  DBUG_PRINT("info", ("default_part_db_type = %u", head[61]));
#endif
  legacy_db_type= (enum legacy_db_type) (uint) *(head+3);
  DBUG_ASSERT(share->db_plugin == NULL);
  /*
    if the storage engine is dynamic, no point in resolving it by its
    dynamically allocated legacy_db_type. We will resolve it later by name.
  */
  if (legacy_db_type > DB_TYPE_UNKNOWN && 
      legacy_db_type < DB_TYPE_FIRST_DYNAMIC)
    share->db_plugin= ha_lock_engine(NULL, 
                                     ha_checktype(thd, legacy_db_type, 0, 0));
  share->db_create_options= db_create_options= uint2korr(head+30);
  share->db_options_in_use= share->db_create_options;
  share->mysql_version= uint4korr(head+51);
  share->null_field_first= 0;
  if (!head[32])				// New frm file in 3.23
  {
    share->avg_row_length= uint4korr(head+34);
    share->transactional= (ha_choice) (head[39] & 3);
    share->page_checksum= (ha_choice) ((head[39] >> 2) & 3);
    share->row_type= (row_type) head[40];
    share->table_charset= get_charset((uint) head[38],MYF(0));
    share->null_field_first= 1;
  }
  if (!share->table_charset)
  {
    /* unknown charset in head[38] or pre-3.23 frm */
    if (use_mb(default_charset_info))
    {
      /* Warn that we may be changing the size of character columns */
      sql_print_warning("'%s' had no or invalid character set, "
                        "and default character set is multi-byte, "
                        "so character column sizes may have changed",
                        share->path.str);
    }
    share->table_charset= default_charset_info;
  }
  share->db_record_offset= 1;
  if (db_create_options & HA_OPTION_LONG_BLOB_PTR)
    share->blob_ptr_size= portable_sizeof_char_ptr;
  /* Set temporarily a good value for db_low_byte_first */
  share->db_low_byte_first= test(legacy_db_type != DB_TYPE_ISAM);
  error=4;
  share->max_rows= uint4korr(head+18);
  share->min_rows= uint4korr(head+22);

  /* Read keyinformation */
  key_info_length= (uint) uint2korr(head+28);
  VOID(my_seek(file,(ulong) uint2korr(head+6),MY_SEEK_SET,MYF(0)));
  if (read_string(file,(uchar**) &disk_buff,key_info_length))
    goto err;                                   /* purecov: inspected */
  if (disk_buff[0] & 0x80)
  {
    share->keys=      keys=      (disk_buff[1] << 7) | (disk_buff[0] & 0x7f);
    share->key_parts= key_parts= uint2korr(disk_buff+2);
  }
  else
  {
    share->keys=      keys=      disk_buff[0];
    share->key_parts= key_parts= disk_buff[1];
  }
  share->keys_for_keyread.init(0);
  share->keys_in_use.init(keys);

  n_length=keys*sizeof(KEY)+key_parts*sizeof(KEY_PART_INFO);
  if (!(keyinfo = (KEY*) alloc_root(&share->mem_root,
				    n_length + uint2korr(disk_buff+4))))
    goto err;                                   /* purecov: inspected */
  bzero((char*) keyinfo,n_length);
  share->key_info= keyinfo;
  key_part= my_reinterpret_cast(KEY_PART_INFO*) (keyinfo+keys);
  strpos=disk_buff+6;

  if (!(rec_per_key= (ulong*) alloc_root(&share->mem_root,
					 sizeof(ulong*)*key_parts)))
    goto err;

  for (i=0 ; i < keys ; i++, keyinfo++)
  {
    keyinfo->table= 0;                           // Updated in open_frm
    if (new_frm_ver >= 3)
    {
      keyinfo->flags=	   (uint) uint2korr(strpos) ^ HA_NOSAME;
      keyinfo->key_length= (uint) uint2korr(strpos+2);
      keyinfo->key_parts=  (uint) strpos[4];
      keyinfo->algorithm=  (enum ha_key_alg) strpos[5];
      keyinfo->block_size= uint2korr(strpos+6);
      strpos+=8;
    }
    else
    {
      keyinfo->flags=	 ((uint) strpos[0]) ^ HA_NOSAME;
      keyinfo->key_length= (uint) uint2korr(strpos+1);
      keyinfo->key_parts=  (uint) strpos[3];
      keyinfo->algorithm= HA_KEY_ALG_UNDEF;
      strpos+=4;
    }

    keyinfo->key_part=	 key_part;
    keyinfo->rec_per_key= rec_per_key;
    for (j=keyinfo->key_parts ; j-- ; key_part++)
    {
      *rec_per_key++=0;
      key_part->fieldnr=	(uint16) (uint2korr(strpos) & FIELD_NR_MASK);
      key_part->offset= (uint) uint2korr(strpos+2)-1;
      key_part->key_type=	(uint) uint2korr(strpos+5);
      // key_part->field=	(Field*) 0;	// Will be fixed later
      if (new_frm_ver >= 1)
      {
	key_part->key_part_flag= *(strpos+4);
	key_part->length=	(uint) uint2korr(strpos+7);
	  /* Modified by DT project--BEGIN--*/
	    if(use_dtformat)
		{
			key_part->wide_partlen=(uint) uint2korr(strpos+9);
			//char *tc=(char *)&key_part->wide_partlen;
			//char t=tc[0];
			//tc[0]=tc[1];tc[1]=7;
			key_part->wide_partlen&=0x00ff;
			strpos+=11;
		}
		else
  /* Modified by DT project--END--*/
	     strpos+=9;
      }
      else
      {
	key_part->length=	*(strpos+4);
	key_part->key_part_flag=0;
	if (key_part->length > 128)
	{
	  key_part->length&=127;		/* purecov: inspected */
	  key_part->key_part_flag=HA_REVERSE_SORT; /* purecov: inspected */
	}
	strpos+=7;
      }
      key_part->store_length=key_part->length;
    }
  }
  keynames=(char*) key_part;
  strpos+= (strmov(keynames, (char *) strpos) - keynames)+1;

  share->reclength = uint2korr((head+16));
  if (*(head+26) == 1)
    share->system= 1;				/* one-record-database */
#ifdef HAVE_CRYPTED_FRM
  else if (*(head+26) == 2)
  {
    crypted= get_crypt_for_frm();
    share->crypted= 1;
  }
#endif

  record_offset= (ulong) (uint2korr(head+6)+
                          ((uint2korr(head+14) == 0xffff ?
                            uint4korr(head+47) : uint2korr(head+14))));
 
  if ((n_length= uint4korr(head+55)))
  {
    /* Read extra data segment */
    uchar *buff, *next_chunk, *buff_end;
    DBUG_PRINT("info", ("extra segment size is %u bytes", n_length));
    if (!(next_chunk= buff= (uchar*) my_malloc(n_length, MYF(MY_WME))))
      goto err;
    if (my_pread(file, buff, n_length, record_offset + share->reclength,
                 MYF(MY_NABP)))
    {
      my_free(buff, MYF(0));
      goto err;
    }
    share->connect_string.length= uint2korr(buff);
    if (!(share->connect_string.str= strmake_root(&share->mem_root,
                                                  (char*) next_chunk + 2,
                                                  share->connect_string.
                                                  length)))
    {
      my_free(buff, MYF(0));
      goto err;
    }
    next_chunk+= share->connect_string.length + 2;
    buff_end= buff + n_length;
    if (next_chunk + 2 < buff_end)
    {
      uint str_db_type_length= uint2korr(next_chunk);
      LEX_STRING name;
      name.str= (char*) next_chunk + 2;
      name.length= str_db_type_length;

      plugin_ref tmp_plugin= ha_resolve_by_name(thd, &name);
      if (tmp_plugin != NULL && !plugin_equals(tmp_plugin, share->db_plugin))
      {
        if (legacy_db_type > DB_TYPE_UNKNOWN &&
            legacy_db_type < DB_TYPE_FIRST_DYNAMIC &&
            legacy_db_type != ha_legacy_type(
                plugin_data(tmp_plugin, handlerton *)))
        {
          /* bad file, legacy_db_type did not match the name */
          my_free(buff, MYF(0));
          goto err;
        }
        /*
          tmp_plugin is locked with a local lock.
          we unlock the old value of share->db_plugin before
          replacing it with a globally locked version of tmp_plugin
        */
        plugin_unlock(NULL, share->db_plugin);
        share->db_plugin= my_plugin_lock(NULL, &tmp_plugin);
        DBUG_PRINT("info", ("setting dbtype to '%.*s' (%d)",
                            str_db_type_length, next_chunk + 2,
                            ha_legacy_type(share->db_type())));
      }
#ifdef WITH_PARTITION_STORAGE_ENGINE
      else if (str_db_type_length == 9 &&
               !strncmp((char *) next_chunk + 2, "partition", 9))
      {
        /*
          Use partition handler
          tmp_plugin is locked with a local lock.
          we unlock the old value of share->db_plugin before
          replacing it with a globally locked version of tmp_plugin
        */
        plugin_unlock(NULL, share->db_plugin);
        share->db_plugin= ha_lock_engine(NULL, partition_hton);
        DBUG_PRINT("info", ("setting dbtype to '%.*s' (%d)",
                            str_db_type_length, next_chunk + 2,
                            ha_legacy_type(share->db_type())));
      }
#endif
      else if (!tmp_plugin)
      {
        /* purecov: begin inspected */
        error= 8;
        my_error(ER_UNKNOWN_STORAGE_ENGINE, MYF(0), name.str);
        my_free(buff, MYF(0));
        goto err;
        /* purecov: end */
      }
      next_chunk+= str_db_type_length + 2;
    }
    if (next_chunk + 5 < buff_end)
    {
      uint32 partition_info_len = uint4korr(next_chunk);
#ifdef WITH_PARTITION_STORAGE_ENGINE
      if ((share->partition_info_buffer_size=
             share->partition_info_len= partition_info_len))
      {
        if (!(share->partition_info= (char*)
              memdup_root(&share->mem_root, next_chunk + 4,
                          partition_info_len + 1)))
        {
          my_free(buff, MYF(0));
          goto err;
        }
      }
#else
      if (partition_info_len)
      {
        DBUG_PRINT("info", ("WITH_PARTITION_STORAGE_ENGINE is not defined"));
        my_free(buff, MYF(0));
        goto err;
      }
#endif
      next_chunk+= 5 + partition_info_len;
    }
#if MYSQL_VERSION_ID < 50200
    if (share->mysql_version >= 50106 && share->mysql_version <= 50109)
    {
      /*
         Partition state array was here in version 5.1.6 to 5.1.9, this code
         makes it possible to load a 5.1.6 table in later versions. Can most
         likely be removed at some point in time. Will only be used for
         upgrades within 5.1 series of versions. Upgrade to 5.2 can only be
         done from newer 5.1 versions.
      */
      next_chunk+= 4;
    }
    else if (share->mysql_version >= 50110)
#endif
    {
      /* New auto_partitioned indicator introduced in 5.1.11 */
#ifdef WITH_PARTITION_STORAGE_ENGINE
      share->auto_partitioned= *next_chunk;
#endif
      next_chunk++;
    }
    keyinfo= share->key_info;
    for (i= 0; i < keys; i++, keyinfo++)
    {
      if (keyinfo->flags & HA_USES_PARSER)
      {
        LEX_STRING parser_name;
        if (next_chunk >= buff_end)
        {
          DBUG_PRINT("error",
                     ("fulltext key uses parser that is not defined in .frm"));
          my_free(buff, MYF(0));
          goto err;
        }
        parser_name.str= (char*) next_chunk;
        parser_name.length= strlen((char*) next_chunk);
        next_chunk+= parser_name.length + 1;
        keyinfo->parser= my_plugin_lock_by_name(NULL, &parser_name,
                                                MYSQL_FTPARSER_PLUGIN);
        if (! keyinfo->parser)
        {
          my_error(ER_PLUGIN_IS_NOT_LOADED, MYF(0), parser_name.str);
          my_free(buff, MYF(0));
          goto err;
        }
      }
    }
    my_free(buff, MYF(0));
  }
  share->key_block_size= uint2korr(head+62);

  error=4;
  extra_rec_buf_length= uint2korr(head+59);
  rec_buff_length= ALIGN_SIZE(share->reclength + 1 + extra_rec_buf_length);
  share->rec_buff_length= rec_buff_length;
  if (!(record= (uchar *) alloc_root(&share->mem_root,
                                     rec_buff_length)))
    goto err;                                   /* purecov: inspected */
  share->default_values= record;
  if (my_pread(file, record, (size_t) share->reclength,
               record_offset, MYF(MY_NABP)))
    goto err;                                   /* purecov: inspected */

  VOID(my_seek(file,pos,MY_SEEK_SET,MYF(0)));
  if (my_read(file, head,288,MYF(MY_NABP)))
    goto err;
#ifdef HAVE_CRYPTED_FRM
  if (crypted)
  {
    crypted->decode((char*) head+256,288-256);
    if (sint2korr(head+284) != 0)		// Should be 0
      goto err;                                 // Wrong password
  }
#endif

  share->fields= uint2korr(head+258);
  pos= uint2korr(head+260);			/* Length of all screens */
  n_length= uint2korr(head+268);
  interval_count= uint2korr(head+270);
  interval_parts= uint2korr(head+272);
  int_length= uint2korr(head+274);
  share->null_fields= uint2korr(head+282);
  com_length= uint2korr(head+284);
  share->comment.length=  (int) (head[46]);
  share->comment.str= strmake_root(&share->mem_root, (char*) head+47,
                                   share->comment.length);

  DBUG_PRINT("info",("i_count: %d  i_parts: %d  index: %d  n_length: %d  int_length: %d  com_length: %d", interval_count,interval_parts, share->keys,n_length,int_length, com_length));

  if (!(field_ptr = (Field **)
	alloc_root(&share->mem_root,
		   (uint) ((share->fields+1)*sizeof(Field*)+
			   interval_count*sizeof(TYPELIB)+
			   (share->fields+interval_parts+
			    keys+3)*sizeof(char *)+
			   (n_length+int_length+com_length)))))
    goto err;                                   /* purecov: inspected */

  share->field= field_ptr;
  read_length=(uint) (share->fields * field_pack_length +
		      pos+ (uint) (n_length+int_length+com_length));
  if (read_string(file,(uchar**) &disk_buff,read_length))
    goto err;                                   /* purecov: inspected */
#ifdef HAVE_CRYPTED_FRM
  if (crypted)
  {
    crypted->decode((char*) disk_buff,read_length);
    delete crypted;
    crypted=0;
  }
#endif
  strpos= disk_buff+pos;

  share->intervals= (TYPELIB*) (field_ptr+share->fields+1);
  interval_array= (const char **) (share->intervals+interval_count);
  names= (char*) (interval_array+share->fields+interval_parts+keys+3);
  if (!interval_count)
    share->intervals= 0;			// For better debugging
  memcpy((char*) names, strpos+(share->fields*field_pack_length),
	 (uint) (n_length+int_length));
  comment_pos= names+(n_length+int_length);
  memcpy(comment_pos, disk_buff+read_length-com_length, com_length);

  fix_type_pointers(&interval_array, &share->fieldnames, 1, &names);
  if (share->fieldnames.count != share->fields)
    goto err;
  fix_type_pointers(&interval_array, share->intervals, interval_count,
		    &names);

  {
    /* Set ENUM and SET lengths */
    TYPELIB *interval;
    for (interval= share->intervals;
         interval < share->intervals + interval_count;
         interval++)
    {
      uint count= (uint) (interval->count + 1) * sizeof(uint);
      if (!(interval->type_lengths= (uint *) alloc_root(&share->mem_root,
                                                        count)))
        goto err;
      for (count= 0; count < interval->count; count++)
      {
        char *val= (char*) interval->type_names[count];
        interval->type_lengths[count]= strlen(val);
      }
      interval->type_lengths[count]= 0;
    }
  }

  if (keynames)
    fix_type_pointers(&interval_array, &share->keynames, 1, &keynames);

 /* Allocate handler */
  if (!(handler_file= get_new_handler(share, thd->mem_root,
                                      share->db_type())))
    goto err;

  record= share->default_values-1;              /* Fieldstart = 1 */
  if (share->null_field_first)
  {
    null_flags= null_pos= (uchar*) record+1;
    null_bit_pos= (db_create_options & HA_OPTION_PACK_RECORD) ? 0 : 1;
    /*
      null_bytes below is only correct under the condition that
      there are no bit fields.  Correct values is set below after the
      table struct is initialized
    */
    share->null_bytes= (share->null_fields + null_bit_pos + 7) / 8;
  }
#ifndef WE_WANT_TO_SUPPORT_VERY_OLD_FRM_FILES
  else
  {
    share->null_bytes= (share->null_fields+7)/8;
    null_flags= null_pos= (uchar*) (record + 1 +share->reclength -
                                    share->null_bytes);
    null_bit_pos= 0;
  }
#endif

  use_hash= share->fields >= MAX_FIELDS_BEFORE_HASH;
  if (use_hash)
    use_hash= !hash_init(&share->name_hash,
			 system_charset_info,
			 share->fields,0,0,
			 (hash_get_key) get_field_name,0,0);

  for (i=0 ; i < share->fields; i++, strpos+=field_pack_length, field_ptr++)
  {
    uint pack_flag, interval_nr, unireg_type, recpos, field_length;
    enum_field_types field_type;
    CHARSET_INFO *charset=NULL;
    Field::geometry_type geom_type= Field::GEOM_GEOMETRY;
    LEX_STRING comment;

    if (new_frm_ver >= 3)
    {
      /* new frm file in 4.1 */
      field_length= uint2korr(strpos+3);
      recpos=	    uint3korr(strpos+5);
      pack_flag=    uint2korr(strpos+8);
      unireg_type=  (uint) strpos[10];
      interval_nr=  (uint) strpos[12];
      uint comment_length=uint2korr(strpos+15);
      field_type=(enum_field_types) (uint) strpos[13];
/* Modified by DT project--BEGIN--*/
/*5.1中的字符字段的存储长度，和字符集有关，例如UTF8的字段，存储字节数是实际字段长度的3倍。
   以下试图把他改回去，已满足dbplus存储格式，但似乎在这里改不起作用!暂时放弃*/
//	  if(field_type==MYSQL_TYPE_VARCHAR)
//	  field_length/=strpos[14]==33?3:2;
/* Modified by DT project--END--*/
      /* charset and geometry_type share the same byte in frm */
      if (field_type == MYSQL_TYPE_GEOMETRY)
      {
#ifdef HAVE_SPATIAL
	geom_type= (Field::geometry_type) strpos[14];
	charset= &my_charset_bin;
#else
	error= 4;  // unsupported field type
	goto err;
#endif
      }
      else
      {
        if (!strpos[14])
          charset= &my_charset_bin;
        else if (!(charset=get_charset((uint) strpos[14], MYF(0))))
        {
          error= 5; // Unknown or unavailable charset
          errarg= (int) strpos[14];
          goto err;
        }
      }
      if (!comment_length)
      {
	comment.str= (char*) "";
	comment.length=0;
      }
      else
      {
	comment.str=    (char*) comment_pos;
	comment.length= comment_length;
	comment_pos+=   comment_length;
      }
    }
    else
    {
/* Modified by DT project--BEGIN--*/
// add lines
    if(use_dtformat)
    {
      /* old frm file */
      field_length= uint2korr(strpos+3);
//      field_length= (uint) strpos[3];
      recpos=	    uint2korr(strpos+5),
      pack_flag=    uint2korr(strpos+7);
      unireg_type=  (uint) strpos[9];
      interval_nr=  (uint) strpos[11];
      field_type= (enum_field_types) f_packtype(pack_flag);
    }
	else {
/* Modified by DT project--END--*/
      field_length= (uint) strpos[3];
      recpos=	    uint2korr(strpos+4),
      pack_flag=    uint2korr(strpos+6);
      pack_flag&=   ~FIELDFLAG_NO_DEFAULT;     // Safety for old files
      unireg_type=  (uint) strpos[8];
      interval_nr=  (uint) strpos[10];

      /* old frm file */
      field_type= (enum_field_types) f_packtype(pack_flag);
/* Modified by DT project--BEGIN--*/
// add lines
	}
/* Modified by DT project--END--*/
      if (f_is_binary(pack_flag))
      {
        /*
          Try to choose the best 4.1 type:
          - for 4.0 "CHAR(N) BINARY" or "VARCHAR(N) BINARY" 
            try to find a binary collation for character set.
          - for other types (e.g. BLOB) just use my_charset_bin. 
        */
        if (!f_is_blob(pack_flag))
        {
          // 3.23 or 4.0 string
          if (!(charset= get_charset_by_csname(share->table_charset->csname,
                                               MY_CS_BINSORT, MYF(0))))
            charset= &my_charset_bin;
        }
        else
          charset= &my_charset_bin;
      }
      else
        charset= share->table_charset;
      bzero((char*) &comment, sizeof(comment));
    }

    if (interval_nr && charset->mbminlen > 1)
    {
      /* Unescape UCS2 intervals from HEX notation */
      TYPELIB *interval= share->intervals + interval_nr - 1;
      unhex_type2(interval);
    }
    
#ifndef TO_BE_DELETED_ON_PRODUCTION
    if (field_type == MYSQL_TYPE_NEWDECIMAL && !share->mysql_version)
    {
      /*
        Fix pack length of old decimal values from 5.0.3 -> 5.0.4
        The difference is that in the old version we stored precision
        in the .frm table while we now store the display_length
      */
      uint decimals= f_decimals(pack_flag);
      field_length= my_decimal_precision_to_length(field_length,
                                                   decimals,
                                                   f_is_dec(pack_flag) == 0);
      sql_print_error("Found incompatible DECIMAL field '%s' in %s; "
                      "Please do \"ALTER TABLE '%s' FORCE\" to fix it!",
                      share->fieldnames.type_names[i], share->table_name.str,
                      share->table_name.str);
      push_warning_printf(thd, MYSQL_ERROR::WARN_LEVEL_ERROR,
                          ER_CRASHED_ON_USAGE,
                          "Found incompatible DECIMAL field '%s' in %s; "
                          "Please do \"ALTER TABLE '%s' FORCE\" to fix it!",
                          share->fieldnames.type_names[i],
                          share->table_name.str,
                          share->table_name.str);
      share->crashed= 1;                        // Marker for CHECK TABLE
    }
#endif

    *field_ptr= reg_field=
      make_field(share, record+recpos,
		 (uint32) field_length,
		 null_pos, null_bit_pos,
		 pack_flag,
		 field_type,
		 charset,
		 geom_type,
		 (Field::utype) MTYP_TYPENR(unireg_type),
		 (interval_nr ?
		  share->intervals+interval_nr-1 :
		  (TYPELIB*) 0),
		 share->fieldnames.type_names[i]);
    if (!reg_field)				// Not supported field type
    {
      error= 4;
      goto err;			/* purecov: inspected */
    }

    reg_field->field_index= i;
    reg_field->comment=comment;
    if (field_type == MYSQL_TYPE_BIT && !f_bit_as_char(pack_flag))
    {
      if ((null_bit_pos+= field_length & 7) > 7)
      {
        null_pos++;
        null_bit_pos-= 8;
      }
    }
    if (!(reg_field->flags & NOT_NULL_FLAG))
    {
      if (!(null_bit_pos= (null_bit_pos + 1) & 7))
        null_pos++;
    }
    if (f_no_default(pack_flag))
      reg_field->flags|= NO_DEFAULT_VALUE_FLAG;

    if (reg_field->unireg_check == Field::NEXT_NUMBER)
      share->found_next_number_field= field_ptr;
    if (share->timestamp_field == reg_field)
      share->timestamp_field_offset= i;

    if (use_hash)
      (void) my_hash_insert(&share->name_hash,
                            (uchar*) field_ptr); // never fail
  }
  *field_ptr=0;					// End marker

  /* Fix key->name and key_part->field */
  if (key_parts)
  {
    uint primary_key=(uint) (find_type((char*) primary_key_name,
				       &share->keynames, 3) - 1);
    longlong ha_option= handler_file->ha_table_flags();
    keyinfo= share->key_info;
    key_part= keyinfo->key_part;

    for (uint key=0 ; key < share->keys ; key++,keyinfo++)
    {
      uint usable_parts= 0;
      keyinfo->name=(char*) share->keynames.type_names[key];
      /* Fix fulltext keys for old .frm files */
      if (share->key_info[key].flags & HA_FULLTEXT)
	share->key_info[key].algorithm= HA_KEY_ALG_FULLTEXT;

      if (primary_key >= MAX_KEY && (keyinfo->flags & HA_NOSAME))
      {
	/*
	  If the UNIQUE key doesn't have NULL columns and is not a part key
	  declare this as a primary key.
	*/
	primary_key=key;
	for (i=0 ; i < keyinfo->key_parts ;i++)
	{
	  uint fieldnr= key_part[i].fieldnr;
	  if (!fieldnr ||
	      share->field[fieldnr-1]->null_ptr ||
	      share->field[fieldnr-1]->key_length() !=
	      key_part[i].length)
	  {
	    primary_key=MAX_KEY;		// Can't be used
	    break;
	  }
	}
      }

      for (i=0 ; i < keyinfo->key_parts ; key_part++,i++)
      {
        Field *field;
	if (new_field_pack_flag <= 1)
	  key_part->fieldnr= (uint16) find_field(share->field,
                                                 share->default_values,
                                                 (uint) key_part->offset,
                                                 (uint) key_part->length);
	if (!key_part->fieldnr)
        {
          error= 4;                             // Wrong file
          goto err;
        }
        field= key_part->field= share->field[key_part->fieldnr-1];
        key_part->type= field->key_type();
        if (field->null_ptr)
        {
          key_part->null_offset=(uint) ((uchar*) field->null_ptr -
                                        share->default_values);
          key_part->null_bit= field->null_bit;
          key_part->store_length+=HA_KEY_NULL_LENGTH;
          keyinfo->flags|=HA_NULL_PART_KEY;
          keyinfo->extra_length+= HA_KEY_NULL_LENGTH;
          keyinfo->key_length+= HA_KEY_NULL_LENGTH;
        }
        if (field->type() == MYSQL_TYPE_BLOB ||
            field->real_type() == MYSQL_TYPE_VARCHAR ||
            field->type() == MYSQL_TYPE_GEOMETRY)
        {
          if (field->type() == MYSQL_TYPE_BLOB ||
              field->type() == MYSQL_TYPE_GEOMETRY)
            key_part->key_part_flag|= HA_BLOB_PART;
          else
            key_part->key_part_flag|= HA_VAR_LENGTH_PART;
          keyinfo->extra_length+=HA_KEY_BLOB_LENGTH;
          key_part->store_length+=HA_KEY_BLOB_LENGTH;
          keyinfo->key_length+= HA_KEY_BLOB_LENGTH;
          /*
            Mark that there may be many matching values for one key
            combination ('a', 'a ', 'a  '...)
          */
          if (!(field->flags & BINARY_FLAG))
            keyinfo->flags|= HA_END_SPACE_KEY;
        }
        if (field->type() == MYSQL_TYPE_BIT)
          key_part->key_part_flag|= HA_BIT_PART;

        if (i == 0 && key != primary_key)
          field->flags |= (((keyinfo->flags & HA_NOSAME) &&
                           (keyinfo->key_parts == 1)) ?
                           UNIQUE_KEY_FLAG : MULTIPLE_KEY_FLAG);
        if (i == 0)
          field->key_start.set_bit(key);
        if (field->key_length() == key_part->length &&
/* Modified by DT project--BEGIN--*/
		key_part->wide_partlen==0 &&
    /* Modified by DT project--END--*/
              !(field->flags & BLOB_FLAG))
        {
          if (handler_file->index_flags(key, i, 0) & HA_KEYREAD_ONLY)
          {
            share->keys_for_keyread.set_bit(key);
            field->part_of_key.set_bit(key);
            field->part_of_key_not_clustered.set_bit(key);
          }
          if (handler_file->index_flags(key, i, 1) & HA_READ_ORDER)
            field->part_of_sortkey.set_bit(key);
        }
        if (!(key_part->key_part_flag & HA_REVERSE_SORT) &&
            usable_parts == i)
          usable_parts++;			// For FILESORT
        field->flags|= PART_KEY_FLAG;
        if (key == primary_key)
        {
          field->flags|= PRI_KEY_FLAG;
          /*
            If this field is part of the primary key and all keys contains
            the primary key, then we can use any key to find this column
          */
          if (ha_option & HA_PRIMARY_KEY_IN_READ_INDEX)
          {
            field->part_of_key= share->keys_in_use;
            if (field->part_of_sortkey.is_set(key))
              field->part_of_sortkey= share->keys_in_use;
          }
        }
        if (field->key_length() != key_part->length)
        {
#ifndef TO_BE_DELETED_ON_PRODUCTION
          if (field->type() == MYSQL_TYPE_NEWDECIMAL)
          {
            /*
              Fix a fatal error in decimal key handling that causes crashes
              on Innodb. We fix it by reducing the key length so that
              InnoDB never gets a too big key when searching.
              This allows the end user to do an ALTER TABLE to fix the
              error.
            */
            keyinfo->key_length-= (key_part->length - field->key_length());
            key_part->store_length-= (uint16)(key_part->length -
                                              field->key_length());
            key_part->length= (uint16)field->key_length();
            sql_print_error("Found wrong key definition in %s; "
                            "Please do \"ALTER TABLE '%s' FORCE \" to fix it!",
                            share->table_name.str,
                            share->table_name.str);
            push_warning_printf(thd, MYSQL_ERROR::WARN_LEVEL_ERROR,
                                ER_CRASHED_ON_USAGE,
                                "Found wrong key definition in %s; "
                                "Please do \"ALTER TABLE '%s' FORCE\" to fix "
                                "it!",
                                share->table_name.str,
                                share->table_name.str);
            share->crashed= 1;                // Marker for CHECK TABLE
            goto to_be_deleted;
          }
#endif
          key_part->key_part_flag|= HA_PART_KEY_SEG;
        }

	to_be_deleted:

        /*
          If the field can be NULL, don't optimize away the test
          key_part_column = expression from the WHERE clause
          as we need to test for NULL = NULL.
        */
        if (field->real_maybe_null())
          key_part->key_part_flag|= HA_NULL_PART;
      }
      keyinfo->usable_key_parts= usable_parts; // Filesort

      set_if_bigger(share->max_key_length,keyinfo->key_length+
                    keyinfo->key_parts);
      share->total_key_length+= keyinfo->key_length;
      /*
        MERGE tables do not have unique indexes. But every key could be
        an unique index on the underlying MyISAM table. (Bug #10400)
      */
      if ((keyinfo->flags & HA_NOSAME) ||
          (ha_option & HA_ANY_INDEX_MAY_BE_UNIQUE))
        set_if_bigger(share->max_unique_length,keyinfo->key_length);
    }
    if (primary_key < MAX_KEY &&
	(share->keys_in_use.is_set(primary_key)))
    {
      share->primary_key= primary_key;
      /*
	If we are using an integer as the primary key then allow the user to
	refer to it as '_rowid'
      */
      if (share->key_info[primary_key].key_parts == 1)
      {
	Field *field= share->key_info[primary_key].key_part[0].field;
	if (field && field->result_type() == INT_RESULT)
        {
          /* note that fieldnr here (and rowid_field_offset) starts from 1 */
	  share->rowid_field_offset= (share->key_info[primary_key].key_part[0].
                                      fieldnr);
        }
      }
    }
    else
      share->primary_key = MAX_KEY; // we do not have a primary key
  }
  else
    share->primary_key= MAX_KEY;
  x_free((uchar*) disk_buff);
  disk_buff=0;
  if (new_field_pack_flag <= 1)
  {
    /* Old file format with default as not null */
    uint null_length= (share->null_fields+7)/8;
    bfill(share->default_values + (null_flags - (uchar*) record),
          null_length, 255);
  }

  if (share->found_next_number_field)
  {
    reg_field= *share->found_next_number_field;
    if ((int) (share->next_number_index= (uint)
	       find_ref_key(share->key_info, share->keys,
                            share->default_values, reg_field,
			    &share->next_number_key_offset,
                            &share->next_number_keypart)) < 0)
    {
      /* Wrong field definition */
      error= 4;
      goto err;
    }
    else
      reg_field->flags |= AUTO_INCREMENT_FLAG;
  }

  if (share->blob_fields)
  {
    Field **ptr;
    uint k, *save;

    /* Store offsets to blob fields to find them fast */
    if (!(share->blob_field= save=
	  (uint*) alloc_root(&share->mem_root,
                             (uint) (share->blob_fields* sizeof(uint)))))
      goto err;
    for (k=0, ptr= share->field ; *ptr ; ptr++, k++)
    {
      if ((*ptr)->flags & BLOB_FLAG)
	(*save++)= k;
    }
  }

  /*
    the correct null_bytes can now be set, since bitfields have been taken
    into account
  */
  share->null_bytes= (null_pos - (uchar*) null_flags +
                      (null_bit_pos + 7) / 8);
  share->last_null_bit_pos= null_bit_pos;

  share->db_low_byte_first= handler_file->low_byte_first();
  share->column_bitmap_size= bitmap_buffer_size(share->fields);

  if (!(bitmaps= (my_bitmap_map*) alloc_root(&share->mem_root,
                                             share->column_bitmap_size)))
    goto err;
  bitmap_init(&share->all_set, bitmaps, share->fields, FALSE);
  bitmap_set_all(&share->all_set);

  delete handler_file;
#ifndef DBUG_OFF
  if (use_hash)
    (void) hash_check(&share->name_hash);
#endif
  DBUG_RETURN (0);

 err:
  share->error= error;
  share->open_errno= my_errno;
  share->errarg= errarg;
  x_free((uchar*) disk_buff);
  delete crypted;
  delete handler_file;
  hash_free(&share->name_hash);

  open_table_error(share, error, share->open_errno, errarg);
  DBUG_RETURN(error);
} /* open_binary_frm */


/*
  Open a table based on a TABLE_SHARE

  SYNOPSIS
    open_table_from_share()
    thd			Thread handler
    share		Table definition
    alias       	Alias for table
    db_stat		open flags (for example HA_OPEN_KEYFILE|
    			HA_OPEN_RNDFILE..) can be 0 (example in
                        ha_example_table)
    prgflag   		READ_ALL etc..
    ha_open_flags	HA_OPEN_ABORT_IF_LOCKED etc..
    outparam       	result table

  RETURN VALUES
   0	ok
   1	Error (see open_table_error)
   2    Error (see open_table_error)
   3    Wrong data in .frm file
   4    Error (see open_table_error)
   5    Error (see open_table_error: charset unavailable)
   7    Table definition has changed in engine
*/

int open_table_from_share(THD *thd, TABLE_SHARE *share, const char *alias,
                          uint db_stat, uint prgflag, uint ha_open_flags,
                          TABLE *outparam, bool is_create_table)
{
  int error;
  uint records, i, bitmap_size;
  bool error_reported= FALSE;
  uchar *record, *bitmaps;
  Field **field_ptr;
  DBUG_ENTER("open_table_from_share");
  DBUG_PRINT("enter",("name: '%s.%s'  form: 0x%lx", share->db.str,
                      share->table_name.str, (long) outparam));

  /* Parsing of partitioning information from .frm needs thd->lex set up. */
  DBUG_ASSERT(thd->lex->is_lex_started);

  error= 1;
  bzero((char*) outparam, sizeof(*outparam));
  outparam->in_use= thd;
  outparam->s= share;
  outparam->db_stat= db_stat;
  outparam->write_row_record= NULL;

  init_sql_alloc(&outparam->mem_root, TABLE_ALLOC_BLOCK_SIZE, 0);

  if (!(outparam->alias= my_strdup(alias, MYF(MY_WME))))
    goto err;
  outparam->quick_keys.init();
  outparam->covering_keys.init();
  outparam->keys_in_use_for_query.init();

  /* Allocate handler */
  outparam->file= 0;
  if (!(prgflag & OPEN_FRM_FILE_ONLY))
  {
    if (!(outparam->file= get_new_handler(share, &outparam->mem_root,
                                          share->db_type())))
      goto err;
  }
  else
  {
    DBUG_ASSERT(!db_stat);
  }

  error= 4;
  outparam->reginfo.lock_type= TL_UNLOCK;
  outparam->current_lock= F_UNLCK;
  records=0;
  if ((db_stat & HA_OPEN_KEYFILE) || (prgflag & DELAYED_OPEN))
    records=1;
  if (prgflag & (READ_ALL+EXTRA_RECORD))
    records++;

  if (!(record= (uchar*) alloc_root(&outparam->mem_root,
                                   share->rec_buff_length * records)))
    goto err;                                   /* purecov: inspected */

  if (records == 0)
  {
    /* We are probably in hard repair, and the buffers should not be used */
    outparam->record[0]= outparam->record[1]= share->default_values;
  }
  else
  {
    outparam->record[0]= record;
    if (records > 1)
      outparam->record[1]= record+ share->rec_buff_length;
    else
      outparam->record[1]= outparam->record[0];   // Safety
  }

#ifdef HAVE_purify
  /*
    We need this because when we read var-length rows, we are not updating
    bytes after end of varchar
  */
  if (records > 1)
  {
    memcpy(outparam->record[0], share->default_values, share->rec_buff_length);
    memcpy(outparam->record[1], share->default_values, share->null_bytes);
    if (records > 2)
      memcpy(outparam->record[1], share->default_values,
             share->rec_buff_length);
  }
#endif

  if (!(field_ptr = (Field **) alloc_root(&outparam->mem_root,
                                          (uint) ((share->fields+1)*
                                                  sizeof(Field*)))))
    goto err;                                   /* purecov: inspected */

  outparam->field= field_ptr;

  record= (uchar*) outparam->record[0]-1;	/* Fieldstart = 1 */
  if (share->null_field_first)
    outparam->null_flags= (uchar*) record+1;
  else
    outparam->null_flags= (uchar*) (record+ 1+ share->reclength -
                                    share->null_bytes);

  /* Setup copy of fields from share, but use the right alias and record */
  for (i=0 ; i < share->fields; i++, field_ptr++)
  {
    if (!((*field_ptr)= share->field[i]->clone(&outparam->mem_root, outparam)))
      goto err;
  }
  (*field_ptr)= 0;                              // End marker

  if (share->found_next_number_field)
    outparam->found_next_number_field=
      outparam->field[(uint) (share->found_next_number_field - share->field)];
  if (share->timestamp_field)
    outparam->timestamp_field= (Field_timestamp*) outparam->field[share->timestamp_field_offset];


  /* Fix key->name and key_part->field */
  if (share->key_parts)
  {
    KEY	*key_info, *key_info_end;
    KEY_PART_INFO *key_part;
    uint n_length;
    n_length= share->keys*sizeof(KEY) + share->key_parts*sizeof(KEY_PART_INFO);
    if (!(key_info= (KEY*) alloc_root(&outparam->mem_root, n_length)))
      goto err;
    outparam->key_info= key_info;
    key_part= (my_reinterpret_cast(KEY_PART_INFO*) (key_info+share->keys));
    
    memcpy(key_info, share->key_info, sizeof(*key_info)*share->keys);
    memcpy(key_part, share->key_info[0].key_part, (sizeof(*key_part) *
                                                   share->key_parts));

    for (key_info_end= key_info + share->keys ;
         key_info < key_info_end ;
         key_info++)
    {
      KEY_PART_INFO *key_part_end;

      key_info->table= outparam;
      key_info->key_part= key_part;

      for (key_part_end= key_part+ key_info->key_parts ;
           key_part < key_part_end ;
           key_part++)
      {
        Field *field= key_part->field= outparam->field[key_part->fieldnr-1];

        if (field->key_length() != key_part->length &&
            !(field->flags & BLOB_FLAG))
        {
          /*
            We are using only a prefix of the column as a key:
            Create a new field for the key part that matches the index
          */
          field= key_part->field=field->new_field(&outparam->mem_root,
                                                  outparam, 0);
          field->field_length= key_part->length;
        }
      }
    }
  }

#ifdef WITH_PARTITION_STORAGE_ENGINE
  if (share->partition_info_len && outparam->file)
  {
  /*
    In this execution we must avoid calling thd->change_item_tree since
    we might release memory before statement is completed. We do this
    by changing to a new statement arena. As part of this arena we also
    set the memory root to be the memory root of the table since we
    call the parser and fix_fields which both can allocate memory for
    item objects. We keep the arena to ensure that we can release the
    free_list when closing the table object.
    SEE Bug #21658
  */

    Query_arena *backup_stmt_arena_ptr= thd->stmt_arena;
    Query_arena backup_arena;
    Query_arena part_func_arena(&outparam->mem_root, Query_arena::INITIALIZED);
    thd->set_n_backup_active_arena(&part_func_arena, &backup_arena);
    thd->stmt_arena= &part_func_arena;
    bool tmp;
    bool work_part_info_used;

    tmp= mysql_unpack_partition(thd, share->partition_info,
                                share->partition_info_len,
                                share->part_state,
                                share->part_state_len,
                                outparam, is_create_table,
                                share->default_part_db_type,
                                &work_part_info_used);
    if (tmp)
    {
      thd->stmt_arena= backup_stmt_arena_ptr;
      thd->restore_active_arena(&part_func_arena, &backup_arena);
      goto partititon_err;
    }
    outparam->part_info->is_auto_partitioned= share->auto_partitioned;
    DBUG_PRINT("info", ("autopartitioned: %u", share->auto_partitioned));
    /* we should perform the fix_partition_func in either local or
       caller's arena depending on work_part_info_used value
    */
    if (!work_part_info_used)
      tmp= fix_partition_func(thd, outparam, is_create_table);
    thd->stmt_arena= backup_stmt_arena_ptr;
    thd->restore_active_arena(&part_func_arena, &backup_arena);
    if (!tmp)
    {
      if (work_part_info_used)
        tmp= fix_partition_func(thd, outparam, is_create_table);
      outparam->part_info->item_free_list= part_func_arena.free_list;
    }
partititon_err:
    if (tmp)
    {
      if (is_create_table)
      {
        /*
          During CREATE/ALTER TABLE it is ok to receive errors here.
          It is not ok if it happens during the opening of an frm
          file as part of a normal query.
        */
        error_reported= TRUE;
      }
      goto err;
    }
  }
#endif

  /* Allocate bitmaps */

  bitmap_size= share->column_bitmap_size;
  if (!(bitmaps= (uchar*) alloc_root(&outparam->mem_root, bitmap_size*3)))
    goto err;
  bitmap_init(&outparam->def_read_set,
              (my_bitmap_map*) bitmaps, share->fields, FALSE);
  bitmap_init(&outparam->def_write_set,
              (my_bitmap_map*) (bitmaps+bitmap_size), share->fields, FALSE);
  bitmap_init(&outparam->tmp_set,
              (my_bitmap_map*) (bitmaps+bitmap_size*2), share->fields, FALSE);
  outparam->default_column_bitmaps();

  /* The table struct is now initialized;  Open the table */
  error= 2;
  if (db_stat)
  {
    int ha_err;
    if ((ha_err= (outparam->file->
                  ha_open(outparam, share->normalized_path.str,
                          (db_stat & HA_READ_ONLY ? O_RDONLY : O_RDWR),
                          (db_stat & HA_OPEN_TEMPORARY ? HA_OPEN_TMP_TABLE :
                           ((db_stat & HA_WAIT_IF_LOCKED) ||
                            (specialflag & SPECIAL_WAIT_IF_LOCKED)) ?
                           HA_OPEN_WAIT_IF_LOCKED :
                           (db_stat & (HA_ABORT_IF_LOCKED | HA_GET_INFO)) ?
                          HA_OPEN_ABORT_IF_LOCKED :
                           HA_OPEN_IGNORE_IF_LOCKED) | ha_open_flags))))
    {
      /* Set a flag if the table is crashed and it can be auto. repaired */
      share->crashed= ((ha_err == HA_ERR_CRASHED_ON_USAGE) &&
                       outparam->file->auto_repair() &&
                       !(ha_open_flags & HA_OPEN_FOR_REPAIR));

      switch (ha_err)
      {
        case HA_ERR_NO_SUCH_TABLE:
	  /*
            The table did not exists in storage engine, use same error message
            as if the .frm file didn't exist
          */
	  error= 1;
	  my_errno= ENOENT;
          break;
        case EMFILE:
	  /*
            Too many files opened, use same error message as if the .frm
            file can't open
           */
          DBUG_PRINT("error", ("open file: %s failed, too many files opened (errno: %d)", 
		  share->normalized_path.str, ha_err));
	  error= 1;
	  my_errno= EMFILE;
          break;
        default:
          outparam->file->print_error(ha_err, MYF(0));
          error_reported= TRUE;
          if (ha_err == HA_ERR_TABLE_DEF_CHANGED)
            error= 7;
          break;
      }
      goto err;                                 /* purecov: inspected */
    }
  }

#if defined(HAVE_purify) && !defined(DBUG_OFF)
  bzero((char*) bitmaps, bitmap_size*3);
#endif

  outparam->no_replicate= outparam->file &&
                          test(outparam->file->ha_table_flags() &
                               HA_HAS_OWN_BINLOGGING);
  thd->status_var.opened_tables++;

  DBUG_RETURN (0);

 err:
  if (! error_reported)
    open_table_error(share, error, my_errno, 0);
  delete outparam->file;
#ifdef WITH_PARTITION_STORAGE_ENGINE
  if (outparam->part_info)
    free_items(outparam->part_info->item_free_list);
#endif
  outparam->file= 0;				// For easier error checking
  outparam->db_stat=0;
  free_root(&outparam->mem_root, MYF(0));       // Safe to call on bzero'd root
  my_free((char*) outparam->alias, MYF(MY_ALLOW_ZERO_PTR));
  DBUG_RETURN (error);
}


/*
  Free information allocated by openfrm

  SYNOPSIS
    closefrm()
    table		TABLE object to free
    free_share		Is 1 if we also want to free table_share
*/

int closefrm(register TABLE *table, bool free_share)
{
  int error=0;
  uint idx;
  KEY *key_info;
  DBUG_ENTER("closefrm");
  DBUG_PRINT("enter", ("table: 0x%lx", (long) table));

  if (table->db_stat)
    error=table->file->close();
  key_info= table->key_info;
  for (idx= table->s->keys; idx; idx--, key_info++)
  {
    if (key_info->flags & HA_USES_PARSER)
    {
      plugin_unlock(NULL, key_info->parser);
      key_info->flags= 0;
    }
  }
  my_free((char*) table->alias, MYF(MY_ALLOW_ZERO_PTR));
  table->alias= 0;
  if (table->field)
  {
    for (Field **ptr=table->field ; *ptr ; ptr++)
      delete *ptr;
    table->field= 0;
  }
  delete table->file;
  table->file= 0;				/* For easier errorchecking */
#ifdef WITH_PARTITION_STORAGE_ENGINE
  if (table->part_info)
  {
    free_items(table->part_info->item_free_list);
    table->part_info->item_free_list= 0;
    table->part_info= 0;
  }
#endif
  if (free_share)
  {
    if (table->s->tmp_table == NO_TMP_TABLE)
      release_table_share(table->s, RELEASE_NORMAL);
    else
      free_table_share(table->s);
  }
  free_root(&table->mem_root, MYF(0));
  DBUG_RETURN(error);
}


/* Deallocate temporary blob storage */

void free_blobs(register TABLE *table)
{
  uint *ptr, *end;
  for (ptr= table->s->blob_field, end=ptr + table->s->blob_fields ;
       ptr != end ;
       ptr++)
    ((Field_blob*) table->field[*ptr])->free();
}


/**
  Reclaim temporary blob storage which is bigger than 
  a threshold.
 
  @param table A handle to the TABLE object containing blob fields
  @param size The threshold value.
 
*/

void free_field_buffers_larger_than(TABLE *table, uint32 size)
{
  uint *ptr, *end;
  for (ptr= table->s->blob_field, end=ptr + table->s->blob_fields ;
       ptr != end ;
       ptr++)
  {
    Field_blob *blob= (Field_blob*) table->field[*ptr];
    if (blob->get_field_buffer_size() > size)
        blob->free();
  }
}

	/* Find where a form starts */
	/* if formname is NullS then only formnames is read */

ulong get_form_pos(File file, uchar *head, TYPELIB *save_names)
{
  uint a_length,names,length;
  uchar *pos,*buf;
  ulong ret_value=0;
  DBUG_ENTER("get_form_pos");

  names=uint2korr(head+8);
  a_length=(names+2)*sizeof(char *);		/* Room for two extra */

  if (!save_names)
    a_length=0;
  else
    save_names->type_names=0;			/* Clear if error */

  if (names)
  {
    length=uint2korr(head+4);
    VOID(my_seek(file,64L,MY_SEEK_SET,MYF(0)));
    if (!(buf= (uchar*) my_malloc((size_t) length+a_length+names*4,
				  MYF(MY_WME))) ||
	my_read(file, buf+a_length, (size_t) (length+names*4),
		MYF(MY_NABP)))
    {						/* purecov: inspected */
      x_free((uchar*) buf);			/* purecov: inspected */
      DBUG_RETURN(0L);				/* purecov: inspected */
    }
    pos= buf+a_length+length;
    ret_value=uint4korr(pos);
  }
  if (! save_names)
  {
    if (names)
      my_free((uchar*) buf,MYF(0));
  }
  else if (!names)
    bzero((char*) save_names,sizeof(save_names));
  else
  {
    char *str;
    str=(char *) (buf+a_length);
    fix_type_pointers((const char ***) &buf,save_names,1,&str);
  }
  DBUG_RETURN(ret_value);
}


/*
  Read string from a file with malloc

  NOTES:
    We add an \0 at end of the read string to make reading of C strings easier
*/

int read_string(File file, uchar**to, size_t length)
{
  DBUG_ENTER("read_string");

  x_free(*to);
  if (!(*to= (uchar*) my_malloc(length+1,MYF(MY_WME))) ||
      my_read(file, *to, length,MYF(MY_NABP)))
  {
    x_free(*to);                              /* purecov: inspected */
    *to= 0;                                   /* purecov: inspected */
    DBUG_RETURN(1);                           /* purecov: inspected */
  }
  *((char*) *to+length)= '\0';
  DBUG_RETURN (0);
} /* read_string */


	/* Add a new form to a form file */

ulong make_new_entry(File file, uchar *fileinfo, TYPELIB *formnames,
		     const char *newname)
{
  uint i,bufflength,maxlength,n_length,length,names;
  ulong endpos,newpos;
  uchar buff[IO_SIZE];
  uchar *pos;
  DBUG_ENTER("make_new_entry");

  length=(uint) strlen(newname)+1;
  n_length=uint2korr(fileinfo+4);
  maxlength=uint2korr(fileinfo+6);
  names=uint2korr(fileinfo+8);
  newpos=uint4korr(fileinfo+10);

  if (64+length+n_length+(names+1)*4 > maxlength)
  {						/* Expand file */
    newpos+=IO_SIZE;
    int4store(fileinfo+10,newpos);
    endpos=(ulong) my_seek(file,0L,MY_SEEK_END,MYF(0));/* Copy from file-end */
    bufflength= (uint) (endpos & (IO_SIZE-1));	/* IO_SIZE is a power of 2 */

    while (endpos > maxlength)
    {
      VOID(my_seek(file,(ulong) (endpos-bufflength),MY_SEEK_SET,MYF(0)));
      if (my_read(file, buff, bufflength, MYF(MY_NABP+MY_WME)))
	DBUG_RETURN(0L);
      VOID(my_seek(file,(ulong) (endpos-bufflength+IO_SIZE),MY_SEEK_SET,
		   MYF(0)));
      if ((my_write(file, buff,bufflength,MYF(MY_NABP+MY_WME))))
	DBUG_RETURN(0);
      endpos-=bufflength; bufflength=IO_SIZE;
    }
    bzero(buff,IO_SIZE);			/* Null new block */
    VOID(my_seek(file,(ulong) maxlength,MY_SEEK_SET,MYF(0)));
    if (my_write(file,buff,bufflength,MYF(MY_NABP+MY_WME)))
	DBUG_RETURN(0L);
    maxlength+=IO_SIZE;				/* Fix old ref */
    int2store(fileinfo+6,maxlength);
    for (i=names, pos= (uchar*) *formnames->type_names+n_length-1; i-- ;
	 pos+=4)
    {
      endpos=uint4korr(pos)+IO_SIZE;
      int4store(pos,endpos);
    }
  }

  if (n_length == 1 )
  {						/* First name */
    length++;
    VOID(strxmov((char*) buff,"/",newname,"/",NullS));
  }
  else
    VOID(strxmov((char*) buff,newname,"/",NullS)); /* purecov: inspected */
  VOID(my_seek(file,63L+(ulong) n_length,MY_SEEK_SET,MYF(0)));
  if (my_write(file, buff, (size_t) length+1,MYF(MY_NABP+MY_WME)) ||
      (names && my_write(file,(uchar*) (*formnames->type_names+n_length-1),
			 names*4, MYF(MY_NABP+MY_WME))) ||
      my_write(file, fileinfo+10, 4,MYF(MY_NABP+MY_WME)))
    DBUG_RETURN(0L); /* purecov: inspected */

  int2store(fileinfo+8,names+1);
  int2store(fileinfo+4,n_length+length);
  VOID(my_chsize(file, newpos, 0, MYF(MY_WME)));/* Append file with '\0' */
  DBUG_RETURN(newpos);
} /* make_new_entry */


	/* error message when opening a form file */

void open_table_error(TABLE_SHARE *share, int error, int db_errno, int errarg)
{
  int err_no;
  char buff[FN_REFLEN];
  myf errortype= ME_ERROR+ME_WAITTANG;
  DBUG_ENTER("open_table_error");

  switch (error) {
  case 7:
  case 1:
    if (db_errno == ENOENT)
      my_error(ER_NO_SUCH_TABLE, MYF(0), share->db.str, share->table_name.str);
    else
    {
      strxmov(buff, share->normalized_path.str, reg_ext, NullS);
      my_error((db_errno == EMFILE) ? ER_CANT_OPEN_FILE : ER_FILE_NOT_FOUND,
               errortype, buff, db_errno);
    }
    break;
  case 2:
  {
    handler *file= 0;
    const char *datext= "";
    
    if (share->db_type() != NULL)
    {
      if ((file= get_new_handler(share, current_thd->mem_root,
                                 share->db_type())))
      {
        if (!(datext= *file->bas_ext()))
          datext= "";
      }
    }
    err_no= (db_errno == ENOENT) ? ER_FILE_NOT_FOUND : (db_errno == EAGAIN) ?
      ER_FILE_USED : ER_CANT_OPEN_FILE;
    strxmov(buff, share->normalized_path.str, datext, NullS);
    my_error(err_no,errortype, buff, db_errno);
    delete file;
    break;
  }
  case 5:
  {
    const char *csname= get_charset_name((uint) errarg);
    char tmp[10];
    if (!csname || csname[0] =='?')
    {
      my_snprintf(tmp, sizeof(tmp), "#%d", errarg);
      csname= tmp;
    }
    my_printf_error(ER_UNKNOWN_COLLATION,
                    "Unknown collation '%s' in table '%-.64s' definition", 
                    MYF(0), csname, share->table_name.str);
    break;
  }
  case 6:
    strxmov(buff, share->normalized_path.str, reg_ext, NullS);
    my_printf_error(ER_NOT_FORM_FILE,
                    "Table '%-.64s' was created with a different version "
                    "of MySQL and cannot be read", 
                    MYF(0), buff);
    break;
  case 8:
    break;
  default:				/* Better wrong error than none */
  case 4:
/* Modified by DT project--BEGIN--*/
  	THD *thd=current_thd;
	if(thd->dtmsg[0]!=0) 
	  my_message(ER_ERROR_MESSAGES,thd->dtmsg,errortype);
	else {
/* Modified by DT project--END--*/
    strxmov(buff, share->normalized_path.str, reg_ext, NullS);
    my_error(ER_NOT_FORM_FILE, errortype, buff, 0);
/* Modified by DT project--BEGIN--*/
	}
/* Modified by DT project--END--*/
    break;
  }
  DBUG_VOID_RETURN;
} /* open_table_error */


	/*
	** fix a str_type to a array type
	** typeparts separated with some char. differents types are separated
	** with a '\0'
	*/

static void
fix_type_pointers(const char ***array, TYPELIB *point_to_type, uint types,
		  char **names)
{
  char *type_name, *ptr;
  char chr;

  ptr= *names;
  while (types--)
  {
    point_to_type->name=0;
    point_to_type->type_names= *array;

    if ((chr= *ptr))			/* Test if empty type */
    {
      while ((type_name=strchr(ptr+1,chr)) != NullS)
      {
	*((*array)++) = ptr+1;
	*type_name= '\0';		/* End string */
	ptr=type_name;
      }
      ptr+=2;				/* Skip end mark and last 0 */
    }
    else
      ptr++;
    point_to_type->count= (uint) (*array - point_to_type->type_names);
    point_to_type++;
    *((*array)++)= NullS;		/* End of type */
  }
  *names=ptr;				/* Update end */
  return;
} /* fix_type_pointers */


TYPELIB *typelib(MEM_ROOT *mem_root, List<String> &strings)
{
  TYPELIB *result= (TYPELIB*) alloc_root(mem_root, sizeof(TYPELIB));
  if (!result)
    return 0;
  result->count=strings.elements;
  result->name="";
  uint nbytes= (sizeof(char*) + sizeof(uint)) * (result->count + 1);
  if (!(result->type_names= (const char**) alloc_root(mem_root, nbytes)))
    return 0;
  result->type_lengths= (uint*) (result->type_names + result->count + 1);
  List_iterator<String> it(strings);
  String *tmp;
  for (uint i=0; (tmp=it++) ; i++)
  {
    result->type_names[i]= tmp->ptr();
    result->type_lengths[i]= tmp->length();
  }
  result->type_names[result->count]= 0;		// End marker
  result->type_lengths[result->count]= 0;
  return result;
}


/*
 Search after a field with given start & length
 If an exact field isn't found, return longest field with starts
 at right position.
 
 NOTES
   This is needed because in some .frm fields 'fieldnr' was saved wrong

 RETURN
   0  error
   #  field number +1
*/

static uint find_field(Field **fields, uchar *record, uint start, uint length)
{
  Field **field;
  uint i, pos;

  pos= 0;
  for (field= fields, i=1 ; *field ; i++,field++)
  {
    if ((*field)->offset(record) == start)
    {
      if ((*field)->key_length() == length)
	return (i);
      if (!pos || fields[pos-1]->pack_length() <
	  (*field)->pack_length())
	pos= i;
    }
  }
  return (pos);
}


	/* Check that the integer is in the internal */

int set_zone(register int nr, int min_zone, int max_zone)
{
  if (nr<=min_zone)
    return (min_zone);
  if (nr>=max_zone)
    return (max_zone);
  return (nr);
} /* set_zone */

	/* Adjust number to next larger disk buffer */

ulong next_io_size(register ulong pos)
{
  reg2 ulong offset;
  if ((offset= pos & (IO_SIZE-1)))
    return pos-offset+IO_SIZE;
  return pos;
} /* next_io_size */


/*
  Store an SQL quoted string.

  SYNOPSIS  
    append_unescaped()
    res		result String
    pos		string to be quoted
    length	it's length

  NOTE
    This function works correctly with utf8 or single-byte charset strings.
    May fail with some multibyte charsets though.
*/

void append_unescaped(String *res, const char *pos, uint length)
{
  const char *end= pos+length;
  res->append('\'');

  for (; pos != end ; pos++)
  {
#if defined(USE_MB) && MYSQL_VERSION_ID < 40100
    uint mblen;
    if (use_mb(default_charset_info) &&
        (mblen= my_ismbchar(default_charset_info, pos, end)))
    {
      res->append(pos, mblen);
      pos+= mblen;
      continue;
    }
#endif

    switch (*pos) {
    case 0:				/* Must be escaped for 'mysql' */
      res->append('\\');
      res->append('0');
      break;
    case '\n':				/* Must be escaped for logs */
      res->append('\\');
      res->append('n');
      break;
    case '\r':
      res->append('\\');		/* This gives better readability */
      res->append('r');
      break;
    case '\\':
      res->append('\\');		/* Because of the sql syntax */
      res->append('\\');
      break;
    case '\'':
      res->append('\'');		/* Because of the sql syntax */
      res->append('\'');
      break;
    default:
      res->append(*pos);
      break;
    }
  }
  res->append('\'');
}


	/* Create a .frm file */

File create_frm(THD *thd, const char *name, const char *db,
                const char *table, uint reclength, uchar *fileinfo,
  		HA_CREATE_INFO *create_info, uint keys)
{
  register File file;
  ulong length;
  uchar fill[IO_SIZE];
  int create_flags= O_RDWR | O_TRUNC;

  if (create_info->options & HA_LEX_CREATE_TMP_TABLE)
    create_flags|= O_EXCL | O_NOFOLLOW;

  /* Fix this when we have new .frm files;  Current limit is 4G rows (QQ) */
  if (create_info->max_rows > UINT_MAX32)
    create_info->max_rows= UINT_MAX32;
  if (create_info->min_rows > UINT_MAX32)
    create_info->min_rows= UINT_MAX32;

  if ((file= my_create(name, CREATE_MODE, create_flags, MYF(0))) >= 0)
  {
    uint key_length, tmp_key_length;
    uint tmp;
    bzero((char*) fileinfo,64);
    /* header */
    fileinfo[0]=(uchar) 254;
/* Modified by DT project--BEGIN--*/
//新格式的标志，frm文件的版本不变化。
    fileinfo[1]= 2;
/* Modified by DT project--END--*/
//修改前：
//    fileinfo[1]= 1;
    fileinfo[2]= FRM_VER+3+ test(create_info->varchar);

    fileinfo[3]= (uchar) ha_legacy_type(
          ha_checktype(thd,ha_legacy_type(create_info->db_type),0,0));
    fileinfo[4]=1;
    int2store(fileinfo+6,IO_SIZE);		/* Next block starts here */
    /*
      Keep in sync with pack_keys() in unireg.cc
      For each key:
      8 bytes for the key header
      9 bytes for each key-part (MAX_REF_PARTS)
      NAME_LEN bytes for the name
      1 byte for the NAMES_SEP_CHAR (before the name)
      For all keys:
      6 bytes for the header
      1 byte for the NAMES_SEP_CHAR (after the last name)
      9 extra bytes (padding for safety? alignment?)
    */
    key_length= keys * (8 + MAX_REF_PARTS * 9 + NAME_LEN + 1) + 16;
    length= next_io_size((ulong) (IO_SIZE+key_length+reclength+
                                  create_info->extra_size));
    int4store(fileinfo+10,length);
    tmp_key_length= (key_length < 0xffff) ? key_length : 0xffff;
    int2store(fileinfo+14,tmp_key_length);
    int2store(fileinfo+16,reclength);
    int4store(fileinfo+18,create_info->max_rows);
    int4store(fileinfo+22,create_info->min_rows);
    /* fileinfo[26] is set in mysql_create_frm() */
    fileinfo[27]=2;				// Use long pack-fields
    /* fileinfo[28 & 29] is set to key_info_length in mysql_create_frm() */
    create_info->table_options|=HA_OPTION_LONG_BLOB_PTR; // Use portable blob pointers
    int2store(fileinfo+30,create_info->table_options);
    fileinfo[32]=0;				// No filename anymore
    fileinfo[33]=5;                             // Mark for 5.0 frm file
    int4store(fileinfo+34,create_info->avg_row_length);
    fileinfo[38]= (create_info->default_table_charset ?
		   create_info->default_table_charset->number : 0);
    fileinfo[39]= (uchar) ((uint) create_info->transactional |
                           ((uint) create_info->page_checksum << 2));
    fileinfo[40]= (uchar) create_info->row_type;
    /* Next few bytes where for RAID support */
    fileinfo[41]= 0;
    fileinfo[42]= 0;
    fileinfo[43]= 0;
    fileinfo[44]= 0;
    fileinfo[45]= 0;
    fileinfo[46]= 0;
    int4store(fileinfo+47, key_length);
    tmp= MYSQL_VERSION_ID;          // Store to avoid warning from int4store
    int4store(fileinfo+51, tmp);
    int4store(fileinfo+55, create_info->extra_size);
    /*
      59-60 is reserved for extra_rec_buf_length,
      61 for default_part_db_type
    */
    int2store(fileinfo+62, create_info->key_block_size);
    bzero(fill,IO_SIZE);
    for (; length > IO_SIZE ; length-= IO_SIZE)
    {
      if (my_write(file,fill, IO_SIZE, MYF(MY_WME | MY_NABP)))
      {
	VOID(my_close(file,MYF(0)));
	VOID(my_delete(name,MYF(0)));
	return(-1);
      }
    }
  }
  else
  {
    if (my_errno == ENOENT)
      my_error(ER_BAD_DB_ERROR,MYF(0),db);
    else
      my_error(ER_CANT_CREATE_TABLE,MYF(0),table,my_errno);
  }
  return (file);
} /* create_frm */


void update_create_info_from_table(HA_CREATE_INFO *create_info, TABLE *table)
{
  TABLE_SHARE *share= table->s;
  DBUG_ENTER("update_create_info_from_table");

  create_info->max_rows= share->max_rows;
  create_info->min_rows= share->min_rows;
  create_info->table_options= share->db_create_options;
  create_info->avg_row_length= share->avg_row_length;
  create_info->row_type= share->row_type;
  create_info->default_table_charset= share->table_charset;
  create_info->table_charset= 0;
  create_info->comment= share->comment;

  DBUG_VOID_RETURN;
}

int
rename_file_ext(const char * from,const char * to,const char * ext)
{
  char from_b[FN_REFLEN],to_b[FN_REFLEN];
  VOID(strxmov(from_b,from,ext,NullS));
  VOID(strxmov(to_b,to,ext,NullS));
  return (my_rename(from_b,to_b,MYF(MY_WME)));
}


/*
  Allocate string field in MEM_ROOT and return it as String

  SYNOPSIS
    get_field()
    mem   	MEM_ROOT for allocating
    field 	Field for retrieving of string
    res         result String

  RETURN VALUES
    1   string is empty
    0	all ok
*/

bool get_field(MEM_ROOT *mem, Field *field, String *res)
{
  char buff[MAX_FIELD_WIDTH], *to;
  String str(buff,sizeof(buff),&my_charset_bin);
  uint length;

  field->val_str(&str);
  if (!(length= str.length()))
  {
    res->length(0);
    return 1;
  }
  if (!(to= strmake_root(mem, str.ptr(), length)))
    length= 0;                                  // Safety fix
  res->set(to, length, ((Field_str*)field)->charset());
  return 0;
}


/*
  Allocate string field in MEM_ROOT and return it as NULL-terminated string

  SYNOPSIS
    get_field()
    mem   	MEM_ROOT for allocating
    field 	Field for retrieving of string

  RETURN VALUES
    NullS  string is empty
    #      pointer to NULL-terminated string value of field
*/

char *get_field(MEM_ROOT *mem, Field *field)
{
  char buff[MAX_FIELD_WIDTH], *to;
  String str(buff,sizeof(buff),&my_charset_bin);
  uint length;

  field->val_str(&str);
  length= str.length();
  if (!length || !(to= (char*) alloc_root(mem,length+1)))
    return NullS;
  memcpy(to,str.ptr(),(uint) length);
  to[length]=0;
  return to;
}

/*
  DESCRIPTION
    given a buffer with a key value, and a map of keyparts
    that are present in this value, returns the length of the value
*/
uint calculate_key_len(TABLE *table, uint key, const uchar *buf,
                       key_part_map keypart_map)
{
  /* works only with key prefixes */
  DBUG_ASSERT(((keypart_map + 1) & keypart_map) == 0);

  KEY *key_info= table->s->key_info+key;
  KEY_PART_INFO *key_part= key_info->key_part;
  KEY_PART_INFO *end_key_part= key_part + key_info->key_parts;
  uint length= 0;

  while (key_part < end_key_part && keypart_map)
  {
    length+= key_part->store_length;
    keypart_map >>= 1;
    key_part++;
  }
  return length;
}

/*
  Check if database name is valid

  SYNPOSIS
    check_db_name()
    org_name		Name of database and length

  NOTES
    If lower_case_table_names is set then database is converted to lower case

  RETURN
    0	ok
    1   error
*/

bool check_db_name(LEX_STRING *org_name)
{
  char *name= org_name->str;
  uint name_length= org_name->length;

  if (!name_length || name_length > NAME_LEN)
    return 1;

  if (lower_case_table_names && name != any_db)
    my_casedn_str(files_charset_info, name);

#if defined(USE_MB) && defined(USE_MB_IDENT)
  if (use_mb(system_charset_info))
  {
    name_length= 0;
    bool last_char_is_space= TRUE;
    char *end= name + org_name->length;
    while (name < end)
    {
      int len;
      last_char_is_space= my_isspace(system_charset_info, *name);
      len= my_ismbchar(system_charset_info, name, end);
      if (!len)
        len= 1;
      name+= len;
      name_length++;
    }
    return (last_char_is_space || name_length > NAME_CHAR_LEN);
  }
  else
#endif
    return ((org_name->str[org_name->length - 1] != ' ') ||
            (name_length > NAME_CHAR_LEN)); /* purecov: inspected */
}


/*
  Allow anything as a table name, as long as it doesn't contain an
  ' ' at the end
  returns 1 on error
*/


bool check_table_name(const char *name, uint length)
{
  uint name_length= 0;  // name length in symbols
  const char *end= name+length;
  if (!length || length > NAME_LEN)
    return 1;
#if defined(USE_MB) && defined(USE_MB_IDENT)
  bool last_char_is_space= FALSE;
#else
  if (name[length-1]==' ')
    return 1;
#endif

  while (name != end)
  {
#if defined(USE_MB) && defined(USE_MB_IDENT)
    last_char_is_space= my_isspace(system_charset_info, *name);
    if (use_mb(system_charset_info))
    {
      int len=my_ismbchar(system_charset_info, name, end);
      if (len)
      {
        name += len;
        name_length++;
        continue;
      }
    }
#endif
    name++;
    name_length++;
  }
#if defined(USE_MB) && defined(USE_MB_IDENT)
  return (last_char_is_space || name_length > NAME_CHAR_LEN) ;
#else
  return 0;
#endif
}


bool check_column_name(const char *name)
{
  uint name_length= 0;  // name length in symbols
  bool last_char_is_space= TRUE;
  
  while (*name)
  {
#if defined(USE_MB) && defined(USE_MB_IDENT)
    last_char_is_space= my_isspace(system_charset_info, *name);
    if (use_mb(system_charset_info))
    {
      int len=my_ismbchar(system_charset_info, name, 
                          name+system_charset_info->mbmaxlen);
      if (len)
      {
        name += len;
        name_length++;
        continue;
      }
    }
#else
    last_char_is_space= *name==' ';
#endif
    if (*name == NAMES_SEP_CHAR)
      return 1;
    name++;
    name_length++;
  }
  /* Error if empty or too long column name */
  return last_char_is_space || (uint) name_length > NAME_CHAR_LEN;
}


/**
  Checks whether a table is intact. Should be done *just* after the table has
  been opened.

  @param[in] table             The table to check
  @param[in] table_f_count     Expected number of columns in the table
  @param[in] table_def         Expected structure of the table (column name
                               and type)

  @retval  FALSE  OK
  @retval  TRUE   There was an error. An error message is output
                  to the error log.  We do not push an error
                  message into the error stack because this
                  function is currently only called at start up,
                  and such errors never reach the user.
*/

my_bool
table_check_intact(TABLE *table, const uint table_f_count,
                   const TABLE_FIELD_W_TYPE *table_def)
{
  uint i;
  my_bool error= FALSE;
  my_bool fields_diff_count;
  DBUG_ENTER("table_check_intact");
  DBUG_PRINT("info",("table: %s  expected_count: %d",
                     table->alias, table_f_count));

  fields_diff_count= (table->s->fields != table_f_count);
  if (fields_diff_count)
  {
    DBUG_PRINT("info", ("Column count has changed, checking the definition"));

    /* previous MySQL version */
    if (MYSQL_VERSION_ID > table->s->mysql_version)
    {
      sql_print_error(ER(ER_COL_COUNT_DOESNT_MATCH_PLEASE_UPDATE),
                      table->alias, table_f_count, table->s->fields,
                      table->s->mysql_version, MYSQL_VERSION_ID);
      DBUG_RETURN(TRUE);
    }
    else if (MYSQL_VERSION_ID == table->s->mysql_version)
    {
      sql_print_error(ER(ER_COL_COUNT_DOESNT_MATCH_CORRUPTED), table->alias,
                      table_f_count, table->s->fields);
      DBUG_RETURN(TRUE);
    }
    /*
      Something has definitely changed, but we're running an older
      version of MySQL with new system tables.
      Let's check column definitions. If a column was added at
      the end of the table, then we don't care much since such change
      is backward compatible.
    */
  }
  char buffer[STRING_BUFFER_USUAL_SIZE];
  for (i=0 ; i < table_f_count; i++, table_def++)
  {
    String sql_type(buffer, sizeof(buffer), system_charset_info);
    sql_type.length(0);
    if (i < table->s->fields)
    {
      Field *field= table->field[i];

      if (strncmp(field->field_name, table_def->name.str,
                  table_def->name.length))
      {
        /*
          Name changes are not fatal, we use ordinal numbers to access columns.
          Still this can be a sign of a tampered table, output an error
          to the error log.
        */
        sql_print_error("Incorrect definition of table %s.%s: "
                        "expected column '%s' at position %d, found '%s'.",
                        table->s->db.str, table->alias, table_def->name.str, i,
                        field->field_name);
      }
      field->sql_type(sql_type);
      /*
        Generally, if column types don't match, then something is
        wrong.

        However, we only compare column definitions up to the
        length of the original definition, since we consider the
        following definitions compatible:

        1. DATETIME and DATETIM
        2. INT(11) and INT(11
        3. SET('one', 'two') and SET('one', 'two', 'more')

        For SETs or ENUMs, if the same prefix is there it's OK to
        add more elements - they will get higher ordinal numbers and
        the new table definition is backward compatible with the
        original one.
       */
      if (strncmp(sql_type.c_ptr_safe(), table_def->type.str,
                  table_def->type.length - 1))
      {
        sql_print_error("Incorrect definition of table %s.%s: "
                        "expected column '%s' at position %d to have type "
                        "%s, found type %s.", table->s->db.str, table->alias,
                        table_def->name.str, i, table_def->type.str,
                        sql_type.c_ptr_safe());
        error= TRUE;
      }
      else if (table_def->cset.str && !field->has_charset())
      {
        sql_print_error("Incorrect definition of table %s.%s: "
                        "expected the type of column '%s' at position %d "
                        "to have character set '%s' but the type has no "
                        "character set.", table->s->db.str, table->alias,
                        table_def->name.str, i, table_def->cset.str);
        error= TRUE;
      }
      else if (table_def->cset.str &&
               strcmp(field->charset()->csname, table_def->cset.str))
      {
        sql_print_error("Incorrect definition of table %s.%s: "
                        "expected the type of column '%s' at position %d "
                        "to have character set '%s' but found "
                        "character set '%s'.", table->s->db.str, table->alias,
                        table_def->name.str, i, table_def->cset.str,
                        field->charset()->csname);
        error= TRUE;
      }
    }
    else
    {
      sql_print_error("Incorrect definition of table %s.%s: "
                      "expected column '%s' at position %d to have type %s "
                      " but the column is not found.",
                      table->s->db.str, table->alias,
                      table_def->name.str, i, table_def->type.str);
      error= TRUE;
    }
  }
  DBUG_RETURN(error);
}


/*
  Create Item_field for each column in the table.

  SYNPOSIS
    st_table::fill_item_list()
      item_list          a pointer to an empty list used to store items

  DESCRIPTION
    Create Item_field object for each column in the table and
    initialize it with the corresponding Field. New items are
    created in the current THD memory root.

  RETURN VALUE
    0                    success
    1                    out of memory
*/

bool st_table::fill_item_list(List<Item> *item_list) const
{
  /*
    All Item_field's created using a direct pointer to a field
    are fixed in Item_field constructor.
  */
  for (Field **ptr= field; *ptr; ptr++)
  {
    Item_field *item= new Item_field(*ptr);
    if (!item || item_list->push_back(item))
      return TRUE;
  }
  return FALSE;
}

/*
  Reset an existing list of Item_field items to point to the
  Fields of this table.

  SYNPOSIS
    st_table::fill_item_list()
      item_list          a non-empty list with Item_fields

  DESCRIPTION
    This is a counterpart of fill_item_list used to redirect
    Item_fields to the fields of a newly created table.
    The caller must ensure that number of items in the item_list
    is the same as the number of columns in the table.
*/

void st_table::reset_item_list(List<Item> *item_list) const
{
  List_iterator_fast<Item> it(*item_list);
  for (Field **ptr= field; *ptr; ptr++)
  {
    Item_field *item_field= (Item_field*) it++;
    DBUG_ASSERT(item_field != 0);
    item_field->reset_field(*ptr);
  }
}

/*
  calculate md5 of query

  SYNOPSIS
    TABLE_LIST::calc_md5()
    buffer	buffer for md5 writing
*/

void  TABLE_LIST::calc_md5(char *buffer)
{
  my_MD5_CTX context;
  uchar digest[16];
  my_MD5Init(&context);
  my_MD5Update(&context,(uchar *) select_stmt.str, select_stmt.length);
  my_MD5Final(digest, &context);
  sprintf((char *) buffer,
	    "%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x",
	    digest[0], digest[1], digest[2], digest[3],
	    digest[4], digest[5], digest[6], digest[7],
	    digest[8], digest[9], digest[10], digest[11],
	    digest[12], digest[13], digest[14], digest[15]);
}


/**
   @brief Set underlying table for table place holder of view.

   @details

   Replace all views that only use one table with the table itself.  This
   allows us to treat the view as a simple table and even update it (it is a
   kind of optimization).

   @note 

   This optimization is potentially dangerous as it makes views
   masquerade as base tables: Views don't have the pointer TABLE_LIST::table
   set to non-@c NULL.

   We may have the case where a view accesses tables not normally accessible
   in the current Security_context (only in the definer's
   Security_context). According to the table's GRANT_INFO (TABLE::grant),
   access is fulfilled, but this is implicitly meant in the definer's security
   context. Hence we must never look at only a TABLE's GRANT_INFO without
   looking at the one of the referring TABLE_LIST.
*/

void TABLE_LIST::set_underlying_merge()
{
  TABLE_LIST *tbl;

  if ((tbl= merge_underlying_list))
  {
    /* This is a view. Process all tables of view */
    DBUG_ASSERT(view && effective_algorithm == VIEW_ALGORITHM_MERGE);
    do
    {
      if (tbl->merge_underlying_list)          // This is a view
      {
        DBUG_ASSERT(tbl->view &&
                    tbl->effective_algorithm == VIEW_ALGORITHM_MERGE);
        /*
          This is the only case where set_ancestor is called on an object
          that may not be a view (in which case ancestor is 0)
        */
        tbl->merge_underlying_list->set_underlying_merge();
      }
    } while ((tbl= tbl->next_local));

    if (!multitable_view)
    {
      table= merge_underlying_list->table;
      schema_table= merge_underlying_list->schema_table;
    }
  }
}


/*
  setup fields of placeholder of merged VIEW

  SYNOPSIS
    TABLE_LIST::setup_underlying()
    thd		    - thread handler

  DESCRIPTION
    It is:
    - preparing translation table for view columns
    If there are underlying view(s) procedure first will be called for them.

  RETURN
    FALSE - OK
    TRUE  - error
*/

bool TABLE_LIST::setup_underlying(THD *thd)
{
  DBUG_ENTER("TABLE_LIST::setup_underlying");

  if (!field_translation && merge_underlying_list)
  {
    Field_translator *transl;
    SELECT_LEX *select= &view->select_lex;
    Item *item;
    TABLE_LIST *tbl;
    List_iterator_fast<Item> it(select->item_list);
    uint field_count= 0;

    if (check_stack_overrun(thd, STACK_MIN_SIZE, (uchar*) &field_count))
    {
      DBUG_RETURN(TRUE);
    }

    for (tbl= merge_underlying_list; tbl; tbl= tbl->next_local)
    {
      if (tbl->merge_underlying_list &&
          tbl->setup_underlying(thd))
      {
        DBUG_RETURN(TRUE);
      }
    }

    /* Create view fields translation table */

    if (!(transl=
          (Field_translator*)(thd->stmt_arena->
                              alloc(select->item_list.elements *
                                    sizeof(Field_translator)))))
    {
      DBUG_RETURN(TRUE);
    }

    while ((item= it++))
    {
      transl[field_count].name= item->name;
      transl[field_count++].item= item;
    }
    field_translation= transl;
    field_translation_end= transl + field_count;
    /* TODO: use hash for big number of fields */

    /* full text function moving to current select */
    if (view->select_lex.ftfunc_list->elements)
    {
      Item_func_match *ifm;
      SELECT_LEX *current_select= thd->lex->current_select;
      List_iterator_fast<Item_func_match>
        li(*(view->select_lex.ftfunc_list));
      while ((ifm= li++))
        current_select->ftfunc_list->push_front(ifm);
    }
  }
  DBUG_RETURN(FALSE);
}


/*
  Prepare where expression of view

  SYNOPSIS
    TABLE_LIST::prep_where()
    thd             - thread handler
    conds           - condition of this JOIN
    no_where_clause - do not build WHERE or ON outer qwery do not need it
                      (it is INSERT), we do not need conds if this flag is set

  NOTE: have to be called befor CHECK OPTION preparation, because it makes
  fix_fields for view WHERE clause

  RETURN
    FALSE - OK
    TRUE  - error
*/

bool TABLE_LIST::prep_where(THD *thd, Item **conds,
                               bool no_where_clause)
{
  DBUG_ENTER("TABLE_LIST::prep_where");

  for (TABLE_LIST *tbl= merge_underlying_list; tbl; tbl= tbl->next_local)
  {
    if (tbl->view && tbl->prep_where(thd, conds, no_where_clause))
    {
      DBUG_RETURN(TRUE);
    }
  }

  if (where)
  {
    if (!where->fixed && where->fix_fields(thd, &where))
    {
      DBUG_RETURN(TRUE);
    }

    /*
      check that it is not VIEW in which we insert with INSERT SELECT
      (in this case we can't add view WHERE condition to main SELECT_LEX)
    */
    if (!no_where_clause && !where_processed)
    {
      TABLE_LIST *tbl= this;
      Query_arena *arena= thd->stmt_arena, backup;
      arena= thd->activate_stmt_arena_if_needed(&backup);  // For easier test

      /* Go up to join tree and try to find left join */
      for (; tbl; tbl= tbl->embedding)
      {
        if (tbl->outer_join)
        {
          /*
            Store WHERE condition to ON expression for outer join, because
            we can't use WHERE to correctly execute left joins on VIEWs and
            this expression will not be moved to WHERE condition (i.e. will
            be clean correctly for PS/SP)
          */
          tbl->on_expr= and_conds(tbl->on_expr,
                                  where->copy_andor_structure(thd));
          break;
        }
      }
      if (tbl == 0)
        *conds= and_conds(*conds, where->copy_andor_structure(thd));
      if (arena)
        thd->restore_active_arena(arena, &backup);
      where_processed= TRUE;
    }
  }

  DBUG_RETURN(FALSE);
}


/*
  Merge ON expressions for a view

  SYNOPSIS
    merge_on_conds()
    thd             thread handle
    table           table for the VIEW
    is_cascaded     TRUE <=> merge ON expressions from underlying views

  DESCRIPTION
    This function returns the result of ANDing the ON expressions
    of the given view and all underlying views. The ON expressions
    of the underlying views are added only if is_cascaded is TRUE.

  RETURN
    Pointer to the built expression if there is any.
    Otherwise and in the case of a failure NULL is returned.
*/

static Item *
merge_on_conds(THD *thd, TABLE_LIST *table, bool is_cascaded)
{
  DBUG_ENTER("merge_on_conds");

  Item *cond= NULL;
  DBUG_PRINT("info", ("alias: %s", table->alias));
  if (table->on_expr)
    cond= table->on_expr->copy_andor_structure(thd);
  if (!table->nested_join)
    DBUG_RETURN(cond);
  List_iterator<TABLE_LIST> li(table->nested_join->join_list);
  while (TABLE_LIST *tbl= li++)
  {
    if (tbl->view && !is_cascaded)
      continue;
    cond= and_conds(cond, merge_on_conds(thd, tbl, is_cascaded));
  }
  DBUG_RETURN(cond);
}


/*
  Prepare check option expression of table

  SYNOPSIS
    TABLE_LIST::prep_check_option()
    thd             - thread handler
    check_opt_type  - WITH CHECK OPTION type (VIEW_CHECK_NONE,
                      VIEW_CHECK_LOCAL, VIEW_CHECK_CASCADED)
                      we use this parameter instead of direct check of
                      effective_with_check to change type of underlying
                      views to VIEW_CHECK_CASCADED if outer view have
                      such option and prevent processing of underlying
                      view check options if outer view have just
                      VIEW_CHECK_LOCAL option.

  NOTE
    This method builds check option condition to use it later on
    every call (usual execution or every SP/PS call).
    This method have to be called after WHERE preparation
    (TABLE_LIST::prep_where)

  RETURN
    FALSE - OK
    TRUE  - error
*/

bool TABLE_LIST::prep_check_option(THD *thd, uint8 check_opt_type)
{
  DBUG_ENTER("TABLE_LIST::prep_check_option");
  bool is_cascaded= check_opt_type == VIEW_CHECK_CASCADED;

  for (TABLE_LIST *tbl= merge_underlying_list; tbl; tbl= tbl->next_local)
  {
    /* see comment of check_opt_type parameter */
    if (tbl->view && tbl->prep_check_option(thd, (is_cascaded ?
                                                  VIEW_CHECK_CASCADED :
                                                  VIEW_CHECK_NONE)))
      DBUG_RETURN(TRUE);
  }

  if (check_opt_type && !check_option_processed)
  {
    Query_arena *arena= thd->stmt_arena, backup;
    arena= thd->activate_stmt_arena_if_needed(&backup);  // For easier test

    if (where)
    {
      DBUG_ASSERT(where->fixed);
      check_option= where->copy_andor_structure(thd);
    }
    if (is_cascaded)
    {
      for (TABLE_LIST *tbl= merge_underlying_list; tbl; tbl= tbl->next_local)
      {
        if (tbl->check_option)
          check_option= and_conds(check_option, tbl->check_option);
      }
    }
    check_option= and_conds(check_option,
                            merge_on_conds(thd, this, is_cascaded));

    if (arena)
      thd->restore_active_arena(arena, &backup);
    check_option_processed= TRUE;

  }

  if (check_option)
  {
    const char *save_where= thd->where;
    thd->where= "check option";
    if (!check_option->fixed &&
        check_option->fix_fields(thd, &check_option) ||
        check_option->check_cols(1))
    {
      DBUG_RETURN(TRUE);
    }
    thd->where= save_where;
  }
  DBUG_RETURN(FALSE);
}


/**
  Hide errors which show view underlying table information

  @param[in,out]  thd     thread handler

  @pre This method can be called only if there is an error.
*/

void TABLE_LIST::hide_view_error(THD *thd)
{
  /* Hide "Unknown column" or "Unknown function" error */
  DBUG_ASSERT(thd->is_error());

  if (thd->main_da.sql_errno() == ER_BAD_FIELD_ERROR ||
      thd->main_da.sql_errno() == ER_SP_DOES_NOT_EXIST ||
      thd->main_da.sql_errno() == ER_PROCACCESS_DENIED_ERROR ||
      thd->main_da.sql_errno() == ER_COLUMNACCESS_DENIED_ERROR ||
      thd->main_da.sql_errno() == ER_TABLEACCESS_DENIED_ERROR ||
      thd->main_da.sql_errno() == ER_TABLE_NOT_LOCKED ||
      thd->main_da.sql_errno() == ER_NO_SUCH_TABLE)
  {
    TABLE_LIST *top= top_table();
    thd->clear_error();
    my_error(ER_VIEW_INVALID, MYF(0), top->view_db.str, top->view_name.str);
  }
  else if (thd->main_da.sql_errno() == ER_NO_DEFAULT_FOR_FIELD)
  {
    TABLE_LIST *top= top_table();
    thd->clear_error();
    // TODO: make correct error message
    my_error(ER_NO_DEFAULT_FOR_VIEW_FIELD, MYF(0),
             top->view_db.str, top->view_name.str);
  }
}


/*
  Find underlying base tables (TABLE_LIST) which represent given
  table_to_find (TABLE)

  SYNOPSIS
    TABLE_LIST::find_underlying_table()
    table_to_find table to find

  RETURN
    0  table is not found
    found table reference
*/

TABLE_LIST *TABLE_LIST::find_underlying_table(TABLE *table_to_find)
{
  /* is this real table and table which we are looking for? */
  if (table == table_to_find && merge_underlying_list == 0)
    return this;

  for (TABLE_LIST *tbl= merge_underlying_list; tbl; tbl= tbl->next_local)
  {
    TABLE_LIST *result;
    if ((result= tbl->find_underlying_table(table_to_find)))
      return result;
  }
  return 0;
}

/*
  cleanup items belonged to view fields translation table

  SYNOPSIS
    TABLE_LIST::cleanup_items()
*/

void TABLE_LIST::cleanup_items()
{
  if (!field_translation)
    return;

  for (Field_translator *transl= field_translation;
       transl < field_translation_end;
       transl++)
    transl->item->walk(&Item::cleanup_processor, 0, 0);
}


/*
  check CHECK OPTION condition

  SYNOPSIS
    TABLE_LIST::view_check_option()
    ignore_failure ignore check option fail

  RETURN
    VIEW_CHECK_OK     OK
    VIEW_CHECK_ERROR  FAILED
    VIEW_CHECK_SKIP   FAILED, but continue
*/

int TABLE_LIST::view_check_option(THD *thd, bool ignore_failure)
{
  if (check_option && check_option->val_int() == 0)
  {
    TABLE_LIST *main_view= top_table();
    if (ignore_failure)
    {
      push_warning_printf(thd, MYSQL_ERROR::WARN_LEVEL_ERROR,
                          ER_VIEW_CHECK_FAILED, ER(ER_VIEW_CHECK_FAILED),
                          main_view->view_db.str, main_view->view_name.str);
      return(VIEW_CHECK_SKIP);
    }
    my_error(ER_VIEW_CHECK_FAILED, MYF(0), main_view->view_db.str,
             main_view->view_name.str);
    return(VIEW_CHECK_ERROR);
  }
  return(VIEW_CHECK_OK);
}


/*
  Find table in underlying tables by mask and check that only this
  table belong to given mask

  SYNOPSIS
    TABLE_LIST::check_single_table()
    table_arg	reference on variable where to store found table
		(should be 0 on call, to find table, or point to table for
		unique test)
    map         bit mask of tables
    view_arg    view for which we are looking table

  RETURN
    FALSE table not found or found only one
    TRUE  found several tables
*/

bool TABLE_LIST::check_single_table(TABLE_LIST **table_arg,
                                       table_map map,
                                       TABLE_LIST *view_arg)
{
  for (TABLE_LIST *tbl= merge_underlying_list; tbl; tbl= tbl->next_local)
  {
    if (tbl->table)
    {
      if (tbl->table->map & map)
      {
	if (*table_arg)
	  return TRUE;
        *table_arg= tbl;
        tbl->check_option= view_arg->check_option;
      }
    }
    else if (tbl->check_single_table(table_arg, map, view_arg))
      return TRUE;
  }
  return FALSE;
}


/*
  Set insert_values buffer

  SYNOPSIS
    set_insert_values()
    mem_root   memory pool for allocating

  RETURN
    FALSE - OK
    TRUE  - out of memory
*/

bool TABLE_LIST::set_insert_values(MEM_ROOT *mem_root)
{
  if (table)
  {
    if (!table->insert_values &&
        !(table->insert_values= (uchar *)alloc_root(mem_root,
                                                   table->s->rec_buff_length)))
      return TRUE;
  }
  else
  {
    DBUG_ASSERT(view && merge_underlying_list);
    for (TABLE_LIST *tbl= merge_underlying_list; tbl; tbl= tbl->next_local)
      if (tbl->set_insert_values(mem_root))
        return TRUE;
  }
  return FALSE;
}


/*
  Test if this is a leaf with respect to name resolution.

  SYNOPSIS
    TABLE_LIST::is_leaf_for_name_resolution()

  DESCRIPTION
    A table reference is a leaf with respect to name resolution if
    it is either a leaf node in a nested join tree (table, view,
    schema table, subquery), or an inner node that represents a
    NATURAL/USING join, or a nested join with materialized join
    columns.

  RETURN
    TRUE if a leaf, FALSE otherwise.
*/
bool TABLE_LIST::is_leaf_for_name_resolution()
{
  return (view || is_natural_join || is_join_columns_complete ||
          !nested_join);
}


/*
  Retrieve the first (left-most) leaf in a nested join tree with
  respect to name resolution.

  SYNOPSIS
    TABLE_LIST::first_leaf_for_name_resolution()

  DESCRIPTION
    Given that 'this' is a nested table reference, recursively walk
    down the left-most children of 'this' until we reach a leaf
    table reference with respect to name resolution.

  IMPLEMENTATION
    The left-most child of a nested table reference is the last element
    in the list of children because the children are inserted in
    reverse order.

  RETURN
    If 'this' is a nested table reference - the left-most child of
      the tree rooted in 'this',
    else return 'this'
*/

TABLE_LIST *TABLE_LIST::first_leaf_for_name_resolution()
{
  TABLE_LIST *cur_table_ref;
  NESTED_JOIN *cur_nested_join;
  LINT_INIT(cur_table_ref);

  if (is_leaf_for_name_resolution())
    return this;
  DBUG_ASSERT(nested_join);

  for (cur_nested_join= nested_join;
       cur_nested_join;
       cur_nested_join= cur_table_ref->nested_join)
  {
    List_iterator_fast<TABLE_LIST> it(cur_nested_join->join_list);
    cur_table_ref= it++;
    /*
      If the current nested join is a RIGHT JOIN, the operands in
      'join_list' are in reverse order, thus the first operand is
      already at the front of the list. Otherwise the first operand
      is in the end of the list of join operands.
    */
    if (!(cur_table_ref->outer_join & JOIN_TYPE_RIGHT))
    {
      TABLE_LIST *next;
      while ((next= it++))
        cur_table_ref= next;
    }
    if (cur_table_ref->is_leaf_for_name_resolution())
      break;
  }
  return cur_table_ref;
}


/*
  Retrieve the last (right-most) leaf in a nested join tree with
  respect to name resolution.

  SYNOPSIS
    TABLE_LIST::last_leaf_for_name_resolution()

  DESCRIPTION
    Given that 'this' is a nested table reference, recursively walk
    down the right-most children of 'this' until we reach a leaf
    table reference with respect to name resolution.

  IMPLEMENTATION
    The right-most child of a nested table reference is the first
    element in the list of children because the children are inserted
    in reverse order.

  RETURN
    - If 'this' is a nested table reference - the right-most child of
      the tree rooted in 'this',
    - else - 'this'
*/

TABLE_LIST *TABLE_LIST::last_leaf_for_name_resolution()
{
  TABLE_LIST *cur_table_ref= this;
  NESTED_JOIN *cur_nested_join;

  if (is_leaf_for_name_resolution())
    return this;
  DBUG_ASSERT(nested_join);

  for (cur_nested_join= nested_join;
       cur_nested_join;
       cur_nested_join= cur_table_ref->nested_join)
  {
    cur_table_ref= cur_nested_join->join_list.head();
    /*
      If the current nested is a RIGHT JOIN, the operands in
      'join_list' are in reverse order, thus the last operand is in the
      end of the list.
    */
    if ((cur_table_ref->outer_join & JOIN_TYPE_RIGHT))
    {
      List_iterator_fast<TABLE_LIST> it(cur_nested_join->join_list);
      TABLE_LIST *next;
      cur_table_ref= it++;
      while ((next= it++))
        cur_table_ref= next;
    }
    if (cur_table_ref->is_leaf_for_name_resolution())
      break;
  }
  return cur_table_ref;
}


/*
  Register access mode which we need for underlying tables

  SYNOPSIS
    register_want_access()
    want_access          Acess which we require
*/

void TABLE_LIST::register_want_access(ulong want_access)
{
  /* Remove SHOW_VIEW_ACL, because it will be checked during making view */
  want_access&= ~SHOW_VIEW_ACL;
  if (belong_to_view)
  {
    grant.want_privilege= want_access;
    if (table)
      table->grant.want_privilege= want_access;
  }
  for (TABLE_LIST *tbl= merge_underlying_list; tbl; tbl= tbl->next_local)
    tbl->register_want_access(want_access);
}


/*
  Load security context information for this view

  SYNOPSIS
    TABLE_LIST::prepare_view_securety_context()
    thd                  [in] thread handler

  RETURN
    FALSE  OK
    TRUE   Error
*/

#ifndef NO_EMBEDDED_ACCESS_CHECKS
bool TABLE_LIST::prepare_view_securety_context(THD *thd)
{
  DBUG_ENTER("TABLE_LIST::prepare_view_securety_context");
  DBUG_PRINT("enter", ("table: %s", alias));

  DBUG_ASSERT(!prelocking_placeholder && view);
  if (view_suid)
  {
    DBUG_PRINT("info", ("This table is suid view => load contest"));
    DBUG_ASSERT(view && view_sctx);
    if (acl_getroot_no_password(view_sctx,
                                definer.user.str,
                                definer.host.str,
                                definer.host.str,
                                thd->db))
    {
      if ((thd->lex->sql_command == SQLCOM_SHOW_CREATE) ||
          (thd->lex->sql_command == SQLCOM_SHOW_FIELDS))
      {
        push_warning_printf(thd, MYSQL_ERROR::WARN_LEVEL_NOTE, 
                            ER_NO_SUCH_USER, 
                            ER(ER_NO_SUCH_USER),
                            definer.user.str, definer.host.str);
      }
      else
      {
        if (thd->security_ctx->master_access & SUPER_ACL)
        {
          my_error(ER_NO_SUCH_USER, MYF(0), definer.user.str, definer.host.str);

        }
        else
        {
           my_error(ER_ACCESS_DENIED_ERROR, MYF(0),
                    thd->security_ctx->priv_user,
                    thd->security_ctx->priv_host,
                    (thd->password ?  ER(ER_YES) : ER(ER_NO)));
        }
        DBUG_RETURN(TRUE);
      }
    }
  }
  DBUG_RETURN(FALSE);
}
#endif


/*
  Find security context of current view

  SYNOPSIS
    TABLE_LIST::find_view_security_context()
    thd                  [in] thread handler

*/

#ifndef NO_EMBEDDED_ACCESS_CHECKS
Security_context *TABLE_LIST::find_view_security_context(THD *thd)
{
  Security_context *sctx;
  TABLE_LIST *upper_view= this;
  DBUG_ENTER("TABLE_LIST::find_view_security_context");

  DBUG_ASSERT(view);
  while (upper_view && !upper_view->view_suid)
  {
    DBUG_ASSERT(!upper_view->prelocking_placeholder);
    upper_view= upper_view->referencing_view;
  }
  if (upper_view)
  {
    DBUG_PRINT("info", ("Securety context of view %s will be used",
                        upper_view->alias));
    sctx= upper_view->view_sctx;
    DBUG_ASSERT(sctx);
  }
  else
  {
    DBUG_PRINT("info", ("Current global context will be used"));
    sctx= thd->security_ctx;
  }
  DBUG_RETURN(sctx);
}
#endif


/*
  Prepare security context and load underlying tables priveleges for view

  SYNOPSIS
    TABLE_LIST::prepare_security()
    thd                  [in] thread handler

  RETURN
    FALSE  OK
    TRUE   Error
*/

bool TABLE_LIST::prepare_security(THD *thd)
{
  List_iterator_fast<TABLE_LIST> tb(*view_tables);
  TABLE_LIST *tbl;
  DBUG_ENTER("TABLE_LIST::prepare_security");
#ifndef NO_EMBEDDED_ACCESS_CHECKS
  Security_context *save_security_ctx= thd->security_ctx;

  DBUG_ASSERT(!prelocking_placeholder);
  if (prepare_view_securety_context(thd))
    DBUG_RETURN(TRUE);
  thd->security_ctx= find_view_security_context(thd);
  while ((tbl= tb++))
  {
    DBUG_ASSERT(tbl->referencing_view);
    char *local_db, *local_table_name;
    if (tbl->view)
    {
      local_db= tbl->view_db.str;
      local_table_name= tbl->view_name.str;
    }
    else
    {
      local_db= tbl->db;
      local_table_name= tbl->table_name;
    }
    fill_effective_table_privileges(thd, &tbl->grant, local_db,
                                    local_table_name);
    if (tbl->table)
      tbl->table->grant= grant;
  }
  thd->security_ctx= save_security_ctx;
#else
  while ((tbl= tb++))
    tbl->grant.privilege= ~NO_ACCESS;
#endif
  DBUG_RETURN(FALSE);
}


Natural_join_column::Natural_join_column(Field_translator *field_param,
                                         TABLE_LIST *tab)
{
  DBUG_ASSERT(tab->field_translation);
  view_field= field_param;
  table_field= NULL;
  table_ref= tab;
  is_common= FALSE;
}


Natural_join_column::Natural_join_column(Item_field *field_param,
                                         TABLE_LIST *tab)
{
  DBUG_ASSERT(tab->table == field_param->field->table);
  table_field= field_param;
  view_field= NULL;
  table_ref= tab;
  is_common= FALSE;
}


const char *Natural_join_column::name()
{
  if (view_field)
  {
    DBUG_ASSERT(table_field == NULL);
    return view_field->name;
  }

  return table_field->field_name;
}


Item *Natural_join_column::create_item(THD *thd)
{
  if (view_field)
  {
    DBUG_ASSERT(table_field == NULL);
    return create_view_field(thd, table_ref, &view_field->item,
                             view_field->name);
  }
  return table_field;
}


Field *Natural_join_column::field()
{
  if (view_field)
  {
    DBUG_ASSERT(table_field == NULL);
    return NULL;
  }
  return table_field->field;
}


const char *Natural_join_column::table_name()
{
  DBUG_ASSERT(table_ref);
  return table_ref->alias;
}


const char *Natural_join_column::db_name()
{
  if (view_field)
    return table_ref->view_db.str;

  /*
    Test that TABLE_LIST::db is the same as st_table_share::db to
    ensure consistency. An exception are I_S schema tables, which
    are inconsistent in this respect.
  */
  DBUG_ASSERT(!strcmp(table_ref->db,
                      table_ref->table->s->db.str) ||
              (table_ref->schema_table &&
               table_ref->table->s->db.str[0] == 0));
  return table_ref->db;
}


GRANT_INFO *Natural_join_column::grant()
{
  if (view_field)
    return &(table_ref->grant);
  return &(table_ref->table->grant);
}


void Field_iterator_view::set(TABLE_LIST *table)
{
  DBUG_ASSERT(table->field_translation);
  view= table;
  ptr= table->field_translation;
  array_end= table->field_translation_end;
}


const char *Field_iterator_table::name()
{
  return (*ptr)->field_name;
}


Item *Field_iterator_table::create_item(THD *thd)
{
  SELECT_LEX *select= thd->lex->current_select;

  Item_field *item= new Item_field(thd, &select->context, *ptr);
  if (item && thd->variables.sql_mode & MODE_ONLY_FULL_GROUP_BY &&
      !thd->lex->in_sum_func && select->cur_pos_in_select_list != UNDEF_POS)
  {
    select->non_agg_fields.push_back(item);
    item->marker= select->cur_pos_in_select_list;
  }
  return item;
}


const char *Field_iterator_view::name()
{
  return ptr->name;
}


Item *Field_iterator_view::create_item(THD *thd)
{
  return create_view_field(thd, view, &ptr->item, ptr->name);
}

Item *create_view_field(THD *thd, TABLE_LIST *view, Item **field_ref,
                        const char *name)
{
  bool save_wrapper= thd->lex->select_lex.no_wrap_view_item;
  Item *field= *field_ref;
  DBUG_ENTER("create_view_field");

  if (view->schema_table_reformed)
  {
    /*
      Translation table items are always Item_fields and already fixed
      ('mysql_schema_table' function). So we can return directly the
      field. This case happens only for 'show & where' commands.
    */
    DBUG_ASSERT(field && field->fixed);
    DBUG_RETURN(field);
  }

  DBUG_ASSERT(field);
  thd->lex->current_select->no_wrap_view_item= TRUE;
  if (!field->fixed)
  {
    if (field->fix_fields(thd, field_ref))
    {
      thd->lex->current_select->no_wrap_view_item= save_wrapper;
      DBUG_RETURN(0);
    }
    field= *field_ref;
  }
  thd->lex->current_select->no_wrap_view_item= save_wrapper;
  if (save_wrapper)
  {
    DBUG_RETURN(field);
  }
  Item *item= new Item_direct_view_ref(&view->view->select_lex.context,
                                       field_ref, view->alias,
                                       name);
  DBUG_RETURN(item);
}


void Field_iterator_natural_join::set(TABLE_LIST *table_ref)
{
  DBUG_ASSERT(table_ref->join_columns);
  column_ref_it.init(*(table_ref->join_columns));
  cur_column_ref= column_ref_it++;
}


void Field_iterator_natural_join::next()
{
  cur_column_ref= column_ref_it++;
  DBUG_ASSERT(!cur_column_ref || ! cur_column_ref->table_field ||
              cur_column_ref->table_ref->table ==
              cur_column_ref->table_field->field->table);
}


void Field_iterator_table_ref::set_field_iterator()
{
  DBUG_ENTER("Field_iterator_table_ref::set_field_iterator");
  /*
    If the table reference we are iterating over is a natural join, or it is
    an operand of a natural join, and TABLE_LIST::join_columns contains all
    the columns of the join operand, then we pick the columns from
    TABLE_LIST::join_columns, instead of the  orginial container of the
    columns of the join operator.
  */
  if (table_ref->is_join_columns_complete)
  {
    /* Necesary, but insufficient conditions. */
    DBUG_ASSERT(table_ref->is_natural_join ||
                table_ref->nested_join ||
                table_ref->join_columns &&
                /* This is a merge view. */
                ((table_ref->field_translation &&
                  table_ref->join_columns->elements ==
                  (ulong)(table_ref->field_translation_end -
                          table_ref->field_translation)) ||
                 /* This is stored table or a tmptable view. */
                 (!table_ref->field_translation &&
                  table_ref->join_columns->elements ==
                  table_ref->table->s->fields)));
    field_it= &natural_join_it;
    DBUG_PRINT("info",("field_it for '%s' is Field_iterator_natural_join",
                       table_ref->alias));
  }
  /* This is a merge view, so use field_translation. */
  else if (table_ref->field_translation)
  {
    DBUG_ASSERT(table_ref->view &&
                table_ref->effective_algorithm == VIEW_ALGORITHM_MERGE);
    field_it= &view_field_it;
    DBUG_PRINT("info", ("field_it for '%s' is Field_iterator_view",
                        table_ref->alias));
  }
  /* This is a base table or stored view. */
  else
  {
    DBUG_ASSERT(table_ref->table || table_ref->view);
    field_it= &table_field_it;
    DBUG_PRINT("info", ("field_it for '%s' is Field_iterator_table",
                        table_ref->alias));
  }
  field_it->set(table_ref);
  DBUG_VOID_RETURN;
}


void Field_iterator_table_ref::set(TABLE_LIST *table)
{
  DBUG_ASSERT(table);
  first_leaf= table->first_leaf_for_name_resolution();
  last_leaf=  table->last_leaf_for_name_resolution();
  DBUG_ASSERT(first_leaf && last_leaf);
  table_ref= first_leaf;
  set_field_iterator();
}


void Field_iterator_table_ref::next()
{
  /* Move to the next field in the current table reference. */
  field_it->next();
  /*
    If all fields of the current table reference are exhausted, move to
    the next leaf table reference.
  */
  if (field_it->end_of_fields() && table_ref != last_leaf)
  {
    table_ref= table_ref->next_name_resolution_table;
    DBUG_ASSERT(table_ref);
    set_field_iterator();
  }
}


const char *Field_iterator_table_ref::get_table_name()
{
  if (table_ref->view)
    return table_ref->view_name.str;
  else if (table_ref->is_natural_join)
    return natural_join_it.column_ref()->table_name();

  DBUG_ASSERT(!strcmp(table_ref->table_name,
                      table_ref->table->s->table_name.str));
  return table_ref->table_name;
}


const char *Field_iterator_table_ref::get_db_name()
{
  if (table_ref->view)
    return table_ref->view_db.str;
  else if (table_ref->is_natural_join)
    return natural_join_it.column_ref()->db_name();

  /*
    Test that TABLE_LIST::db is the same as st_table_share::db to
    ensure consistency. An exception are I_S schema tables, which
    are inconsistent in this respect.
  */
  DBUG_ASSERT(!strcmp(table_ref->db, table_ref->table->s->db.str) ||
              (table_ref->schema_table &&
               table_ref->table->s->db.str[0] == 0));

  return table_ref->db;
}


GRANT_INFO *Field_iterator_table_ref::grant()
{
  if (table_ref->view)
    return &(table_ref->grant);
  else if (table_ref->is_natural_join)
    return natural_join_it.column_ref()->grant();
  return &(table_ref->table->grant);
}


/*
  Create new or return existing column reference to a column of a
  natural/using join.

  SYNOPSIS
    Field_iterator_table_ref::get_or_create_column_ref()
    parent_table_ref  the parent table reference over which the
                      iterator is iterating

  DESCRIPTION
    Create a new natural join column for the current field of the
    iterator if no such column was created, or return an already
    created natural join column. The former happens for base tables or
    views, and the latter for natural/using joins. If a new field is
    created, then the field is added to 'parent_table_ref' if it is
    given, or to the original table referene of the field if
    parent_table_ref == NULL.

  NOTES
    This method is designed so that when a Field_iterator_table_ref
    walks through the fields of a table reference, all its fields
    are created and stored as follows:
    - If the table reference being iterated is a stored table, view or
      natural/using join, store all natural join columns in a list
      attached to that table reference.
    - If the table reference being iterated is a nested join that is
      not natural/using join, then do not materialize its result
      fields. This is OK because for such table references
      Field_iterator_table_ref iterates over the fields of the nested
      table references (recursively). In this way we avoid the storage
      of unnecessay copies of result columns of nested joins.

  RETURN
    #     Pointer to a column of a natural join (or its operand)
    NULL  No memory to allocate the column
*/

Natural_join_column *
Field_iterator_table_ref::get_or_create_column_ref(THD *thd, TABLE_LIST *parent_table_ref)
{
  Natural_join_column *nj_col;
  bool is_created= TRUE;
  uint field_count;
  TABLE_LIST *add_table_ref= parent_table_ref ?
                             parent_table_ref : table_ref;
  LINT_INIT(field_count);

  if (field_it == &table_field_it)
  {
    /* The field belongs to a stored table. */
    Field *tmp_field= table_field_it.field();
    Item_field *tmp_item=
      new Item_field(thd, &thd->lex->current_select->context, tmp_field);
    if (!tmp_item)
      return NULL;
    nj_col= new Natural_join_column(tmp_item, table_ref);
    field_count= table_ref->table->s->fields;
  }
  else if (field_it == &view_field_it)
  {
    /* The field belongs to a merge view or information schema table. */
    Field_translator *translated_field= view_field_it.field_translator();
    nj_col= new Natural_join_column(translated_field, table_ref);
    field_count= table_ref->field_translation_end -
                 table_ref->field_translation;
  }
  else
  {
    /*
      The field belongs to a NATURAL join, therefore the column reference was
      already created via one of the two constructor calls above. In this case
      we just return the already created column reference.
    */
    DBUG_ASSERT(table_ref->is_join_columns_complete);
    is_created= FALSE;
    nj_col= natural_join_it.column_ref();
    DBUG_ASSERT(nj_col);
  }
  DBUG_ASSERT(!nj_col->table_field ||
              nj_col->table_ref->table == nj_col->table_field->field->table);

  /*
    If the natural join column was just created add it to the list of
    natural join columns of either 'parent_table_ref' or to the table
    reference that directly contains the original field.
  */
  if (is_created)
  {
    /* Make sure not all columns were materialized. */
    DBUG_ASSERT(!add_table_ref->is_join_columns_complete);
    if (!add_table_ref->join_columns)
    {
      /* Create a list of natural join columns on demand. */
      if (!(add_table_ref->join_columns= new List<Natural_join_column>))
        return NULL;
      add_table_ref->is_join_columns_complete= FALSE;
    }
    add_table_ref->join_columns->push_back(nj_col);
    /*
      If new fields are added to their original table reference, mark if
      all fields were added. We do it here as the caller has no easy way
      of knowing when to do it.
      If the fields are being added to parent_table_ref, then the caller
      must take care to mark when all fields are created/added.
    */
    if (!parent_table_ref &&
        add_table_ref->join_columns->elements == field_count)
      add_table_ref->is_join_columns_complete= TRUE;
  }

  return nj_col;
}


/*
  Return an existing reference to a column of a natural/using join.

  SYNOPSIS
    Field_iterator_table_ref::get_natural_column_ref()

  DESCRIPTION
    The method should be called in contexts where it is expected that
    all natural join columns are already created, and that the column
    being retrieved is a Natural_join_column.

  RETURN
    #     Pointer to a column of a natural join (or its operand)
    NULL  No memory to allocate the column
*/

Natural_join_column *
Field_iterator_table_ref::get_natural_column_ref()
{
  Natural_join_column *nj_col;

  DBUG_ASSERT(field_it == &natural_join_it);
  /*
    The field belongs to a NATURAL join, therefore the column reference was
    already created via one of the two constructor calls above. In this case
    we just return the already created column reference.
  */
  nj_col= natural_join_it.column_ref();
  DBUG_ASSERT(nj_col &&
              (!nj_col->table_field ||
               nj_col->table_ref->table == nj_col->table_field->field->table));
  return nj_col;
}

/*****************************************************************************
  Functions to handle column usage bitmaps (read_set, write_set etc...)
*****************************************************************************/

/* Reset all columns bitmaps */

void st_table::clear_column_bitmaps()
{
  /*
    Reset column read/write usage. It's identical to:
    bitmap_clear_all(&table->def_read_set);
    bitmap_clear_all(&table->def_write_set);
  */
  bzero((char*) def_read_set.bitmap, s->column_bitmap_size*2);
  column_bitmaps_set(&def_read_set, &def_write_set);
}


/*
  Tell handler we are going to call position() and rnd_pos() later.
  
  NOTES:
  This is needed for handlers that uses the primary key to find the
  row. In this case we have to extend the read bitmap with the primary
  key fields.
*/

void st_table::prepare_for_position()
{
  DBUG_ENTER("st_table::prepare_for_position");

  if ((file->ha_table_flags() & HA_PRIMARY_KEY_IN_READ_INDEX) &&
      s->primary_key < MAX_KEY)
  {
    mark_columns_used_by_index_no_reset(s->primary_key, read_set);
    /* signal change */
    file->column_bitmaps_signal();
  }
  DBUG_VOID_RETURN;
}


/*
  Mark that only fields from one key is used

  NOTE:
    This changes the bitmap to use the tmp bitmap
    After this, you can't access any other columns in the table until
    bitmaps are reset, for example with st_table::clear_column_bitmaps()
    or st_table::restore_column_maps_after_mark_index()
*/

void st_table::mark_columns_used_by_index(uint index)
{
  MY_BITMAP *bitmap= &tmp_set;
  DBUG_ENTER("st_table::mark_columns_used_by_index");

  (void) file->extra(HA_EXTRA_KEYREAD);
  bitmap_clear_all(bitmap);
  mark_columns_used_by_index_no_reset(index, bitmap);
  column_bitmaps_set(bitmap, bitmap);
  DBUG_VOID_RETURN;
}


/*
  Restore to use normal column maps after key read

  NOTES
    This reverse the change done by mark_columns_used_by_index

  WARNING
    For this to work, one must have the normal table maps in place
    when calling mark_columns_used_by_index
*/

void st_table::restore_column_maps_after_mark_index()
{
  DBUG_ENTER("st_table::restore_column_maps_after_mark_index");

  key_read= 0;
  (void) file->extra(HA_EXTRA_NO_KEYREAD);
  default_column_bitmaps();
  file->column_bitmaps_signal();
  DBUG_VOID_RETURN;
}


/*
  mark columns used by key, but don't reset other fields
*/

void st_table::mark_columns_used_by_index_no_reset(uint index,
                                                   MY_BITMAP *bitmap)
{
  KEY_PART_INFO *key_part= key_info[index].key_part;
  KEY_PART_INFO *key_part_end= (key_part +
                                key_info[index].key_parts);
  for (;key_part != key_part_end; key_part++)
    bitmap_set_bit(bitmap, key_part->fieldnr-1);
}


/*
  Mark auto-increment fields as used fields in both read and write maps

  NOTES
    This is needed in insert & update as the auto-increment field is
    always set and sometimes read.
*/

void st_table::mark_auto_increment_column()
{
  DBUG_ASSERT(found_next_number_field);
  /*
    We must set bit in read set as update_auto_increment() is using the
    store() to check overflow of auto_increment values
  */
  bitmap_set_bit(read_set, found_next_number_field->field_index);
  bitmap_set_bit(write_set, found_next_number_field->field_index);
  if (s->next_number_keypart)
    mark_columns_used_by_index_no_reset(s->next_number_index, read_set);
  file->column_bitmaps_signal();
}


/*
  Mark columns needed for doing an delete of a row

  DESCRIPTON
    Some table engines don't have a cursor on the retrieve rows
    so they need either to use the primary key or all columns to
    be able to delete a row.

    If the engine needs this, the function works as follows:
    - If primary key exits, mark the primary key columns to be read.
    - If not, mark all columns to be read

    If the engine has HA_REQUIRES_KEY_COLUMNS_FOR_DELETE, we will
    mark all key columns as 'to-be-read'. This allows the engine to
    loop over the given record to find all keys and doesn't have to
    retrieve the row again.
*/

void st_table::mark_columns_needed_for_delete()
{
  if (triggers)
    triggers->mark_fields_used(TRG_EVENT_DELETE);
  if (file->ha_table_flags() & HA_REQUIRES_KEY_COLUMNS_FOR_DELETE)
  {
    Field **reg_field;
    for (reg_field= field ; *reg_field ; reg_field++)
    {
      if ((*reg_field)->flags & PART_KEY_FLAG)
        bitmap_set_bit(read_set, (*reg_field)->field_index);
    }
    file->column_bitmaps_signal();
  }
  if (file->ha_table_flags() & HA_PRIMARY_KEY_REQUIRED_FOR_DELETE)
  {
    /*
      If the handler has no cursor capabilites, we have to read either
      the primary key, the hidden primary key or all columns to be
      able to do an delete
    */
    if (s->primary_key == MAX_KEY)
      file->use_hidden_primary_key();
    else
    {
      mark_columns_used_by_index_no_reset(s->primary_key, read_set);
      file->column_bitmaps_signal();
    }
  }
}


/*
  Mark columns needed for doing an update of a row

  DESCRIPTON
    Some engines needs to have all columns in an update (to be able to
    build a complete row). If this is the case, we mark all not
    updated columns to be read.

    If this is no the case, we do like in the delete case and mark
    if neeed, either the primary key column or all columns to be read.
    (see mark_columns_needed_for_delete() for details)

    If the engine has HA_REQUIRES_KEY_COLUMNS_FOR_DELETE, we will
    mark all USED key columns as 'to-be-read'. This allows the engine to
    loop over the given record to find all changed keys and doesn't have to
    retrieve the row again.
*/

void st_table::mark_columns_needed_for_update()
{
  DBUG_ENTER("mark_columns_needed_for_update");
  if (triggers)
    triggers->mark_fields_used(TRG_EVENT_UPDATE);
  if (file->ha_table_flags() & HA_REQUIRES_KEY_COLUMNS_FOR_DELETE)
  {
    /* Mark all used key columns for read */
    Field **reg_field;
    for (reg_field= field ; *reg_field ; reg_field++)
    {
      /* Merge keys is all keys that had a column refered to in the query */
      if (merge_keys.is_overlapping((*reg_field)->part_of_key))
        bitmap_set_bit(read_set, (*reg_field)->field_index);
    }
    file->column_bitmaps_signal();
  }
  if (file->ha_table_flags() & HA_PRIMARY_KEY_REQUIRED_FOR_DELETE)
  {
    /*
      If the handler has no cursor capabilites, we have to read either
      the primary key, the hidden primary key or all columns to be
      able to do an update
    */
    if (s->primary_key == MAX_KEY)
      file->use_hidden_primary_key();
    else
    {
      mark_columns_used_by_index_no_reset(s->primary_key, read_set);
      file->column_bitmaps_signal();
    }
  }
  DBUG_VOID_RETURN;
}


/*
  Mark columns the handler needs for doing an insert

  For now, this is used to mark fields used by the trigger
  as changed.
*/

void st_table::mark_columns_needed_for_insert()
{
  if (triggers)
  {
    /*
      We don't need to mark columns which are used by ON DELETE and
      ON UPDATE triggers, which may be invoked in case of REPLACE or
      INSERT ... ON DUPLICATE KEY UPDATE, since before doing actual
      row replacement or update write_record() will mark all table
      fields as used.
    */
    triggers->mark_fields_used(TRG_EVENT_INSERT);
  }
  if (found_next_number_field)
    mark_auto_increment_column();
}


/**
  @brief Check if this is part of a MERGE table with attached children.

  @return       status
    @retval     TRUE            children are attached
    @retval     FALSE           no MERGE part or children not attached

  @detail
    A MERGE table consists of a parent TABLE and zero or more child
    TABLEs. Each of these TABLEs is called a part of a MERGE table.
*/

bool st_table::is_children_attached(void)
{
  return((child_l && children_attached) ||
         (parent && parent->children_attached));
}

/*
  Cleanup this table for re-execution.

  SYNOPSIS
    TABLE_LIST::reinit_before_use()
*/

void TABLE_LIST::reinit_before_use(THD *thd)
{
  /*
    Reset old pointers to TABLEs: they are not valid since the tables
    were closed in the end of previous prepare or execute call.
  */
  table= 0;
  /* Reset is_schema_table_processed value(needed for I_S tables */
  schema_table_state= NOT_PROCESSED;

  TABLE_LIST *embedded; /* The table at the current level of nesting. */
  TABLE_LIST *parent_embedding= this; /* The parent nested table reference. */
  do
  {
    embedded= parent_embedding;
    if (embedded->prep_on_expr)
      embedded->on_expr= embedded->prep_on_expr->copy_andor_structure(thd);
    parent_embedding= embedded->embedding;
  }
  while (parent_embedding &&
         parent_embedding->nested_join->join_list.head() == embedded);
}

/*
  Return subselect that contains the FROM list this table is taken from

  SYNOPSIS
    TABLE_LIST::containing_subselect()
 
  RETURN
    Subselect item for the subquery that contains the FROM list
    this table is taken from if there is any
    0 - otherwise

*/

Item_subselect *TABLE_LIST::containing_subselect()
{    
  return (select_lex ? select_lex->master_unit()->item : 0);
}

/*
  Compiles the tagged hints list and fills up the bitmasks.

  SYNOPSIS
    process_index_hints()
      table         the TABLE to operate on.

  DESCRIPTION
    The parser collects the index hints for each table in a "tagged list" 
    (TABLE_LIST::index_hints). Using the information in this tagged list
    this function sets the members st_table::keys_in_use_for_query, 
    st_table::keys_in_use_for_group_by, st_table::keys_in_use_for_order_by,
    st_table::force_index and st_table::covering_keys.

    Current implementation of the runtime does not allow mixing FORCE INDEX
    and USE INDEX, so this is checked here. Then the FORCE INDEX list 
    (if non-empty) is appended to the USE INDEX list and a flag is set.

    Multiple hints of the same kind are processed so that each clause 
    is applied to what is computed in the previous clause.
    For example:
        USE INDEX (i1) USE INDEX (i2)
    is equivalent to
        USE INDEX (i1,i2)
    and means "consider only i1 and i2".
        
    Similarly
        USE INDEX () USE INDEX (i1)
    is equivalent to
        USE INDEX (i1)
    and means "consider only the index i1"

    It is OK to have the same index several times, e.g. "USE INDEX (i1,i1)" is
    not an error.
        
    Different kind of hints (USE/FORCE/IGNORE) are processed in the following
    order:
      1. All indexes in USE (or FORCE) INDEX are added to the mask.
      2. All IGNORE INDEX

    e.g. "USE INDEX i1, IGNORE INDEX i1, USE INDEX i1" will not use i1 at all
    as if we had "USE INDEX i1, USE INDEX i1, IGNORE INDEX i1".

    As an optimization if there is a covering index, and we have 
    IGNORE INDEX FOR GROUP/ORDER, and this index is used for the JOIN part, 
    then we have to ignore the IGNORE INDEX FROM GROUP/ORDER.

  RETURN VALUE
    FALSE                no errors found
    TRUE                 found and reported an error.
*/
bool TABLE_LIST::process_index_hints(TABLE *tbl)
{
  /* initialize the result variables */
  tbl->keys_in_use_for_query= tbl->keys_in_use_for_group_by= 
    tbl->keys_in_use_for_order_by= tbl->s->keys_in_use;

  /* index hint list processing */
  if (index_hints)
  {
    key_map index_join[INDEX_HINT_FORCE + 1];
    key_map index_order[INDEX_HINT_FORCE + 1];
    key_map index_group[INDEX_HINT_FORCE + 1];
    Index_hint *hint;
    int type;
    bool have_empty_use_join= FALSE, have_empty_use_order= FALSE, 
         have_empty_use_group= FALSE;
    List_iterator <Index_hint> iter(*index_hints);

    /* initialize temporary variables used to collect hints of each kind */
    for (type= INDEX_HINT_IGNORE; type <= INDEX_HINT_FORCE; type++)
    {
      index_join[type].clear_all();
      index_order[type].clear_all();
      index_group[type].clear_all();
    }

    /* iterate over the hints list */
    while ((hint= iter++))
    {
      uint pos;

      /* process empty USE INDEX () */
      if (hint->type == INDEX_HINT_USE && !hint->key_name.str)
      {
        if (hint->clause & INDEX_HINT_MASK_JOIN)
        {
          index_join[hint->type].clear_all();
          have_empty_use_join= TRUE;
        }
        if (hint->clause & INDEX_HINT_MASK_ORDER)
        {
          index_order[hint->type].clear_all();
          have_empty_use_order= TRUE;
        }
        if (hint->clause & INDEX_HINT_MASK_GROUP)
        {
          index_group[hint->type].clear_all();
          have_empty_use_group= TRUE;
        }
        continue;
      }

      /* 
        Check if an index with the given name exists and get his offset in 
        the keys bitmask for the table 
      */
      if (tbl->s->keynames.type_names == 0 ||
          (pos= find_type(&tbl->s->keynames, hint->key_name.str,
                          hint->key_name.length, 1)) <= 0)
      {
        my_error(ER_KEY_DOES_NOT_EXITS, MYF(0), hint->key_name.str, alias);
        return 1;
      }

      pos--;

      /* add to the appropriate clause mask */
      if (hint->clause & INDEX_HINT_MASK_JOIN)
        index_join[hint->type].set_bit (pos);
      if (hint->clause & INDEX_HINT_MASK_ORDER)
        index_order[hint->type].set_bit (pos);
      if (hint->clause & INDEX_HINT_MASK_GROUP)
        index_group[hint->type].set_bit (pos);
    }

    /* cannot mix USE INDEX and FORCE INDEX */
    if ((!index_join[INDEX_HINT_FORCE].is_clear_all() ||
         !index_order[INDEX_HINT_FORCE].is_clear_all() ||
         !index_group[INDEX_HINT_FORCE].is_clear_all()) &&
        (!index_join[INDEX_HINT_USE].is_clear_all() ||  have_empty_use_join ||
         !index_order[INDEX_HINT_USE].is_clear_all() || have_empty_use_order ||
         !index_group[INDEX_HINT_USE].is_clear_all() || have_empty_use_group))
    {
      my_error(ER_WRONG_USAGE, MYF(0), index_hint_type_name[INDEX_HINT_USE],
               index_hint_type_name[INDEX_HINT_FORCE]);
      return 1;
    }

    /* process FORCE INDEX as USE INDEX with a flag */
    if (!index_join[INDEX_HINT_FORCE].is_clear_all() ||
        !index_order[INDEX_HINT_FORCE].is_clear_all() ||
        !index_group[INDEX_HINT_FORCE].is_clear_all())
    {
      tbl->force_index= TRUE;
      index_join[INDEX_HINT_USE].merge(index_join[INDEX_HINT_FORCE]);
      index_order[INDEX_HINT_USE].merge(index_order[INDEX_HINT_FORCE]);
      index_group[INDEX_HINT_USE].merge(index_group[INDEX_HINT_FORCE]);
    }

    /* apply USE INDEX */
    if (!index_join[INDEX_HINT_USE].is_clear_all() || have_empty_use_join)
      tbl->keys_in_use_for_query.intersect(index_join[INDEX_HINT_USE]);
    if (!index_order[INDEX_HINT_USE].is_clear_all() || have_empty_use_order)
      tbl->keys_in_use_for_order_by.intersect (index_order[INDEX_HINT_USE]);
    if (!index_group[INDEX_HINT_USE].is_clear_all() || have_empty_use_group)
      tbl->keys_in_use_for_group_by.intersect (index_group[INDEX_HINT_USE]);

    /* apply IGNORE INDEX */
    tbl->keys_in_use_for_query.subtract (index_join[INDEX_HINT_IGNORE]);
    tbl->keys_in_use_for_order_by.subtract (index_order[INDEX_HINT_IGNORE]);
    tbl->keys_in_use_for_group_by.subtract (index_group[INDEX_HINT_IGNORE]);
  }

  /* make sure covering_keys don't include indexes disabled with a hint */
  tbl->covering_keys.intersect(tbl->keys_in_use_for_query);
  return 0;
}


size_t max_row_length(TABLE *table, const uchar *data)
{
  TABLE_SHARE *table_s= table->s;
  size_t length= table_s->reclength + 2 * table_s->fields;
  uint *const beg= table_s->blob_field;
  uint *const end= beg + table_s->blob_fields;

  for (uint *ptr= beg ; ptr != end ; ++ptr)
  {
    Field_blob* const blob= (Field_blob*) table->field[*ptr];
    length+= blob->get_length((const uchar*)
                              (data + blob->offset(table->record[0]))) +
      HA_KEY_BLOB_LENGTH;
  }
  return length;
}

/*****************************************************************************
** Instansiate templates
*****************************************************************************/

#ifdef HAVE_EXPLICIT_TEMPLATE_INSTANTIATION
template class List<String>;
template class List_iterator<String>;
#endif
