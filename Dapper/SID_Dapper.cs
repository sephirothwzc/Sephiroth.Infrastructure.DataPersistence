using DapperExtensions.Mapper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Sephiroth.Infrastructure.Common.Enums;
using Sephiroth.Infrastructure.Common.Result;
using MySql.Data.MySqlClient;
using Oracle.ManagedDataAccess.Client;
using Newtonsoft.Json;
using System.ComponentModel;


/*************************************************************************************
   * CLR 版本：       4.0.30319.42000
   * 类 名 称：       SID_Dapper
   * 机器名称：       BE27
   * 命名空间：       Sephiroth.Infrastructure.DataPersistence.Dapper
   * 文 件 名：       SID_Dapper
   * 创建时间：       2017/5/18 上午9:49:02
   * 作    者：       吴占超
   * 说    明：        
   * 修改时间：
   * 修 改 人：
  *************************************************************************************/

namespace Sephiroth.Infrastructure.DataPersistence.Dapper
{
    /// <summary>
    /// dapper 数据持久层
    /// </summary>
    public class SID_Dapper
    {
        /// <summary>
        /// 异常日志委托
        /// </summary>
        private Action<string> errorlog;

        /// <summary>
        /// Dapper构造方法
        /// </summary>
        /// <param name="d">数据库配置对象</param>
        /// <param name="e">异常日志委托</param>
        public SID_Dapper(DBcon d, Action<string> e = null)
        {
            this.db = d;
            this.errorlog = e;
        }

        #region 数据库基础设置
        /// <summary>
        /// 数据库配置参数
        /// </summary>
        public DBcon db { get; set; }

        private static string mysql_connstr = @"Host={0};UserName={1};Password={2};Database={3};Port={4};CharSet=utf8;Allow Zero Datetime=true;default command timeout={5}";
        /// <summary>
        /// 数据库连接格式化字符串MySql
        /// </summary>
        private static string MySql_connstr
        {
            get
            {
                return mysql_connstr;
            }
            set { mysql_connstr = value; }
        }

        private static string mssql_connstr = @"Data Source = {0},{4};Initial Catalog = {3};User Id = {1};Password = {2};";
        /// <summary>
        /// 数据库连接格式化字符串MsSql
        /// </summary>
        private static string MsSql_connstr
        {
            get
            {
                return mssql_connstr;
            }
            set
            {
                mssql_connstr = value;
            }
        }

        private static string oracle_connstr = @"DATA SOURCE={0}:{4}/{3};PASSWORD={2};PERSIST SECURITY INFO=True;USER ID={1};";
        /// <summary>
        /// 数据库连接格式化字符串Oracle
        /// </summary>
        private static string Oracle_connstr { get { return oracle_connstr; } set { oracle_connstr = value; } }

        private Dictionary<DBcon.dbtype, string> di_con = new Dictionary<DBcon.dbtype, string>();
        /// <summary>
        /// 数据库连接字符串及配置参数集合
        /// </summary>
        public Dictionary<DBcon.dbtype, string> Di_con
        {
            get
            {
                if (di_con.Count <= 0)
                {
                    di_con.Add(DBcon.dbtype.MsSql, MsSql_connstr);
                    di_con.Add(DBcon.dbtype.MySql, MySql_connstr);
                    di_con.Add(DBcon.dbtype.Oracle, Oracle_connstr);
                }
                return di_con;
            }
        }

        /// <summary>
        /// 创建DbConnection
        /// </summary>
        /// <returns></returns>
        public IDbConnection GetSqlConnection()
        {
            var conn = string.Format(Di_con[this.db.dbType].ToString(), db.dbaddress, db.dbusername, db.dbpassword, db.dbname, db.Port, db.timeout);
            DapperExtensions.DapperExtensions.DefaultMapper = typeof(ClassMapper<>);
            if (DBcon.dbtype.MySql.Equals(this.db.dbType))
            {// MySql数据库
                DapperExtensions.DapperExtensions.SqlDialect = new DapperExtensions.Sql.MySqlDialect();
                return new MySqlConnection(conn);
            }
            else if (DBcon.dbtype.Oracle.Equals(this.db.dbType))
            {// Oracle数据库
                DapperExtensions.DapperExtensions.SqlDialect = new DapperExtensions.Sql.OracleDialect();
                return new OracleConnection(conn);
            }
            // MsSql数据库
            DapperExtensions.DapperExtensions.SqlDialect = new DapperExtensions.Sql.SqlServerDialect();
            return new SqlConnection(conn);
        }
        #endregion

        #region 无返回值事务执行
        /// <summary>
        /// 事务执行操作 无返回值
        /// </summary>
        /// <param name="action">要执行的委托</param>
        /// <returns></returns>
        public SIC_Result Execute(Action<IDbConnection, IDbTransaction> action)
        {
            using (IDbConnection cn = this.GetSqlConnection())
            {
                // 打开数据库连接
                cn.Open();
                // 开启事务
                IDbTransaction tran = cn.BeginTransaction();
                try
                {
                    // 执行委托
                    action.Invoke(cn, tran);
                    // 事务提交
                    tran.Commit();
                }
                catch (Exception exception)
                {
                    // 异常事务回滚
                    tran.Rollback();
                    // 执行异常日志委托，记录错误日志
                    if (errorlog != null)
                        errorlog.Invoke(exception.Message + exception.StackTrace);
                    throw exception;
                }
                finally
                {
                    cn.Close();
                }

            }
            return new SIC_Result
            {
                State = SIC_Result.e_state.成功,
            };
        }
        #endregion

        #region 事务执行操作 根据function返回值 判断是否提交事务
        /// <summary>
        /// 事务执行操作 根据function返回值 判断是否提交事务
        /// </summary>
        /// <param name="action">要执行的委托</param>
        /// <returns></returns>
        public SIC_Result Execute(Func<IDbConnection, IDbTransaction, SIC_Result> action)
        {
            SIC_Result rd = new SIC_Result();
            using (IDbConnection cn = this.GetSqlConnection())
            {
                // 打开数据库连接
                cn.Open();
                // 开启事务
                IDbTransaction tran = cn.BeginTransaction();
                try
                {
                    // 执行委托
                    rd = action.Invoke(cn, tran);
                    if (SIC_Result.e_state.成功.Equals(rd.State))
                        tran.Commit();
                    else
                        tran.Rollback();
                }
                catch (Exception exception)
                {
                    tran.Rollback();
                    if (errorlog != null)
                        errorlog.Invoke(exception.Message + exception.StackTrace);
                    throw exception;
                }
                finally
                {
                    cn.Close();
                }

            }
            return rd;
        }
        #endregion

        #region 自定义返回 自定义提交事务
        /// <summary>
        /// 自定义返回 自定义提交事务
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="action"></param>
        /// <returns></returns>
        public T Execute<T>(Func<IDbConnection, IDbTransaction, T> action)
        {
            T t = default(T);
            using (IDbConnection cn = this.GetSqlConnection())
            {
                cn.Open();
                IDbTransaction tran = cn.BeginTransaction();
                try
                {
                    t = action.Invoke(cn, tran);
                    tran.Commit();
                }
                catch (Exception exception)
                {
                    tran.Rollback();
                    if (errorlog != null)
                        errorlog.Invoke(exception.Message + exception.StackTrace);
                    throw exception;
                }
                finally
                {
                    cn.Close();
                }

            }
            return t;
        }
        #endregion

        #region Execute Action
        /// <summary>
        /// Execute the specified action.
        /// </summary>
        /// <returns>The execute.</returns>
        /// <param name="action">Action.</param>
        public SIC_Result Execute(Action<IDbConnection> action)
        {
            using (IDbConnection cn = this.GetSqlConnection())
            {
                cn.Open();
                try
                {
                    action.Invoke(cn);
                }
                catch (Exception exception)
                {
                    if (errorlog != null)
                        errorlog.Invoke(exception.Message + exception.StackTrace);
                    throw exception;
                }
                finally
                {
                    cn.Close();
                }

            }
            return new SIC_Result
            {
                State = SIC_Result.e_state.成功
            };
        }
        #endregion

        #region Execute Func
        /// <summary>
        /// 无事务执行委托 有返回值
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="action"></param>
        /// <returns></returns>
        public T Execute<T>(Func<IDbConnection, T> action)
        {
            // 初始化返回接收对象t，default(T) 可以对值类型、引用类型做对应的初始化
            T t = default(T);
            using (IDbConnection cn = this.GetSqlConnection())
            {
                cn.Open();
                try
                {
                    t = action.Invoke(cn);
                }
                catch (Exception exception)
                {
                    string error = JsonConvert.SerializeObject(action.Target);
                    if (errorlog != null)
                        errorlog.Invoke(error + ":\r\n" + exception.Message + exception.StackTrace);
#if DEBUG
                    var ex = new Exception(error + ":\r\n" + exception.Message, exception);
#endif
#if !DEBUG
                    var ex = new Exception(exception.Message, exception);
#endif

                    throw ex;
                }
                finally
                {
                    cn.Close();
                }

            }
            return t;
        }
        #endregion

        #region BeginTransaction
        /// <summary>
        /// The tran conn.
        /// </summary>
        internal IDbConnection tran_conn;
        /// <summary>
        /// The itran.
        /// </summary>
        internal IDbTransaction itran;
        /// <summary>
        /// Begins the transaction.
        /// </summary>
        public void BeginTransaction()
        {
            if (tran_conn != null)
                throw new Exception("事务已经创建，请勿重复开启！");
            tran_conn = this.GetSqlConnection();
            tran_conn.Open();
            itran = tran_conn.BeginTransaction();
        }

        #endregion
    }

    /// <summary>
    /// 数据库连接配置类
    /// </summary>
    public class DBcon
    {
        /// <summary>
        /// 枚举-数据库类型
        /// </summary>
        public enum dbtype
        {
            MsSql,
            MySql,
            Oracle,
        }

        private dbtype _dbtype = dbtype.MsSql;
        /// <summary>
        /// 数据库类型（默认MsSql）
        /// </summary>
        public dbtype dbType { get { return _dbtype; } set { _dbtype = value; } }
        /// <summary>
        /// 数据库ip地址
        /// </summary>
        public string dbaddress { get; set; }
        /// <summary>
        /// 连接用户名
        /// </summary>
        public string dbusername { get; set; }
        /// <summary>
        /// 连接密码
        /// </summary>
        public string dbpassword { get; set; }
        /// <summary>
        /// 数据库名称
        /// </summary>
        public string dbname { get; set; }

        private int port = 3306;
        /// <summary>
        /// 数据库端口（默认3306）
        /// </summary>
        public int Port { get { return port; } set { port = value; } }

        private int _timeout = 3000;
        /// <summary>
        /// 数据库连接超时设置（默认3000秒）
        /// </summary>
        public int timeout { get { return _timeout; } set { _timeout = value; } }

    }
}
