using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using Dapper;
using DapperExtensions;
using DapperExtensions.Mapper;
using Sephiroth.Infrastructure.Common.Enums;
using Sephiroth.Infrastructure.Common.Log4Net;
using Sephiroth.Infrastructure.Common.Attributes;

namespace Sephiroth.Infrastructure.DataPersistence.Dapper
{
    public class SID_DAO<T> where T : class
    {
        public SID_DAO()
        {
        }


        #region Query
        /// <summary>
        /// 查询sql
        /// </summary>
        /// <param name="key"></param>
        /// <param name="sql"></param>
        /// <returns></returns>
        public KeyValuePair<string, IEnumerable<M>> Query<M>(string key, string sql, object param = null)
        {
            return this.SIDDapper.Execute((cn) =>
            {
                var data = this.Query<M>(sql, param);
                return new KeyValuePair<string, IEnumerable<M>>(key, data);
            });
        }

        /// <summary>
        /// 根据sql脚本返回泛型list,以及对应名称，推荐异步使用
        /// </summary>
        /// <returns>The sql.</returns>
        /// <param name="key">Key.</param>
        /// <param name="sql">Sql.</param>
        public KeyValuePair<string, IEnumerable<T>> Query(string key, string sql, object param = null)
        {
            return this.Query<T>(key, sql, param);
        }

        /// <summary>
        /// 根据sql脚本返回泛型list
        /// </summary>
        /// <returns>The sql.</returns>
        /// <param name="sql">Sql.</param>
        /// <param name="param">Parameter.</param>
        /// <typeparam name="ET">The 1st type parameter.</typeparam>
        public IEnumerable<M> Query<M>(string sql, object param = null)
        {
            if (this.SIDDapper.tran_conn != null && this.siddapper.itran != null)
                return this.siddapper.tran_conn.Query<M>(sql, param, this.siddapper.itran);

            return this.SIDDapper.Execute((cn) =>
            {
                return cn.Query<M>(sql, param);
            });
        }

        /// <summary>
        /// 根据sql脚本返回泛型list
        /// </summary>
        /// <returns>The sql.</returns>
        /// <param name="sql">Sql.</param>
        /// <param name="param">Parameter.</param>
        /// <typeparam name="ET">The 1st type parameter.</typeparam>
        public M QueryFirstOrDefault<M>(string sql, object param = null)
        {
            if (this.SIDDapper.tran_conn != null && this.siddapper.itran != null)
                return this.siddapper.tran_conn.QueryFirstOrDefault<M>(sql, param, this.siddapper.itran);

            return this.SIDDapper.Execute((cn) =>
            {
                return cn.QueryFirstOrDefault<M>(sql, param);
            });
        }


        /// <summary>
        /// 根据sql脚本返回泛型list
        /// </summary>
        /// <returns>The sql.</returns>
        /// <param name="sql">Sql.</param>
        /// <param name="param">Parameter.</param>
		public IEnumerable<T> Query(string sql, object param = null)
        {
            return this.Query<T>(sql, param);
        }

        /// <summary>
        /// 首行首列值查询
        /// </summary>
        /// <returns>The single sql.</returns>
        /// <param name="sql">Sql.</param>
        /// <param name="param">Parameter.</param>
        /// <typeparam name="ET">The 1st type parameter.</typeparam>
        public M QuerySingle<M>(string sql, object param = null)
        {
            if (this.SIDDapper.tran_conn != null && this.siddapper.itran != null)
                return this.siddapper.tran_conn.QuerySingle<M>(sql, param, this.siddapper.itran);

            return this.SIDDapper.Execute((cn) =>
            {
                return cn.QuerySingle<M>(sql, param);
            });
        }


        /// <summary>
        /// 首行首列值查询
        /// </summary>
        /// <returns>The single sql.</returns>
        /// <param name="sql">Sql.</param>
        /// <param name="param">Parameter.</param>
        public object QuerySingle(string sql, object param = null)
        {
            return this.QuerySingle<object>(sql, param);
        }


        /// <summary>
        /// Queries the multiple.
        /// </summary>
        /// <returns>The multiple.</returns>
        /// <param name="sql">Sql.</param>
        /// <param name="param">Parameter.</param>
        /// <typeparam name="M">The 1st type parameter.</typeparam>
        public void QueryMultiple(Action<SqlMapper.GridReader> get,string sql,object param = null)
        {
            
			if (this.SIDDapper.tran_conn != null && this.siddapper.itran != null)
            {
                var gr = this.siddapper.tran_conn.QueryMultiple(sql, param, this.siddapper.itran);
                if (get != null)
                    get.Invoke(gr);
            }
				
            
            this.SIDDapper.Execute(cn => 
            {
                var gr = cn.QueryMultiple(sql, param);
                if (get != null)
                    get.Invoke(gr);
            });
        }


        /// <summary>
        /// Queries the multiple.
        /// </summary>
        /// <returns>The multiple.</returns>
        /// <param name="sql">Sql.</param>
        /// <param name="param">Parameter.</param>
        /// <typeparam name="M">The 1st type parameter.</typeparam>
        public M QueryMultiple<M>(Func<SqlMapper.GridReader,M> get, string sql, object param = null)
        {

            if (this.SIDDapper.tran_conn != null && this.siddapper.itran != null)
            {
                var gr = this.siddapper.tran_conn.QueryMultiple(sql, param, this.siddapper.itran);
                return get.Invoke(gr);
            }


            return this.SIDDapper.Execute(cn =>
            {
                var gr = cn.QueryMultiple(sql, param);
                return get.Invoke(gr);
            });
        }
        #endregion

        #region insert update del
        /// <summary>
        /// Insert the specified entity.
        /// </summary>
        /// <returns>The insert.</returns>
        /// <param name="entity">Entity.</param>
        public object Insert(T entity)
        {
            if (this.SIDDapper.tran_conn != null && this.siddapper.itran != null)
                return this.siddapper.tran_conn.Insert(entity, this.siddapper.itran);

            return this.SIDDapper.Execute((cn) =>
            {
                return cn.Insert(entity);
            });
        }
        /// <summary>
        /// Insert the specified entity.
        /// </summary>
        /// <returns>The insert.</returns>
        /// <param name="entity">Entity.</param>
        public object Insert(List<T> entity)
        {
            return entity.Select(e => 
            {
                return this.Insert(e);
            }).ToList();

        }

        /// <summary>
        /// Insert the specified entity.
        /// </summary>
        /// <returns>The insert.</returns>
        /// <param name="entity">Entity.</param>
        public object Insert<M>(M entity)where M:class
        {
            if (this.SIDDapper.tran_conn != null && this.siddapper.itran != null)
                return this.siddapper.tran_conn.Insert(entity, this.siddapper.itran);

            return this.SIDDapper.Execute((cn) =>
            {
                return cn.Insert(entity);
            });
        }

        /// <summary>
        /// Insert the specified entity.
        /// </summary>
        /// <returns>The insert.</returns>
        /// <param name="entity">Entity.</param>
        public object Insert<M>(List<M> entity) where M : class
        {
            return entity.Select(e =>
            {
                return this.Insert(e);
            }).ToList();
        }

        /// <summary>
        /// Update the specified entity.
        /// </summary>
        /// <returns>The update.</returns>
        /// <param name="entity">Entity.</param>
		public bool Update(T entity)
        {
            if (this.SIDDapper.tran_conn != null && this.siddapper.itran != null)
                return this.siddapper.tran_conn.Update(entity, this.siddapper.itran);

            return this.SIDDapper.Execute((cn) =>
            {
                return cn.Update(entity);
            });
        }

        /// <summary>
        /// Update the specified entity.
        /// </summary>
        /// <returns>The update.</returns>
        /// <param name="entity">Entity.</param>
        public object Update(List<T> entity)
        {
            return entity.Select(e =>
            {
                return this.Update(e);
            }).ToList();
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="entity"></param>
        /// <returns></returns>
        public bool Update<M>(M entity)where M:class
        {
            if (this.SIDDapper.tran_conn != null && this.siddapper.itran != null)
                return this.siddapper.tran_conn.Update(entity, this.siddapper.itran);

            return this.SIDDapper.Execute((cn) =>
            {
                return cn.Update(entity);
            });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="Listentity"></param>
        /// <returns></returns>
        public object Update<M>(List<M> Listentity) where M : class
        {
            return Listentity.Select(e =>
            {
                return this.Update(e);
            }).ToList();
        }

        /// <summary>
        /// Execute the specified sql and param.
        /// </summary>
        /// <returns>The execute.</returns>
        /// <param name="sql">Sql.</param>
        /// <param name="param">Parameter.</param>
		public int Execute(string sql, object param = null)
        {
            if (this.SIDDapper.tran_conn != null && this.siddapper.itran != null)
                return this.siddapper.tran_conn.Execute(sql, param, this.siddapper.itran);

            return this.SIDDapper.Execute((cn) =>
            {
                return cn.Execute(sql, param);
            });
        }


        /// <summary>
        /// Delete the specified sql.
        /// </summary>
        /// <returns>The delete.</returns>
        /// <param name="sql">Sql.</param>
        public bool Delete(string sql)
        {
            if (this.SIDDapper.tran_conn != null && this.siddapper.itran != null)
                return this.siddapper.tran_conn.Delete(sql, this.siddapper.itran);

            return this.SIDDapper.Execute((cn) =>
            {
                return cn.Delete(sql);
            });
        }

        /// <summary>
        /// Delete the specified entity.
        /// </summary>
        /// <returns>The delete.</returns>
        /// <param name="entity">Entity.</param>
		public bool Delete(object entity)
        {
            if (this.SIDDapper.tran_conn != null && this.siddapper.itran != null)
                return this.siddapper.tran_conn.Delete(entity, this.siddapper.itran);

            return this.SIDDapper.Execute((cn) =>
            {
                return cn.Delete(entity);
            });
        }
        #endregion

        #region conn

        /// <summary>
        /// 获取链接对象
        /// </summary>
        /// <param name="before">appsettings key前缀 默认没有前缀</param>
        /// <returns></returns>
        public DBcon GetDBcon(string before = "")
        {
            return new DBcon
            {
                dbaddress = ConfigurationManager.AppSettings[before + "dbaddress"].ToString(),
                dbname = ConfigurationManager.AppSettings[before + "dbname"].ToString(),
                dbpassword = ConfigurationManager.AppSettings[before + "dbpassword"].ToString(),
                dbType = SIC_Enum.Parse<DBcon.dbtype>(ConfigurationManager.AppSettings[before + "dbType"].ToString()),
                dbusername = ConfigurationManager.AppSettings[before + "dbusername"].ToString(),
                Port = Convert.ToInt32(ConfigurationManager.AppSettings[before + "Port"].ToString() ?? "3306"),
                timeout = Convert.ToInt32(ConfigurationManager.AppSettings[before + "timeout"].ToString() ?? "3000"),
            };
        }

        /// <summary>
        /// 默认当前对象的数据链接前缀 空
        /// </summary>
        [DefaultSettingValue("")]
        public string dbbefore { get; set; }

        /// <summary>
        /// dapper基础类库
        /// </summary>
        private SID_Dapper siddapper;

        /// <summary>
        /// 获取默认对象
        /// </summary>
        /// <value>The SIDD apper.</value>
        public SID_Dapper SIDDapper
        {
            get
            {
                if (siddapper == null)
                    siddapper = new SID_Dapper(GetDBcon(dbbefore),(msg)=> 
                    {
                        SIC_log4net.WriteLog(typeof(T), msg);
                    });
                return siddapper;
            }
        }
        #endregion

        #region sql string
        /// <summary>
        /// 获取查询sql
        /// </summary>
        /// <returns>The sel.</returns>
        /// <param name="coloumns">Coloumns.</param>
        public string StringSel(string coloumns = "")
        {
            var map = DapperExtensions.DapperExtensions.GetMap<T>();
            if (coloumns == "")
                coloumns = string.Join(",", map.Properties.Select(x => x.ColumnName).ToList());
            return string.Format("select {0} from {1} where 1=1 ", coloumns, map.TableName);
        }

        /// <summary>
        /// 获取分页用sql
        /// </summary>
        /// <returns>The paging sql.</returns>
        /// <param name="sql">Sql.</param>
        /// <param name="page">Page.</param>
        /// <param name="resultsPerPage">Results per page.</param>
        //public string GetPagingSql(string sql,int page,int resultsPerPage)
        //{
        //    return DapperExtensions.DapperExtensions.SqlDialect.GetPagingSql(sql, page, resultsPerPage, new Dictionary<string, object>());
        //}
        #endregion

        #region transaction
        /// <summary>
        /// Begins the transaction.
        /// </summary>
        public void BeginTransaction()
        {
            //this.siddapper = new SID_Dapper(GetDBcon(dbbefore));
            //this.siddapper.BeginTransaction();

            this.SIDDapper.BeginTransaction();
        }
        #endregion

        #region transaction Commit
        /// <summary>
        /// Commit this instance.
        /// </summary>
        public void Commit()
        {
            this.SIDDapper.itran.Commit();
			this.SIDDapper.itran = null;
            this.SIDDapper.tran_conn.Close();
            this.SIDDapper.tran_conn.Dispose();
            this.SIDDapper.tran_conn = null;
        }
        #endregion

        #region Rollback--事物回滚
        /// <summary>
        /// 事物回滚
        /// </summary>
        public void Rollback()
        {
            if (this.SIDDapper.itran != null)
                this.SIDDapper.itran.Rollback();
            this.SIDDapper.itran = null;
			this.SIDDapper.tran_conn.Close();
			this.SIDDapper.tran_conn.Dispose();
            this.SIDDapper.tran_conn = null;
        }
        #endregion

        #region WhereForConditionAttribute--根据attribute创建where条件
        /// <summary>
        /// 根据attribute创建where条件
        /// </summary>
        /// <returns></returns>
        public Tuple<string,M> WhereForConditionAttribute<M>(M param) where M:class
        {
            var dbPar = this.SIDDapper.db.dbType == DBcon.dbtype.Oracle ? ":" : "@";
            var str = new List<string>();
            param.GetType().GetProperties().ToList().ForEach(p =>
            {
                //为空则放弃拼接
                var pvalue = p.GetValue(param);
                if (pvalue == null || string.IsNullOrEmpty((pvalue ?? "").ToString()))
                    return;
                var pio = (SIC_ConditionAttribute)p.GetCustomAttributes(typeof(SIC_ConditionAttribute), true).FirstOrDefault();
                if (pio == null)
                    return;

                if (("string".Equals(p.PropertyType.Name) ||
                    "String".Equals(p.PropertyType.Name))&& pio.type == SIC_ConditionAttribute.condition.like)
                    p.SetValue(param, "%" + p.GetValue(param) + "%");

                str.Add(string.Format(" {0} {1} {2}{3}"
                             , string.IsNullOrEmpty(pio.column) ? p.Name : pio.column
                             , SIC_ConditionAttribute.conditionStr[pio.type]
                             , dbPar
                             , p.Name
                             ));
            });
            return new Tuple<string, M>(string.Join(" and ", str), param);
        }
        #endregion

        #region GetPagination--获取分页 支持 oracle mysql mssql
        /// <summary>
        /// 获取分页 支持 oracle mysql mssql
        /// </summary>
        /// <typeparam name="M"></typeparam>
        /// <param name="RowSql"></param>
        /// <param name="CountSql"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public Tuple<int,List<M>> GetPagination<M, M1>(string RowSql, string CountSql, M1 param)
            where M1 : PaginationParam
            where M : class, new()
        {
            if (this.SIDDapper.db.dbType == DBcon.dbtype.MySql)
                RowSql = this.GetMySqlSQL(RowSql, param);
            else if (this.SIDDapper.db.dbType == DBcon.dbtype.Oracle)
                RowSql = this.GetOracleSQL(RowSql, param);

            var rows = this.Query<M>(RowSql, param).ToList();
            var total = this.QuerySingle<int>(CountSql, param);
            return new Tuple<int,List<M>>(total, rows);
        } 

        private string GetOracleSQL<M1>(string sql,M1 param)where M1:PaginationParam
        {
            if (param.pageNumber <= 0 || param.pageStart <= -1)
                return sql;
            sql = string.Format(@"select * from (
select tp01.*,rownum rn from ({0}) tp01
) tp02 
where rn >= :{1} and rn <= :{2} "
                    , sql
                    , "pageStart"
                    , "pageEnd");
            return sql;
        }

        private string GetMySqlSQL<M1>(string sql,M1 param)where M1:PaginationParam
        {
            if (param.pageNumber <= 0 || param.pageStart <= -1)
                return sql;
            sql = string.Format(@"{0} limit @{1} ,@{2} "
                    , sql
                    , "pageStart"
                    , "pageNumber");
            return sql;
        }
        #endregion
    }
}
