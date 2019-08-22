package com.utils

import java.sql.{Connection, ResultSet, Statement}
import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.dbcp2.{BasicDataSource, BasicDataSourceFactory}

object JdbcUtils {

  import java.io.InputStream
  import java.sql.SQLException
  //数据库连接池需实现javax.sql.DataSource接口，DBCP连接池是javax.sql.DataSource接口的具体实现//数据库连接池需实现javax.sql.DataSource接口，DBCP连接池是javax.sql.DataSource接口的具体实现

  private var ds:BasicDataSource = new BasicDataSource()

  def init()=
   {
    try {
      //加载dbcpconfig.properties配置文件
      val load: Config = ConfigFactory.load()
      val prop = new Properties()
      prop.setProperty("username",load.getString("jdbc.user"))
      prop.setProperty("password",load.getString("jdbc.password"))
      prop.setProperty("driverClassName",load.getString("jdbc.driver"))
      prop.setProperty("url",load.getString("jdbc.url"))
      prop.setProperty("initialSize",load.getString("initialSize"))
      prop.setProperty("maxActive",load.getString("maxActive"))
      prop.setProperty("minIdle",load.getString("minIdle"))
      prop.setProperty("maxIdle",load.getString("maxIdle"))
      prop.setProperty("maxWait",load.getString("maxWait"))
      prop.setProperty("defaultAutoCommit",load.getString("defaultAutoCommit"))

      //创建数据源
      ds = BasicDataSourceFactory.createDataSource(prop)

    } catch  {
      case  e:Exception => e.printStackTrace()
    }
  }
  private val it = init()

  /**
    * @Method:getConnection
    * @Description:从数据源中获取数据库连接
    * @return
    * @throws SQLException
    */
  @throws[SQLException]
  def getConnection: Connection = { //从数据源中获取数据库连接
    ds.getConnection
  }

  /**
    * @Method:release
    * @Description:释放资源(数据库连接对象conn,负责执行SQL命令的Statement对象，存储查询结果的ResultSet对象)
    * @param conn
    * @param st
    * @param rs
    */
  def release(conn: Connection, st: Statement,  rs: ResultSet): Unit = {
    if (rs != null) {
      try //关闭存储查询结果的ResultSet对象
      rs.close
      catch {
        case e: Exception =>
          e.printStackTrace()
      }

    }
    if (st != null) try
      st.close
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
    if (conn != null) try //将Connection连接对象还给数据库连接池
    conn.close
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }
}
