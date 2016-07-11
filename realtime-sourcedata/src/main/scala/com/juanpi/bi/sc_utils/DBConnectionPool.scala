package com.juanpi.bi.sc_utils

import scalikejdbc.ConnectionPool
import scalikejdbc.config.DBs

object DBConnectionPool {
  def init(name: Symbol = ConnectionPool.DEFAULT_NAME): Unit ={
    if(!ConnectionPool.isInitialized(name)){
      DBs.setup(name)
    }
  }

  def close() = DBs.closeAll()
}
