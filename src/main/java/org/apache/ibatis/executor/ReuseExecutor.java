/**
 *    Copyright 2009-2018 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.apache.ibatis.executor;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.logging.Log;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.transaction.Transaction;

/**
 * The Class ReuseExecutor.
 *
 * @author Clinton Begin
 */

/**
 * 执行update或select，以sql 作为key查询Statement 对象，存在就使用不存在就创建
 * 用完之后不关闭Statement对象，而是将其放置在Map<String,Statement>内，供下一次使用
 */
public class ReuseExecutor extends BaseExecutor {

  /** 用于存储Statement. */
  private final Map<String, Statement> statementMap = new HashMap<>();

  /**
   * Instantiates a new reuse executor.
   *
   * @param configuration the configuration
   * @param transaction the transaction
   */
  public ReuseExecutor(Configuration configuration, Transaction transaction) {
    super(configuration, transaction);
  }

  /**
   * 执行完成之后不关闭Statement.
   *
   * @param ms the ms
   * @param parameter the parameter
   * @return the int
   * @throws SQLException the SQL exception
   */
  @Override
  public int doUpdate(MappedStatement ms, Object parameter) throws SQLException {
    Configuration configuration = ms.getConfiguration();
    StatementHandler handler = configuration.newStatementHandler(this, ms, parameter, RowBounds.DEFAULT, null, null);
    Statement stmt = prepareStatement(handler, ms.getStatementLog());
    return handler.update(stmt);
  }

  /* (non-Javadoc)
   * @see org.apache.ibatis.executor.BaseExecutor#doQuery(org.apache.ibatis.mapping.MappedStatement, java.lang.Object, org.apache.ibatis.session.RowBounds, org.apache.ibatis.session.ResultHandler, org.apache.ibatis.mapping.BoundSql)
   */
  @Override
  public <E> List<E> doQuery(MappedStatement ms, Object parameter, RowBounds rowBounds, ResultHandler resultHandler, BoundSql boundSql) throws SQLException {
    Configuration configuration = ms.getConfiguration();
    StatementHandler handler = configuration.newStatementHandler(wrapper, ms, parameter, rowBounds, resultHandler, boundSql);
    Statement stmt = prepareStatement(handler, ms.getStatementLog());
    return handler.<E>query(stmt, resultHandler);
  }

  /* (non-Javadoc)
   * @see org.apache.ibatis.executor.BaseExecutor#doQueryCursor(org.apache.ibatis.mapping.MappedStatement, java.lang.Object, org.apache.ibatis.session.RowBounds, org.apache.ibatis.mapping.BoundSql)
   */
  @Override
  protected <E> Cursor<E> doQueryCursor(MappedStatement ms, Object parameter, RowBounds rowBounds, BoundSql boundSql) throws SQLException {
    Configuration configuration = ms.getConfiguration();
    StatementHandler handler = configuration.newStatementHandler(wrapper, ms, parameter, rowBounds, null, boundSql);
    Statement stmt = prepareStatement(handler, ms.getStatementLog());
    return handler.<E>queryCursor(stmt);
  }

  /**
   * 在这个位置关闭Statement.
   *
   * @param isRollback the is rollback
   * @return the list
   * @throws SQLException the SQL exception
   */
  @Override
  public List<BatchResult> doFlushStatements(boolean isRollback) throws SQLException {
//    关闭Statement
    for (Statement stmt : statementMap.values()) {
      closeStatement(stmt);
    }
    statementMap.clear();
    return Collections.emptyList();
  }

  /**
   * Prepare statement.
   *
   * @param handler the handler
   * @param statementLog the statement log
   * @return the statement
   * @throws SQLException the SQL exception
   */
  private Statement prepareStatement(StatementHandler handler, Log statementLog) throws SQLException {
    Statement stmt;
    BoundSql boundSql = handler.getBoundSql();
    String sql = boundSql.getSql();
    //如果存在Statement 并且连接还没有关闭
    if (hasStatementFor(sql)) {
      //冲StatementMap中获取Statement
      stmt = getStatement(sql);
      applyTransactionTimeout(stmt);
    } else {
      //如果没有
      Connection connection = getConnection(statementLog);
      stmt = handler.prepare(connection, transaction.getTimeout());
      //将Statement放到StatementMap中
      putStatement(sql, stmt);
    }
    handler.parameterize(stmt);
    return stmt;
  }

  /**
   * Checks for statement for.
   *
   * @param sql the sql
   * @return true, if successful
   */
  private boolean hasStatementFor(String sql) {
    try {
      return statementMap.keySet().contains(sql) && !statementMap.get(sql).getConnection().isClosed();
    } catch (SQLException e) {
      return false;
    }
  }

  /**
   * Gets the statement.
   *
   * @param s the s
   * @return the statement
   */
  private Statement getStatement(String s) {
    return statementMap.get(s);
  }

  
  /**
   * Put statement.
   *
   * @param sql the sql
   * @param stmt the stmt
   */
  private void putStatement(String sql, Statement stmt) {
    statementMap.put(sql, stmt);
  }

}
