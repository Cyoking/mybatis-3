/*
 *    Copyright 2009-2023 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.apache.ibatis.executor.statement;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.executor.parameter.ParameterHandler;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.session.ResultHandler;

/**
 * @author Clinton Begin
 */
public interface StatementHandler {

  /**
   * // 获取底层的 Statement 对象
   * @param connection 数据库连接，一般通过事务管理器取获取
   * @param transactionTimeout 事务超时时间
   * @return 返回Statement对象
   * @throws SQLException
   */
  Statement prepare(Connection connection, Integer transactionTimeout) throws SQLException;

  /**
   * 设置 Statement 对象的参数, 对于普通的Statement是不需要的，对于PrepareStatement和CallableStatement才需要
   * @param statement
   * @throws SQLException
   */
  void parameterize(Statement statement) throws SQLException;

  /**
   *  批量执行 SQL
   * @param statement
   * @throws SQLException
   */
  void batch(Statement statement) throws SQLException;

  /**
   * 执行更新操作，返回影响的行数
   * @param statement
   * @return
   * @throws SQLException
   */
  int update(Statement statement) throws SQLException;

  /**
   * 执行查询操作，返回 ResultSet 对象会被ResultSetHandler处理
   * @param statement
   * @param resultHandler
   * @return
   * @param <E>
   * @throws SQLException
   */
  <E> List<E> query(Statement statement, ResultHandler resultHandler) throws SQLException;

  <E> Cursor<E> queryCursor(Statement statement) throws SQLException;

  /**
   * 获取 BoundSql 对象，包含 SQL 语句和参数映射
   * @return
   */
  BoundSql getBoundSql();

  /**
   * 获取 ParameterHandler 对象，用于处理参数映射
   * @return
   */
  ParameterHandler getParameterHandler();

}
