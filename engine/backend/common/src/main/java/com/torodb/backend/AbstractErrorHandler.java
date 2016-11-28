/*
 * ToroDB
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.backend;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.torodb.backend.exceptions.BackendException;
import com.torodb.core.exceptions.ToroRuntimeException;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.RollbackException;
import org.jooq.exception.DataAccessException;

import java.sql.SQLException;
import java.util.Optional;
import java.util.function.Function;

import javax.inject.Singleton;

/**
 *
 */
@Singleton
public abstract class AbstractErrorHandler implements ErrorHandler {

  private final ImmutableList<RollbackRule> rollbackRules;
  private final ImmutableList<UserRule> userRules;

  protected AbstractErrorHandler(Rule... rules) {
    ImmutableList.Builder<RollbackRule> rollbackRulesBuilder =
        ImmutableList.builder();
    ImmutableList.Builder<UserRule> userRulesBuilder =
        ImmutableList.builder();

    for (Rule rule : rules) {
      if (rule instanceof RollbackRule) {
        rollbackRulesBuilder.add((RollbackRule) rule);
      } else if (rule instanceof UserRule) {
        userRulesBuilder.add((UserRule) rule);
      }
    }

    this.rollbackRules = rollbackRulesBuilder.build();
    this.userRules = userRulesBuilder.build();
  }

  @Override
  public ToroRuntimeException handleException(Context context, SQLException sqlException) throws
      RollbackException {
    try {
      return handleUserException(context, sqlException);
    } catch (UserException userException) {
      return new BackendException(context, sqlException);
    }
  }

  @Override
  public ToroRuntimeException handleException(Context context,
      DataAccessException dataAccessException) throws RollbackException {
    try {
      return handleUserException(context, dataAccessException);
    } catch (UserException userException) {
      return new BackendException(context, dataAccessException);
    }
  }

  @Override
  public ToroRuntimeException handleUserException(Context context, SQLException sqlException) throws
      UserException, RollbackException {
    if (applyToUserRule(context, sqlException.getSQLState())) {
      throw createUserException(context, sqlException.getSQLState(), new BackendException(context,
          sqlException));
    }

    if (applyToRollbackRule(context, sqlException.getSQLState())) {
      throw new RollbackException(sqlException);
    }

    return new BackendException(context, sqlException);
  }

  @Override
  public ToroRuntimeException handleUserException(Context context,
      DataAccessException dataAccessException) throws UserException, RollbackException {
    if (applyToUserRule(context, dataAccessException.sqlState())) {
      throw createUserException(context, dataAccessException.sqlState(), new BackendException(
          context, dataAccessException));
    }

    if (applyToRollbackRule(context, dataAccessException.sqlState())) {
      throw new RollbackException(dataAccessException);
    }

    return new BackendException(context, dataAccessException);
  }

  private boolean applyToRollbackRule(Context context, String sqlState) {
    return rollbackRules.stream()
        .anyMatch(r ->
            r.getSqlCode().equals(sqlState) && (r.getContexts().isEmpty() || r.getContexts()
            .contains(context)));
  }

  private boolean applyToUserRule(Context context, String sqlState) {
    return userRules.stream()
        .anyMatch(r ->
            r.getSqlCode().equals(sqlState) && (r.getContexts().isEmpty() || r.getContexts()
            .contains(context)));
  }

  private UserException createUserException(Context context, String sqlState,
      BackendException backendException) {
    Optional<UserRule> userRule = userRules.stream()
        .filter(r ->
            r.getSqlCode().equals(sqlState) && (r.getContexts().isEmpty() || r.getContexts()
            .contains(context)))
        .findFirst();
    if (userRule.isPresent()) {
      return userRule.get().translate(backendException);
    }

    throw new IllegalArgumentException("User exception not found for context " + context
        + " and sqlState " + sqlState);
  }

  protected static Rule rollbackRule(String sqlCode, Context... contexts) {
    return new RollbackRule(sqlCode, contexts);
  }

  protected static Rule userRule(
      String sqlCode, Function<BackendException, UserException> translateFunction,
      Context... contexts) {
    return new UserRule(sqlCode, contexts, translateFunction);
  }

  protected abstract static class Rule {

    private final String sqlCode;
    private final ImmutableSet<Context> contexts;

    private Rule(String code, Context[] contexts) {
      this.sqlCode = code;
      this.contexts = ImmutableSet.copyOf(contexts);
    }

    public String getSqlCode() {
      return sqlCode;
    }

    public ImmutableSet<Context> getContexts() {
      return contexts;
    }
  }

  protected static class RollbackRule extends Rule {

    private RollbackRule(String code, Context[] contexts) {
      super(code, contexts);
    }
  }

  protected static class UserRule extends Rule {

    private final Function<BackendException, UserException> translateFunction;

    private UserRule(String code, Context[] contexts,
        Function<BackendException, UserException> translateFunction) {
      super(code, contexts);

      this.translateFunction = translateFunction;
    }

    public UserException translate(BackendException backendException) {
      return translateFunction.apply(backendException);
    }
  }
}
