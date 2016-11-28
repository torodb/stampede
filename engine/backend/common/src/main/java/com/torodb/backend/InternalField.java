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

import com.torodb.core.d2r.DocPartRow;
import org.jooq.Field;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

public abstract class InternalField<T> extends DelegatorField<T> {

  private static final long serialVersionUID = 1L;

  public InternalField(Field<T> field) {
    super(field);
  }

  public abstract void set(PreparedStatement preparedStatement, int index, DocPartRow docPartRow)
      throws SQLException;

  public abstract T getValue(ResultSet rs, int index) throws SQLException;

  public abstract T getValue(DocPartRow docPartRow);

  public boolean isDid() {
    return false;
  }

  public boolean isRid() {
    return false;
  }

  public boolean isPid() {
    return false;
  }

  public boolean isSeq() {
    return false;
  }

  public boolean isNullable() {
    return false;
  }

  public static class DidInternalField extends InternalField<Integer> {

    private static final long serialVersionUID = 1L;

    public DidInternalField(Field<Integer> field) {
      super(field);
    }

    @Override
    public boolean isDid() {
      return true;
    }

    @Override
    public Integer getValue(ResultSet rs, int index) throws SQLException {
      return rs.getInt(index);
    }

    @Override
    public Integer getValue(DocPartRow docPartRow) {
      return docPartRow.getDid();
    }

    @Override
    public void set(PreparedStatement preparedStatement, int index, DocPartRow docPartRow) throws
        SQLException {
      preparedStatement.setInt(index, docPartRow.getDid());
    }
  }

  public static class RidInternalField extends InternalField<Integer> {

    private static final long serialVersionUID = 1L;

    public RidInternalField(Field<Integer> field) {
      super(field);
    }

    @Override
    public boolean isRid() {
      return true;
    }

    @Override
    public Integer getValue(ResultSet rs, int index) throws SQLException {
      return rs.getInt(index);
    }

    @Override
    public Integer getValue(DocPartRow docPartRow) {
      return docPartRow.getRid();
    }

    @Override
    public void set(PreparedStatement preparedStatement, int index, DocPartRow docPartRow) throws
        SQLException {
      preparedStatement.setInt(index, docPartRow.getRid());
    }

  }

  public static class PidInternalField extends InternalField<Integer> {

    private static final long serialVersionUID = 1L;

    public PidInternalField(Field<Integer> field) {
      super(field);
    }

    @Override
    public boolean isPid() {
      return true;
    }

    @Override
    public Integer getValue(ResultSet rs, int index) throws SQLException {
      return rs.getInt(index);
    }

    @Override
    public Integer getValue(DocPartRow docPartRow) {
      return docPartRow.getPid();
    }

    @Override
    public void set(PreparedStatement preparedStatement, int index, DocPartRow docPartRow) throws
        SQLException {
      preparedStatement.setInt(index, docPartRow.getPid());
    }

  }

  public static class SeqInternalField extends InternalField<Integer> {

    private static final long serialVersionUID = 1L;

    public SeqInternalField(Field<Integer> field) {
      super(field);
    }

    @Override
    public boolean isSeq() {
      return true;
    }

    @Override
    public boolean isNullable() {
      return true;
    }

    @Override
    public Integer getValue(ResultSet rs, int index) throws SQLException {
      return rs.getInt(index);
    }

    @Override
    public Integer getValue(DocPartRow docPartRow) {
      return docPartRow.getSeq();
    }

    @Override
    public void set(PreparedStatement preparedStatement, int index, DocPartRow docPartRow) throws
        SQLException {
      if (docPartRow.getSeq() != null) {
        preparedStatement.setInt(index, docPartRow.getSeq());
      } else {
        preparedStatement.setNull(index, Types.INTEGER);
      }
    }

  }
}
