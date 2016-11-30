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

package com.torodb.d2r;

import com.torodb.core.d2r.DocPartResult;
import com.torodb.core.d2r.DocPartResultRow;
import com.torodb.core.d2r.IllegalDocPartRowException;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.kvdocument.values.*;
import com.torodb.kvdocument.values.heap.StringKvString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class MockedDocPartResult implements DocPartResult {

  private final MetaDocPart metaDocPart;
  private Iterator<MockedRow> iterator;
  private MockedRow current = null;

  public MockedDocPartResult(MetaDocPart metaDocPart, MockedRow... rows) {
    this.metaDocPart = metaDocPart;
    List<MockedRow> rowsList = Arrays.asList(rows);
    iterator = rowsList.iterator();
  }

  public MockedDocPartResult(MetaDocPart metaDocPart, List<MockedRow> rows) {
    this.metaDocPart = metaDocPart;
    List<MockedRow> rowsList = new ArrayList<>(rows);
    iterator = rowsList.iterator();
  }

  @Override
  public MetaDocPart getMetaDocPart() {
    return metaDocPart;
  }

  @Override
  public DocPartResultRow next() throws IllegalDocPartRowException {
    return iterator.next();
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  public MockedRow getCurrent() {
    return current;
  }

  public static class MockedRow implements DocPartResultRow {

    private List<Object> values;

    private Integer did = null;
    private Integer pid = null;
    private Integer rid = null;
    private Integer seq = null;

    public MockedRow(Integer did, Integer pid, Integer rid, Integer seq, Object... values) {
      this.values = Arrays.asList(values);
      this.did = did;
      this.pid = pid != null ? pid : did;
      this.rid = rid != null ? rid : did;
      this.seq = seq;
    }

    public int getDid() {
      return did;
    }

    public int getPid() {
      return pid;
    }

    public int getRid() {
      return rid;
    }

    public Integer getSeq() {
      return seq;
    }

    @Override
    public KvValue<?> getUserValue(int fieldIndex, FieldType fieldType) {
      return convertValue(fieldType, values.get(fieldIndex));
    }

    private static KvValue<?> convertValue(FieldType type, Object value) {
      if (type == FieldType.NULL) {
        if (value == null) {
          return null;
        }
        if (value.equals(true)) {
          return KvNull.getInstance();
        } else {
          return null;
        }
      }
      if (value instanceof String) {
        return new StringKvString((String) value);
      } else if (value instanceof Integer) {
        return KvInteger.of((Integer) value);
      } else if (value instanceof Double) {
        return KvDouble.of((Double) value);
      } else if (value instanceof Long) {
        return KvLong.of((Long) value);
      } else if (value instanceof Boolean) {
        return KvBoolean.from((boolean) value);
      } else {
        return null;
      }
    }
  }

  @Override
  public void close() {
  }

}
