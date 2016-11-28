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

package com.torodb.d2r.model;

import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;

//TODO: Add constraint annotations and asserts
public class PathStack {

  private final TableRefFactory tableRefFactory;

  private PathInfo top = null;

  public enum PathNodeType {
    Field,
    Object,
    Array,
    Idx
  }

  public PathStack(TableRefFactory tableRefFactory) {
    this.tableRefFactory = tableRefFactory;
    this.top = new PathField("", null);
  }

  public PathInfo peek() {
    return top;
  }

  public PathInfo pop() {
    PathInfo topper = top;
    top = top.getParent();
    return topper;
  }

  public void pushField(String name) {
    top = top.appendField(name);
  }

  public void pushObject(DocPartRowImpl rowInfo) {
    top = top.appendObject(rowInfo);
  }

  public void pushArray() {
    if (top == null) {
      throw new IllegalArgumentException("Building an array on root document");
    }
    top = top.appendArray();
  }

  public void pushArrayIdx(int idx) {
    if (top == null) {
      throw new IllegalArgumentException("Building an array index on root document");
    } else if (top.getNodeType() != PathNodeType.Array) {
      throw new IllegalArgumentException("Building an array index on document");
    }
    top = ((PathArray) top).appendIdx(idx);
  }

  public void pushArrayIdx(int idx, DocPartRowImpl rowInfo) {
    if (top == null) {
      throw new IllegalArgumentException("Building an array index on root document");
    } else if (top.getNodeType() != PathNodeType.Array) {
      throw new IllegalArgumentException("Building an array index on document");
    }
    top = ((PathArray) top).appendIdx(idx, rowInfo);
  }

  public abstract class PathInfo {

    protected PathInfo parent;
    protected TableRef tableRef;

    private PathInfo(PathInfo parent) {
      this.parent = parent;
    }

    PathObject appendObject(DocPartRowImpl rowInfo) {
      return new PathObject(this, rowInfo);
    }

    PathField appendField(String name) {
      return new PathField(name, this);
    }

    PathArray appendArray() {
      return new PathArray(1, this);
    }

    public TableRef getTableRef() {
      return tableRef;
    }

    public PathInfo getParent() {
      return this.parent;
    }

    @Override
    public int hashCode() {
      return this.getPath().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (!(obj instanceof PathInfo)) {
        return false;
      }
      PathInfo other = (PathInfo) obj;
      return this.getPath().equals(other.getPath());
    }

    public boolean is(PathNodeType type) {
      return getNodeType().equals(type);
    }

    public abstract DocPartRowImpl findParentRowInfo();

    public abstract String getPath();

    public abstract PathNodeType getNodeType();

  }

  public class PathField extends PathInfo {

    private String fieldName;
    private String path;

    private PathField(String fieldName, PathInfo parent) {
      super(parent);
      this.fieldName = fieldName;
      this.path = calcPath();
      if (parent == null) {
        this.tableRef = tableRefFactory.createRoot();
      } else {
        this.tableRef = tableRefFactory.createChild(parent.getTableRef(), fieldName);
      }
    }

    private String calcPath() {
      if (parent == null || parent.getPath().length() == 0) {
        return getFieldName();
      }
      return parent.getPath() + "." + getFieldName();
    }

    public PathObject getParentObject() {
      return (PathObject) parent;
    }

    @Override
    public String getPath() {
      return path;
    }

    public String getFieldName() {
      return fieldName;
    }

    @Override
    public PathNodeType getNodeType() {
      return PathNodeType.Field;
    }

    @Override
    public String toString() {
      if (parent == null || parent.getPath().length() == 0) {
        return getFieldName();
      }
      return parent.toString() + "." + getFieldName();
    }

    @Override
    public DocPartRowImpl findParentRowInfo() {
      if (parent != null) {
        return parent.findParentRowInfo();
      }
      return null;
    }
  }

  public class PathObject extends PathInfo {

    private DocPartRowImpl rowInfo;

    private PathObject(PathInfo parent, DocPartRowImpl rowInfo) {
      super(parent);
      this.rowInfo = rowInfo;
      this.tableRef = parent.getTableRef();
    }

    @Override
    public String getPath() {
      return parent.getPath();
    }

    @Override
    public PathNodeType getNodeType() {
      return PathNodeType.Object;
    }

    @Override
    public String toString() {
      return parent.toString();
    }

    public DocPartRowImpl findParentRowInfo() {
      return rowInfo;
    }
  }

  public class PathArray extends PathInfo {

    private int dimension;
    private String path;

    private PathArray(int dimension, PathInfo parent) {
      super(parent);
      this.dimension = dimension;
      this.path = calcPath();
      if (dimension == 1) {
        this.tableRef = parent.getTableRef();
      } else {
        this.tableRef = tableRefFactory.createChild(parent.tableRef, dimension);
      }
    }

    PathArrayIdx appendIdx(int idx) {
      return new PathArrayIdx(idx, this);
    }

    PathArrayIdx appendIdx(int idx, DocPartRowImpl rowInfo) {
      return new PathArrayIdx(idx, this, rowInfo);
    }

    @Override
    public String getPath() {
      return path;
    }

    private String calcPath() {
      if (dimension == 1) {
        return parent.getPath();
      }
      PathInfo noArray = parent;
      while (noArray.getNodeType() == PathNodeType.Array || noArray.getNodeType()
          == PathNodeType.Idx) {
        noArray = noArray.getParent();
      }
      return noArray.getPath() + "$" + dimension;
    }

    @Override
    public String toString() {
      return parent.toString();
    }

    @Override
    public PathNodeType getNodeType() {
      return PathNodeType.Array;
    }

    public DocPartRowImpl findParentRowInfo() {
      if (parent != null) {
        return parent.findParentRowInfo();
      }
      return null;
    }
  }

  // TODO maybe create to types: with and without rowinfo
  public class PathArrayIdx extends PathInfo {

    private int idx;
    private DocPartRowImpl rowInfo;

    private PathArrayIdx(int idx, PathInfo parent) {
      this(idx, parent, null);
    }

    private PathArrayIdx(int idx, PathInfo parent, DocPartRowImpl rowInfo) {
      super(parent);
      assert parent.getNodeType() == PathNodeType.Array;
      this.idx = idx;
      this.rowInfo = rowInfo;
      this.tableRef = parent.tableRef;
    }

    PathArray appendArray() {
      return new PathArray(((PathArray) parent).dimension + 1, this);
    }

    @Override
    public String getPath() {
      return parent.getPath();
    }

    @Override
    public PathNodeType getNodeType() {
      return PathNodeType.Idx;
    }

    @Override
    public String toString() {
      return parent.toString() + "[]";
    }

    public int getIdx() {
      return idx;
    }

    public DocPartRowImpl findParentRowInfo() {
      if (rowInfo != null) {
        return rowInfo;
      }
      if (parent != null) {
        return parent.findParentRowInfo();
      }
      return null;
    }
  }

}
