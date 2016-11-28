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

package com.torodb.mongodb.core;

import com.google.common.collect.ImmutableList;

/**
 *
 */
public final class MongoLayerConstants {

  public static final int VERSION_MAJOR = 3;
  public static final int VERSION_MINOR = 0;
  public static final int VERSION_PATCH = 0;
  public static final String VERSION_STRING =
      VERSION_MAJOR + "." + VERSION_MINOR + "." + VERSION_PATCH;
  public static final ImmutableList VERSION = ImmutableList.of(
      MongoLayerConstants.VERSION_MAJOR,
      MongoLayerConstants.VERSION_MINOR,
      MongoLayerConstants.VERSION_PATCH
  );

  public static final int MAX_WIRE_VERSION = 3;
  public static final int MIN_WIRE_VERSION = 0;

  /**
   * Obtained from
   * <a href="http://docs.mongodb.org/manual/reference/limits/">MongoDB Limits and Thresholds</a>.
   *
   */
  public static final int MAX_BSON_DOCUMENT_SIZE = 16 * 1024 * 1024;

  public static final int MAX_WRITE_BATCH_SIZE = 1000;

  /**
   * Obtained from
   * <a href="https://github.com/mongodb/mongo/blob/v2.6/src/mongo/util/net/message.h">mongo / src /
   * mongo / util / net / message.h</a>. Also explained in the
   * <a href="http://docs.mongodb.org/master/reference/command/isMaster/">isMaster function</a>
   *
   */
  public static final int MAX_MESSAGE_SIZE_BYTES = 48 * 1000 * 1000;

  /**
   * Obtained from
   * <a href="http://docs.mongodb.org/manual/core/cursors/">Cursors / Cursor Batches</a>.
   */
  public static final int MONGO_CURSOR_LIMIT = 101;

  private MongoLayerConstants() {
  }

}
