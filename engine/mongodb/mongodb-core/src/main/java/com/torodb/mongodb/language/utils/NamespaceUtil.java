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

package com.torodb.mongodb.language.utils;

public class NamespaceUtil {

  public static final String NAMESPACES_COLLECTION = "system.namespaces";
  public static final String INDEXES_COLLECTION = "system.indexes";
  public static final String PROFILE_COLLECTION = "system.profile";
  public static final String JS_COLLECTION = "system.js";
  public static final String LIST_COLLECTIONS_GET_MORE_COLLECTION = "$cmd.listCollections";

  public static boolean isNamespacesMetaCollection(String collection) {
    return collection.equals(NAMESPACES_COLLECTION);
  }

  public static boolean isIndexesMetaCollection(String collection) {
    return collection.equals(INDEXES_COLLECTION);
  }

  public static boolean isProfileMetaCollection(String collection) {
    return collection.equals(PROFILE_COLLECTION);
  }

  public static boolean isJsMetaCollection(String collection) {
    return collection.equals(JS_COLLECTION);
  }

  public static boolean isSystem(String collection) {
    return collection.startsWith("system.");
  }

  public static boolean isCommand(String collection) {
    return collection.equals("$cmd");
  }

  public static boolean isConfigDb(String database) {
    return database.equals("config");
  }

  public static boolean isOplog(String database, String collection) {
    return database.equals("local") && collection.startsWith("oplog.");
  }

  public static boolean isSpecialCommand(String collection) {
    return collection.equals("$cmd.sys");
  }

  public static boolean isNormal(String database, String collection) {
    if (collection.indexOf('$') != -1) {
      return true;
    }
    return isOplog(database, collection);
  }

  public static boolean isSpecial(String database, String collection) {
    return !isNormal(database, collection) || isSystem(collection);
  }

  public static boolean isListCollectionsGetMore(String collection) {
    return collection.equals(LIST_COLLECTIONS_GET_MORE_COLLECTION);
  }

  public static boolean isListIndexesGetMore(String collection) {
    return collection.startsWith("$cmd.listIndexes.");
  }

  public static boolean isAdmin(String database) {
    return database != null && database.equals("admin");
  }

  /**
   * Returns true iff the namespace corresponds with a system collection on which users can write,
   * like <em>system.users</em>.
   *
   * @param database
   * @param collection
   * @return
   */
  public static boolean isSystemAndUserWritable(String database, String collection) {
    if (!isSystem(collection)) {
      return true;
    }
    if (database.equals("local") && collection.equals("system.replset")) {
      return true;
    }

    if (collection.equals("system.users")) {
      return true;
    }

    if (database.equals("admin")) {
      if (collection.equals("system.roles")
          || collection.equals("system.version")
          || collection.equals("system.new_users")
          || collection.equals("system.backup_users")) {
        return true;
      }
    }

    return collection.equals(JS_COLLECTION);
  }

  public static boolean isUserWritable(String database, String collection) {
    return isSystemAndUserWritable(database, collection) || !isTorodbCollection(collection);
  }

  public static boolean isTorodbCollection(String collection) {
    return collection.equals("torodb");
  }
}
