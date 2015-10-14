
package com.torodb.torod.mongodb.utils;

/**
 *
 */
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

    public static boolean isJSMetaCollection(String collection) {
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
     * Returns true iff the namespace corresponds with a system collection on
     * which users can write, like <em>system.users</em>.
     *
     * @param database
     * @param collection
     * @return
     */
    public static boolean isSystemAndUserWritable(String database, String collection) {
        if (!isSystem(collection)) {
            return true;
        }
        if (database.equals("local")) {
            if (collection.equals("system.replset")) {
                return true;
            }
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
