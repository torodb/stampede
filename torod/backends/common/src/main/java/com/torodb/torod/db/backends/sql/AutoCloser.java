
package com.torodb.torod.db.backends.sql;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.annotation.Nullable;

/**
 *
 */
public class AutoCloser {

    private AutoCloser() {}
    
    public static void close(@Nullable Closeable closeable) {
        close(closeable, RuntimeWrapIOErrorCallback.getInstance());
    }
    
    public static void close(@Nullable ResultSet rs) {
        close(rs, RuntimeWrapIOErrorCallback.getInstance());
    }
    
    public static void close(@Nullable Statement st) {
        close(st, RuntimeWrapIOErrorCallback.getInstance());
    }
    
    public static void close(@Nullable Connection connection) {
        close(connection, RuntimeWrapIOErrorCallback.getInstance());
    }
    
    public static void close(@Nullable Closeable closeable, IOErrorCallback callback) {
        if (closeable != null) {
            try {
                closeable.close();
            }
            catch (IOException ex) {
                callback.onError(closeable, ex);
            }
        }
    }
    
    public static void close(@Nullable ResultSet rs, SQLErrorCallback callback) {
        if (rs != null) {
            try {
                rs.close();
            }
            catch (SQLException ex) {
                callback.onError(rs, ex);
            }
        }
    }
    
    public static void close(@Nullable Statement st, SQLErrorCallback callback) {
        if (st != null) {
            try {
                st.close();
            }
            catch (SQLException ex) {
                callback.onError(st, ex);
            }
        }
    }
    
    public static void close(@Nullable Connection connection, SQLErrorCallback callback) {
        if (connection != null) {
            try {
                connection.close();
            }
            catch (SQLException ex) {
                callback.onError(connection, ex);
            }
        }
    }
    
    
    public static interface IOErrorCallback {
        public void onError(Closeable closeable, IOException ex);
    }
    
    public static interface SQLErrorCallback {
        public void onError(ResultSet closeable, SQLException ex);
        public void onError(Statement closeable, SQLException ex);
        public void onError(Connection closeable, SQLException ex);
    }
    
    public static class RuntimeWrapIOErrorCallback implements IOErrorCallback, SQLErrorCallback {

        private RuntimeWrapIOErrorCallback() {}
        
        public static RuntimeWrapIOErrorCallback getInstance() {
            return Holder.INSTANCE;
        }
        
        @Override
        public void onError(Closeable closeable, IOException ex) {
            throw new RuntimeException(ex);
        }

        @Override
        public void onError(ResultSet closeable, SQLException ex) {
            throw new RuntimeException(ex);
        }

        @Override
        public void onError(Statement closeable, SQLException ex) {
            throw new RuntimeException(ex);
        }

        @Override
        public void onError(Connection closeable, SQLException ex) {
            throw new RuntimeException(ex);
        }
        
        private static class Holder {
            private static final RuntimeWrapIOErrorCallback INSTANCE = new RuntimeWrapIOErrorCallback();
        }
    }
    
    
}
