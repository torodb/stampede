package com.torodb.backend;

import java.sql.SQLException;
import java.util.List;

import com.google.inject.Injector;
import com.google.inject.Module;

public interface DatabaseForTest {

    List<Module> getModules();
    void cleanDatabase(Injector injector) throws SQLException;

}
