package com.torodb.config.backend.greenplum;

import javax.xml.bind.annotation.XmlType;

import com.torodb.config.Config;
import com.torodb.config.backend.postgres.Postgres;

/**
 * GreenPlum configuration
 */
@XmlType(namespace=Config.TOROCONFIG_NAMESPACE,
propOrder={})
public class Greenplum extends Postgres {
}
