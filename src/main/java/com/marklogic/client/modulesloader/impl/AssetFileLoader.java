package com.marklogic.client.modulesloader.impl;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.ext.file.GenericFileLoader;
import com.marklogic.client.modulesloader.ModulesManager;

/**
 * File loaded for "assets", as defined by the REST API - basically, any server module. Be sure to use a DatabaseClient
 * that points to your modules database.
 */
public class AssetFileLoader extends GenericFileLoader {

	public final static String DEFAULT_PERMISSIONS = "rest-admin,read,rest-admin,update,rest-extension-user,execute";

	public AssetFileLoader(DatabaseClient modulesDatabaseClient) {
		this(modulesDatabaseClient, null);
	}

	public AssetFileLoader(DatabaseClient modulesDatabaseClient, ModulesManager modulesManager) {
		super(modulesDatabaseClient);
		addFileFilter(new AssetFileFilter());
		addDocumentFileProcessor(new ExtDocumentFileProcessor());
		if (modulesManager != null) {
			addDocumentFileProcessor(new ModulesManagerDocumentFileProcessor(modulesManager));
		}
		setPermissions(DEFAULT_PERMISSIONS);
	}
}
