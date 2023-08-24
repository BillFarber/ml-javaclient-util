/*
 * Copyright (c) 2023 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.marklogic.client.ext.file;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.ext.AbstractIntegrationTest;
import com.marklogic.client.io.DocumentMetadataHandle;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

public class CascadeCollectionsAndPermissionsFileProcessorTest extends AbstractIntegrationTest {

	final private String PARENT_COLLECTION = "ParentCollection";
	final private String CHILD_COLLECTION = "ChildCollection";

	@BeforeEach
	public void setup() {
		client = newClient(MODULES_DATABASE);
		DatabaseClient modulesClient = client;
		modulesClient.newServerEval().xquery("cts:uris((), (), cts:true-query()) ! xdmp:document-delete(.)").eval();
	}

	@Test
	public void parentSettingsOnlyCascadeWhenChildHasNoSettings() {
		String directory = "src/test/resources/process-files/cascading-metadata-test/parent1-withCP";
		GenericFileLoader loader = new GenericFileLoader(client);
		List<DocumentFile> files = loader.loadFiles(directory);

		Optional<DocumentFile> optionalTestFile = files.stream().filter(file -> "/child1_1-noCP/test.json".equals(file.getUri())).findFirst();
		assertTrue(optionalTestFile.isPresent());
		DocumentFile testFile = optionalTestFile.get();
		assertFalse(testFile.getDocumentMetadata().getCollections().contains(CHILD_COLLECTION));
		assertTrue(testFile.getDocumentMetadata().getCollections().contains(PARENT_COLLECTION));
		assertNull(testFile.getDocumentMetadata().getPermissions().get("rest-reader"));
		assertTrue(testFile.getDocumentMetadata().getPermissions().get("rest-writer").contains(DocumentMetadataHandle.Capability.UPDATE));

		optionalTestFile = files.stream().filter(file -> "/child1_2-withCP/test.json".equals(file.getUri())).findFirst();
		assertTrue(optionalTestFile.isPresent());
		testFile = optionalTestFile.get();
		assertTrue(testFile.getDocumentMetadata().getCollections().contains(CHILD_COLLECTION));
		assertFalse(testFile.getDocumentMetadata().getCollections().contains(PARENT_COLLECTION));
		assertTrue(testFile.getDocumentMetadata().getPermissions().get("rest-reader").contains(DocumentMetadataHandle.Capability.READ));
		assertNull(testFile.getDocumentMetadata().getPermissions().get("rest-writer"));
	}

	@Test
	public void ChildHasSettingsUsedWhenParentHasNoSettings() {
		String directory = "src/test/resources/process-files/cascading-metadata-test/parent2-noCP";
		GenericFileLoader loader = new GenericFileLoader(client);
		List<DocumentFile> files = loader.loadFiles(directory);

		Optional<DocumentFile> optionalTestFile = files.stream().filter(file -> "/child2_1-withCP/test.json".equals(file.getUri())).findFirst();
		assertTrue(optionalTestFile.isPresent());
		DocumentFile testFile = optionalTestFile.get();
		assertTrue(testFile.getDocumentMetadata().getCollections().contains(CHILD_COLLECTION));
		assertFalse(testFile.getDocumentMetadata().getCollections().contains(PARENT_COLLECTION));
		assertTrue(testFile.getDocumentMetadata().getPermissions().get("rest-reader").contains(DocumentMetadataHandle.Capability.READ));
		assertNull(testFile.getDocumentMetadata().getPermissions().get("rest-writer"));
	}

	@Test
	public void GrandchildGetsChildSettingsWhenParentHasSettings() {
		String directory = "src/test/resources/process-files/cascading-metadata-test/parent3-withCP";
		GenericFileLoader loader = new GenericFileLoader(client);
		List<DocumentFile> files = loader.loadFiles(directory);

		Optional<DocumentFile> optionalTestFile = files.stream().filter(file -> "/child3_1-withCP/grandchild3_1_1-noCP/test.json".equals(file.getUri())).findFirst();
		assertTrue(optionalTestFile.isPresent());
		DocumentFile testFile = optionalTestFile.get();
		assertTrue(testFile.getDocumentMetadata().getCollections().contains(CHILD_COLLECTION));
		assertFalse(testFile.getDocumentMetadata().getCollections().contains(PARENT_COLLECTION));
		assertTrue(testFile.getDocumentMetadata().getPermissions().get("rest-reader").contains(DocumentMetadataHandle.Capability.READ));
		assertNull(testFile.getDocumentMetadata().getPermissions().get("rest-writer"));
	}

	@Test
	public void CollectionsAndPermissionsAreSetIndependently() {
		String directory = "src/test/resources/process-files/cascading-metadata-test/parent4-withCnoP";
		GenericFileLoader loader = new GenericFileLoader(client);
		List<DocumentFile> files = loader.loadFiles(directory);

		Optional<DocumentFile> optionalTestFile = files.stream().filter(file -> "/child4_1-withPnoC/test.json".equals(file.getUri())).findFirst();
		assertTrue(optionalTestFile.isPresent());
		DocumentFile testFile = optionalTestFile.get();
		assertFalse(testFile.getDocumentMetadata().getCollections().contains(CHILD_COLLECTION));
		assertTrue(testFile.getDocumentMetadata().getCollections().contains(PARENT_COLLECTION));
		assertTrue(testFile.getDocumentMetadata().getPermissions().get("rest-reader").contains(DocumentMetadataHandle.Capability.READ));
		assertNull(testFile.getDocumentMetadata().getPermissions().get("rest-writer"));
	}
}
