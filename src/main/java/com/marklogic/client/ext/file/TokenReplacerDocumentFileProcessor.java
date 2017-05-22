package com.marklogic.client.ext.file;

import com.marklogic.client.helper.LoggingObject;
import com.marklogic.client.io.Format;
import com.marklogic.client.ext.tokenreplacer.TokenReplacer;
import org.springframework.util.FileCopyUtils;

import java.io.File;
import java.io.IOException;

public class TokenReplacerDocumentFileProcessor extends LoggingObject implements DocumentFileProcessor {

	private TokenReplacer tokenReplacer;

	public TokenReplacerDocumentFileProcessor(TokenReplacer tokenReplacer) {
		this.tokenReplacer = tokenReplacer;
	}

	@Override
	public DocumentFile processDocumentFile(DocumentFile documentFile) {
		if (tokenReplacer != null && moduleCanBeReadAsString(documentFile.getFormat())) {
			String text = documentFile.getModifiedContent();
			if (text == null) {
				File file = documentFile.getFile();
				if (file != null) {
					try {
						text = new String(FileCopyUtils.copyToByteArray(file));
					} catch (IOException ie) {
						logger.warn("Unable to replace tokens in file: " + file.getAbsolutePath() + "; cause: " + ie.getMessage());
					}
				}
			}
			if (text != null) {
				text = tokenReplacer.replaceTokens(text);
				documentFile.setModifiedContent(text);
			}
		}
		return documentFile;
	}

	protected boolean moduleCanBeReadAsString(Format format) {
		return format != null && (format.equals(Format.JSON) || format.equals(Format.TEXT)
			|| format.equals(Format.XML));
	}
}
