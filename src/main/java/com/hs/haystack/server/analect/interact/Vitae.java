/**
 * 
 */
package com.hs.haystack.server.analect.interact;

import java.io.File;

import com.hs.haystack.models.common.file.FileProperties;

/**
 * @author vinay
 *
 */
public interface Vitae {
	
	/**
	 * Help extract potable information from desired document set
	 * @param recordToProcess is the desired document set
	 * @return the potable file to be analysed
	 */
	public File extractEnrichedContent(FileProperties recordToProcess);
	
	/**
	 * Help instantiate natural language processing engine
	 * @param path is the location to navigate
	 */
	public void initializeNLPEngine(String path);
	
	/**
	 * Analyse incoming profile information
	 * @param enrichedContent is the information to be processed
	 * @return the interpreted information from journal
	 */
	public Object processJournal(File enrichedContent);

}
