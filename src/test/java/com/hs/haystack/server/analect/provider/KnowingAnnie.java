/**
 * 
 */
package com.hs.haystack.server.analect.provider;

import java.util.*;
import java.io.*;
import java.net.*;

import java.io.IOException;

import gate.CorpusController;
import gate.Gate;
import gate.util.GateException;
import gate.util.Out;
import gate.util.persistence.PersistenceManager;

/**
 * @author vinay
 *
 */
public class KnowingAnnie {
	
	  /** The Corpus Pipeline application to contain ANNIE */
	  private CorpusController annieController;
	  
	  public void initAnnie() throws GateException, IOException {
		    Out.prln("Initialising processing engine...");

		    Gate.setGateHome(getFileFromURL("gate"));
		    
		    File gateHome = Gate.getGateHome();
		    Gate.setUserConfigFile(new File(gateHome, "user-gate.xml")); 
		    
		    
		    Gate.setPluginsHome(getFileFromURL("gate/plugins"));
		    File pluginsHome = Gate.getPluginsHome();
		    
		    

		    File annieGapp = new File(gateHome, "ANNIEResumeParser.gapp");
		    Gate.init();
		 // load plugins, for example... 
		    Gate.getCreoleRegister().registerDirectories(
		    		this.getClass().getClassLoader().getResource("gate/plugins/ANNIE"));
		    annieController = (CorpusController) PersistenceManager.loadObjectFromFile(annieGapp);

		    Out.prln("...processing engine loaded");
		  }
	  
	  public File getFileFromURL(String path) {
		    URL url = this.getClass().getClassLoader().getResource(path);
		    File file = null;
		    try {
		        file = new File(url.toURI());
		    } catch (URISyntaxException e) {
		        file = new File(url.getPath());
		    } finally {
		        return file;
		    }
		}

	/**
	 * @param args
	 * @throws IOException 
	 * @throws GateException 
	 */
	public static void main(String[] args) throws GateException, IOException {
		new KnowingAnnie().initAnnie();
	}

}
