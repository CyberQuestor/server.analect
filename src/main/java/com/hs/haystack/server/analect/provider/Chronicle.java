/**
 * 
 */
package com.hs.haystack.server.analect.provider;

import static gate.Utils.stringFor;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Iterator;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.Logger;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.sax.ToXMLContentHandler;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import com.hs.haystack.models.common.error.runtime.analect.ContentEnrichmentRuntimeException;
import com.hs.haystack.models.common.error.runtime.analect.JournalRuntimeException;
import com.hs.haystack.models.common.error.runtime.analect.NLPEngineRuntimeException;
import com.hs.haystack.models.common.file.FileProperties;
import com.hs.haystack.server.analect.interact.Vitae;
import com.hs.haystack.utilities.fabricator.LogFabricator;
import com.hs.haystack.utilities.interpreter.repose.PropertyAccessor;

import gate.Annotation;
import gate.AnnotationSet;
import gate.Corpus;
import gate.CorpusController;
import gate.Document;
import gate.Factory;
import gate.FeatureMap;
import gate.Gate;
import gate.creole.ExecutionException;
import gate.creole.ResourceInstantiationException;
import gate.util.GateException;
import gate.util.persistence.PersistenceManager;

/**
 * @author vinay
 *
 */
public class Chronicle implements Vitae {

	private static Logger logger = LogFabricator.instance.spawnLogHabit(Chronicle.class);
	private static boolean gateInitiated = false;

	private static CorpusController annieController;
	private String OUTPUT_FILE_NAME;

	@Override
	public File extractEnrichedContent(FileProperties recordToProcess) {
		// determine extension
		String ext = FilenameUtils.getExtension(recordToProcess.getFileName());
		String outputFileFormat = "";

		if (ext.equalsIgnoreCase("html") | ext.equalsIgnoreCase("pdf") | ext.equalsIgnoreCase("doc")
				| ext.equalsIgnoreCase("docx")) {
			outputFileFormat = ".html";
			// handler = new ToXMLContentHandler();
		} else if (ext.equalsIgnoreCase("txt") | ext.equalsIgnoreCase("rtf")) {
			outputFileFormat = ".txt";
		} else {
			logger.info("Input format of the file " + recordToProcess.getFileName() + " is not supported.");
			return null;
		}
		OUTPUT_FILE_NAME = FilenameUtils.removeExtension(recordToProcess.getFileName()) + outputFileFormat;

		// handle content
		ContentHandler handler = new ToXMLContentHandler();

		InputStream stream = getFileStream(recordToProcess);
		AutoDetectParser parser = new AutoDetectParser();
		Metadata metadata = new Metadata();
		try {
			parser.parse(stream, handler, metadata);
			File toProcess = new File(PropertyAccessor.CONNECTION.getProperties().getProperty("nlp.tmp.path"),
					OUTPUT_FILE_NAME);

			// if file doesnt exists, then create it
			if (!toProcess.exists()) {
				toProcess.createNewFile();
			}
			FileWriter htmlFileWriter = new FileWriter(toProcess);
			htmlFileWriter.write(handler.toString());
			htmlFileWriter.flush();
			htmlFileWriter.close();
			return toProcess;
		} catch (IOException | SAXException | TikaException e) {
			throw new ContentEnrichmentRuntimeException();
		} finally {
			try {
				stream.close();
			} catch (IOException e) {
				throw new ContentEnrichmentRuntimeException();
			}
		}
	}

	/**
	 * Help convert file byte buffer to stream
	 * 
	 * @param fileProperties
	 * @return
	 */
	private InputStream getFileStream(FileProperties fileProperties) {
		InputStream requiredProperties = null;

		if (fileProperties.getFile().hasArray()) {
			// use heap buffer; no array is created; only the reference is used
			requiredProperties = new ByteArrayInputStream(fileProperties.getFile().array());
		}
		return requiredProperties;
	}

	/**
	 * Aid in retrieving resource located in stated path
	 * 
	 * @param path
	 *            is the provided location
	 * @return the file or folder set required
	 */
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
	 * Help instantiate Annie plugin specific resources
	 * 
	 * @throws GateException
	 *             when gate instantiation has failed
	 * @throws IOException
	 *             when throughput interference is detected
	 */
	private void initAnnie() throws GateException, IOException {
		// load the ANNIE application from the saved state in plugins/ANNIE
		File gateHome = Gate.getGateHome();
		File annieGapp = new File(gateHome, "ANNIEResumeParser.gapp");
		annieController = (CorpusController) PersistenceManager.loadObjectFromFile(annieGapp);
	}

	@Override
	public void initializeNLPEngine(String path) {
		if (!gateInitiated && Gate.getGateHome() == null) {
			logger.info("Initialising Gate");
			if (path != null && !path.isEmpty()) {
				path = path + "/gate";
			} else {
				path = "gate";
			}

			try {
				Gate.setGateHome(new File(path));
				File gateHome = Gate.getGateHome();
				Gate.setUserConfigFile(new File(gateHome, "user-gate.xml"));

				path = path + "/plugins";
				Gate.setPluginsHome(new File(path));

				Gate.init();
				path = path + "/ANNIE";
				Gate.getCreoleRegister().registerDirectories(new File(path).toURI().toURL());
				logger.info("Gate initialised");

				// initialise ANNIE (this may take several minutes)
				logger.info("Initialising Annie");
				this.initAnnie();
				logger.info("Annie initialised");

				gateInitiated = true;
			} catch (GateException | IOException e) {
				throw new NLPEngineRuntimeException();
			}
		}
	}

	@Override
	public JSONObject processJournal(File enrichedContent) {
		// create a corpus and add document to it
		Corpus corpus = null;
		JSONObject requiredObject = null;
		try {
			corpus = Factory.newCorpus("Annie corpus");

			URL u = enrichedContent.toURI().toURL();
			FeatureMap params = Factory.newFeatureMap();
			params.put("sourceUrl", u);
			params.put("preserveOriginalContent", new Boolean(true));
			params.put("collectRepositioningInfo", new Boolean(true));
			logger.info("Creating doc for " + u);
			Document resume = (Document) Factory.createResource("gate.corpora.DocumentImpl", params);
			corpus.add(resume);

			// tell the pipeline about the corpus and run it
			annieController.setCorpus(corpus);
			logger.info("Run Annie");
			annieController.execute();
			logger.info("Annie's run complete");

			logger.info("Begin to parse");
			requiredObject = spliceMetadata(corpus);

		} catch (ResourceInstantiationException | ExecutionException | MalformedURLException e) {
			throw new JournalRuntimeException();
		} finally {
			// clear residual file
			File tika = null;
			tika = new File(PropertyAccessor.CONNECTION.getProperties().getProperty("nlp.tmp.path"), OUTPUT_FILE_NAME);
			if (tika != null && tika.exists()) {
				tika.delete();
			}
		}

		return requiredObject;

	}

	/**
	 * Helps interpret information stated in corpus
	 * 
	 * @param corpus
	 *            is the data set to be processed
	 * @return the interpreted data set
	 */
	private JSONObject spliceMetadata(Corpus corpus) {
		if (corpus == null) {
			return null;
		}
		Iterator iter = corpus.iterator();
		JSONObject parsedJSON = new JSONObject();
		logger.info("Start to parse");
		if (iter.hasNext()) { // repeat this if there is more than one document
			JSONObject profileJSON = new JSONObject();
			// change to user profile object
			Document doc = (Document) iter.next();
			AnnotationSet defaultAnnotSet = doc.getAnnotations();

			AnnotationSet curAnnSet;
			Iterator it;
			Annotation currAnnot;

			// get name
			curAnnSet = defaultAnnotSet.get("NameFinder");
			if (curAnnSet.iterator().hasNext()) { // a single name discovery is
													// allowed
				currAnnot = (Annotation) curAnnSet.iterator().next();
				String gender = (String) currAnnot.getFeatures().get("gender");
				if (gender != null && gender.length() > 0) {
					profileJSON.put("gender", gender);
				}

				// required name parts
				JSONObject nameJson = new JSONObject();
				String[] nameFeatures = new String[] { "firstName", "middleName", "surname" };

				for (String feature : nameFeatures) {
					String s = (String) currAnnot.getFeatures().get(feature);
					if (s != null && s.length() > 0) {
						nameJson.put(feature, s);
					}
				}
				profileJSON.put("name", nameJson);
			} // name

			// title
			curAnnSet = defaultAnnotSet.get("TitleFinder");
			if (curAnnSet.iterator().hasNext()) { // a single title discovery is
													// allowed
				currAnnot = (Annotation) curAnnSet.iterator().next();
				String title = stringFor(doc, currAnnot);
				if (title != null && title.length() > 0) {
					profileJSON.put("title", title);
				}
			} // title

			// email,address,phone,url
			String[] annSections = new String[] { "EmailFinder", "AddressFinder", "PhoneFinder", "URLFinder" };
			String[] annKeys = new String[] { "email", "address", "phone", "url" };
			for (short i = 0; i < annSections.length; i++) {
				String annSection = annSections[i];
				curAnnSet = defaultAnnotSet.get(annSection);
				it = curAnnSet.iterator();
				JSONArray sectionArray = new JSONArray();
				while (it.hasNext()) { // process all available values for these
					currAnnot = (Annotation) it.next();
					String s = stringFor(doc, currAnnot);
					if (s != null && s.length() > 0) {
						sectionArray.add(s);
					}
				}
				if (sectionArray.size() > 0) {
					profileJSON.put(annKeys[i], sectionArray);
				}
			}
			if (!profileJSON.isEmpty()) {
				parsedJSON.put("basics", profileJSON);
			}

			// awards,credibility,education_and_training,extracurricular,misc,skills,summary
			String[] otherSections = new String[] { "summary", "education_and_training", "skills", "accomplishments",
					"awards", "credibility", "extracurricular", "misc" };
			for (String otherSection : otherSections) {
				curAnnSet = defaultAnnotSet.get(otherSection);
				it = curAnnSet.iterator();
				JSONArray subSections = new JSONArray();
				while (it.hasNext()) {
					JSONObject subSection = new JSONObject();
					currAnnot = (Annotation) it.next();
					String key = (String) currAnnot.getFeatures().get("sectionHeading");
					String value = stringFor(doc, currAnnot);
					if (!StringUtils.isBlank(key) && !StringUtils.isBlank(value)) {
						subSection.put(key, value);
					}
					if (!subSection.isEmpty()) {
						subSections.add(subSection);
					}
				}
				if (!subSections.isEmpty()) {
					parsedJSON.put(otherSection, subSections);
				}
			}

			// work_experience
			curAnnSet = defaultAnnotSet.get("work_experience");
			it = curAnnSet.iterator();
			JSONArray workExperiences = new JSONArray();
			while (it.hasNext()) {
				JSONObject workExperience = new JSONObject();
				currAnnot = (Annotation) it.next();
				String key = (String) currAnnot.getFeatures().get("sectionHeading");
				if (key.equals("work_experience_marker")) {
					// JSONObject details = new JSONObject();
					String[] annotations = new String[] { "date_start", "date_end", "jobtitle", "organization" };
					for (String annotation : annotations) {
						String v = (String) currAnnot.getFeatures().get(annotation);
						if (!StringUtils.isBlank(v)) {
							// details.put(annotation, v);
							workExperience.put(annotation, v);
						}
					}
					// if (!details.isEmpty()) {
					// workExperience.put("work_details", details);
					// }
					key = "text";

				}
				String value = stringFor(doc, currAnnot);
				if (!StringUtils.isBlank(key) && !StringUtils.isBlank(value)) {
					workExperience.put(key, value);
				}
				if (!workExperience.isEmpty()) {
					workExperiences.add(workExperience);
				}

			}
			if (!workExperiences.isEmpty()) {
				parsedJSON.put("work_experience", workExperiences);
			}

		}
		logger.info("Finish parsing");
		return parsedJSON;
	}

}
