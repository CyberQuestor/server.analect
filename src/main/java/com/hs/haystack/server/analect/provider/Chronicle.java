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
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.Logger;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.sax.ToXMLContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import com.hs.haystack.models.common.error.HaystackRuntimeException;
import com.hs.haystack.models.common.error.runtime.analect.ContentEnrichmentRuntimeException;
import com.hs.haystack.models.common.error.runtime.analect.JournalRuntimeException;
import com.hs.haystack.models.common.error.runtime.analect.NLPEngineRuntimeException;
import com.hs.haystack.models.common.file.Implant;
import com.hs.haystack.models.common.profile.Address;
import com.hs.haystack.models.common.profile.AddressType;
import com.hs.haystack.models.common.profile.Company;
import com.hs.haystack.models.common.profile.Phone;
import com.hs.haystack.models.common.profile.PhoneType;
import com.hs.haystack.models.common.profile.Qualification;
import com.hs.haystack.models.common.profile.SkillSet;
import com.hs.haystack.models.common.profile.UserProfile;
import com.hs.haystack.server.analect.interact.Vitae;
import com.hs.haystack.utilities.fabricator.LogFabricator;
import com.hs.haystack.utilities.interpreter.repose.Genus;
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
	public File extractEnrichedContent(Implant recordToProcess) {
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
			throw new ContentEnrichmentRuntimeException(e, recordToProcess);
		} catch (Exception e) {
			throw new HaystackRuntimeException(e, recordToProcess);
		} finally {
			try {
				stream.close();
			} catch (IOException e) {
				throw new ContentEnrichmentRuntimeException(e, recordToProcess);
			} catch (Exception e) {
				throw new HaystackRuntimeException(e, recordToProcess);
			}
		}
	}

	/**
	 * Help convert file byte buffer to stream
	 * 
	 * @param fileProperties
	 * @return
	 */
	private InputStream getFileStream(Implant fileProperties) {
		InputStream requiredProperties = null;
		try {
			if (fileProperties.getFile().hasArray()) {
				// use heap buffer; no array is created; only the reference is
				// used
				requiredProperties = new ByteArrayInputStream(fileProperties.getFile().array());
			}
		} catch (Exception e) {
			throw new HaystackRuntimeException(e, fileProperties);
		}
		return requiredProperties;
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
				throw new NLPEngineRuntimeException(e, path);
			} catch (Exception e) {
				throw new HaystackRuntimeException(e, path);
			}
		}
	}

	@Override
	public Object processJournal(File enrichedContent) {
		// create a corpus and add document to it
		Corpus corpus = null;
		Object requiredObject = null;
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
			throw new JournalRuntimeException(e, enrichedContent);
		} catch (Exception e) {
			throw new HaystackRuntimeException(e, enrichedContent);
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
	private Object spliceMetadata(Corpus corpus) {
		if (corpus == null) {
			return null;
		}
		Iterator<Document> iter = corpus.iterator();
		UserProfile parsedProfile = new UserProfile();
		logger.info("Start to parse");
		if (iter.hasNext()) { // repeat this if there is more than one document
			// change to user profile object
			Document doc = (Document) iter.next();
			AnnotationSet defaultAnnotSet = doc.getAnnotations();

			AnnotationSet curAnnSet;
			Iterator<Annotation> it;
			Annotation currAnnot;

			// get name
			curAnnSet = defaultAnnotSet.get("NameFinder");
			if (curAnnSet.iterator().hasNext()) { // a single name discovery is
													// allowed
				currAnnot = (Annotation) curAnnSet.iterator().next();
				String gender = (String) currAnnot.getFeatures().get("gender");
				if (!Genus.analyzeNullString(gender)) {
					parsedProfile.setGender(gender);
				}

				// required name parts
				String[] nameFeatures = new String[] { "firstName", "middleName", "surname" };

				for (String feature : nameFeatures) {
					String s = (String) currAnnot.getFeatures().get(feature);
					if (!Genus.analyzeNullString(s)) {
						switch (feature) {
						case "firstName":
							parsedProfile.setFirstName(s);
							break;
						case "middleName":
							parsedProfile.setMiddleName(s);
							break;
						case "surname":
							parsedProfile.setLastName(s);
							break;

						}
					}
				}
			} // name

			// title
			curAnnSet = defaultAnnotSet.get("TitleFinder");
			if (curAnnSet.iterator().hasNext()) { // a single title discovery is
													// allowed
				currAnnot = (Annotation) curAnnSet.iterator().next();
				String title = stringFor(doc, currAnnot);
				if (!Genus.analyzeNullString(title)) {
					parsedProfile.setCurrentJobTitle(title);
				}
			} // title

			// email
			curAnnSet = defaultAnnotSet.get("EmailFinder");
			it = curAnnSet.iterator();
			// email section - find all possible
			while (it.hasNext()) {
				currAnnot = (Annotation) it.next();
				String s = stringFor(doc, currAnnot);
				if (!Genus.analyzeNullString(s)) {
					parsedProfile.setPrimaryEmail(s);
					break;
				}
			} // email done

			// address
			curAnnSet = defaultAnnotSet.get("AddressFinder");
			it = curAnnSet.iterator();
			// address section - find all possible
			List<Address> foundAddresses = new ArrayList<Address>();
			Address localAddress = null;
			while (it.hasNext()) {
				currAnnot = (Annotation) it.next();
				String s = stringFor(doc, currAnnot);
				if (!Genus.analyzeNullString(s)) {
					localAddress = new Address();
					localAddress.setAdressType(AddressType.Current);
					localAddress.setAddressLine1(s);
					foundAddresses.add(localAddress);
				}
				parsedProfile.setAvailableAddresses(foundAddresses);
			} // address done

			// address
			curAnnSet = defaultAnnotSet.get("PhoneFinder");
			it = curAnnSet.iterator();
			// address section - find all possible
			List<Phone> foundPhones = new ArrayList<Phone>();
			Phone localPhone = null;
			while (it.hasNext()) {
				currAnnot = (Annotation) it.next();
				String s = stringFor(doc, currAnnot);
				if (!Genus.analyzeNullString(s)) {
					localPhone = new Phone();
					localPhone.setType(PhoneType.Mobile);
					localPhone.setNumber(s);
					foundPhones.add(localPhone);
				}
				parsedProfile.setAvailablePhones(foundPhones);
			} // address done

			// do it for skills now
			curAnnSet = defaultAnnotSet.get("skills");
			it = curAnnSet.iterator();
			List<SkillSet> foundSkills = new ArrayList<SkillSet>();
			SkillSet localSkill = null;
			while (it.hasNext()) {
				currAnnot = (Annotation) it.next();
				String value = stringFor(doc, currAnnot);
				String[] values = value.split("[,\\s]\\s*");
				for (String localValue : values) {
					if (!Genus.analyzeNullString(value)) {
						// TODO - build in split and loop routine - check with
						// solr
						localSkill = new SkillSet();
						localSkill.setName(localValue);
						foundSkills.add(localSkill);
					}
				}
			}
			parsedProfile.setAvailableSkills(foundSkills);
			// skills done

			// do it for qualification now
			curAnnSet = defaultAnnotSet.get("education_and_training");
			it = curAnnSet.iterator();
			List<Qualification> foundQualifications = new ArrayList<Qualification>();
			Qualification localQualification = null;
			while (it.hasNext()) {
				currAnnot = (Annotation) it.next();
				String value = stringFor(doc, currAnnot);
				if (!Genus.analyzeNullString(value)) {
					// TODO - build in split and loop routine
					localQualification = new Qualification();
					localQualification.setName(value);
					foundQualifications.add(localQualification);
				}
			}
			// set at next section
			// qualification done

			// do it for qualification again
			curAnnSet = defaultAnnotSet.get("accomplishments");
			it = curAnnSet.iterator();
			while (it.hasNext()) {
				currAnnot = (Annotation) it.next();
				String value = stringFor(doc, currAnnot);
				if (!Genus.analyzeNullString(value)) {
					// TODO - build in split and loop routine
					localQualification = new Qualification();
					localQualification.setName(value);
					foundQualifications.add(localQualification);
				}
			}
			parsedProfile.setAvailableQualifications(foundQualifications);
			// qualification done

			// do it for work experience now
			curAnnSet = defaultAnnotSet.get("work_experience");
			it = curAnnSet.iterator();
			List<Company> foundCompanies = new ArrayList<Company>();
			Company localCompany = null;
			while (it.hasNext()) {
				currAnnot = (Annotation) it.next();
				String key = (String) currAnnot.getFeatures().get("sectionHeading");
				localCompany = new Company();
				if (key.equals("work_experience_marker")) {
					// get attributes
					String dateStart = (String) currAnnot.getFeatures().get("date_start");
					String dateEnd = (String) currAnnot.getFeatures().get("date_end");
					String jobTitle = (String) currAnnot.getFeatures().get("jobtitle");
					String organization = (String) currAnnot.getFeatures().get("organization");

					if (!Genus.analyzeNullString(dateEnd)) {
						// end date set
						Date endDate = Genus.attemptDateParse(dateEnd);
						if (endDate != null) {
							localCompany.setEndDate(endDate);
						}

						localCompany.setDivision("endDate:" + dateEnd);
					}
					if (!Genus.analyzeNullString(dateStart)) {
						// start date set
						Date startDate = Genus.attemptDateParse(dateStart);
						if (startDate != null) {
							localCompany.setStartDate(startDate);
						}
						localCompany.setDivision(localCompany.getDivision().concat("\nstartDate:" + dateStart));
					}
					if (!Genus.analyzeNullString(jobTitle)) {
						// title set
						localCompany.setTitle(jobTitle);
					}
					if (!Genus.analyzeNullString(organization)) {
						// organization set
						localCompany.setName(organization);
					}
				}

				String value = stringFor(doc, currAnnot);
				if (!Genus.analyzeNullString(value)) {
					localCompany.setDepartment(value);
				}

				foundCompanies.add(localCompany);
			}
			parsedProfile.setAvailableCompanies(foundCompanies);
			// done with work experience

		}
		logger.info("Finish parsing");
		return parsedProfile;
	}

}
