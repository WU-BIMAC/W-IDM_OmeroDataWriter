package edu.umassmed.OmeroImporter;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import loci.formats.in.DefaultMetadataOptions;
import loci.formats.in.MetadataLevel;
import ome.formats.OMEROMetadataStoreClient;
import ome.formats.importer.ImportCandidates;
import ome.formats.importer.ImportConfig;
import ome.formats.importer.ImportLibrary;
import ome.formats.importer.OMEROWrapper;
import ome.formats.importer.cli.ErrorHandler;
import ome.formats.importer.cli.LoggingImportMonitor;
import omero.gateway.Gateway;
import omero.gateway.LoginCredentials;
import omero.gateway.SecurityContext;
import omero.gateway.exception.DSAccessException;
import omero.gateway.exception.DSOutOfServiceException;
import omero.gateway.facility.AdminFacility;
import omero.gateway.facility.BrowseFacility;
import omero.gateway.facility.DataManagerFacility;
import omero.gateway.model.DatasetData;
import omero.gateway.model.ExperimenterData;
import omero.gateway.model.ProjectData;
import omero.log.Logger;
import omero.log.SimpleLogger;
import omero.model.Dataset;
import omero.model.DatasetI;
import omero.model.Project;
import omero.model.ProjectDatasetLink;
import omero.model.ProjectDatasetLinkI;
import omero.model.ProjectI;

public class OmeroImporter {
	private final ImportConfig config;
	private OMEROMetadataStoreClient store;
	private OMEROWrapper reader;
	private ErrorHandler handler;
	private ImportLibrary library;
	private BrowseFacility browser;
	private AdminFacility admin;
	private DataManagerFacility dataManager;
	private SecurityContext ctx;
	private Gateway gateway;
	private final LoginCredentials cred;

	private final String extFilter = "";
	private final String nameFilter = "";

	public OmeroImporter(final String hostName_arg, final Integer port_arg,
			final String userName_arg, final String psw_arg) {
		
		this.config = new ome.formats.importer.ImportConfig();
		
		this.config.email.set("");
		this.config.sendFiles.set(true);
		this.config.sendReport.set(false);
		this.config.contOnError.set(false);
		this.config.debug.set(false);
		
		final String hostName = hostName_arg;
		final Integer port = port_arg;
		final String userName = userName_arg;
		final String psw = psw_arg;
		
		this.config.hostname.set(hostName);
		this.config.port.set(port);
		this.config.username.set(userName);
		this.config.password.set(psw);
		
		this.cred = new LoginCredentials(userName, psw, hostName, port);
	}

	public void init() throws Exception {
		this.store = this.config.createStore();
		this.store.logVersionInfo(this.config.getIniVersionNumber());
		this.reader = new OMEROWrapper(this.config);
		this.library = new ImportLibrary(this.store, this.reader);
		this.handler = new ErrorHandler(this.config);
		this.library.addObserver(new LoggingImportMonitor());

		final Logger simpleLogger = new SimpleLogger();
		this.gateway = new Gateway(simpleLogger);
		this.browser = this.gateway.getFacility(BrowseFacility.class);
		this.admin = this.gateway.getFacility(AdminFacility.class);
		this.dataManager = this.gateway.getFacility(DataManagerFacility.class);
		final ExperimenterData user = this.gateway.connect(this.cred);
		this.ctx = new SecurityContext(user.getGroupId());
	}

	public Long retrieveDatasetId(final String datasetName,
			final String projectName) throws DSOutOfServiceException,
			DSAccessException {
		final List<Long> datasetIDs = this
				.retrieveDatasetIDsFromProject(projectName);
		final Collection<DatasetData> datasets = this.browser.getDatasets(
				this.ctx, datasetIDs);
		
		final Iterator<DatasetData> i = datasets.iterator();
		DatasetData dataset;
		while (i.hasNext()) {
			dataset = i.next();
			if (dataset.getName().equals(datasetName))
				return dataset.getId();
		}
		return -1L;
	}
	
	public List<Long> retrieveDatasetIDsFromProject(final String projectName)
			throws DSOutOfServiceException, DSAccessException {
		final List<Long> datasetIDs = new ArrayList<Long>();
		final Collection<ProjectData> projects = this.browser
				.getProjects(this.ctx);
		final Iterator<ProjectData> i = projects.iterator();
		ProjectData project;
		while (i.hasNext()) {
			project = i.next();
			if (project.getName().equals(projectName)) {
				for (final DatasetData dataset : project.getDatasets()) {
					datasetIDs.add(dataset.getId());
				}
			}
		}
		
		return datasetIDs;
	}

	public Long retrieveProjectId(final String projectName)
			throws DSOutOfServiceException, DSAccessException {
		final Collection<ProjectData> projects = this.browser
				.getProjects(this.ctx);
		
		final Iterator<ProjectData> i = projects.iterator();
		ProjectData project;
		while (i.hasNext()) {
			project = i.next();
			if (project.getName().equals(projectName))
				return project.getId();
		}
		return -1L;
	}
	
	public Long retrieveUserId(final String userName)
			throws DSOutOfServiceException, DSAccessException {
		final ExperimenterData experimenter = this.admin.lookupExperimenter(
				this.ctx, userName);
		if (experimenter == null)
			return -1L;
		return experimenter.getId();
	}

	public void close() {
		this.gateway.disconnect();
		this.store.logout();
	}

	private void importImages(final String path)
			throws DSOutOfServiceException, DSAccessException {
		final File rootDir = new File(path);

		if (!rootDir.isDirectory()) {
			System.out.println(path + " is not a valid directory");
			return;
		}

		for (final File projectDir : rootDir.listFiles()) {
			if (!projectDir.isDirectory()) {
				continue;
			}
			final String projectName = projectDir.getName();
			Long projectId = this.retrieveProjectId(projectName);
			if (projectId == -1L) {
				System.out.println(projectName
						+ " is not a valid OMERO project");
				final Project project = new ProjectI();
				project.setName(omero.rtypes.rstring(projectName));
				// project.setDescription(omero.rtypes
				// .rstring("new description 1"));
				this.dataManager.saveAndReturnObject(this.ctx, project);
				projectId = this.retrieveProjectId(projectName);
				if (projectId == -1L) {
					System.out.println("ERRORE SAVING PROJECT - SKIP");
					continue;
				}
			}

			for (final File datasetDir : projectDir.listFiles()) {
				if (!datasetDir.isDirectory()) {
					continue;
				}
				final String datasetName = datasetDir.getName();
				Long datasetId = this.retrieveDatasetId(datasetName,
						projectName);
				if (datasetId == -1L) {
					System.out.println("Creating dataset " + datasetName);
					final Dataset dataset = new DatasetI();
					dataset.setName(omero.rtypes.rstring(datasetName));
					// dataset.setDescription(omero.rtypes
					// .rstring("new description 1"));
					this.dataManager.saveAndReturnObject(this.ctx, dataset);
					final ProjectDatasetLink link = new ProjectDatasetLinkI();
					link.setChild(dataset);
					link.setParent(new ProjectI(projectId, false));
					this.dataManager.saveAndReturnObject(this.ctx, link);
					datasetId = this
							.retrieveDatasetId(datasetName, projectName);
					if (datasetId == -1L) {
						System.out.println("ERRORE SAVING DATASET - SKIP");
						continue;
					}
					// continue;
				}
				// } else {
				// this.config.targetClass.set("omero.model.Dataset");
				// this.config.targetId.set(datasetId);
				// }
				
				this.config.targetClass.set("omero.model.Dataset");
				this.config.targetId.set(datasetId);

				final List<String> paths = new ArrayList<String>();
				for (final File image : datasetDir.listFiles()) {
					if (image.isDirectory()) {
						continue;
					}
					if (!this.extFilter.isEmpty()
							&& !image.getName().endsWith(this.extFilter)) {
						continue;
					}
					if (!this.nameFilter.isEmpty()
							&& !image.getName().contains(this.nameFilter)) {
						continue;
					}
					image.getName();
					paths.add(image.getAbsolutePath().toString());
				}

				final String[] imagePaths = new String[paths.size()];
				paths.toArray(imagePaths);
				try {
					final ImportCandidates candidates = new ImportCandidates(
							this.reader, imagePaths, this.handler);
					for (final String imagePath : candidates.getPaths()) {
						System.out.println("WARNING: " + imagePath
								+ " will be imported");
					}
					this.reader.setMetadataOptions(new DefaultMetadataOptions(
							MetadataLevel.ALL));
					this.library.importCandidates(this.config, candidates);
				} catch (final Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void main(final String[] args) {
		String hostName = "localhost", port = "4064", userName = null, password = null;
		String target = System.getProperty(Paths.get(".").toAbsolutePath()
				.normalize().toString());
		if (args.length == 0) {
			System.out.println("-h for help");
		}
		if ((args.length == 1) && (args[0].equals("-h"))) {
			System.out.println("-H <hostName>, localhost by default");
			System.out.println("-P <port>, 4064 by default");
			System.out.println("-u <userName>");
			System.out.println("-p <password>");
			System.out
					.println("-t <target>, target directory to launch the importer");
		}

		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-H")) {
				hostName = args[i + 1];
			}
			if (args[i].equals("-P")) {
				port = args[i + 1];
			}
			if (args[i].equals("-u")) {
				userName = args[i + 1];
			}
			if (args[i].equals("-p")) {
				password = args[i + 1];
			}
			if (args[i].equals("-t")) {
				target = args[i + 1];
			}
		}

		if ((userName == null) || (password == null)) {
			System.out.println("ERROR: username or password not specified");
			return;
		}
		
		Integer portI = null;
		try {
			portI = Integer.valueOf(port);
		} catch (final Exception ex) {
			// ex.printStackTrace();
		}
		if (portI == null) {
			System.out.println("ERROR: the port specified is invalid");
			return;
		}

		final File f = new File(target);
		if (!f.exists() || !f.isDirectory()) {
			System.out.println("ERROR: the target specified is invalid");
			return;
		}
		
		final OmeroImporter importer = new OmeroImporter(hostName, portI,
				userName, password);
		try {
			importer.init();
		} catch (final Exception ex) {
			System.out.println("ERROR: " + ex.getMessage());
			importer.close();
			return;
		}

		try {
			importer.importImages(target);
		} catch (final DSOutOfServiceException ex) {
			System.out.println("ERROR: " + ex.getMessage());
			importer.close();
			return;
		} catch (final DSAccessException ex) {
			System.out.println("ERROR: " + ex.getMessage());
			importer.close();
			return;
		}

		importer.close();
	}

}
