package edu.umassmed.OmeroImporter;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

import ome.formats.OMEROMetadataStoreClient;
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
import omero.gateway.facility.TablesFacility;
import omero.gateway.model.DatasetData;
import omero.gateway.model.ExperimenterData;
import omero.gateway.model.FileAnnotationData;
import omero.gateway.model.ImageData;
import omero.gateway.model.MapAnnotationData;
import omero.gateway.model.ProjectData;
import omero.gateway.model.TableData;
import omero.gateway.model.TableDataColumn;
import omero.log.Logger;
import omero.log.SimpleLogger;
import omero.model.NamedValue;

public class OmeroDataWriter {
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

	public OmeroDataWriter(final String hostName_arg, final Integer port_arg,
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

	public ImageData retrieveImage(final String imageName,
			final DatasetData dataset) throws DSOutOfServiceException,
			DSAccessException {
		final List<Long> datasetIDs = new ArrayList<Long>();
		datasetIDs.add(dataset.getId());
		final Collection<ImageData> images = this.browser.getImagesForDatasets(
				this.ctx, datasetIDs);

		final Iterator<ImageData> i = images.iterator();
		ImageData image;
		while (i.hasNext()) {
			image = i.next();
			if (image.getName().equals(imageName))
				return image;
		}

		return null;
	}

	public DatasetData retrieveDataset(final String datasetName,
			final ProjectData project) throws DSOutOfServiceException,
			DSAccessException {
		final List<Long> datasetIDs = this
				.retrieveDatasetIDsFromProject(project);
		final Collection<DatasetData> datasets = this.browser.getDatasets(
				this.ctx, datasetIDs);

		final Iterator<DatasetData> i = datasets.iterator();
		DatasetData dataset;
		while (i.hasNext()) {
			dataset = i.next();
			if (dataset.getName().equals(datasetName))
				return dataset;
		}
		return null;
	}

	public List<Long> retrieveDatasetIDsFromProject(final ProjectData project)
			throws DSOutOfServiceException, DSAccessException {
		final List<Long> datasetIDs = new ArrayList<Long>();
		final Collection<ProjectData> projects = this.browser
				.getProjects(this.ctx);
		final Iterator<ProjectData> i = projects.iterator();
		ProjectData localProject;
		while (i.hasNext()) {
			localProject = i.next();
			if (localProject.getId() == project.getId()) {
				for (final DatasetData dataset : project.getDatasets()) {
					datasetIDs.add(dataset.getId());
				}
			}
		}

		return datasetIDs;
	}
	
	public ProjectData retrieveProject(final String projectName)
			throws DSOutOfServiceException, DSAccessException {
		final Collection<ProjectData> projects = this.browser
				.getProjects(this.ctx);

		final Iterator<ProjectData> i = projects.iterator();
		ProjectData project;
		while (i.hasNext()) {
			project = i.next();
			if (project.getName().equals(projectName))
				return project;
		}
		return null;
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

	public void writeDataToProject(final ProjectData project)
			throws DSOutOfServiceException, DSAccessException {
		final List<NamedValue> result = new ArrayList<NamedValue>();
		result.add(new NamedValue("mitomycin-A", "20mM"));
		result.add(new NamedValue("PBS", "10mM"));
		result.add(new NamedValue("incubation", "5min"));
		result.add(new NamedValue("temperature", "37"));
		result.add(new NamedValue("Organism", "Homo sapiens"));
		final MapAnnotationData data = new MapAnnotationData();
		data.setContent(result);
		data.setDescription("Training Example Project");
		// Use the following namespace if you want the annotation to be editable
		// in the webclient and insight
		data.setNameSpace(MapAnnotationData.NS_CLIENT_CREATED);
		this.dataManager.attachAnnotation(this.ctx, data, project);
	}

	public void writeDataToDataset(final DatasetData dataset)
			throws DSOutOfServiceException, DSAccessException {
		final List<NamedValue> result = new ArrayList<NamedValue>();
		result.add(new NamedValue("mitomycin-A", "20mM"));
		result.add(new NamedValue("PBS", "10mM"));
		result.add(new NamedValue("incubation", "5min"));
		result.add(new NamedValue("temperature", "37"));
		result.add(new NamedValue("Organism", "Homo sapiens"));
		final MapAnnotationData data = new MapAnnotationData();
		data.setContent(result);
		data.setDescription("Training Example Dataset");
		// Use the following namespace if you want the annotation to be editable
		// in the webclient and insight
		data.setNameSpace(MapAnnotationData.NS_CLIENT_CREATED);
		this.dataManager.attachAnnotation(this.ctx, data, dataset);
	}
	
	public void writeDataToImage(final ImageData image)
			throws DSOutOfServiceException, DSAccessException {
		final List<NamedValue> result = new ArrayList<NamedValue>();
		result.add(new NamedValue("mitomycin-A", "20mM"));
		result.add(new NamedValue("PBS", "10mM"));
		result.add(new NamedValue("incubation", "5min"));
		result.add(new NamedValue("temperature", "37"));
		result.add(new NamedValue("Organism", "Homo sapiens"));
		final MapAnnotationData data = new MapAnnotationData();
		data.setContent(result);
		data.setDescription("Training Example Image");
		// Use the following namespace if you want the annotation to be editable
		// in the webclient and insight
		data.setNameSpace(MapAnnotationData.NS_CLIENT_CREATED);
		this.dataManager.attachAnnotation(this.ctx, data, image);
	}

	public void writeDataTableToImage(final ImageData image)
			throws DSOutOfServiceException, DSAccessException,
			ExecutionException {
		final TableDataColumn[] columns = new TableDataColumn[3];
		columns[0] = new TableDataColumn("ID", 0, Long.class);
		columns[1] = new TableDataColumn("Name", 1, String.class);
		columns[2] = new TableDataColumn("Value", 2, Double.class);
		
		final Object[][] data = new Object[3][5];
		data[0] = new Long[] { 62102L, 62102L, 62102L, 62102L, 62102L };
		data[1] = new String[] { "one", "two", "three", "four", "five" };
		data[2] = new Double[] { 1d, 2d, 3d, 4d, 5d };
		
		TableData tableData = new TableData(columns, data);
		
		final TablesFacility tabFac = this.gateway
				.getFacility(TablesFacility.class);
		
		// Attach the table to the image
		tableData = tabFac.addTable(this.ctx, image, "My Data", tableData);
		
		// Find the table again
		final Collection<FileAnnotationData> tables = tabFac
				.getAvailableTables(this.ctx, image);
		final long fileId = tables.iterator().next().getFileID();
		
		// Request second and third column of the first three rows
		final TableData tableData2 = tabFac.getTable(this.ctx, fileId, 0, 2, 1,
				2);
		
		// do something, e.g. print to System.out
		final int nRows = tableData2.getData()[0].length;
		for (int row = 0; row < nRows; row++) {
			for (int col = 0; col < tableData2.getColumns().length; col++) {
				final Object o = tableData2.getData()[col][row];
				System.out.print(o + " ["
						+ tableData2.getColumns()[col].getType() + "]\t");
			}
			System.out.println();
		}
	}

	public static void main(final String[] args) {
		String hostName = "localhost", port = "4064", userName = null, password = null;
		System.getProperty(Paths.get(".").toAbsolutePath().normalize()
				.toString());
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

		// final File f = new File(target);
		// if (!f.exists() || !f.isDirectory()) {
		// System.out.println("ERROR: the target specified is invalid");
		// return;
		// }

		final OmeroDataWriter dw = new OmeroDataWriter(hostName, portI,
				userName, password);
		try {
			dw.init();
		} catch (final Exception ex) {
			System.out.println("ERROR: " + ex.getMessage());
			dw.close();
			return;
		}
		
		try {
			final ProjectData proj = dw.retrieveProject("ImportTestProject");
			final DatasetData dataset = dw.retrieveDataset("ImportTestDataset",
					proj);
			final ImageData image = dw.retrieveImage(
					"VIRUS snr 1 density low_tSr.tif.ome.tif", dataset);
			dw.writeDataToProject(proj);
			dw.writeDataToDataset(dataset);
			dw.writeDataToImage(image);
			dw.writeDataTableToImage(image);
		} catch (final DSOutOfServiceException ex) {
			System.out.println("ERROR: " + ex.getMessage());
			dw.close();
			return;
		} catch (final DSAccessException ex) {
			System.out.println("ERROR: " + ex.getMessage());
			dw.close();
			return;
		} catch (final ExecutionException ex) {
			System.out.println("ERROR: " + ex.getMessage());
			dw.close();
			return;
		}
		
		dw.close();
	}
}
