package edu.umassmed.OmeroDataWriter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import ome.formats.OMEROMetadataStoreClient;
import ome.formats.importer.ImportConfig;
import ome.formats.importer.ImportLibrary;
import ome.formats.importer.OMEROWrapper;
import ome.formats.importer.cli.ErrorHandler;
import ome.formats.importer.cli.LoggingImportMonitor;
import omero.ServerError;
import omero.api.RawFileStorePrx;
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
import omero.model.ChecksumAlgorithm;
import omero.model.ChecksumAlgorithmI;
import omero.model.DatasetAnnotationLink;
import omero.model.DatasetAnnotationLinkI;
import omero.model.FileAnnotation;
import omero.model.FileAnnotationI;
import omero.model.ImageAnnotationLink;
import omero.model.ImageAnnotationLinkI;
import omero.model.NamedValue;
import omero.model.OriginalFile;
import omero.model.OriginalFileI;
import omero.model.ProjectAnnotationLink;
import omero.model.ProjectAnnotationLinkI;
import omero.model.enums.ChecksumAlgorithmSHA1160;

public class OmeroDataWriter {
	private final ImportConfig config;
	private OMEROMetadataStoreClient store;
	private OMEROWrapper reader;
	private ErrorHandler handler;
	private ImportLibrary library;
	private BrowseFacility browser;
	private AdminFacility admin;
	RawFileStorePrx rawFileStore;
	private DataManagerFacility dataManager;
	private SecurityContext ctx;
	private Gateway gateway;
	private final LoginCredentials cred;

	private static int INC = 262144;
	private static String JSON_FILEANN_NS = "micro-meta-app.json";
	private static String JSON_FILETYPE = "application/json";
	
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

		this.rawFileStore = this.gateway.getRawFileService(this.ctx);
	}
	
	private ImageData retrieveImage(final String imageName,
			final DatasetData dataset)
			throws DSOutOfServiceException, DSAccessException {
		final List<Long> datasetIDs = new ArrayList<Long>();
		datasetIDs.add(dataset.getId());
		final Collection<ImageData> images = this.browser
				.getImagesForDatasets(this.ctx, datasetIDs);
		
		final Iterator<ImageData> i = images.iterator();
		ImageData image;
		while (i.hasNext()) {
			image = i.next();
			if (image.getName().equals(imageName))
				return image;
		}
		
		return null;
	}
	
	private ProjectData retrieveProject(final Long id)
			throws DSOutOfServiceException, DSAccessException {
		final List<Long> IDs = new ArrayList<Long>();
		IDs.add(id);
		final Collection<ProjectData> projects = this.browser
				.getProjects(this.ctx, IDs);
		final Iterator<ProjectData> i = projects.iterator();
		ProjectData project;
		while (i.hasNext()) {
			project = i.next();
			return project;
		}
		return null;
	}
	
	private DatasetData retrieveDataset(final Long id)
			throws DSOutOfServiceException, DSAccessException {
		final List<Long> IDs = new ArrayList<Long>();
		IDs.add(id);
		final Collection<DatasetData> datasets = this.browser
				.getDatasets(this.ctx, IDs);
		final Iterator<DatasetData> i = datasets.iterator();
		DatasetData dataset;
		while (i.hasNext()) {
			dataset = i.next();
			return dataset;
		}
		return null;
	}
	
	private ImageData retrieveImage(final Long id)
			throws DSOutOfServiceException, DSAccessException {
		final List<Long> IDs = new ArrayList<Long>();
		IDs.add(id);
		final Collection<ImageData> images = this.browser.getImages(this.ctx,
				IDs);
		final Iterator<ImageData> i = images.iterator();
		ImageData image;
		while (i.hasNext()) {
			image = i.next();
			return image;
		}
		return null;
	}
	
	private DatasetData retrieveDataset(final String datasetName,
			final ProjectData project)
			throws DSOutOfServiceException, DSAccessException {
		final List<Long> datasetIDs = this
				.retrieveDatasetIDsFromProject(project);
		final Collection<DatasetData> datasets = this.browser
				.getDatasets(this.ctx, datasetIDs);
		
		final Iterator<DatasetData> i = datasets.iterator();
		DatasetData dataset;
		while (i.hasNext()) {
			dataset = i.next();
			if (dataset.getName().equals(datasetName))
				return dataset;
		}
		return null;
	}
	
	private List<Long> retrieveDatasetIDsFromProject(final ProjectData project)
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
	
	private ProjectData retrieveProject(final String projectName)
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
	
	private Long retrieveUserId(final String userName)
			throws DSOutOfServiceException, DSAccessException {
		final ExperimenterData experimenter = this.admin
				.lookupExperimenter(this.ctx, userName);
		if (experimenter == null)
			return -1L;
		return experimenter.getId();
	}
	
	public void close() {
		if (this.gateway != null) {
			this.gateway.disconnect();
		}
		if (this.store != null) {
			this.store.logout();
		}
	}
	
	public void writeDataToProject(final String projectName,
			final String description, final Map<String, String> keyValueData)
			throws DSOutOfServiceException, DSAccessException {
		final ProjectData project = this.retrieveProject(projectName);
		this.writeDataToProject(project.getId(), description, keyValueData);
	}
	
	public void writeDataToProject(final Long id, final String description,
			final Map<String, String> keyValueData)
			throws DSOutOfServiceException, DSAccessException {
		final ProjectData project = this.retrieveProject(id);
		final List<NamedValue> result = new ArrayList<NamedValue>();
		for (final String key : keyValueData.keySet()) {
			result.add(new NamedValue(key, keyValueData.get(key)));
		}
		final MapAnnotationData data = new MapAnnotationData();
		data.setContent(result);
		data.setDescription(description);
		// Use the following namespace if you want the annotation to be editable
		// in the webclient and insight
		data.setNameSpace(MapAnnotationData.NS_CLIENT_CREATED);
		this.dataManager.attachAnnotation(this.ctx, data, project);
	}
	
	public void writeDataToDataset(final String projectName,
			final String datasetName, final String description,
			final Map<String, String> keyValueData)
			throws DSOutOfServiceException, DSAccessException {
		final ProjectData project = this.retrieveProject(projectName);
		final DatasetData dataset = this.retrieveDataset(datasetName, project);
		this.writeDataToDataset(dataset.getId(), description, keyValueData);
	}
	
	public void writeDataToDataset(final Long id, final String description,
			final Map<String, String> keyValueData)
			throws DSOutOfServiceException, DSAccessException {
		final DatasetData dataset = this.retrieveDataset(id);
		final List<NamedValue> result = new ArrayList<NamedValue>();
		for (final String key : keyValueData.keySet()) {
			result.add(new NamedValue(key, keyValueData.get(key)));
		}
		final MapAnnotationData data = new MapAnnotationData();
		data.setContent(result);
		data.setDescription(description);
		// Use the following namespace if you want the annotation to be editable
		// in the webclient and insight
		data.setNameSpace(MapAnnotationData.NS_CLIENT_CREATED);
		this.dataManager.attachAnnotation(this.ctx, data, dataset);
	}
	
	public void writeDataToImage(final String projectName,
			final String datasetName, final String imageName,
			final String description, final Map<String, String> keyValueData)
			throws DSOutOfServiceException, DSAccessException {
		final ProjectData project = this.retrieveProject(projectName);
		final DatasetData dataset = this.retrieveDataset(datasetName, project);
		final ImageData image = this.retrieveImage(imageName, dataset);
		this.writeDataToImage(image.getId(), description, keyValueData);
	}
	
	public void writeDataToImage(final Long id, final String description,
			final Map<String, String> keyValueData)
			throws DSOutOfServiceException, DSAccessException {
		final ImageData image = this.retrieveImage(id);
		final List<NamedValue> result = new ArrayList<NamedValue>();
		for (final String key : keyValueData.keySet()) {
			result.add(new NamedValue(key, keyValueData.get(key)));
		}
		final MapAnnotationData data = new MapAnnotationData();
		data.setContent(result);
		data.setDescription(description);
		// Use the following namespace if you want the annotation to be editable
		// in the webclient and insight
		data.setNameSpace(MapAnnotationData.NS_CLIENT_CREATED);
		this.dataManager.attachAnnotation(this.ctx, data, image);
	}

	public void writeFileAnnotationToImage(final Long id, final File f)
			throws DSOutOfServiceException, DSAccessException,
			FileNotFoundException, IOException, ServerError {
		final ImageData image = this.retrieveImage(id);
		final FileAnnotationData data = new FileAnnotationData(f);
		data.setNameSpace("micro-meta-app.json");
		// this.dataManager.attachAnnotation(this.ctx, data, image);

		final String name = f.getName();
		final String absolutePath = f.getAbsolutePath();
		final String path = absolutePath.substring(0,
				absolutePath.length() - name.length());
		// create the original file object.
		OriginalFile originalFile = new OriginalFileI();
		originalFile.setName(omero.rtypes.rstring(f.getName()));
		originalFile.setPath(omero.rtypes.rstring(path));
		originalFile.setSize(omero.rtypes.rlong(f.length()));
		final ChecksumAlgorithm checksumAlgorithm = new ChecksumAlgorithmI();
		checksumAlgorithm
				.setValue(omero.rtypes.rstring(ChecksumAlgorithmSHA1160.value));
		originalFile.setHasher(checksumAlgorithm);
		originalFile.setMimetype(
				omero.rtypes.rstring(OmeroDataWriter.JSON_FILETYPE));
		// Now we save the originalFile object
		originalFile = (OriginalFile) this.dataManager
				.saveAndReturnObject(this.ctx, originalFile);

		// Initialize the service to load the raw data
		final RawFileStorePrx rawFileStore = this.gateway
				.getRawFileService(this.ctx);

		long pos = 0;
		int rlen;
		final byte[] buf = new byte[OmeroDataWriter.INC];
		ByteBuffer bbuf;
		// Open file and read stream
		try (FileInputStream stream = new FileInputStream(f)) {
			rawFileStore.setFileId(originalFile.getId().getValue());
			while ((rlen = stream.read(buf)) > 0) {
				rawFileStore.write(buf, pos, rlen);
				pos += rlen;
				bbuf = ByteBuffer.wrap(buf);
				bbuf.limit(rlen);
			}
			originalFile = rawFileStore.save();
		} finally {
			rawFileStore.close();
		}

		FileAnnotation fa = new FileAnnotationI();
		fa.setFile(originalFile);
		// fa.setDescription(omero.rtypes.rstring(description));
		fa.setNs(omero.rtypes.rstring(OmeroDataWriter.JSON_FILEANN_NS));
		fa = (FileAnnotation) this.dataManager.saveAndReturnObject(this.ctx,
				fa);

		// now link the image and the annotation
		ImageAnnotationLink link = new ImageAnnotationLinkI();
		link.setChild(fa);
		link.setParent(image.asImage());
		// save the link back to the server.
		link = (ImageAnnotationLink) this.dataManager
				.saveAndReturnObject(this.ctx, link);
	}
	
	public void writeDataTableToProject(final String projectName,
			final String datasetName, final String name, final String desc,
			final List<String> columnNames,
			final List<List<? extends Object>> tableColumnsData,
			final boolean saveAsCSV) throws DSOutOfServiceException,
			DSAccessException, ExecutionException, ServerError, IOException {
		final ProjectData project = this.retrieveProject(projectName);
		this.writeDataTableToProject(project.getId(), name, desc, columnNames,
				tableColumnsData, saveAsCSV);
	}
	
	public void writeDataTableToProject(final Long id, final String name,
			final String desc, final List<String> columnNames,
			final List<List<? extends Object>> tableColumnsData,
			final boolean saveAsCSV) throws DSOutOfServiceException,
			DSAccessException, ExecutionException, ServerError, IOException {
		final ProjectData project = this.retrieveProject(id);
		final TableDataColumn[] columns = new TableDataColumn[columnNames
				.size()];
		int maxSize = -1;
		for (int i = 0; i < columnNames.size(); i++) {
			final List<? extends Object> listData = tableColumnsData.get(i);
			final Object val = listData.get(0);
			if (maxSize < listData.size()) {
				maxSize = listData.size();
			}
			columns[i] = new TableDataColumn(columnNames.get(i), 0,
					val.getClass());
		}
		
		final Object[][] data = new Object[columnNames.size()][maxSize];
		for (int i = 0; i < tableColumnsData.size(); i++) {
			final List<? extends Object> listData = tableColumnsData.get(i);
			final Object val = listData.get(0);
			final Class clazz = val.getClass();
			data[i] = (Object[]) Array.newInstance(clazz, maxSize);
			for (int y = 0; y < listData.size(); y++) {
				data[i][y] = listData.get(y);
			}
		}
		
		TableData tableData = new TableData(columns, data);
		
		final TablesFacility tabFac = this.gateway
				.getFacility(TablesFacility.class);
		
		// Attach the table to the image
		tableData = tabFac.addTable(this.ctx, project, name, tableData);
		
		if (!saveAsCSV)
			return;
		
		final FileAnnotation fa = this.createCSVFile(name, desc, columnNames,
				tableColumnsData);
		
		// now link the image and the annotation
		ProjectAnnotationLink link = new ProjectAnnotationLinkI();
		link.setChild(fa);
		link.setParent(project.asProject());
		// save the link back to the server.
		link = (ProjectAnnotationLink) this.dataManager
				.saveAndReturnObject(this.ctx, link);
		// o attach to a Dataset use DatasetAnnotationLink;
	}
	
	public void writeDataTableToDataset(final String projectName,
			final String datasetName, final String name, final String desc,
			final List<String> columnNames,
			final List<List<? extends Object>> tableColumnsData,
			final boolean saveAsCSV) throws DSOutOfServiceException,
			DSAccessException, ExecutionException, ServerError, IOException {
		final ProjectData project = this.retrieveProject(projectName);
		final DatasetData dataset = this.retrieveDataset(datasetName, project);
		this.writeDataTableToDataset(dataset.getId(), name, desc, columnNames,
				tableColumnsData, saveAsCSV);
	}
	
	public void writeDataTableToDataset(final Long id, final String name,
			final String desc, final List<String> columnNames,
			final List<List<? extends Object>> tableColumnsData,
			final boolean saveAsCSV) throws DSOutOfServiceException,
			DSAccessException, ExecutionException, ServerError, IOException {
		final DatasetData dataset = this.retrieveDataset(id);
		final TableDataColumn[] columns = new TableDataColumn[columnNames
				.size()];
		int maxSize = -1;
		for (int i = 0; i < columnNames.size(); i++) {
			final List<? extends Object> listData = tableColumnsData.get(i);
			final Object val = listData.get(0);
			if (maxSize < listData.size()) {
				maxSize = listData.size();
			}
			columns[i] = new TableDataColumn(columnNames.get(i), 0,
					val.getClass());
		}
		
		final Object[][] data = new Object[columnNames.size()][maxSize];
		for (int i = 0; i < tableColumnsData.size(); i++) {
			final List<? extends Object> listData = tableColumnsData.get(i);
			final Object val = listData.get(0);
			final Class clazz = val.getClass();
			data[i] = (Object[]) Array.newInstance(clazz, maxSize);
			for (int y = 0; y < listData.size(); y++) {
				data[i][y] = listData.get(y);
			}
		}
		
		TableData tableData = new TableData(columns, data);
		
		final TablesFacility tabFac = this.gateway
				.getFacility(TablesFacility.class);
		
		// Attach the table to the image
		tableData = tabFac.addTable(this.ctx, dataset, name, tableData);
		
		if (!saveAsCSV)
			return;
		
		final FileAnnotation fa = this.createCSVFile(name, desc, columnNames,
				tableColumnsData);
		
		// now link the image and the annotation
		DatasetAnnotationLink link = new DatasetAnnotationLinkI();
		link.setChild(fa);
		link.setParent(dataset.asDataset());
		// save the link back to the server.
		link = (DatasetAnnotationLink) this.dataManager
				.saveAndReturnObject(this.ctx, link);
		// o attach to a Dataset use DatasetAnnotationLink;
	}
	
	public void writeDataTableToImage(final String projectName,
			final String datasetName, final String imageName, final String name,
			final String desc, final List<String> columnNames,
			final List<List<? extends Object>> tableColumnsData,
			final boolean saveAsCSV) throws DSOutOfServiceException,
			DSAccessException, ExecutionException, ServerError, IOException {
		final ProjectData project = this.retrieveProject(projectName);
		final DatasetData dataset = this.retrieveDataset(datasetName, project);
		final ImageData image = this.retrieveImage(imageName, dataset);
		this.writeDataTableToImage(image.getId(), name, desc, columnNames,
				tableColumnsData, saveAsCSV);
	}
	
	public void writeDataTableToImage(final Long id, final String name,
			final String desc, final List<String> columnNames,
			final List<List<? extends Object>> tableColumnsData,
			final boolean saveAsCSV) throws DSOutOfServiceException,
			DSAccessException, ExecutionException, ServerError, IOException {
		final ImageData image = this.retrieveImage(id);
		final TableDataColumn[] columns = new TableDataColumn[columnNames
				.size()];
		int maxSize = -1;
		for (int i = 0; i < columnNames.size(); i++) {
			final List<? extends Object> listData = tableColumnsData.get(i);
			final Object val = listData.get(0);
			if (maxSize < listData.size()) {
				maxSize = listData.size();
			}
			columns[i] = new TableDataColumn(columnNames.get(i), 0,
					val.getClass());
		}
		
		final Object[][] data = new Object[columnNames.size()][maxSize];
		for (int i = 0; i < tableColumnsData.size(); i++) {
			final List<? extends Object> listData = tableColumnsData.get(i);
			final Object val = listData.get(0);
			final Class clazz = val.getClass();
			data[i] = (Object[]) Array.newInstance(clazz, maxSize);
			for (int y = 0; y < listData.size(); y++) {
				data[i][y] = listData.get(y);
			}
		}
		
		TableData tableData = new TableData(columns, data);
		
		final TablesFacility tabFac = this.gateway
				.getFacility(TablesFacility.class);
		
		// Attach the table to the image
		tableData = tabFac.addTable(this.ctx, image, name, tableData);
		
		if (!saveAsCSV)
			return;
		
		final FileAnnotation fa = this.createCSVFile(name, desc, columnNames,
				tableColumnsData);
		
		// now link the image and the annotation
		ImageAnnotationLink link = new ImageAnnotationLinkI();
		link.setChild(fa);
		link.setParent(image.asImage());
		// save the link back to the server.
		link = (ImageAnnotationLink) this.dataManager
				.saveAndReturnObject(this.ctx, link);
		// o attach to a Dataset use DatasetAnnotationLink;
	}
	
	public Object[] getImageInformation(final Long imageID)
			throws DSOutOfServiceException, DSAccessException {
		
		final Collection<ProjectData> projects = this.browser
				.getProjects(this.ctx);
		
		final Object[] infos = new Object[4];
		String name = "";
		
		final Iterator<ProjectData> projIt = projects.iterator();
		ProjectData project;
		while (projIt.hasNext()) {
			project = projIt.next();
			name = project.getName() + "/";
			final Iterator<DatasetData> datIt = project.getDatasets()
					.iterator();
			DatasetData dataset;
			while (datIt.hasNext()) {
				dataset = datIt.next();
				final String name2 = name + dataset.getName() + "/";
				final List<Long> datasetIDs = new ArrayList<Long>();
				datasetIDs.add(dataset.getId());
				final Collection<ImageData> images = this.browser
						.getImagesForDatasets(this.ctx, datasetIDs);
				final Iterator<ImageData> imgIt = images.iterator();
				ImageData image;
				while (imgIt.hasNext()) {
					image = imgIt.next();
					final String name3 = name2 + image.getName();
					if (image.getId() == imageID) {
						infos[0] = image.getId();
						infos[1] = dataset.getId();
						infos[2] = project.getId();
						infos[3] = name3;
						return infos;
					}
				}
			}
		}
		return null;
	}
	
	private FileAnnotation createCSVFile(final String name, final String desc,
			final List<String> columnNames,
			final List<List<? extends Object>> tableColumnsData)
			throws ServerError, IOException, DSOutOfServiceException,
			DSAccessException {
		final int INC = 262144;
		
		// To retrieve the image see above.
		final String csvName = name + "_CSV";
		final File file = File.createTempFile(csvName, ".csv");
		final String localName = file.getName();
		final String absolutePath = file.getAbsolutePath();
		final String path = absolutePath.substring(0,
				absolutePath.length() - localName.length());
		
		final FileWriter fw = new FileWriter(file);
		final BufferedWriter bw = new BufferedWriter(fw);
		int maxSize = 0;
		for (int i = 0; i < columnNames.size(); i++) {
			if (maxSize < tableColumnsData.get(i).size()) {
				maxSize = tableColumnsData.get(i).size();
			}
			if (i > 0) {
				bw.write(",");
			}
			bw.write(columnNames.get(i));
			
		}
		bw.write("\n");
		for (int y = 0; y < maxSize; y++) {
			for (int i = 0; i < columnNames.size(); i++) {
				if (i > 0) {
					bw.write(",");
				}
				final List<? extends Object> tableColumnData = tableColumnsData
						.get(i);
				if (tableColumnData.size() > y) {
					bw.write(String.valueOf(tableColumnData.get(y)));
				} else {
					bw.write("");
				}
			}
			bw.write("\n");
		}
		bw.flush();
		bw.close();
		fw.close();
		
		// create the original file object.
		OriginalFile originalFile = new OriginalFileI();
		originalFile.setName(omero.rtypes.rstring(csvName + ".csv"));
		originalFile.setPath(omero.rtypes.rstring(path));
		originalFile.setSize(omero.rtypes.rlong(file.length()));
		final ChecksumAlgorithm checksumAlgorithm = new ChecksumAlgorithmI();
		checksumAlgorithm
				.setValue(omero.rtypes.rstring(ChecksumAlgorithmSHA1160.value));
		originalFile.setHasher(checksumAlgorithm);
		originalFile.setMimetype(omero.rtypes.rstring("fileMimeType")); // or
																		// "application/octet-stream"
		// Now we save the originalFile object
		originalFile = (OriginalFile) this.dataManager
				.saveAndReturnObject(this.ctx, originalFile);
		
		long pos = 0;
		int rlen;
		final byte[] buf = new byte[INC];
		ByteBuffer bbuf;
		// Open file and read stream
		try (FileInputStream stream = new FileInputStream(file)) {
			this.rawFileStore.setFileId(originalFile.getId().getValue());
			while ((rlen = stream.read(buf)) > 0) {
				this.rawFileStore.write(buf, pos, rlen);
				pos += rlen;
				bbuf = ByteBuffer.wrap(buf);
				bbuf.limit(rlen);
			}
			originalFile = this.rawFileStore.save();
		} finally {
			this.rawFileStore.close();
		}
		// now we have an original File in DB and raw data uploaded.
		// We now need to link the Original file to the image using
		// the File annotation object. That's the way to do it.
		FileAnnotation fa = new FileAnnotationI();
		fa.setFile(originalFile);
		// The description set above e.g. PointsModel
		fa.setDescription(omero.rtypes.rstring(desc));
		// The name space you have set to identify the file annotation.
		fa.setNs(omero.rtypes.rstring(MapAnnotationData.NS_CLIENT_CREATED));
		
		// save the file annotation.
		fa = (FileAnnotation) this.dataManager.saveAndReturnObject(this.ctx,
				fa);
		
		return fa;
	}
	
	public static void main(final String[] args) {
		String hostName = "localhost", port = "4064", userName = null,
				password = null;
		System.getProperty(
				Paths.get(".").toAbsolutePath().normalize().toString());
		if (args.length == 0) {
			System.out.println("-h for help");
		}
		if ((args.length == 1) && (args[0].equals("-h"))) {
			System.out.println("-H <hostName>, localhost by default");
			System.out.println("-P <port>, 4064 by default");
			System.out.println("-u <userName>");
			System.out.println("-p <password>");
			System.out.println(
					"-t <target>, target directory to launch the importer");
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
			// final Map<String, String> map = new LinkedHashMap<String,
			// String>();
			// map.put("key_1", "val_1");
			// map.put("key_2", "val_2");
			// map.put("key_3", "val_3");
			// map.put("key_4", "val_4");
			// map.put("key_5", "val_5");
			// dw.writeDataToProject((long) 1206, "test", map);
			// dw.writeDataToDataset((long) 1525, "test", map);
			// dw.writeDataToImage((long) 62102, "test", map);
			// final List<String> colNames = new ArrayList<String>();
			// colNames.add("ID");
			// colNames.add("Name");
			// colNames.add("Value");
			// final List<List<? extends Object>> data = new ArrayList<List<?
			// extends Object>>();
			// // data[0] = new Long[] {1l, 2l, 3l, 4l, 5l};
			// // data[1] = new String[] {"one", "two", "three", "four",
			// "five"};
			// // data[2] = new Double[] {1d, 2d, 3d, 4d, 5d};
			// final List<Long> col1 = new ArrayList<Long>();
			// col1.add(1L);
			// col1.add(2L);
			// col1.add(3L);
			// col1.add(4L);
			// col1.add(5L);
			// final List<String> col2 = new ArrayList<String>();
			// col2.add("one");
			// col2.add("two");
			// col2.add("three");
			// col2.add("four");
			// col2.add("five");
			// final List<Double> col3 = new ArrayList<Double>();
			// col3.add(1D);
			// col3.add(2D);
			// col3.add(3D);
			// col3.add(4D);
			// col3.add(5D);
			// data.add(col1);
			// data.add(col2);
			// data.add(col3);
			// dw.writeDataTableToImage((long) 62102, "My_Test_Data",
			// "csv file desc", colNames, data, true);
			
			final Object[] infos = dw.getImageInformation((long) 62102);
			System.out.println(infos[0] + " / " + infos[1] + " / " + infos[2]
					+ " / " + infos[3]);
		} catch (final DSOutOfServiceException ex) {
			System.out.println("ERROR: " + ex.getMessage());
			dw.close();
			return;
		} catch (final DSAccessException ex) {
			System.out.println("ERROR: " + ex.getMessage());
			dw.close();
			return;
		}
		// } catch (final ExecutionException ex) {
		// System.out.println("ERROR: " + ex.getMessage());
		// dw.close();
		// return;
		// } catch (final ServerError ex) {
		// System.out.println("ERROR: " + ex.getMessage());
		// dw.close();
		// return;
		// } catch (final IOException ex) {
		// System.out.println("ERROR: " + ex.getMessage());
		// dw.close();
		// return;
		// }
		
		dw.close();
	}
}
