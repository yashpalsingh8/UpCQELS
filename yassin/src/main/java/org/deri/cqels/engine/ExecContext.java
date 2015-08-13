package org.deri.cqels.engine;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.OutputStream;
import java.io.Writer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;

import org.deri.cqels.lang.cqels.ParserCQELS;
import org.omg.CORBA.portable.InputStream;

import com.hp.hpl.jena.rdf.model.RDFErrorHandler;
import com.hp.hpl.jena.rdf.model.RDFWriter;
import com.hp.hpl.jena.rdf.model.RDFWriterF;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.tdb.base.file.FileFactory;
import com.hp.hpl.jena.tdb.base.file.FileSet;
import com.hp.hpl.jena.tdb.base.file.Location;
import com.hp.hpl.jena.tdb.index.IndexBuilder;
import com.hp.hpl.jena.tdb.nodetable.NodeTable;
import com.hp.hpl.jena.tdb.nodetable.NodeTableNative;
import com.hp.hpl.jena.tdb.solver.OpExecutorTDB;
import com.hp.hpl.jena.tdb.store.DatasetGraphTDB;
import com.hp.hpl.jena.tdb.store.bulkloader.BulkLoader;
import com.hp.hpl.jena.tdb.sys.SystemTDB;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import virtuoso.jena.driver.VirtGraph;
import virtuoso.jena.driver.VirtuosoQueryExecution;
import virtuoso.jena.driver.VirtuosoQueryExecutionFactory;
/** 
 * This class implements CQELS execution context
 * 
 * @author		Danh Le Phuoc
 * @author 		Chan Le Van
 * @organization DERI Galway, NUIG, Ireland  www.deri.ie
 * @email 	danh.lephuoc@deri.org
 * @email   chan.levan@deri.org
 */
public class ExecContext {
	CQELSEngine engine;
	RoutingPolicy policy;
	Properties config;
	HashMap<String, Object> hashMap;
	HashMap<Integer,OpRouter> routers;
	DatasetGraphTDB dataset;
	NodeTable  dictionary;
	Location location;
	Environment env;
	ExecutionContext arqExCtx;
	private Query query;
	private VirtuosoQueryExecution qExecution;
	private ResultSet res;
	private QuerySolution qSlution;
    /**
	 * @param path home path containing dataset
	 * @param cleanDataset a flag indicates whether the old dataset will be cleaned or not
	 */
	public ExecContext(String path, boolean cleanDataset) {
		this.hashMap = new HashMap<String, Object>();
		//combine cache and disk-based dictionary
//		this.dictionary = new NodeTableNative(IndexBuilder.mem().newIndex(FileSet.mem(), 
//											  		SystemTDB.nodeRecordFactory), 
//											  FileFactory.createObjectFileMem(path));
		this.dictionary = new NodeTableNative(IndexBuilder.mem().newIndex(FileSet.mem(), 
		  						SystemTDB.nodeRecordFactory), 
		  						FileFactory.createObjectFileMem());		
		
//		given by feng
//		this.dictionary = new NodeTableNative(IndexBuilder.mem().newIndex(
//				FileSet.mem(), SystemTDB.nodeRecordFactory),
//				FileFactory.createObjectFileDisk(path + "/dict"));
		
		setEngine(new CQELSEngine(this));
		createCache(path + "/cache");
		if (cleanDataset) {
			cleanNCreate(path + "/datasets");
		}
		createDataSet(path + "/datasets");
		
		this.routers = new HashMap<Integer, OpRouter>();
		this.policy = new HeuristicRoutingPolicy(this);
	}
	
	static void cleanNCreate(String path) {
		deleteDir(new File(path));
		if(!(new File(path)).mkdir()) {
			System.out.println("can not create working directory"+path);
		}
	}
	
	/**
	 * to delete a directory
	 * @param dir directory will be deleted
	 */
	public static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i=0; i<children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                	System.out.println("can not delete" +dir);
                    return false;
                }
            }
        }
        return dir.delete();
	}
	/**
	 * get the ARQ context
	 */
	public ExecutionContext getARQExCtx() {
		return this.arqExCtx;
	}
	/**
	 * create a dataset with a specified location
	 * @param location the specified location string
	 */
	public void createDataSet(String location) {
		//this.dataset = ((DatasetGraphTransaction)TDBFactory.createDatasetGraph(location)).getDatasetGraphToQuery();
		this.dataset = TDBFactory.createDatasetGraph(location);
		this.arqExCtx = new ExecutionContext(this.dataset.getContext(), 
											this.dataset.getDefaultGraph(), 
											this.dataset, OpExecutorTDB.OpExecFactoryTDB);
	}
	
	/**
	 * load a dataset with the specified graph uri and data uri
	 * @param graphUri
	 * @param dataUri 
	 */
	public void loadDataset(String graphUri, String dataUri) {
		BulkLoader.loadNamedGraph(this.dataset, 
					Node.createURI(graphUri),Arrays.asList(dataUri) , false);
	}
	
	/**
	 * load a dataset with the specified data uri
	 * @param dataUri 
	 */
	public void loadDefaultDataset(String dataUri) {
		BulkLoader.loadDefaultGraph(this.dataset, Arrays.asList(dataUri) , false);
		
	}
	
//	Ali
	public void loadDefaultDataset(String dataUri, String userId, String userPass, String queryString) 
	{
		VirtGraph virtGraph = new VirtGraph("http://www.ins.cwi.nl/sib/",dataUri, userId, userPass);
		
//		System.out.println("virtgraph"+virtGraph.toString());
		
		Query query = new Query();
		
		
//		QueryExecution qe = QueryExecutionFactory.sparqlService(dataUri, queryString);
		
//		
	    ParserCQELS parser=new ParserCQELS();
	    parser.parse(query, queryString);
		this.query=QueryFactory.create(query);
		
		VirtuosoQueryExecution vqLex = VirtuosoQueryExecutionFactory.create(query, virtGraph);
		res = vqLex.execSelect();
		Model m =  res.getResourceModel();
//		Iterator it= (Iterator) m;
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		m.write(out, "N-TRIPLE", null);
		
		
		ByteArrayInputStream input = new ByteArrayInputStream(out.toByteArray());
	//	res = qe.execSelect();
				
		
		
		
//		ResultSetFormatter.out(out, res);
//		byte[] data = out.toByteArray();
//		ByteArrayInputStream input = new ByteArrayInputStream(data);
		
//		Model m = res.getResourceModel();
//		String outStream = m.toString();
//		Model outStream = m.write(out, "N-TRIPLE");
		
		
//		VirtuosoQueryExecution vqLex = VirtuosoQueryExecutionFactory.create(query, virtGraph);
//		
//		Dataset dataset = vqLex.getDataset();
		
//		qExecution=VirtuosoQueryExecutionFactory.create(this.query,virtGraph);
		
//		String outStream;
		
		BulkLoader.loadDataset(this.dataset, input, false);
		
//		String outStream = res.toString();
//		.asText(res);
//		String s = outStream.toString();
//		
//		return res;
		
//		Connection conn = DriverManager.getConnection("jdbc:virtuoso://localhost:8890/conductor", "dba", "dba");
//		Connection con = JDBC Connection (dataUri, userId, userPass);
		
//		rs.outputFormat("RDF/XML");
//		ResultSet rs = con.executeQuery("Select * wehre {?s ?p ?o}");
//		BulkLoader.loadDataset(rs, dataUri, false);
	}
	
	/**
	 * get the dataset
	 * @param dataUri 
	 */
	public DatasetGraphTDB getDataset() { 
		return dataset; 
	};
	
	/**
	 * create cache with the specified path
	 * @param cachePath path string 
	 */
	public void createCache(String cachePath) {
		cleanNCreate(cachePath);
        createEnv(cachePath);
	}
	
	private void createEnv(String path) {
	    EnvironmentConfig config = new EnvironmentConfig();
		config.setAllowCreate(true);
		this.env = new Environment(new File(path), config);
	}
	
	/**
	 * get environment
	 */
	public Environment env() { return this.env; }
	
	/**
	 * get CQELS engine
	 */
	public CQELSEngine engine() { return this.engine; }
	
	/**
	 * set CQELS engine
	 * @param engine
	 */
	public  void setEngine(CQELSEngine engine) { this.engine = engine; }
	
	/**
	 * get routing policy
	 */
	public RoutingPolicy policy() { return this.policy; }
	
	/**
	 * set routing policy with the specified policy
	 * @param policy specified policy and mostly heuristic policy in this version
	 */
	public void setPolicy(RoutingPolicy policy) { this.policy = policy; };
	
	/**
	 * put key and value to the map
	 * @param key
	 * @param value
	 */
	public void put(String key, Object value) { this.hashMap.put(key, value); }
	
	/**
	 * get the value with the specified key
	 * @param key 
	 */
	public Object get(String key) { return this.hashMap.get(key); }
	
	/**
	 * init TDB graph with the specified directory
	 * @param directory  
	 */
	public void initTDBGraph(String directory) { 
		//this.dataset = TDBFactory.createDatasetGraph(directory);
		//this.dataset = ((DatasetGraphTransaction)TDBFactory.createDatasetGraph(location)).getBaseDatasetGraph();
		this.dataset = TDBFactory.createDatasetGraph(directory);
	}
	
	/**
	 * load graph pattern
	 * @param op operator
	 * @return query iterator  
	 */
	public QueryIterator loadGraphPattern(Op op) { 
		return Algebra.exec(op, this.dataset); 
	}
	
	/**
	 * load graph pattern with the specified dataset
	 * @param op operator
	 * @param ds specified dataset
	 * @return query iterator  
	 */
	public QueryIterator loadGraphPattern(Op op, DatasetGraph ds) {
		return Algebra.exec(op, ds); 
	}
	
	/**
	 * get the cache location
	 * @return cache location
	 */
	public Location cacheLocation() { return this.location; }
	
	/**
	 * get the dictionary
	 * @return dictionary  
	 */
	public NodeTable dictionary() { return this.dictionary; }
	
	/**
	 * get cache configuration
	 * @return cache configuration  
	 */
	public Properties cacheConfig() { return  this.config; }
	
	/**
	 * @param idx 
	 * @return router   
	 */
	public void router(int idx, OpRouter router) { 
		this.routers.put(Integer.valueOf(idx), router);
//		// for printing plan ---yashpal
//		OpRouter[] e = (OpRouter[]) routers.entrySet().toArray();
//		for (int i = 0; i < e.length; i++) {
//			System.out.println("plan"+e[i].getOp().toString());
//			
//		}
		System.out.println("plan"+this.routers.get(Integer.valueOf(idx)).getOp());
	}
	
	/**
	 * @param idx 
	 * @return router   
	 */
	public OpRouter router(int idx) {
		return this.routers.get(Integer.valueOf(idx));
	}
	
	/**
	 * register a select query
	 * @param queryStr query string 
	 * @return this method return an instance of ContinuousSelect interface  
	 */
	public ContinuousSelect registerSelect(String queryStr) {
		 Query query = new Query();
	     ParserCQELS parser=new ParserCQELS();
	     parser.parse(query, queryStr);
	     return this.policy.registerSelectQuery(query);   
	}
	
	/**
	 * register a construct query
	 * @param queryStr query string 
	 * @return this method return an instance of ContinuousConstruct interface  
	 */
	public ContinuousConstruct registerConstruct(String queryStr) {
		 Query query = new Query();
	     ParserCQELS parser = new ParserCQELS();
	     parser.parse(query, queryStr);
	     return this.policy.registerConstructQuery(query);   
	}
}
