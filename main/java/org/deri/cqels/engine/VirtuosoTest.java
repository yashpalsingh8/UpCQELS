package org.deri.cqels.engine;


import java.util.Iterator;

import virtuoso.jena.driver.VirtGraph;
import virtuoso.jena.driver.VirtuosoQueryExecution;
import virtuoso.jena.driver.VirtuosoQueryExecutionFactory;
import virtuoso.jena.driver.VirtuosoUpdateFactory;
import virtuoso.jena.driver.VirtuosoUpdateRequest;


//import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
//import com.hp.hpl.jena.query.ResultSetFormatter;
//import com.hp.hpl.jena.rdf.model.RDFNode;

/**
 * @author ali
 *
 */
  public class VirtuosoTest  {
	  /**
		 * @param args
		 */
	  /**
	 * 
	 */
	  private String userName; 
	  private String password;
		private String virtuosoAddress;

		private VirtGraph virtGraph; 
		private VirtuosoUpdateRequest virtUpdateFactory;
		private VirtuosoQueryExecution qExecution;
		private Query query;
		private ResultSet res;
		private QuerySolution qSlution;
private String triples;

		private StringBuffer strBuffer;
	
		
		 VirtuosoTest() {
			
		}
		
	/**
	 * @return the userName
	 */
	public String getUserName() {
		return userName;
	}

	/**
	 * @param userName the userName to set
	 */
	public void setUserName(String userName) {
		this.userName = userName;
	}

	/**
	 * @return the password
	 */
	public String getPassword() {
		return password;
	}

	/**
	 * @param password the password to set
	 */
	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * @return the virtuosoAddress
	 */
	public String getVirtuosoAddress() {
		return virtuosoAddress;
	}

	/**
	 * @param virtuosoAddress the virtuosoAddress to set
	 */
	public void setVirtuosoAddress(String virtuosoAddress) {
		this.virtuosoAddress = virtuosoAddress;
	}

	/**
	 * @return the virtGraph
	 */
	public VirtGraph getVirtGraph() {
		return virtGraph;
	}

	/**
	 * @param virtGraph the virtGraph to set
	 */
	public void setVirtGraph(VirtGraph virtGraph) {
		this.virtGraph = virtGraph;
	}

	/**
	 * @return the query
	 */
	public Query getQuery() {
		return query;
	}

	/**
	 * @param query the query to set
	 */
	public void setQuery(Query query) {
		this.query = query;
	}

	
	

	/* (non-Javadoc)
	 * @see com.deri.ie.connection.ConnectToVirtuoso#connect(java.lang.String, java.lang.String, java.lang.String)
	
	 *
	 */


	public void connect(String virtuosoAddress, String userName, String userPass) {
		// TODO Auto-generated method stub
		virtGraph= new VirtGraph(virtuosoAddress,userName,userPass);
		this.setVirtGraph(virtGraph);
	}

	/* (non-Javadoc)
	 * @see com.deri.ie.connection.ConnectToVirtuoso#executeSelect(java.lang.String)
	 */
	public ResultSet executeSelect(String query) {
	
		// TODO Auto-generated method stub
			this.query=QueryFactory.create(query);
			virtGraph= this.getVirtGraph();
		
			qExecution=VirtuosoQueryExecutionFactory.create(this.query,virtGraph);
			
			res = qExecution.execSelect();
			
			return res;
	}

	
	
	/* (non-Javadoc)
	 * @see com.deri.ie.connection.ConnectToVirtuoso#executeInsert(java.lang.String)
	 */
	public void executeInsert(String graphName, String triples) {
		// TODO Auto-generated method stub
		String insertGraph= "CLEAR GRAPH"+graphName;
		
		virtUpdateFactory= VirtuosoUpdateFactory.create(graphName, virtGraph);
		
		
		insertGraph="insert into graph <"+graphName +">{" + triples +"}";
		
		virtUpdateFactory=VirtuosoUpdateFactory.create(insertGraph, virtGraph);
		
		virtUpdateFactory.exec();
	}

	/* (non-Javadoc)
	 * @see com.deri.ie.connection.ConnectToVirtuoso#endConnection()
	 */
	
	public void endConnection() {
		// TODO Auto-generated method stub
		virtGraph.close();
		
	}

	/* (non-Javadoc)
	 * @see com.deri.ie.connection.ConnectToVirtuoso#getResults(com.hp.hpl.jena.query.ResultSet)
	 */
	public String getResults(ResultSet res) {
		// TODO Auto-generated method stub
	//System.out.println("inise get results" + res.getResultVars());
	String v= "";
	strBuffer = new StringBuffer();
		while(res.hasNext())
		{
		
			
			qSlution = res.nextSolution();
		
			
		
			
			Iterator<String> i = qSlution.varNames();
		
			while(i.hasNext())
					{
				strBuffer.append(" " + qSlution.get(i.next().toString()) );
				
		}
			strBuffer.append("\n");
			}
		
	
	return strBuffer.toString();
		
	}
	
//	public static void main(String[] args) {
//		
//	}
//	

}
