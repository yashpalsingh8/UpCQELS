package org.deri.cqels.main;

//import java.net.*;
//import java.io.*;
//import java.net.URL;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Console;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
//import java.util.Scanner;
//
//import javax.imageio.stream.FileImageInputStream;



import java.util.TimerTask;
import java.util.Timer;

import org.deri.cqels.data.Mapping;
import org.deri.cqels.engine.ContinuousListener;
import org.deri.cqels.engine.ContinuousSelect;
import org.deri.cqels.engine.ExecContext;
//import org.omg.CORBA.portable.InputStream;
//import org.ckan.*;



















import com.hp.hpl.jena.sparql.core.Var;
import com.sleepycat.persist.impl.Store.SyncHook;

public class CQELSMain {

	public static void main(String[] args) throws IOException {
//		try {
//		      //create a buffered reader that connects to the console, we use it so we can read lines
//			File file = new File("output.txt");
//			FileOutputStream fis = new FileOutputStream(file);
//			PrintStream out = new PrintStream(fis);
//			System.setOut(out);
//		   }
//		      catch(IOException e1) {
//		        System.out.println("Error during reading/writing");
//		   }
		// TODO Auto-generated method stub
		// changed name from ali to yashpal in below address
		final ExecContext context = new ExecContext("/Users/yashpal/cqels-code/data/", true);

		// Initialize the RDF stream
//		context.loadDefaultDataset("/Users/yashpal/cqels-code/data/floorplan.rdf");
		
		context.loadDefaultDataset("/Users/yashpal/cqels-code/data/mr0_sibdataset1000.rdf");
		
		
		// context.loadDataset("http://deri.org/floorplan",
		// "data/floorplan.rdf");

		// TextStream RDFStream= new TextStream(context,
		// "http://rdfweatherexample.net/streams/weather", "weather.nt");
		System.out.println("sucess");
		
//		TextStream RDFStream = new TextStream(context,
//				"http://deri.org/streams/rfid", "data/rfid_1000.stream"); // "rdfPhotoStream1000.nt");
		
		TextStream RDFStream1 = new TextStream(context,
				"http://deri.org/streams/poststream", "data/rdfPostStream1000.stream"); // "rdfPhotoStream1000.nt");
		
		TextStream RDFStream2 = new TextStream(context,
				"http://deri.org/streams/likedpoststream", "data/rdfPostLikeStream1000.stream");		
		
		
		
		// TextStream RDFStream= new TextStream(context,
		// "http://www.cwi.nl/SRBench/observations", "LSDdataset.n3");
		// Register the query to CQELS engine
		//
		//
		// String queryString=
		// "PREFIX om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#>"
		// + "PREFIX weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#>"
		// + "SELECT DISTINCT ?sensor ?value ?uom"
		// + "WHERE {"
		// + "STREAM <http://www.cwi.nl/SRBench/observations> [RANGE 3600s]"
		// + "{?observation om-owl:procedure ?sensor ;"
		// + "a weather:RainfallObservation ;"
		// + "om-owl:result ?result}"
		// + "{?result om-owl:floatValue ?value ;"
		// + "om-owl:uom ?uom }"
		// + "}";

		// String queryString = "PREFIX lv: <http://deri.org/floorplan/>" //its
		// running well-
		// // + "PREFIX dc: <http://purl.org/dc/elements/1.1/>"
		// // + "PREFIX foaf: <http://xmlns.com/foaf/0.1/>"
		// + "SELECT ?s ?p ?o FROM NAMED <http://deri.org/floorplan/>"
		// + "WHERE {" +
		// "STREAM <http://deri.org/streams/rfid> [NOW] {?s ?p ?o.}}";
		//
		// String queryString =
		// "PREFIX lv: <http://rdfweatherexample.net/weather/> " ////running
		// well-on the weather data
		// + "SELECT "
		// + "?day ?forecast FROM NAMED <http://rdfweatherexample.net/>"
		// + "WHERE "
		// + "{"
		// +
		// "STREAM <http://rdfweatherexample.net/streams/weather> [NOW] {?day ?forecast lv:ENE .}"
		// // +
		// "STREAM <http://rdfweatherexample.net/streams/weather> [NOW] {?day lv:date ?date .}"
		// // +
		// "STREAM <http://rdfweatherexample.net/streams/weather> [NOW] {?day lv:windDir ?windDir .} "
		// // +
		// "STREAM <http://rdfweatherexample.net/streams/weather> [NOW] {?day lv:tempCelcius ?tempC .}"
		// + "}";

		//
		//
		// String queryString = "PREFIX foaf: <http://xmlns.com/foaf/0.1/>"
		// + "PREFIX sioc: <http://rdfs.org/sioc/ns#>"
		// + "select ?friend ?post"
		// + "where{ "
		// + "STREAM <http://deri.org/poststream> [RANGE 5s]{"
		// + "?friend sioc:creator_of ?post."
		// + "}"
		// + "?user foaf:knows ?friend."
		// + "?user sioc:account_of <http://www.ins.cwi.nl/sib/person/p984>. "
		// + "}";

		// String queryString =
		// "PREFIX sib:  <http://www.ins.cwi.nl/sib/vocabulary/>" //not
		// working...... copied from lsbench
		// + "PREFIX foaf: <http://xmlns.com/foaf/0.1/>"
		// + "PREFIX sioc: <http://rdfs.org/sioc/ns#>"
		// + "select ?post"
		// + "where{ "
		// + "STREAM <http://deri.org/poststream> [RANGE 1s]"
		// + "{?user sioc:creator_of ?post.}"
		// + "?user sioc:account_of <http://www.ins.cwi.nl/sib/person/p984>. "
		// + "}";
		
//		String query ="select ?s ?p ?o "
//						+"where "
//						+"{?s ?p ?o }";
//
//		context.loadDefaultDataset("jdbc:virtuoso://deri-srvgal35.nuig.ie:1111/charset=UTF-8", "dba", "dba", query);
						

		String queryString = "PREFIX sib: <http://www.ins.cwi.nl/sib/vocabulary/>"
							 +"PREFIX foaf: <http://xmlns.com/foaf/0.1/>"
							 +"PREFIX sioc: <http://rdfs.org/sioc/ns#>"
//							 +"select *"
							 +"select ?user ?friend ?post ?channel "  //?channel     // removing ?post just to check static data result.
							 +"where {" 
//							 +"GRAPH <http://www.ins.cwi.nl/sib/>"
							 +"{?user foaf:knows ?friend.}"
				  +"{?user sioc:subscriber_of ?channel.}"
				  +"STREAM <http://deri.org/streams/poststream> [RANGE 2s]"
				  +"{"
				   +"?channel sioc:container_of ?post"
				  +"}"
				  +"STREAM <http://deri.org/streams/likedpoststream> [RANGE 5s]"
				  +"{?friend sib:like ?post}"	  
				+"}";
		
		
//		String queryString = "PREFIX lv: <http://deri.org/floorplan/>" // Query-1
//																		// working
//																		// well
//				+"SELECT  ?person1 ?person2 "
////				 + "FROM NAMED <http://deri.org/floorplan/>"
//				+ "WHERE {"
////				+ "GRAPH <http://deri.org/floorplan/> "
//				+ "{?loc1 lv:connected ?loc2}"
//				+ "STREAM <http://deri.org/streams/rfid> [NOW] "
//				+ "{?person1 lv:detectedAt ?loc1} "
//				+ "STREAM <http://deri.org/streams/rfid> [NOW] {?person2 lv:detectedAt ?loc2}"
//				+ "}";

		// queryString =
		// "Select ?s from Named <http://deri.org/floorplan/> where {?s ?p ?o}";

		// With the select-type query
		ContinuousSelect selQuery = context.registerSelect(queryString);
		
//		TimerTask task = new TimerTask() {
//		      @Override
//		      public void run() {
//		        // task to run goes here
//		    	  context.loadDefaultDataset("/Users/yashpal/cqels-code/data/mr0_sibdataset1000.rdf");
//		      }
//		    };
//		    Timer timer = new Timer();
//		    long delay = 0;
//		    long intevalPeriod = 1 * 10000; 
//		    
//		    // schedules the task to be run in an interval 
//		    timer.scheduleAtFixedRate(task, delay,
//		                                intevalPeriod);
		
		selQuery.register(new ContinuousListener() {
			
			// yashpal comments-was getting error for this override --->
			// @Override
			public void update(Mapping mapping) {
				System.out.println("Result Print Start");
				String result = "";
				for (Iterator<Var> vars = mapping.vars(); vars.hasNext();)
					// Use context.engine().decode(...) to decode the encoded
					// value to RDF Node
					result += " "
							+ context.engine().decode(mapping.get(vars.next()));
				System.out.println(result);
			}

		});
		// //With the construct-type query
		// ContinuousConstruct consQuery=context.registerConstruct(queryString);
		// consQuery.register(new ConstructListener(context) {
		//
		// @Override
		// public void update(List<Triple> graph) {
		// for(Triple t : graph) {
		// System.out.println(t.getSubject() + " " + t.getPredicate() + " " +
		// t.getObject());
		// }
		// }};)
		// Start streaming thread
		
//		(new Thread(RDFStream)).start();
		
		(new Thread(RDFStream1)).start();
		(new Thread(RDFStream2)).start();
		

	}

}
