package org.deri.cqels.main;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.deri.cqels.engine.ExecContext;
import org.deri.cqels.engine.RDFStream;
import com.hp.hpl.jena.graph.Node;

public class TextStream extends RDFStream implements Runnable {
	String txtFile;
	boolean stop = false;
	long sleep = 1000;

	public TextStream(ExecContext context, String uri, String txtFile) {
		super(context, uri);
		this.txtFile = txtFile;
	}

	@Override
	public void stop() {
		stop = true;
	}

	public void setRate(int rate) {
		sleep = 1000 / rate;
	}

	public void run() {
		// TODO Auto-generated method stub
		try {
			BufferedReader reader = new BufferedReader(new FileReader(txtFile));
			String strLine;
			// System.out.println("ksdjkand");
			while ((strLine = reader.readLine()) != null && (!stop)) {
				String[] data = strLine.split(" ");
				stream(n(data[0]), n(data[1]), n(data[2])); // For streaming RDF
															// triples
//				System.out.println(data[0] + data[1] + data[2]);
				// System.out.println(txtFile.toString());
				if (sleep > 0) {
					try {
						Thread.sleep(sleep);
						// ///////////////////
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static Node n(String st) {
		return Node.createURI(st);
	}

}
