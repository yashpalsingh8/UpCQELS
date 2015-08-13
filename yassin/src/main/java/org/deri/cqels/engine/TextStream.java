package org.deri.cqels.engine;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.deri.cqels.engine.ExecContext;
import org.deri.cqels.engine.RDFStream;

import com.hp.hpl.jena.graph.Node;

public class TextStream extends RDFStream implements Runnable{
        String txtFile;
        String HOME="" ;
        boolean stop=false;
        long sleep=500;
        ExecContext context=new ExecContext(HOME, false);
        
        public TextStream(ExecContext context, String uri,String txtFile) {
                super(context, uri);
                this.txtFile=txtFile;
                context.loadDefaultDataset("{DIRECTORY TO LOAD DEFAULT DATASET}");
                
        }

        @Override
        public void stop() {
                stop=true;
        }
        public void setRate(int rate){
                sleep=1000/rate;
        }
        
        @SuppressWarnings("resource")
		public void run() {
                // TODO Auto-generated method stub
                try {
                        BufferedReader reader = new BufferedReader(new FileReader(txtFile));
                        String strLine;
                        while ((strLine = reader.readLine()) != null &&(!stop))   {
                            String[] data=strLine.split(" ");
                                stream(n(data[0]),n(data[1]),n(data[2])); // For streaming RDF triples
                                
                                if(sleep>0){
                                        try {
                                                Thread.sleep(sleep);
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
        
        public static  Node n(String st){
                return Node.createURI(st);
        }

}