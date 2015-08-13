package org.deri.cqels.engine;

import org.deri.cqels.data.Mapping;
import org.deri.cqels.engine.iterator.MappingIterator;

import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.core.Var;
/**
 *This class implements the basic behaviors of a router
 * @author		Danh Le Phuoc
 * @author 		Chan Le Van
 * @organization DERI Galway, NUIG, Ireland  www.deri.ie
 * @email 	danh.lephuoc@deri.org
 * @email   chan.levan@deri.org
 * @see OpRouter
 */

public abstract class OpRouterBase implements OpRouter {
	static int count = 0;
	Op op;
	static int usercount=0;
	static int postcount=0;
	static int friendcount=0;
	static int channelcount=0;
	 Var var;
	/** An execution context that the router is working on */
	ExecContext context;
	int id;
	public OpRouterBase(ExecContext context, Op op) {
		this.context = context;
		this.op = op;
		id = ++count;
		//System.out.println("new op "+op);
		context.router(id, this);
	}
	
	public Op getOp() {
		// TODO Auto-generated method stub
		return op;
	}
	
	public int getId() { 
		return id;
	}
	
	public void _route( Mapping mapping) {
		//System.out.println("_route "+mapping);
		System.out.println("New Mapping"+ mapping);
		
	
		if(mapping.vars().next().getName().equalsIgnoreCase("user"))		
		{
			usercount++;
			System.out.println(mapping.vars().next());
			System.out.println("user code=="+mapping.get(mapping.vars().next()));
			System.out.println("User count=="+usercount);
			postcount=0;
			friendcount=0;
			channelcount=0;
		}
		if(mapping.vars().next().getName().equalsIgnoreCase("post"))		
		{
			postcount++;
			System.out.println(mapping.vars().next());
			System.out.println("post code=="+mapping.get(mapping.vars().next()));
			System.out.println("Post count=="+postcount);
			usercount=0;
			friendcount=0;
			channelcount=0;
		}
		if(mapping.vars().next().getName().equalsIgnoreCase("friend"))		
		{
			friendcount++;
			System.out.println(mapping.vars().next());
			System.out.println("friend code=="+mapping.get(mapping.vars().next()));
			System.out.println("friend count=="+friendcount);
			usercount=0;
			postcount=0;
			channelcount=0;
		}
		if(mapping.vars().next().getName().equalsIgnoreCase("channel"))		
		{
			channelcount++;
			System.out.println(mapping.vars().next());
			System.out.println("channel code=="+mapping.get(mapping.vars().next()));
			System.out.println("channel count=="+channelcount);
			usercount=0;
			postcount=0;
			friendcount=0;
		}
		mapping.from(this);
		context.policy().next(this, mapping).route(mapping);
		System.out.println("mapping_after_route"+ mapping);
	}
	
	public void route(Mapping mapping) {
		// do nothing
	}
	
	public MappingIterator searchBuff4Match(Mapping mapping) {
		//TODO: missing
		return null;
	}
	
	public MappingIterator getBuff() {
		// TODO Auto-generated method stub
		return null;
	}

}
