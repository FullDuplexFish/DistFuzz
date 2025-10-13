package sqlancer;

import java.util.*;
import java.io.*;
import sqlancer.Randomly;
import sqlancer.GlobalState;
import sqlancer.common.schema.*;
import sqlancer.common.query.*;

public abstract class AbstractSeedPool{
	private List<List<String> > pool;
	public ExpectedErrors errors;
	

	public AbstractSeedPool(){

		this.pool = new ArrayList<List<String> >();
		this.errors = new ExpectedErrors();


	}
	public abstract void initPool(String path);
	
	public abstract List<String> getConstructSeed();
	public abstract List<String> getSelectSeed();
	public abstract List<List<String> > getConstructSeedCache();
	public abstract List<List<String> > getSelectSeedCache();
	public abstract void addToConstructSeedCache(List<String> list);
	public abstract void addToSelectSeedCache(List<String> list);

	public List<List<String> > getPool() {
		return pool;
	}





}
