package sqlancer;

import java.util.*;
import java.io.*;
import sqlancer.Randomly;
import sqlancer.GlobalState;
import sqlancer.common.schema.*;
import sqlancer.common.query.*;

public abstract class AbstractSeedPool{
    private GlobalState state;


	

	public AbstractSeedPool(GlobalState state){
        this.state = state;
	}
	public abstract void initPool(String path);
	
    public abstract void addSeed(String seed);
    public GlobalState getGlobalState() {
        return state;
    }







}
