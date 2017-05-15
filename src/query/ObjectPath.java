package query;



import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Vector;

import com.gs.collections.api.iterator.MutableIntIterator;
import com.gs.collections.api.set.primitive.MutableIntSet;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.map.mutable.primitive.IntObjectHashMap;

import data.Settings;

public class ObjectPath {
	public int id;
	public int ranking_pos;
	public ArrayList<LinkObjectPath> previous_links_cores;
	public ArrayList<LinkObjectPath> links_satellite;
	public short[] selfLoop;
	public boolean isLiteralOrUri;
	public int literalOrUriCode;
	public IntObjectHashMap< short[] > satellites_in;
	public IntObjectHashMap< short[] > satellites_out;
	
	public ObjectPath(int id, int rank_pos){
		this.id = id;
		this.ranking_pos = rank_pos;
		previous_links_cores = new ArrayList<LinkObjectPath>();
		selfLoop = null;
		isLiteralOrUri = false;
		literalOrUriCode = Settings.VARIABLE;
		satellites_in = new IntObjectHashMap< short[]>();
		satellites_out = new IntObjectHashMap< short[]>();
	}
	
	public void addLink(int previous_id, int rank_previous_id, short[] dims){
		previous_links_cores.add( new LinkObjectPath(previous_id, rank_previous_id, dims));
	}
	
	public void sort(){
		Collections.sort(previous_links_cores);
	}
	
	public String toString(){
		String st="id: "+id+" ranking_pos: "+ranking_pos+"\n";
		for (LinkObjectPath p: previous_links_cores){
			st+="\tprevious_id: "+p.previous_id+" rank_previous_id: "+p.rank_previous_id+" dims: "+Arrays.toString(p.dims)+"\n";
		}
		if (selfLoop != null)
			st+="selfloop: "+Arrays.toString(selfLoop);
		if (isLiteralOrUri)
			st+="literalOrUriCode: "+literalOrUriCode;
		if (satellites_in.size() > 0){
			MutableIntIterator its = satellites_in.keySet().intIterator();
			while (its.hasNext()){
				int val = its.next();
				st+="\t\t IN "+val+" "+Arrays.toString(satellites_in.get(val))+"\n";
			}
		}
		if (satellites_out.size() > 0){
			MutableIntIterator its = satellites_out.keySet().intIterator();
			while (its.hasNext()){
				int val = its.next();
				st+="\t\t OUT "+val+" "+Arrays.toString(satellites_out.get(val))+"\n";
			}
		}

		
		return st;
	}
	
} 
