package query;

import com.gs.collections.api.iterator.MutableIntIterator;
import com.gs.collections.api.iterator.MutableShortIterator;
import com.gs.collections.api.set.primitive.MutableIntSet;
import com.gs.collections.api.set.primitive.MutableShortSet;
import com.gs.collections.impl.map.mutable.primitive.IntIntHashMap;
import com.gs.collections.impl.map.mutable.primitive.IntObjectHashMap;
import com.gs.collections.impl.map.mutable.primitive.ObjectIntHashMap;
import com.gs.collections.impl.map.mutable.primitive.ShortIntHashMap;
import com.gs.collections.impl.set.mutable.primitive.ShortHashSet;
import com.gs.collections.impl.set.mutable.primitive.IntHashSet;
import com.gs.collections.impl.map.mutable.primitive.IntBooleanHashMap;

import org.khelekore.prtree.SimpleMBR;
import org.khelekore.prtree.SimplePointND;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.list.mutable.primitive.ShortArrayList;
import com.gs.collections.impl.list.mutable.primitive.IntArrayList;

import data.GraphDatabase;
import data.Settings;

public class Query {

	private IntObjectHashMap<IntHashSet> in_list; 
	private IntObjectHashMap<IntHashSet> out_list;
	private HashMap< Pair,ShortHashSet > pair2dims;
	private IntBooleanHashMap is_variable;
	private HashMap< Pair,short[] > pair2dims_vec;
	private ArrayList<String> headerList;
	private IntObjectHashMap<String> id2token;
	private ObjectIntHashMap<String> token2id;
	private IntObjectHashMap<ShortHashSet> selfLoop;
	public IntObjectHashMap<Integer> notVariable;
	private ShortIntHashMap dim_stat;
	private GraphDatabase g;
	IntObjectHashMap<IntHashSet> cores2sats;
	public IntObjectHashMap<IntHashSet> possible_candidates = new IntObjectHashMap<IntHashSet>();
	
	public Query(ShortIntHashMap property_stat, GraphDatabase g){
		in_list = new IntObjectHashMap<IntHashSet>();
		out_list = new IntObjectHashMap<IntHashSet>();
		is_variable = new IntBooleanHashMap();
		pair2dims = new HashMap< Pair,ShortHashSet >();
		pair2dims_vec = new HashMap< Pair,short[] >();
		headerList = new ArrayList<String>();
		id2token = new IntObjectHashMap<String>();
		token2id = new ObjectIntHashMap<String>();
		selfLoop = new IntObjectHashMap<ShortHashSet>();
		notVariable = new IntObjectHashMap<Integer>();
		dim_stat = property_stat;
		this.g = g;
		cores2sats = new IntObjectHashMap<IntHashSet>();
	}

	
	
	public void decompose(){
		MutableIntSet node_set = id2token.keySet();
		MutableIntIterator itr = node_set.intIterator();
		IntHashSet satellites = new IntHashSet();
		IntHashSet cores = new IntHashSet();
		while (itr.hasNext()){
			int node_id = itr.next();
			IntHashSet temp_neighs = new IntHashSet();
			if (in_list.containsKey(node_id))
				temp_neighs.addAll(in_list.get(node_id));
			if (out_list.containsKey(node_id))
				temp_neighs.addAll(out_list.get(node_id));
			
			if (temp_neighs.size() == 1)
				satellites.add(node_id);
			else
				cores.add(node_id);
		}
			
		itr = cores.intIterator();
		while (itr.hasNext()){
			int core_id = itr.next();
			cores2sats.put(core_id, new IntHashSet());
			MutableIntIterator itr1 = satellites.intIterator();
			while (itr1.hasNext()){
				int sat_id = itr1.next();
				if (in_list.containsKey(core_id) && in_list.get(core_id).contains(sat_id)){
					cores2sats.get(core_id).add(sat_id);
				}
				if (out_list.containsKey(core_id) && out_list.get(core_id).contains(sat_id)){
					cores2sats.get(core_id).add(sat_id);
				}
			}
		}
		
	}
	
	
	public IntHashSet chooseLessMatchedNodes(IntHashSet already_considered, IntHashSet ranked_tie_connecitvity){
		MutableIntIterator itr_not_yet_ranked = ranked_tie_connecitvity.intIterator();
		int best_min_n_matches = Integer.MAX_VALUE;
		IntObjectHashMap<IntHashSet> prop2node = new IntObjectHashMap<IntHashSet>();
		while (itr_not_yet_ranked.hasNext()){
			int not_yet_ranked = itr_not_yet_ranked.next();
			MutableIntIterator itr_already = already_considered.intIterator();
			boolean connected = false;
			while (itr_already.hasNext() && !connected){
				int yet_ordered = itr_already.next();
				
				if (in_list.containsKey(not_yet_ranked) && in_list.get(not_yet_ranked).contains(yet_ordered)){
					connected = true;
				}
					
				if (out_list.containsKey(not_yet_ranked) && out_list.get(not_yet_ranked).contains(yet_ordered)){
					connected = true;
				}
			}
			if (connected){
				int n_matches = possible_candidates.get(not_yet_ranked).size();
				if (!prop2node.containsKey(n_matches))
					prop2node.put(n_matches,new IntHashSet());
				prop2node.get(n_matches).add(not_yet_ranked);

				if (n_matches < best_min_n_matches) best_min_n_matches = n_matches;
			}
		}
		return prop2node.get(best_min_n_matches);
	}
	
/* HEURISTICS TO ORDER THE QUERY NODES FROM THE SECOND TO THE LAST */	
	public IntHashSet chooseMoreSelective(IntHashSet already_considered, IntHashSet ranked_tie_connecitvity){
		MutableIntIterator itr_not_yet_ranked = ranked_tie_connecitvity.intIterator();
		int best_min_dims = Integer.MAX_VALUE;
		
		IntObjectHashMap<IntHashSet> prop2node = new IntObjectHashMap<IntHashSet>();
		
		
		while (itr_not_yet_ranked.hasNext()){
			int not_yet_ranked = itr_not_yet_ranked.next();
			int min_dims = Integer.MAX_VALUE;
			MutableIntIterator itr_already = already_considered.intIterator();
			while (itr_already.hasNext()){
				int yet_ordered = itr_already.next();
				if (in_list.containsKey(not_yet_ranked) && in_list.get(not_yet_ranked).contains(yet_ordered)){
					MutableShortIterator itrs = pair2dims.get(new Pair(yet_ordered, not_yet_ranked)).shortIterator();
					while (itrs.hasNext()){
						min_dims = Math.min(min_dims,dim_stat.get(itrs.next()));
					}
				}
					
				if (out_list.containsKey(not_yet_ranked) && out_list.get(not_yet_ranked).contains(yet_ordered)){
					MutableShortIterator itrs = pair2dims.get(new Pair(not_yet_ranked, yet_ordered)).shortIterator();
					while (itrs.hasNext()){
						min_dims = Math.min(min_dims,dim_stat.get(itrs.next()));
					}

				}
			}
			if (!prop2node.containsKey(min_dims))
				prop2node.put(min_dims,new IntHashSet());
			prop2node.get(min_dims).add(not_yet_ranked);

			if (min_dims < best_min_dims) best_min_dims = min_dims;
		}
		return prop2node.get(best_min_dims);
	}
	
	
	
	public IntHashSet chooseMoreNovelCoverage(IntHashSet already_considered, IntHashSet not_yet_consider){
		//System.out.println(not_yet_consider.size());
		MutableIntIterator itr_not_yet = not_yet_consider.intIterator();
		int max_links = 0;
		IntObjectHashMap<IntHashSet> nlinks2node = new IntObjectHashMap<IntHashSet>();
		
		MutableIntIterator itr_already = already_considered.intIterator();
		IntHashSet frontier = new IntHashSet();
		
		while (itr_already.hasNext()){
			int yet_ordered = itr_already.next();
			if (in_list.containsKey(yet_ordered))
				frontier.addAll(in_list.get(yet_ordered));
			
			if (out_list.containsKey(yet_ordered))
				frontier.addAll(out_list.get(yet_ordered));
		}
		
		
		while (itr_not_yet.hasNext()){
			int not_yet_ordered = itr_not_yet.next();
			IntHashSet not_yet_ordered_frontier = new IntHashSet();
			if (in_list.containsKey(not_yet_ordered))
				not_yet_ordered_frontier.addAll(in_list.get(not_yet_ordered));
			
			if (out_list.containsKey(not_yet_ordered))
				not_yet_ordered_frontier.addAll(out_list.get(not_yet_ordered));
					
			not_yet_ordered_frontier.retainAll(frontier);
			
			if (!nlinks2node.containsKey(not_yet_ordered_frontier.size()))
				nlinks2node.put(not_yet_ordered_frontier.size(), new IntHashSet());
			
			nlinks2node.get(not_yet_ordered_frontier.size()).add(not_yet_ordered);
			if (not_yet_ordered_frontier.size() > max_links) max_links = not_yet_ordered_frontier.size();
		}

		return nlinks2node.get(max_links);
		
	}
	
	public IntHashSet connectivityTopRanked(IntHashSet already_considered, IntHashSet not_yet_consider){
		MutableIntIterator itr_not_yet = not_yet_consider.intIterator();
		int max_links = 0;
		IntObjectHashMap<IntHashSet> nlinks2node = new IntObjectHashMap<IntHashSet>();
		
		while (itr_not_yet.hasNext()){
			int not_yet_ordered = itr_not_yet.next();
			int count_links = 0;
			MutableIntIterator itr_already = already_considered.intIterator();
			while (itr_already.hasNext()){
				int yet_ordered = itr_already.next();
				if (in_list.containsKey(not_yet_ordered) && in_list.get(not_yet_ordered).contains(yet_ordered))
					count_links += pair2dims.get(new Pair(yet_ordered, not_yet_ordered)).size();
					
				if (out_list.containsKey(not_yet_ordered) && out_list.get(not_yet_ordered).contains(yet_ordered))
					count_links += pair2dims.get(new Pair(not_yet_ordered,yet_ordered)).size();
			}
			if (!nlinks2node.containsKey(count_links))
				nlinks2node.put(count_links, new IntHashSet());
			
			nlinks2node.get(count_links).add(not_yet_ordered);
			if (count_links > max_links) max_links = count_links;
		}

		return nlinks2node.get(max_links);
		
	}
	
	
	public IntHashSet connectivityTopRanked2Hop(IntHashSet already_considered, IntHashSet not_yet_consider){
		MutableIntIterator itr_not_yet = not_yet_consider.intIterator();
		int max_links = 0;
		IntObjectHashMap<IntHashSet> nlinks2node = new IntObjectHashMap<IntHashSet>();
		
		while (itr_not_yet.hasNext()){
			int not_yet_ordered = itr_not_yet.next();
			int count_links = 0;
			
			if (in_list.containsKey(not_yet_ordered)){
				MutableIntIterator itr_in_neighs_nod = in_list.get(not_yet_ordered).intIterator();
				MutableIntIterator itr_already = already_considered.intIterator();
				while (itr_already.hasNext()){
					int yet_ordered  = itr_already.next();
					while (itr_in_neighs_nod.hasNext()){
						int in_neigh = itr_in_neighs_nod.next();
						if (in_list.containsKey(in_neigh) && in_list.get(in_neigh).contains(yet_ordered))
							count_links += pair2dims.get(new Pair(in_neigh, not_yet_ordered)).size();
							
						if (out_list.containsKey(in_neigh) && out_list.get(in_neigh).contains(yet_ordered))
							count_links += pair2dims.get(new Pair(in_neigh,yet_ordered)).size();
					}
				}
			}
			
			if (out_list.containsKey(not_yet_ordered)){
				MutableIntIterator itr_out_neighs_nod = out_list.get(not_yet_ordered).intIterator();		
				MutableIntIterator itr_already = already_considered.intIterator();
				while (itr_already.hasNext()){
					int yet_ordered  = itr_already.next();
					while (itr_out_neighs_nod.hasNext()){
						int out_neigh = itr_out_neighs_nod.next();
						if (in_list.containsKey(out_neigh) && in_list.get(out_neigh).contains(yet_ordered))
							count_links += pair2dims.get(new Pair(out_neigh, not_yet_ordered)).size();
							
						if (out_list.containsKey(out_neigh) && out_list.get(out_neigh).contains(yet_ordered))
							count_links += pair2dims.get(new Pair(out_neigh,yet_ordered)).size();
					}
				}
				
			}
			if (!nlinks2node.containsKey(count_links))
				nlinks2node.put(count_links, new IntHashSet());
			
			nlinks2node.get(count_links).add(not_yet_ordered);
			if (count_links > max_links) max_links = count_links;
			
		}

		return nlinks2node.get(max_links);
		
	}
	
	
	public IntHashSet chooseMaxSatNodes(IntHashSet already_considered, IntHashSet not_yet_consider){
		MutableIntIterator itr_not_yet = not_yet_consider.intIterator();
		int max_links = 0;
		IntObjectHashMap<IntHashSet> nlinks2node = new IntObjectHashMap<IntHashSet>();
		
		while (itr_not_yet.hasNext()){
			int not_yet_ordered = itr_not_yet.next();
			int count_links = 0;
			MutableIntIterator itr_already = already_considered.intIterator();
			while (itr_already.hasNext()){
				int yet_ordered = itr_already.next();
				if (in_list.containsKey(not_yet_ordered) && in_list.get(not_yet_ordered).contains(yet_ordered))
					count_links = Math.max(count_links, cores2sats.get(not_yet_ordered).size());
					
				if (out_list.containsKey(not_yet_ordered) && out_list.get(not_yet_ordered).contains(yet_ordered))
					count_links = Math.max(count_links, cores2sats.get(not_yet_ordered).size());
			}
			if (!nlinks2node.containsKey(count_links))
				nlinks2node.put(count_links, new IntHashSet());
			
			nlinks2node.get(count_links).add(not_yet_ordered);
			if (count_links > max_links) max_links = count_links;
		}
		return nlinks2node.get(max_links);

	}
	
	
	
	/* HEURISTICS TO ORDER THE FIRST QUERY NODE */
	public IntHashSet chooseMaxLiteralorURI(MutableIntSet core_set){
		int max_links_literal_uri = 0;
		IntObjectHashMap<IntHashSet> nlinks2node = new IntObjectHashMap<IntHashSet>();
		MutableIntIterator itr = core_set.intIterator();
		while (itr.hasNext()){
			int node_id = itr.next();
			int current_links = 0;
			if (in_list.containsKey(node_id)){
				MutableIntIterator it1 = in_list.get(node_id).intIterator();
				while (it1.hasNext()){
					if (notVariable.containsKey(it1.next())){
						current_links++;
					}
				}
			}
			if (out_list.containsKey(node_id)){
				MutableIntIterator it1 = out_list.get(node_id).intIterator();
				while (it1.hasNext()){
					if (notVariable.containsKey(it1.next())){
						current_links++;
					}
				}
			}
			
			if (!nlinks2node.containsKey(current_links))
				nlinks2node.put(current_links, new IntHashSet());
			
			nlinks2node.get(current_links).add(node_id);
			
			if (current_links > max_links_literal_uri) max_links_literal_uri = current_links;
		}
		return nlinks2node.get(max_links_literal_uri);
	}
	
	
	public IntHashSet chooseMaxLinks(MutableIntSet core_set){
		int max_links = 0;
		IntObjectHashMap<IntHashSet> nlinks2node = new IntObjectHashMap<IntHashSet>();
		MutableIntIterator itr = core_set.intIterator();
		while (itr.hasNext()){
			int node_id = itr.next();
			int current_links = 0;
			if (in_list.containsKey(node_id)){
				current_links += in_list.get(node_id).size();
			}
			if (out_list.containsKey(node_id)){
				current_links += out_list.get(node_id).size();
			}
			if (!nlinks2node.containsKey(current_links))
				nlinks2node.put(current_links, new IntHashSet());
			
			nlinks2node.get(current_links).add(node_id);	
			if (current_links > max_links) max_links = current_links;
		}
		//System.out.println("max_links "+nlinks2node.get(max_links));
		return nlinks2node.get(max_links);

	}
	
	public IntHashSet chooseMaxSatNodes(MutableIntSet core_set){
		int max_links = 0;
		IntObjectHashMap<IntHashSet> nlinks2node = new IntObjectHashMap<IntHashSet>();
		MutableIntIterator itr = core_set.intIterator();
		while (itr.hasNext()){
			int node_id = itr.next();
			int current_links = cores2sats.get(node_id).size();
			if (current_links > max_links) max_links = current_links;		
			if (!nlinks2node.containsKey(current_links))
				nlinks2node.put(current_links, new IntHashSet());
			
			nlinks2node.get(current_links).add(node_id);
		}
		return nlinks2node.get(max_links);

	}
	
	
	
	
	public int[] getOrdering(){
		
		int[] ordering = new int[cores2sats.size()];
		MutableIntSet core_set = cores2sats.keySet();
		MutableIntIterator itr = core_set.intIterator();
		int max_id = -1;
		boolean no_sol = false;
		while (itr.hasNext()){
			int node_id = itr.next();
			int counter = 0;
			if (notVariable.containsKey(node_id)){
				counter = 1;
				IntHashSet temp = new IntHashSet();
				temp.add(notVariable.get(node_id));
				possible_candidates.put(node_id, temp);	
			}else{
				// In the case we know that one of the query vertex node does not have any match in the dataset
				SimplePointND query_r_tree = getSynopsis(node_id);
				IntHashSet retrieved_vertices = g.getNodeList(query_r_tree);
				counter = retrieved_vertices.size();
				possible_candidates.put(node_id, retrieved_vertices);
			}	
			if (counter == 0){
					max_id = node_id;
					no_sol = true;
				}
		}

		//System.out.println("no_sol "+no_sol);
		if (no_sol){
			ordering[0] = max_id;
		}else{
			MutableIntSet chooseFirstNode = core_set;
			chooseFirstNode = chooseMaxSatNodes(chooseFirstNode);
			chooseFirstNode = chooseMaxLinks(chooseFirstNode);
			ordering[0] = chooseFirstNode.min();
		}
		
		IntHashSet already_considered = new IntHashSet();
		IntHashSet not_yet_consider = new IntHashSet();
		not_yet_consider.addAll(core_set);
		
		not_yet_consider.remove(ordering[0]);
		already_considered.add(ordering[0]);
			
		int i = 1;
		
		while (not_yet_consider.size() > 0){
			IntHashSet topRankedTie = not_yet_consider;
			topRankedTie = connectivityTopRanked(already_considered, topRankedTie);
			topRankedTie = connectivityTopRanked2Hop(already_considered, topRankedTie);			
			topRankedTie = chooseMoreSelective(already_considered, topRankedTie);
			topRankedTie = chooseMoreNovelCoverage(already_considered, topRankedTie);
			int temp_node_id = topRankedTie.max();
			ordering[i] = temp_node_id;
			not_yet_consider.remove(temp_node_id);
			already_considered.add(temp_node_id);
			++i;
		}
		return ordering;
	}

	
	public IntArrayList selectVariableProjection(){
		IntArrayList res = new IntArrayList();
		for (String st: headerList){
			res.add(token2id.get(st));
		}
		res.sortThis();
		return res;
	}
	
	
	public void addSelect(String token){
		headerList.add(token);
	}
	
	public int size(){
		TreeSet<Integer> ris = new TreeSet<Integer>();
		MutableIntIterator it = in_list.keySet().intIterator();
		while( it.hasNext()) ris.add(it.next());
		it = out_list.keySet().intIterator();
		while( it.hasNext()) ris.add(it.next());
		return ris.size();
	}
	
	
	public boolean isVariabel(int id){
		return is_variable.get(id);
	}
	
	private void createVectorRepresentation(){
		Set<query.Pair> keys = pair2dims.keySet();
		for (query.Pair el : keys){
			pair2dims_vec.put(el, pair2dims.get(el).toArray());
		}
	}
	
	public short[] getDims(int source, int sink){
		query.Pair p = new query.Pair(source, sink);
		if (pair2dims_vec.containsKey(p)){
			return pair2dims_vec.get(p); 
		}else{
			return new short[0];
		}
		
	}
	
	/*
	 * return the order to process the query nodes
	 *  
	 * */
	public Vector<ObjectPath> getQueryStructure(){
		decompose();
		createVectorRepresentation();
		int[] queryOrder = getOrdering();
		//System.out.println(Arrays.toString(queryOrder));
		
		
		MutableIntSet tokens = cores2sats.keySet();
		MutableIntIterator ii = tokens.intIterator();
		while(ii.hasNext()){
			int val = ii.next();
			//System.out.println(val+" -> "+cores2sats.get(val));
		}
		/**/
	
		Vector<ObjectPath> path = new Vector<ObjectPath>();
		ObjectPath first = new ObjectPath(queryOrder[0],0);
		//manage the selfLoop case in the query structure for node in position 0
		if (selfLoop.containsKey(queryOrder[0]))
			first.selfLoop = selfLoop.get(queryOrder[0]).toArray();
		if (notVariable.containsKey(queryOrder[0])){
			first.isLiteralOrUri = true;
			first.literalOrUriCode = notVariable.get(queryOrder[0]);
		}
		MutableIntIterator it_sat = cores2sats.get(first.id).intIterator();
		
		while (it_sat.hasNext()){
			int id_sat = it_sat.next();
			Pair out_sat = new Pair(first.id, id_sat);
			if (pair2dims_vec.containsKey(out_sat)  ){
				first.satellites_out.put(id_sat, pair2dims_vec.get(out_sat));
			}
			Pair in_sat = new Pair(id_sat, first.id);
			if (pair2dims_vec.containsKey(in_sat)  ){
				first.satellites_in.put(id_sat, pair2dims_vec.get(in_sat));
			}	
		}
		
		
		
		path.add(first);
		for (int i=1; i< queryOrder.length; ++i){
			int vertex_id1 = queryOrder[i];
			ObjectPath current = new ObjectPath(queryOrder[i],i);
			//manage the selfLoop case in the query structure for node in position i
			if (selfLoop.containsKey(queryOrder[i]))
				current.selfLoop = selfLoop.get(queryOrder[i]).toArray();
			if (notVariable.containsKey(queryOrder[i])){
				current.isLiteralOrUri = true;
				current.literalOrUriCode = notVariable.get(queryOrder[i]);
			}
			for (int j=0; j < i; ++j){
				int previous_id = queryOrder[j];
				if (out_list.get(vertex_id1) != null && ((IntHashSet) out_list.get(vertex_id1)).contains(previous_id))
					current.addLink(previous_id, j, Settings.IN, pair2dims_vec.get( new Pair(vertex_id1,previous_id) ));
				if ( in_list.get(vertex_id1) != null && ((IntHashSet) in_list.get(vertex_id1)).contains(previous_id) )
					current.addLink(previous_id, j, Settings.OUT, pair2dims_vec.get( new Pair(previous_id,vertex_id1) ));				
			}
			current.sort();
			it_sat = cores2sats.get(current.id).intIterator();
			while (it_sat.hasNext()){
				int id_sat = it_sat.next();
				Pair out_sat = new Pair(current.id, id_sat);
				if (pair2dims_vec.containsKey(out_sat)  ){
					current.satellites_out.put(id_sat, pair2dims_vec.get(out_sat));
				}
				Pair in_sat = new Pair(id_sat, current.id);
				if (pair2dims_vec.containsKey(in_sat)  ){
					current.satellites_in.put(id_sat, pair2dims_vec.get(in_sat));
				}	
			}
			path.add(current);
		}
		return path;
		
	}
	
	
	//IN (positive) OUT (negative)
	// feature 1: maximum cardinality of a set in the vertex signature
	// feature 2: number of unique dimension in the vertex signature
	// feature 3: minimum index value of the edge type
	// feature 4: maximum index value of the edge type
	public SimplePointND getSynopsis(int v_id){
		short[] syn = new short[Settings.SYNOPSIS_SIZE];
		syn[2] = syn[2+Settings.N_FEAT] = Short.MIN_VALUE;
		MutableIntIterator itr = null;
		IntHashSet unique_dims = null;
			
		/* BUILD THE SYNOPSIS FOR INCOMING LIST */
		if (in_list.get(v_id) != null){
			itr = ( (IntHashSet) in_list.get(v_id)).intIterator();
			unique_dims = new IntHashSet();
			while (itr.hasNext()){
				Pair p_temp = new Pair(itr.next(),v_id);
				ShortHashSet dims = pair2dims.get(p_temp);
				syn[0] = (short) ((dims.size() > syn[0])?pair2dims.get(p_temp).size():syn[0]);
				MutableShortIterator it_short = dims.shortIterator();
				while (it_short.hasNext()){
					short s = it_short.next();
					syn[2] = (short) ((s < (-1*syn[2]))?(-1*s):syn[2]);
					syn[3] = (s > syn[3])?s:syn[3];
					unique_dims.add(s);
				}
			}
			syn[1] = (short) unique_dims.size();
		}
		
		/* BUILD THE SYNOPSIS FOR INCOMING LIST */
		if (out_list.get(v_id) != null){
			itr = ( (IntHashSet) out_list.get(v_id)).intIterator();
			unique_dims = new IntHashSet();
			while (itr.hasNext()){
				Pair p_temp = new Pair(v_id,itr.next());
				ShortHashSet dims = pair2dims.get(p_temp);
				syn[0+Settings.N_FEAT] = (short) ((dims.size() > syn[0+Settings.N_FEAT])?pair2dims.get(p_temp).size():syn[0+Settings.N_FEAT]);
				MutableShortIterator it_short = dims.shortIterator();
				while (it_short.hasNext()){
					short s = it_short.next();
					syn[2+Settings.N_FEAT] = (short) ((s < (-1*syn[2+Settings.N_FEAT]))?(-1*s):syn[2+Settings.N_FEAT]);
					syn[3+Settings.N_FEAT] = (s > syn[3+Settings.N_FEAT])?s:syn[3+Settings.N_FEAT];
					unique_dims.add(s);
				}
			}
			syn[1+Settings.N_FEAT] = (short) unique_dims.size();
		}
		return new SimplePointND(syn);
	}
	
	public void setVariables(IntArrayList variables){
		MutableIntIterator itr = variables.intIterator();
		while (itr.hasNext())
			is_variable.put(itr.next(), true);
	}
	
	public void addTriple(String source, String sink, short dim, int subjectCode, int objectCode){		
		if (!token2id.containsKey(source)   ){
			int val = token2id.size();
			token2id.put(source, val);
			id2token.put(val,source);
		}
		
		if (!token2id.containsKey(sink)){
			int val = token2id.size();
			token2id.put(sink, val);
			id2token.put(val,sink);
		}
		int id_source = token2id.get(source);
		int id_sink = token2id.get(sink);
		
		if (subjectCode != Settings.VARIABLE){
			notVariable.put(id_source, subjectCode);
		}
		
		if (objectCode != Settings.VARIABLE){
			notVariable.put(id_sink, objectCode);
		}
		
		if (id_source == id_sink){
			if (!selfLoop.containsKey(id_source))
				selfLoop.put(id_source, new ShortHashSet());
			selfLoop.get(id_source).add(dim);
		}
		
		if (!in_list.containsKey(id_sink))
			in_list.put(id_sink, new IntHashSet());
		in_list.get(id_sink).add(id_source);
		
		if (!out_list.containsKey(id_source))
			out_list.put(id_source, new IntHashSet());
		out_list.get(id_source).add(id_sink);
		
		Pair pair = new Pair(id_source,id_sink);
		if (!pair2dims.containsKey(pair))
			pair2dims.put(pair, new ShortHashSet());
		pair2dims.get(pair).add(dim);
	}	
}

