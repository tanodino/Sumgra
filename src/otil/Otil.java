
package otil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.TreeSet;

import com.gs.collections.api.iterator.MutableShortIterator;
import com.gs.collections.api.set.primitive.MutableShortSet;
import com.gs.collections.impl.map.mutable.primitive.ShortObjectHashMap;
import com.gs.collections.impl.set.mutable.primitive.IntHashSet;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.list.mutable.primitive.IntArrayList;

import data.Settings;




public class Otil {
	
	private ShortObjectHashMap<FastList<OtilNode>> labelNodeList_in;
	private ShortObjectHashMap<FastList<OtilNode>> labelNodeList_out;
	private ShortObjectHashMap<OtilNode> tree;
	private IntHashSet in_list;
	private IntHashSet out_list;
	
	public Otil(){
		labelNodeList_in = new ShortObjectHashMap<FastList<OtilNode>>();
		labelNodeList_out = new ShortObjectHashMap<FastList<OtilNode>>();
		tree = new ShortObjectHashMap<OtilNode>();
		in_list = new IntHashSet();
		out_list = new IntHashSet();
	}
	
	
	public boolean checkIFNeighExists(short[] labels, int vertex_id, int direction){
		if (direction == Settings.IN){
			if (!in_list.contains(vertex_id)) return false;
		}
		else{
			if (!out_list.contains(vertex_id)) return false;
		}
		FastList<OtilNode> elements = null;
		if (direction == Settings.IN)
			elements = labelNodeList_in.get(labels[labels.length-1]);
		else
			elements = labelNodeList_out.get(labels[labels.length-1]);
		
		if (elements == null){
				return false;
		}
		for (OtilNode node: elements){
			if (direction == Settings.IN && node.vertexIds_in.contains(vertex_id)  && recursiveCheck(node.parent, labels, labels.length-2))
				return true;
			
			if (direction == Settings.OUT && node.vertexIds_out.contains(vertex_id) && recursiveCheck(node.parent, labels, labels.length-2))
				return true;
			
		}
		return false;
	}
	
	public IntHashSet query(short[] labels, int direction){
		IntHashSet sel_neighs = new IntHashSet();
		FastList<OtilNode> elements = null;
		ShortObjectHashMap<FastList<OtilNode>> tempLabelNodeList = (direction == Settings.IN)?labelNodeList_in:labelNodeList_out;
		elements = tempLabelNodeList.get(labels[labels.length-1]);
		if (elements == null){
				return sel_neighs;
		}
		
		for (OtilNode node: elements){
			if (recursiveCheck(node.parent, labels, labels.length-2)   ){
				if (direction == Settings.IN)
					sel_neighs.addAll(node.vertexIds_in);
				else
					sel_neighs.addAll(node.vertexIds_out);
			}
		}
		return sel_neighs;
	}
		
	public boolean recursiveCheck(OtilNode node, short[] labels, int index){
		//if we consume all the labels (we match all the labels of the array)
		if (index == -1)
			return true;

		//if we reach the root of the Otil Tree data structure and we did not consume all the labels
		if (node == null)
			return false;
		
		// if we match only a portion of the array we need to continue the research
		if (node.label == labels[index]){// if the current node label we are analyzing
			return recursiveCheck(node.parent, labels, index-1);
		}
		else // if the current node label does not match the label we are analyzing 
			return recursiveCheck(node.parent, labels, index);
	}	
	
	
	public void add(short[] sequenceLabel, int neigh_id, int direction){
		if (direction == Settings.IN)
			in_list.add(neigh_id);
		else
			out_list.add(neigh_id);
		
		OtilNode n = null;
		short label = sequenceLabel[0];
		ShortObjectHashMap<FastList<OtilNode>> tempLabelNodeList = (direction == Settings.IN)?labelNodeList_in:labelNodeList_out;
		if (tree.containsKey(label)){//in the case the node already exists but it is the first time that is access by IN (or OUT) insertion
			n = (OtilNode) tree.get(label);
			int size_ids = (direction == Settings.IN)?n.vertexIds_in.size():n.vertexIds_out.size();
			if (size_ids == 0){
				if (!tempLabelNodeList.containsKey(label)){
					tempLabelNodeList.put(label, new FastList<OtilNode>());
				}
				tempLabelNodeList.get(label).add(n);
			}
		}else{
			 n = new OtilNode(null, label);
			 tree.put(label, n);			
			 //manage the pionter table from label to linked list of nodes of the same label
			 	 
			 if (! tempLabelNodeList.containsKey(label)){
				 tempLabelNodeList.put(label, new FastList<OtilNode>());
			 }
			 tempLabelNodeList.get(label).add(n);
		}
		if (direction == Settings.IN)
			n.vertexIds_in.add(neigh_id);
		else
			n.vertexIds_out.add(neigh_id);
		
		
		insert(sequenceLabel,  1, n, neigh_id, direction);
	}
	
	private void insert(short[] sequenceLabel, int pos, OtilNode parent, int neigh_id, int direction){
		// If I'm at the end of the sequenceLabel I do backtracking
		if (pos == sequenceLabel.length) 
			return;
		
		// Get the current label of the sequence
		short label = sequenceLabel[pos];
		OtilNode child = parent.getChild(label);
		ShortObjectHashMap<FastList<OtilNode>> tempLabelNodeList = (direction == Settings.IN)?labelNodeList_in:labelNodeList_out;
		//If the child does not exist, create it
		if (child == null){
			child = new OtilNode(parent, label);
			parent.addChild(child);
			if (! tempLabelNodeList.containsKey(label)){
				tempLabelNodeList.put(label, new FastList<OtilNode>());
			}
			tempLabelNodeList.get(label).add(child);
		}else{//in the case the node already exists but it is the first time that is access by IN (or OUT) insertion
			int size_ids = (direction == Settings.IN)?child.vertexIds_in.size():child.vertexIds_out.size();
			if (size_ids == 0){
				if (!tempLabelNodeList.containsKey(label)){
					tempLabelNodeList.put(label, new FastList<OtilNode>());
				}
				tempLabelNodeList.get(label).add(child);
			}			
		}
		//Add the neighborhood to the list of the child node
		if (direction == Settings.IN)
			child.vertexIds_in.add(neigh_id);
		else
			child.vertexIds_out.add(neigh_id);
		//Add the node to the list of the label that is orthogonal to the tree data structure
			
		insert(sequenceLabel, pos+1, child, neigh_id, direction);
	}
	
	public void print2(int direction){
		//System.out.println("size IN: "+labelNodeList_in.size());
		//System.out.println("size OUT: "+labelNodeList_out.size());
		ShortObjectHashMap<FastList<OtilNode>> temp = null;
		if (direction == Settings.IN)
			temp = labelNodeList_in;
		else
			temp = labelNodeList_out;
		
		MutableShortSet keys = temp.keySet();
		MutableShortIterator it = keys.shortIterator();
		while (it.hasNext()){
			short label = it.next();
			FastList<OtilNode> list =  temp.get(label);
			System.out.println(label+" # el: "+list.size());
			for (OtilNode t: list){				
				System.out.println("IN: "+t.vertexIds_in+" OUT: "+t.vertexIds_out);
			}
		}
	}
	
	
	
	public void print(){
		String st = "";
		MutableShortSet keys = tree.keySet();
		MutableShortIterator it = keys.shortIterator();
		while (it.hasNext()){
			OtilNode node = (OtilNode) tree.get(it.next());
			System.out.println(node.label+" with: "+ node.getChildren().size()+" children \n");
			recursivePrint(node, st, "\t");
		}
		
	}
	
	public void recursivePrint(OtilNode parent, String toPrint, String offset){
		//ShortObjectHashMap<OtilNode> children = parent.getChildren();
		if (parent.children == null) return;
		
		MutableShortSet keys = parent.children.keySet();
		MutableShortIterator it = keys.shortIterator();
		while (it.hasNext()){
			OtilNode child = parent.getChild(it.next());
			if (child.getChildren() == null)
				System.out.println(offset+child.label+"\n");
			else
				System.out.println(offset+child.label+" with: "+ child.getChildren().size()+" children \n");		
			recursivePrint(child, toPrint+offset+child.label+"\n", offset+"\t");
		}	
	}
	
}
