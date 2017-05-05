
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Vector;

import com.gs.collections.api.iterator.MutableShortIterator;
import com.gs.collections.api.set.primitive.MutableShortSet;
import com.gs.collections.api.tuple.Pair;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.list.mutable.primitive.IntArrayList;
import com.gs.collections.impl.map.mutable.primitive.IntObjectHashMap;
import com.gs.collections.impl.map.mutable.primitive.ObjectShortHashMap;

import otil.*;
import data.*;
import query.*;
import queryparser.AmberParser;
import queryparser.ParseException;

public class Main {

	//test erick commit
	public static void main(String[] args) throws IOException, ParseException {
		// TODO Auto-generated method stub
		/*
		 * long start = System.currentTimeMillis(); GraphDatabase g = new
		 * GraphDatabase(
		 * "/Users/dinoienco/Documents/workspace/Amber/examples/graph_toy.nt");
		 * Query q = getQuery(
		 * "/Users/dinoienco/Documents/workspace/Amber/examples/query_example1.txt"
		 * );
		 * 
		 * int[] order = new int[q.size()]; for (int i=0; i<order.length;++i)
		 * order[i]=i+1;
		 * 
		 * Vector<ObjectPath> p = q.getQueryStructure(order);
		 * 
		 * FastList<int[]> results = new FastList<int[]>(); g.query(results, p,
		 * q); long end = System.currentTimeMillis(); for (int[] r : results)
		 * System.out.println(Arrays.toString(r));
		 * 
		 * System.out.println("total time: "+(end-start));
		 */
		// GraphDatabase g = new
		// GraphDatabase("/Users/dinoienco/Documents/workspace/Amber/example.nt");
		//
		// GraphDatabase g = new
		// GraphDatabase("/Users/dinoienco/Downloads/watdiv_10M.nt");

		testParser();

		// testOtil();
		// testQuery();
	}

	public static void testParser() throws ParseException, IOException {
		GraphDatabase g = new GraphDatabase("examples/graph_toy.sumgra");
		long start = System.currentTimeMillis();
		// String fileNameData =
		// "/Users/dinoienco/Downloads/annotations-atweb.nt";
		// String fileNameData =
		// "/Users/dinoienco/Documents/workspace/Amber+/examples/graph_toy2.nt";
		// GraphDatabase g = new GraphDatabase(fileNameData);
		long end = System.currentTimeMillis();
		System.out.println("load time: " + (end - start));
		// System.exit(0);

		/*
		 * MutableShortSet prop_set = g.property_stat.keySet();
		 * MutableShortIterator itrs = prop_set.shortIterator(); while
		 * (itrs.hasNext()){ short val = itrs.next();
		 * System.out.println(val+" -> "+g.property_stat.get(val)); }
		 */
		/*
		 * String query_string =
		 * "SELECT ?docid, ?doctitle, ?tableid, ?tabletitle, ?row WHERE {"
		 * +"?doc <http://opendata.inra.fr/resources/atWeb/annotation/hasForID> ?docid ."
		 * +"?doc <http://purl.org/dc/elements/1.1/title> ?doctitle ."
		 * +"?doc <http://opendata.inra.fr/resources/atWeb/annotation/hasTable> ?table ."
		 * +"?table <http://opendata.inra.fr/resources/atWeb/annotation/hasForID> ?tableid ."
		 * +"?table <http://purl.org/dc/elements/1.1/title> ?tabletitle ."
		 * +"?table <http://opendata.inra.fr/resources/atWeb/annotation/hasForRow> ?row ."
		 * +"}";
		 */
		/*
		 * String query_string = "SELECT ?x1, ?x2, ?x3 where { " +
		 * "?x1 <http://dbpedia.org/property/ushrProperty> ?x2 ." +
		 * "?x2 <http://dbpedia.org/property/ushrProperty> ?x3 ." +
		 * "?x2 <http://dbpedia.org/property/ushrProperty> \"B\"@en ." + "}";
		 */
		// InputStream in = new ByteArrayInputStream(query_string.getBytes());

		// Adder parser = new Adder( in ) ;
		start = System.currentTimeMillis();

		// String fileName =
		// "/Users/dinoienco/Documents/workspace/Amber/examples/toy/queries/sparql/7_q";
		// String fileName = "/Users/dinoienco/Downloads/1-M/9_q";
		String fileName = "examples/query_example.txt";

		AmberParser parser = new AmberParser(new FileInputStream(fileName));
		ArrayList<String> vars = new ArrayList<String>();
		ArrayList<String> triples = new ArrayList<String>();
		Query q = new Query(g.property_stat, g);
		parser.Start(vars, triples, g.string2ShortP, g.string2intSO, q);
		end = System.currentTimeMillis();
		System.out.println("query parsing time: " + (end - start));
		start = System.currentTimeMillis();

		end = System.currentTimeMillis();
		System.out.println("query decomposition time: " + (end - start));
		start = System.currentTimeMillis();
		// int[] order = q.getOrdering();
		// System.out.println(Arrays.toString(order));
		// int[] order = new int[q.size()];
		// for (int i=0; i<order.length;++i) order[i]=i;
		// System.out.println("order: "+Arrays.toString(order));
		Vector<ObjectPath> p = q.getQueryStructure();

		System.out.println("====");

		// for (ObjectPath s: p)
		// System.out.println(s);
		/**/

		// System.out.println(q.size());
		// System.exit(0);

		IntArrayList projection = q.selectVariableProjection();
		System.out.println("compute query structure and ordering time: " + (end - start));
		/*
		 * for (ObjectPath s: p) System.out.println(s);
		 */
		start = System.currentTimeMillis();
		FastList<int[]> results = new FastList<int[]>();

		g.query(results, projection, p, q);

		for (int[] r : results) {
			String[] temp_loc = new String[r.length];
			for (int j = 0; j < r.length; ++j) {
				temp_loc[j] = (String) g.int2stringSO.get(r[j]);
			}
			System.out.println(Arrays.toString(temp_loc));
		}
		end = System.currentTimeMillis();
		System.out.println("processing time: " + (end - start));
		System.out.println("res size: " + results.size());
	}

	/*
	 * public static void testQuery(){ String file2read =
	 * "/Users/dinoienco/Documents/workspace/Amber/query_example.txt";
	 * BufferedReader br = null; InputStreamReader isr = null; try { isr = new
	 * InputStreamReader(new FileInputStream(file2read)); br = new
	 * BufferedReader(isr); } catch (FileNotFoundException e) {
	 * e.printStackTrace(); } String cur; Query q = new Query(); try {
	 * while((cur=br.readLine()) != null){ String[] elements= cur.split(" ");
	 * int source = Integer.parseInt(elements[0]); int sink =
	 * Integer.parseInt(elements[1]); String [] preds = elements[2].split(",");
	 * short[] seq = new short[preds.length]; for (int i=0; i<preds.length;++i)
	 * { q.addTriple(""+source, ""+sink, Short.parseShort(preds[i]), true,
	 * true); } //System.out.println("add: "+Arrays.toString(seq)); }
	 * isr.close(); br.close(); }catch(Exception e){ e.printStackTrace(); }
	 * IntArrayList variables = new IntArrayList(); variables.add(1);
	 * variables.add(2); variables.add(3); variables.add(4); variables.add(5);
	 * q.setVariables(variables); int[] order = {2,3,5,1,4}; Vector<ObjectPath>
	 * p = q.getQueryStructure(order); for (ObjectPath op: p){
	 * System.out.println(op); }
	 * 
	 * }
	 */
	public static void testOtil() {
		String file2read = "/Users/dinoienco/Documents/workspace/Amber/example_otil.txt";
		BufferedReader br = null;
		InputStreamReader isr = null;
		try {
			isr = new InputStreamReader(new FileInputStream(file2read));
			br = new BufferedReader(isr);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		String cur;
		Otil o = new Otil();
		try {
			while ((cur = br.readLine()) != null) {
				String[] elements = cur.split(" ");
				int direction = Integer.parseInt(elements[0]);
				int neigh = Integer.parseInt(elements[1]);
				String[] preds = elements[2].split(",");
				short[] seq = new short[preds.length];
				for (int i = 0; i < preds.length; ++i)
					seq[i] = Short.parseShort(preds[i]);
				// System.out.println("add: "+Arrays.toString(seq));
				o.add(seq, neigh, direction);
			}
			isr.close();
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		o.print();
		o.print2(1);

		short[] query1 = { 1, 3 };
		short[] query2 = { 4 };
		short[] query3 = { 5 };

		System.out.println("=============");
		System.out.println(o.query(query1, 1));
		System.out.println(o.query(query2, 1));
		System.out.println(o.query(query3, -1));
		System.out.println("=============");
		short[] lab = { 2, 3, 4 };
		System.out.println(o.checkIFNeighExists(lab, 2, 1));
	}

}
