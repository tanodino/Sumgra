/* AmberParser.java */
/* Generated By:JavaCC: Do not edit this line. AmberParser.java */
package queryparser;
import java.util.ArrayList;
import query.Query;
import com.gs.collections.impl.map.mutable.primitive.ObjectIntHashMap;
import com.gs.collections.impl.map.mutable.primitive.ObjectShortHashMap;


public class AmberParser implements AmberParserConstants {
        public static void main( String[] args )
                throws ParseException, TokenMgrError {
/*
		Adder parser = new Adder( System.in ) ;
		ArrayList<String> vars = new ArrayList<String>();
		ArrayList<String> triples = new ArrayList<String>();
		Query query = new Query();

		parser.Start(vars, triples, query) ; 
		for (String s: vars)
			System.out.println(s);
		System.out.println("====");
		for (String s: triples)
			System.out.println(s);
*/
        }

//TOKEN : { < PLUS : "+" > }
//TOKEN : { < NUMBER : (["0"-"9"])+ > }
  final public 

void Start(ArrayList<String> list_vars, ArrayList<String> triples, ObjectShortHashMap<String> string2ShortP, ObjectIntHashMap<String> string2intSO, Query query) throws ParseException {Token t;
    jj_consume_token(SELECT);
    Variable(list_vars, query);
    label_1:
    while (true) {
      switch ((jj_ntk==-1)?jj_ntk_f():jj_ntk) {
      case VAR:{
        ;
        break;
        }
      default:
        jj_la1[0] = jj_gen;
        break label_1;
      }
      Variable(list_vars, query);
    }
    jj_consume_token(WHERE);
    jj_consume_token(LBRACE);
    label_2:
    while (true) {
      Triple(triples, string2ShortP, string2intSO, query);
      jj_consume_token(DOT);
      switch ((jj_ntk==-1)?jj_ntk_f():jj_ntk) {
      case VAR:
      case URI:{
        ;
        break;
        }
      default:
        jj_la1[1] = jj_gen;
        break label_2;
      }
    }
    jj_consume_token(RBRACE);
  }

  final public void Variable(ArrayList<String> list, Query query) throws ParseException {Token t ;
    t = jj_consume_token(VAR);
list.add(t.image);
                query.addSelect(t.image);
  }

  final public void Triple(ArrayList<String> triple, ObjectShortHashMap<String> string2ShortP, ObjectIntHashMap<String> string2intSO, Query query) throws ParseException {Token t1,t2,t3;
        boolean isT1Variable = true;
        boolean isT3Variable = true;
    switch ((jj_ntk==-1)?jj_ntk_f():jj_ntk) {
    case VAR:{
      t1 = jj_consume_token(VAR);
      break;
      }
    case URI:{
      t1 = jj_consume_token(URI);
isT1Variable = false;
      break;
      }
    default:
      jj_la1[2] = jj_gen;
      jj_consume_token(-1);
      throw new ParseException();
    }
    t2 = jj_consume_token(URI);
    switch ((jj_ntk==-1)?jj_ntk_f():jj_ntk) {
    case LITERAL:{
      t3 = jj_consume_token(LITERAL);
isT3Variable = false;
      break;
      }
    case URI:{
      t3 = jj_consume_token(URI);
isT3Variable = false;
      break;
      }
    case VAR:{
      t3 = jj_consume_token(VAR);
      break;
      }
    default:
      jj_la1[3] = jj_gen;
      jj_consume_token(-1);
      throw new ParseException();
    }
int subject_id = -1;
                int object_id = -1;
                if (!isT1Variable)
                        subject_id = string2intSO.get(t1.image);
                if (!isT3Variable)
                        object_id = string2intSO.get(t3.image);

                query.addTriple(t1.image,t3.image,string2ShortP.get(t2.image), subject_id, object_id);
                //triple.add(t1.image+"_"+t2.image+"_"+t3.image);

  }

  /** Generated Token Manager. */
  public AmberParserTokenManager token_source;
  JavaCharStream jj_input_stream;
  /** Current token. */
  public Token token;
  /** Next token. */
  public Token jj_nt;
  private int jj_ntk;
  private int jj_gen;
  final private int[] jj_la1 = new int[4];
  static private int[] jj_la1_0;
  static private int[] jj_la1_1;
  static {
      jj_la1_init_0();
      jj_la1_init_1();
   }
   private static void jj_la1_init_0() {
      jj_la1_0 = new int[] {0x200,0x40200,0x40200,0xc0200,};
   }
   private static void jj_la1_init_1() {
      jj_la1_1 = new int[] {0x0,0x0,0x0,0x0,};
   }

  /** Constructor with InputStream. */
  public AmberParser(java.io.InputStream stream) {
     this(stream, null);
  }
  /** Constructor with InputStream and supplied encoding */
  public AmberParser(java.io.InputStream stream, String encoding) {
    try { jj_input_stream = new JavaCharStream(stream, encoding, 1, 1); } catch(java.io.UnsupportedEncodingException e) { throw new RuntimeException(e); }
    token_source = new AmberParserTokenManager(jj_input_stream);
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 4; i++) jj_la1[i] = -1;
  }

  /** Reinitialise. */
  public void ReInit(java.io.InputStream stream) {
     ReInit(stream, null);
  }
  /** Reinitialise. */
  public void ReInit(java.io.InputStream stream, String encoding) {
    try { jj_input_stream.ReInit(stream, encoding, 1, 1); } catch(java.io.UnsupportedEncodingException e) { throw new RuntimeException(e); }
    token_source.ReInit(jj_input_stream);
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 4; i++) jj_la1[i] = -1;
  }

  /** Constructor. */
  public AmberParser(java.io.Reader stream) {
    jj_input_stream = new JavaCharStream(stream, 1, 1);
    token_source = new AmberParserTokenManager(jj_input_stream);
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 4; i++) jj_la1[i] = -1;
  }

  /** Reinitialise. */
  public void ReInit(java.io.Reader stream) {
    jj_input_stream.ReInit(stream, 1, 1);
    token_source.ReInit(jj_input_stream);
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 4; i++) jj_la1[i] = -1;
  }

  /** Constructor with generated Token Manager. */
  public AmberParser(AmberParserTokenManager tm) {
    token_source = tm;
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 4; i++) jj_la1[i] = -1;
  }

  /** Reinitialise. */
  public void ReInit(AmberParserTokenManager tm) {
    token_source = tm;
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 4; i++) jj_la1[i] = -1;
  }

  private Token jj_consume_token(int kind) throws ParseException {
    Token oldToken;
    if ((oldToken = token).next != null) token = token.next;
    else token = token.next = token_source.getNextToken();
    jj_ntk = -1;
    if (token.kind == kind) {
      jj_gen++;
      return token;
    }
    token = oldToken;
    jj_kind = kind;
    throw generateParseException();
  }


/** Get the next Token. */
  final public Token getNextToken() {
    if (token.next != null) token = token.next;
    else token = token.next = token_source.getNextToken();
    jj_ntk = -1;
    jj_gen++;
    return token;
  }

/** Get the specific Token. */
  final public Token getToken(int index) {
    Token t = token;
    for (int i = 0; i < index; i++) {
      if (t.next != null) t = t.next;
      else t = t.next = token_source.getNextToken();
    }
    return t;
  }

  private int jj_ntk_f() {
    if ((jj_nt=token.next) == null)
      return (jj_ntk = (token.next=token_source.getNextToken()).kind);
    else
      return (jj_ntk = jj_nt.kind);
  }

  private java.util.List<int[]> jj_expentries = new java.util.ArrayList<int[]>();
  private int[] jj_expentry;
  private int jj_kind = -1;

  /** Generate ParseException. */
  public ParseException generateParseException() {
    jj_expentries.clear();
    boolean[] la1tokens = new boolean[43];
    if (jj_kind >= 0) {
      la1tokens[jj_kind] = true;
      jj_kind = -1;
    }
    for (int i = 0; i < 4; i++) {
      if (jj_la1[i] == jj_gen) {
        for (int j = 0; j < 32; j++) {
          if ((jj_la1_0[i] & (1<<j)) != 0) {
            la1tokens[j] = true;
          }
          if ((jj_la1_1[i] & (1<<j)) != 0) {
            la1tokens[32+j] = true;
          }
        }
      }
    }
    for (int i = 0; i < 43; i++) {
      if (la1tokens[i]) {
        jj_expentry = new int[1];
        jj_expentry[0] = i;
        jj_expentries.add(jj_expentry);
      }
    }
    int[][] exptokseq = new int[jj_expentries.size()][];
    for (int i = 0; i < jj_expentries.size(); i++) {
      exptokseq[i] = jj_expentries.get(i);
    }
    return new ParseException(token, exptokseq, tokenImage);
  }

  /** Enable tracing. */
  final public void enable_tracing() {
  }

  /** Disable tracing. */
  final public void disable_tracing() {
  }

}