package markov_clustering;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.geom.AffineTransform;
import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

import javax.imageio.ImageIO;




public class DisjointComponentVisit {

	public enum Style {
		VERTICAL, HORIZONTAL, LITTLE_CIRCLES, BIG_CIRCLES, CROSS, FILLED, LEFT_DIAG, RIGHT_DIAG,
		SQUARES
	}

	private static final boolean DEBUG = true;
	List<Clustering> allClusterings = new LinkedList<Clustering>();
	List<Color> colors = new LinkedList<Color>(); 
	
	List<Filler> fillers = new LinkedList<Filler>();
	
	String[] colorsString = {
			/*
			 * http://www.paletton.com/#uid=c4Z2-083A0ksEswlsn2ErFMiRFfsgUc
			 */
			"#7F2A75",
			"#CB00B2",
			"#CA53BB",
			"#EE1CD5",
			"#E33B18",
			"#B8523D",
			"#FF2C00",
			"#FF8369",
			"#FF451E",
			"#1A5694",
			"#2E5278",
			"#0B65C4",
			"#598DC2",
			"#2C8AEC",
			"#6BCB15",
			"#6AA436",
			"#70ED00",
			"#A3ED61",
			"#85F91D"
	};
	
	private static final int COLORS_NUMBER = 19;
	
	/* **************************************************************************** */
	/* ***************************  DEFINIZIONE TIPI  ***************************** */
	/* **************************************************************************** */
	
	public class Filler {
		
		public Filler(Color c1, Style s1) {
			c = c1;
			s = s1;
		}
		
		public Color c;
		public Style s;
	}
	
	// Descrive una partizione su un grafo, i.e. un insieme di clusters 
	public class Clustering {
		public List<Cluster> clusters = new LinkedList<Cluster>();
		
		int max_size_cluster = -1;
		int min_size_cluster = Integer.MAX_VALUE;
		int avg_size_cluster = 0;
		
		// Image to draw clusters as edges
		BufferedImage smallClusters = new BufferedImage(1000, 1000, BufferedImage.TYPE_INT_RGB);
		BufferedImage midClusters = new BufferedImage(1000, 1000, BufferedImage.TYPE_INT_RGB);
		BufferedImage bigClusters = new BufferedImage(1000, 1000, BufferedImage.TYPE_INT_RGB);
		
		Graphics2D sCg;
		Graphics2D mCg;
		Graphics2D bCg;
		
		// Image to draw clusters as patchwork
		BufferedImage patch = new BufferedImage(1000, 1000, BufferedImage.TYPE_INT_RGB);
		Graphics2D cpw;	


		public String name;
		
		public Clustering (String n, String p){
			name = n;
		}

		public void add(Cluster c){
			this.clusters.add(c);
		}
		
		public void printResultsAndComputeStats() throws FileNotFoundException, UnsupportedEncodingException{
			
			PrintWriter w = new PrintWriter(outputdir + "clusters-" + name + ".txt", "UTF-8");
			
			int i = 1, s;
			for(Cluster t : this.clusters){
				w.print(i + "\t");
				for(Integer n : t.nodes){
					w.print(n + ",");
				}
				
				s = t.size();
				
				if(s > max_size_cluster){
					max_size_cluster = s;
				}
				
				if(s < min_size_cluster){
					min_size_cluster = s;
				}
				
				i++;
				
				double delta = s - avg_size_cluster;
				avg_size_cluster += delta/(i);
				
				w.print("\n");
			}
			
			w.close();
		}
			
		private void drawArc(Graphics2D g, int src, int dest, Color c){
			
			g.setStroke(new BasicStroke(3, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND));
			g.setColor(c);
			g.setPaint(c);
			
			g.drawLine(
					(src / 100) * 10, 
					(src % 100) * 10, 
					(dest / 100) * 10, 
					(dest % 100) * 10
			);
		}
		
		public void printStats(){
			System.err.println("\n\n");
			System.err.println("-> Found " + clusters.size() + "clusters");
			System.err.println("-> Biggest cluster size: " + max_size_cluster);
			System.err.println("-> Smaller cluster size: " + min_size_cluster);
			System.err.println("-> Avg cluster size: " + avg_size_cluster);
		}

		public String[] getStats() {
			String[] res = new String[5];
			res[0] = name;
			res[1] = clusters.size() + "";
			res[2] = max_size_cluster + "";
			res[3] = min_size_cluster + "";
			res[4] = avg_size_cluster + "";
			return res;
		}
		
		public void remove(Cluster c) {
			this.clusters.remove(c);
		}
	}
	
	/********************************************************** */ 
	/* Classe: Cluster, Descrive un insieme di nodi e di archi  */
	/********************************************************** */
	
	public class Cluster {
		public TreeSet<Integer> nodes = new TreeSet<Integer>();
		public List<Edge> edges = new LinkedList<Edge>(); 
		
		public Filler filler = null;
		public double avg_length;
		public double max_length = Double.MIN_VALUE;
		public double min_length = Double.MAX_VALUE;
		
		/**
		 * Merges two clusters. copy all elements of c2 in this
		 *  
		 * 
		 * 
		 * @param c2 cluster to be merged
		 */
		public void merge(Cluster c2){
			for(Integer i : c2.nodes){
				this.add(i);
			}
			for(Edge e : c2.edges){
				this.add(e);
			}
		}
		
		public int size() {
			return this.nodes.size();
		}

		public void add(Integer i){
			this.nodes.add(i);
		}

		public boolean contains(Integer o) {
			return this.nodes.contains(o);
		}

		public void add(Edge e) {
			this.edges.add(e);
		}
		
	}
	
	
	/********************************************************** */ 
	/* Class: Edge, Describes a connection among two nodes      */
	/********************************************************** */
	public class Edge {
		int x, y, br, bc;
		double val;
		
		public Edge(String s) {
			
			String[] tabSplit = s.split("\t");
			val = Double.parseDouble(tabSplit[1]);
			
			// String[] bids = tabSplit[0].split(",");
			String[] ids = tabSplit[0].split(",");
			
			x = Integer.parseInt(ids[0]);
			y = Integer.parseInt(ids[1]);
			
			// br = Integer.parseInt(bids[0]);
			// bc = Integer.parseInt(bids[1]);
			br = 0;
			bc = 0;
		}
		
		public double length(){
			int src_x = x / 100;
			int src_y =	x % 100;
			int dest_x = y / 100;
			int dest_y = y % 100;
			
			return  Math.sqrt(Math.pow(src_x - dest_x, 2.) + Math.pow(src_y - dest_y, 2.));  
		}
	}
	
	String outputdir = "";
	static int how_many_blocks = 5;
	static int block_size = 10000 / how_many_blocks;
	
	int parsedLineN = 0;
	
	private boolean allNodes = false;
	
	/* **************************************************************************** */
	/* ***************************  FUNZIONI          ***************************** */
	/* **************************************************************************** */

	
	public void parseFile(File f, Clustering c) throws NumberFormatException, IOException {
		
		Cluster c1 = null, c2 = null;
		BufferedReader in  = new BufferedReader(new FileReader(f));
		int block_row_id, block_col_id, src, dest;
		String line = null;

		while((line = in.readLine()) != null){
			parsedLineN++;
			// System.err.println(parsedLineN);
			
			Edge e = new Edge(line);
			
			if( e.val < 0.9){
				continue;
			}
			
			block_row_id = e.br;
			block_col_id = e.bc;
			
			src = new Integer(e.x + (block_row_id * block_size));
			dest = new Integer(e.y + (block_col_id * block_size));
			
			// Search all clusters for the src and dest nodes
			// the contains operation in O(log n ) where n is the
			// size of the cluster
			
			c1 = c2  = null;
				
			
			for(Cluster tmp : c.clusters){
				// search source node in this cluster
				if(c1 == null)
					if(tmp.contains(src)){
						c1 = tmp;
					}
				
				// search dest node in thi cluster
				if(c2 == null){
					if(tmp.contains(dest)){
						c2 = tmp;
					}
				}

				// break the loop if both were found
				if(c1 != null && c2 != null){
					break;
				}
				
			}
			
			// They are in the same cluster already, skip
			if(c1 == c2 && c1 != null) {
				c1.add(e);
				continue;
			}
			
			if(c1 != null){
				if(c2 != null){
					
					// They are in different clusters, merge them
					c1.merge(c2);
					c1.add(e);
					c.remove(c2);
					
				} else {
					
					// dest has no cluster, add it to src cluster 
					c1.add(dest);
					c1.add(e);
				}
			} else {
				if(c2 != null){
					c2.add(src);
					c2.add(e);
				} else {
					// create new cluster
					Cluster nc = new Cluster();
					nc.add(src);
					nc.add(dest);
					nc.add(e);
					
					// Add to this clustering
					c.add(nc);
				}
			}
		}
		
		in.close();
	}
	
	public void findAndDrawClusters(String inputdir, String name) 
			throws NumberFormatException, IOException{
		
		File fs[], d;
	    
	    Clustering c = new Clustering(name, inputdir);
	    
	   // if(allNodes)
	   // 	drawAllNodes(c);
	    
	    d = new File(inputdir);
	    fs = d.listFiles();
	    for (File f: fs){
	    	parseFile(f,c);
	    }
		
	    allClusterings.add(c);
		
		c.printResultsAndComputeStats();
		// c.printStats();
		// drawPatchwork();
	}
	
	private void drawPatchwork(Clustering c) throws IOException {
		c.cpw = c.patch.createGraphics();
		c.cpw.setBackground(Color.white);
	    c.cpw.clearRect(0, 0, 1000, 1000);
	    
	    int x, y;
	    
	    c.cpw.setColor(Color.black);
	    c.cpw.setPaint(Color.black);
	    
	    // c.cpw.setFont(new Font("Free San", Font.PLAIN, 35));
	    // c.cpw.drawString(c.name, 1050, 0);
	    
	    for(Cluster t : c.clusters){
	    	
	    	c.cpw.setColor(t.filler.c);
	    	c.cpw.setPaint(t.filler.c);
	    	
	    	// System.err.println("New hue is " + h);
	    	
	    	int node_size = 10;
	    	
	    	switch(t.filler.s){
	    		case VERTICAL:
	    			for(Integer n : t.nodes){
	    	    		x = (n / 100) * node_size;	
	    	    		y = (n % 100) * node_size;
	    	    		c.cpw.fillRect(x, y, 3, 10);
	    	    		c.cpw.fillRect(x + 5, y, 3, 10);
	    	    	}
	    			break;
	    		case LEFT_DIAG:
	    			for(Integer n : t.nodes){
	    	    		x = (n / 100) * node_size;	
	    	    		y = (n % 100) * node_size;
	    	    		AffineTransform transformer = new AffineTransform();
	    	    		transformer.rotate(-0.785398163);
	    	    		c.cpw.transform(transformer);
	    	    		c.cpw.fillRect(x + 2, y, 6, 10);
	    	    		transformer.rotate(0.785398163);
	    	    		c.cpw.transform(transformer);
	    	    	}
	    			break;
	    		case RIGHT_DIAG:
	    			for(Integer n : t.nodes){
	    	    		x = (n / 100) * node_size;	
	    	    		y = (n % 100) * node_size;
	    	    		AffineTransform transformer = new AffineTransform();
	    	    		transformer.rotate(0.785398163);
	    	    		c.cpw.transform(transformer);
	    	    		c.cpw.fillRect(x + 2, y, 6, 10);
	    	    		transformer.rotate(-0.785398163);
	    	    		c.cpw.transform(transformer);
	    	    	}
	    			break;
	    		case HORIZONTAL:
	    			for(Integer n : t.nodes){
	    	    		x = (n / 100) * node_size;	
	    	    		y = (n % 100) * node_size;
	    	    		c.cpw.fillRect(x, y, 10, 3);
	    	    		c.cpw.fillRect(x, y + 5, 10, 3);
	    	    	}
	    			break;
	    		case CROSS:
	    			for(Integer n : t.nodes){
	    	    		x = (n / 100) * node_size;	
	    	    		y = (n % 100) * node_size;
	    	    		c.cpw.fillRect(x + 2, y + 1, 1, 3); //v
	    	    		c.cpw.fillRect(x + 1, y + 2, 3, 1); //h
	    	    		
	    	    		c.cpw.fillRect(x + 7, y + 1, 1, 3); //v
	    	    		c.cpw.fillRect(x + 6, y + 2, 3, 1); //h
	    	    		
	    	    		c.cpw.fillRect(x + 2, y + 6, 1, 3); //v
	    	    		c.cpw.fillRect(x + 1, y + 7, 3, 1); //h
	    	    		
	    	    		c.cpw.fillRect(x + 7, y + 6, 1, 3); //v
	    	    		c.cpw.fillRect(x + 6, y + 7, 3, 1); //h
	    	    		
	    	    	}
	    			break;
	    		case FILLED:
	    			for(Integer n : t.nodes){
	    	    		x = (n / 100) * node_size;	
	    	    		y = (n % 100) * node_size;
	    	    		c.cpw.fillRect(x, y, 10, 10);
	    	    	}
	    			break;
	    		case LITTLE_CIRCLES:
	    			for(Integer n : t.nodes){
	    	    		x = (n / 100) * node_size;	
	    	    		y = (n % 100) * node_size;
	    	    		c.cpw.fillOval(x + 1, y + 1, 4, 4);
	    	    		c.cpw.fillOval(x+6, y + 1, 4, 4);
	    	    		c.cpw.fillOval(x + 1, y+ 6, 4, 4);
	    	    		c.cpw.fillOval(x+6, y+6, 4, 4);
	    	    	}
	    			break;
	    		case BIG_CIRCLES:
	    			for(Integer n : t.nodes){
	    	    		x = (n / 100) * node_size;	
	    	    		y = (n % 100) * node_size;
	    	    		c.cpw.fillOval(x + 2, y + 2, 8, 8);
	    	    	}
	    			break;
	    		case SQUARES:
	    			for(Integer n : t.nodes){
	    	    		x = (n / 100) * node_size;	
	    	    		y = (n % 100) * node_size;
	    	    		c.cpw.fillRect(x + 2, y + 2, 8, 8);
	    	    	}
	    			break;
	    		default:
	    	}
	    }
	    
	    ImageIO.write(c.patch, "png", new File(outputdir +  c.name + ".png"));
	    
	}
	
	/*
	private void drawAllNodes(Clustering c){
		for(int i = 0; i < 1000; i+=10)
	    	for(int j = 0; j < 1000; j+=10)
	    		c.cg.fillOval(i, j, 5, 5);
	}
	*/

	public void generateColors() {
		
		/* float how_many_saturations = 10; 
		float how_may_hues = this.COLORS_NUMBER / how_many_saturations;
		
		float hue_delta = 1 / how_may_hues;
		float sat_delta = 1 / how_many_saturations;
		
		float h = 0;
		
		for(int i = 0; i < how_may_hues; i++){
			float s = 0;
			for(int j  = 0; j < how_many_saturations; j++){
				this.colors.add(
						Color.getHSBColor(h, s, 0.5f)
				);
				s+=sat_delta;
			}
			h+=hue_delta;
		} */
		
		for(int i = 0; i < this.colorsString.length; i++){
			this.colors.add(Color.decode(this.colorsString[i]));
		}
		for(Color c : this.colors){
			this.fillers.add(new Filler(c, Style.VERTICAL));
			this.fillers.add(new Filler(c, Style.HORIZONTAL));
			this.fillers.add(new Filler(c, Style.FILLED));
			this.fillers.add(new Filler(c, Style.LITTLE_CIRCLES));
			this.fillers.add(new Filler(c, Style.BIG_CIRCLES));
			// this.fillers.add(new Filler(c, Style.CROSS));
			this.fillers.add(new Filler(c, Style.SQUARES));
			// this.fillers.add(new Filler(c, Style.LEFT_DIAG));
			// this.fillers.add(new Filler(c, Style.RIGHT_DIAG));
		}
		
		Collections.shuffle(this.fillers);
		
	}
	
	public void assign_random_colors(Clustering first) {
		Random r = new Random();
		Filler f;
		List<Filler> tmp = new LinkedList<Filler>(this.fillers);
		
		for(Cluster c : first.clusters){
			f = tmp.get(r.nextInt(tmp.size()));
			c.filler = f;
			tmp.remove(f);
		}
	}
	
	public static void main(String[] args) throws IOException {
		
		if(args.length < 1){
			System.out.println("\nComputes and outputs disjoint components.\n"+
					"Writes components to clusters.txt and statistics to stderr.\n" + 
					"Writes an image file to clusters.png.\n" +
					"\n\tUsage: <thisJavaExec> <output_dir> <input_dir1> <input_dir2> ... \n");
			return;
		}
		
		DisjointComponentVisit v = new DisjointComponentVisit();
		
		v.outputdir = args[0];
		
		DEBUG("Output dir is: " + v.outputdir);
		
		String n;
		String[] s;
		for(int i = 1; i < args.length; i++){
			// todo: dare un nome significativo
			DEBUG("Parsing cluster " + (i));
			s = args[i].split("/");
			n = i + s[s.length-1];
			DEBUG("Name is " + n);
			v.findAndDrawClusters(args[i], n);
		}
		
		/**************************************************/
		/*			      ASSIGN COLORS			          */
		/**************************************************/
		
		DEBUG("Computo colori");
		
		// assegna i colori e crea le statistiche
		// primo assegnamento casuale
		Random r = new Random();
		Clustering first = v.allClusterings.get(0);  
		v.generateColors();
		v.assign_random_colors(first);
		int curr = 1, prec = 0; 
		while(curr < v.allClusterings.size()){
			
			Clustering currClustering = v.allClusterings.get(curr);
			Clustering prevClustering = v.allClusterings.get(prec);
			List<Cluster> tmp = new LinkedList<Cluster>(prevClustering.clusters);
			List<Filler> tmpF = new LinkedList<Filler>(v.fillers);
			for(Cluster c2 : currClustering.clusters){
				
				int max = 0;
				Cluster to_remove=null;
				
				for(Cluster c1 : tmp){
					List<Integer> tmpCluster = new ArrayList<Integer>(c1.nodes);
					tmpCluster.retainAll(c2.nodes);
					if(tmpCluster.size() > max){
						max = tmpCluster.size();
						to_remove = c1;							
					}
				}
				
				if(to_remove!=null){
					c2.filler = to_remove.filler;
					tmpF.remove(to_remove.filler);
					tmp.remove(to_remove);
					
				} else {
					int f = r.nextInt(tmpF.size());
					c2.filler = tmpF.get(f);
					tmpF.remove(f);
				}
				
			}
			curr+=1;
			prec+=1;
		}
		
		
		
		/**************************************************/
		/*		      MAKE IMGS AND STATS			      */
		/**************************************************/
		
		String[] columns = {
				"Name",
				"Clus",
				"Max",
				"Min",
				"Avg"
		};
		
		DEBUG("Creo immagini e statistiche");
		
		System.out.println(join(columns, "\t"));
		for(Clustering c : v.allClusterings){
			
			v.drawPatchwork(c);
			v.drawEdges(c);
			System.out.println(join(c.getStats(),"\t"));

		}
	
		
	}
	
	public void drawEdges(Clustering c) throws IOException {
		
		c.sCg = c.smallClusters.createGraphics();
		c.sCg.setBackground(Color.white);
	    c.sCg.clearRect(0, 0, 1000, 1000);
	    
	    c.mCg = c.midClusters.createGraphics();
		c.mCg.setBackground(Color.white);
	    c.mCg.clearRect(0, 0, 1000, 1000);
	    
	    c.bCg = c.bigClusters.createGraphics();
		c.bCg.setBackground(Color.white);
	    c.bCg.clearRect(0, 0, 1000, 1000);
		
	    double total_lengths; 
	    double l;
	    
		for(Cluster t : c.clusters){
			
			total_lengths = 0;
			
			
			for(Edge e: t.edges){
				l = e.length();
				total_lengths += l;
				if(l > t.max_length){
					t.max_length = l;
				}
				if(l < t.min_length){
					t.min_length = l;
				}
			}
			
			t.avg_length = total_lengths / t.nodes.size();
			
			// DEBUG("" + t.avg_length);
			
			Graphics2D gt = null;
			
			if(t.avg_length > 20){
				gt = c.bCg;
			} else if (t.avg_length > 5 ){
				gt = c.mCg;
			} else {
				gt = c.sCg;
			}
			for(Edge e: t.edges){
				c.drawArc(gt, e.x,e.y, t.filler.c);
			}
		}
		
		ImageIO.write(c.smallClusters, "png", new File(outputdir + "edges-"+ c.name + "-small.png"));
		ImageIO.write(c.midClusters, "png", new File(outputdir + "edges-"+ c.name + "-mid.png"));
		ImageIO.write(c.bigClusters, "png", new File(outputdir + "edges-"+ c.name + "-big.png"));
		
	}

	public static void DEBUG(String s){
		if(DEBUG)
			System.err.println("- [DBG] - " + s);
	}
	
	static private String join(String[] a, String sep ){
		String res = "";
		for(int i = 0; i < a.length; i++){
			res = res + sep + a[i];
		}
		return res;
	}


}
