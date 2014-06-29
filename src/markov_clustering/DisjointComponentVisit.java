package markov_clustering;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;

import javax.imageio.ImageIO;

public class DisjointComponentVisit {

	static List<TreeSet<Integer>> clusters = new LinkedList<TreeSet<Integer>>();
	
	static int how_many_blocks = 10;
	static int block_size = 1000;
	static int max_size_cluster = -1;
	static int min_size_cluster = Integer.MAX_VALUE;
	static int avg_size_cluster = 0;
	
	private boolean allNodes = false;
	
	// Image to draw clusters
	BufferedImage bImg = new BufferedImage(1000, 1000, BufferedImage.TYPE_INT_RGB);
	Graphics2D cg;
	
	/**
	 * Merges two clusters. copy all elements of c2 in c1 then
	 * removes c2 from cluster list 
	 * 
	 * @param c1 cluster to be kept
	 * @param c2 cluster to be merged and removed
	 */
	public static void merge(TreeSet<Integer> c1, TreeSet<Integer> c2){
		for(Integer i : c2){
			c1.add(i);
		}
		clusters.remove(c2);
	}
	

	public static void printResults(){
		int i = 1, s;
		for(TreeSet<Integer> t : clusters){
			System.out.print(i + "\t");
			for(Integer n : t){
				System.out.print(n + ",");
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
			
			System.out.print("\n");
		}
	}
	
	public static void printStats(){
		System.err.println("\n\n");
		System.err.println("-> Found " + clusters.size() + "clusters");
		System.err.println("-> Biggest cluster size: " + max_size_cluster);
		System.err.println("-> Smalle cluster size: " + min_size_cluster);
		System.err.println("-> Avg cluster size: " + avg_size_cluster);
	}
	
	
	public void findAndDrawClusters(String inputdir) throws NumberFormatException, IOException{
		String fp;
		File f;
		Integer src, dest;
		TreeSet<Integer> c1 = null, c2 = null;
		
		cg = bImg.createGraphics();
		cg.setBackground(Color.white);
	    cg.clearRect(0, 0, 1000, 1000);
		
	    if(allNodes)
	    	drawAllNodes();
	    
		for(int block_row_id = 0; block_row_id < how_many_blocks; block_row_id++)
			for(int block_col_id = 0; block_col_id < how_many_blocks; block_col_id++){
				
				fp = String.format(inputdir + "/C-%d-%d", block_row_id, block_col_id);
				f = new File(fp); 
				if(!f.exists()){
					continue;
				}
				
				BufferedReader in  = new BufferedReader(new FileReader(f));
				String line = null;
				int i = 0;
				while((line = in.readLine()) != null){
					i++;
					System.err.println(i);
					
					// Parse the string in the block file
					// format is source_node,dest_node \t val
					String[] split = line.split("\t");
					String[] ids = split[0].split(",");
					
					src = new Integer(Integer.parseInt(ids[0]) + (block_row_id * block_size));
					dest = new Integer(Integer.parseInt(ids[1]) + (block_col_id * block_size));
					
					drawArc(src,dest);
					
					// Search all clusters for the src and dest nodes
					// the contains operation in O(log n ) where n is the
					// size of the cluster
					
					c1 = c2  = null;
					for(TreeSet<Integer> tmp : clusters){
						// search source node in this cluster
						if(c1 != null)
							if(tmp.contains(src)){
								c1 = tmp;
							}
						
						// search dest node in thi cluster
						if(c2 != null){
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
					if(c1 == c2 && c1 != null) continue;
					if(c1 != null){
						if(c2 != null){
							
							// They are in different clusters, merge them
							merge(c1,c2);
							
						} else {
							
							// dest has no cluster, add it to src cluster 
							c1.add(dest);
							
						}
					} else {
						if(c2 != null){
							c2.add(src);
						} else {
							// create new cluster
							TreeSet<Integer> nc = new TreeSet<Integer>();
							nc.add(src);
							nc.add(dest);
							clusters.add(nc);
						}
					}
				}
				
				in.close();
				
			}
		
		ImageIO.write(bImg, "png", new File("/tmp/output_image.png"));
		printResults();
		printStats();
	}
	
	private void drawAllNodes(){
		for(int i = 0; i < 1000; i+=10)
	    	for(int j = 0; j < 1000; j+=10)
	    		cg.fillOval(i, j, 5, 5);
	}
	
	private void drawArc(int src, int dest){
		cg.drawLine(
				(src / 100) * 10 + 5, 
				(src % 100) * 10 + 5, 
				(dest / 100) * 10 + 5, 
				(dest % 100) * 10 + 5
		);
	}
	
	public static void main(String[] args) throws IOException {
		
		if(args.length < 1){
			System.out.println("\nComputes and outputs disjoint components.\n"+
					"Writes components to stdout and statistics to sterr.\n" + 
					"\n\tUsage: <thisJavaExec> blocks_dir\n");
			return;
		}
		
		DisjointComponentVisit v = new DisjointComponentVisit();
		
		v.findAndDrawClusters(args[0]);
	}


}
