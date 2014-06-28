package markov_clustering;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;

public class DisjointComponentVisit {

	static List<TreeSet<Integer>> clusters = new LinkedList<TreeSet<Integer>>();
	
	static int how_many_blocks = 10;
	static int block_size = 1000;
	static int max_size_cluster = 0;
	static int min_size_cluster = 0;
	static int avg_size_cluster = 0;
	
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
		System.err.print("\n\n");
		System.err.print("-> Found " + clusters.size() + "clusters");
		System.err.print("-> Biggest cluster size: " + max_size_cluster);
		System.err.print("-> Smalle cluster size: " + min_size_cluster);
		System.err.print("-> Avg cluster size: " + avg_size_cluster);
	}
	
	public static void main(String[] args) throws IOException {
		
		if(args.length < 2){
			System.out.println("\nComputes and outputs disjoint components.\n"+
					"Writes components to stdout and statistics to sterr.\n" + 
					"\n\tUsage: " + args[0] + " blocks_dir\n");
			return;
		}
		
		
		// Legge 100 file contenti blocchi di matrici
		
		// costruisce insiemi corrispondenti a componenti separate del grafo
		
		String f;
		
		Integer src, dest;
		
		TreeSet<Integer> c1 = null, c2 = null;
		
		
		for(int block_row_id = 0; block_row_id < how_many_blocks; block_row_id++)
			for(int block_col_id = 0; block_col_id < how_many_blocks; block_col_id++){
				
				f = String.format(args[1] + "/C-%d-%d", block_row_id, block_col_id);
				BufferedReader in  = new BufferedReader(new FileReader(f));
				String line = null;
				
				while((line = in.readLine()) != null){
					
					// Parse the string in the block file
					// format is source_node,dest_node \t val
					String[] split = line.split("\t");
					String[] ids = split[0].split(",");
					
					src = new Integer(Integer.parseInt(ids[0]) + (block_row_id * block_size));
					dest = new Integer(Integer.parseInt(ids[1]) + (block_col_id * block_size));
					
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
					if(c1 == c2) continue;
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
			}
		
		printResults();
		printStats();
		
		}


}
