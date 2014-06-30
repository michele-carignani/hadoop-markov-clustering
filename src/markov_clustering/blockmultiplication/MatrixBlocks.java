package markov_clustering.blockmultiplication;

import java.util.Iterator;
import java.util.LinkedList;

import markov_clustering.blockmultiplication.Block;

/**
 * Represents a set of matrix blocks.
 */
public class MatrixBlocks implements Iterable<Block>{
	private LinkedList<Block> allBlocks; 
	public MatrixBlocks(int rowId, int colId) {
		allBlocks = new LinkedList<Block>();
		allBlocks.add(new Block(rowId, colId));
	}
	/** Initializes the set with the given collection.
	 * @param blocks Array of 2-dimensional coordinates of the blocks to be inserted in the set
	 */
	public MatrixBlocks(int [][] blocks) {
		allBlocks = new LinkedList<Block>();
		for(int i = 0; i < blocks.length; i++) {
			if (blocks[i].length != 2) throw new IllegalArgumentException("The matrix coordinates are bi-dimensional.");
			addBlock(blocks[i][0], blocks[i][1]);
		}
	}
	/**
	 * Initializes an empty set of blocks
	 */
	public MatrixBlocks() {
		allBlocks = new LinkedList<Block>();
	}
	
	/** Adds a block */
	public void addBlock(int rowId, int colId) {
		allBlocks.add(new Block(rowId, colId));
	}
	
	/**
	 * Add a whole row
	 * @param rowId id of the row to add
	 * @param size size of the row to add
	 */
	public void addRow(int rowId, int size) {
		for (int i = 0; i < size; i++) {
			addBlock(rowId, i);
		}
	}
	/**
	 * Adds a whole column
	 * @param colId id of the column to add
	 * @param size size of the column to add
	 */
	public void addColumn(int colId, int size) {
		for(int i = 0; i < size; i++) {
			addBlock(i, colId);
		}
	}
	@Override
	/**
	 * Returns an iterator for this collection
	 */
	public Iterator<Block> iterator() {
		return allBlocks.iterator();
	}
}
