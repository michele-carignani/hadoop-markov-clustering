package markov_clustering.blockmultiplication;
/**
 * Representation of a matrix block
 *
 */
public class Block {
	private int row;
	private int column;
	public Block(int x, int y) {
		row = x;
		column = y;
	}
	
	public int getRow(){return row;}
	public int getColumn(){return column;}
	
	@Override
	public boolean equals(Object b) {
		if (!b.getClass().equals(Block.class)) return false;
		Block other = (Block) b;
		return row == other.row && column == other.column;
	}
	
}
