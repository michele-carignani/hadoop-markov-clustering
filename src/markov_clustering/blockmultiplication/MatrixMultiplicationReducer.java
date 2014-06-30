package markov_clustering.blockmultiplication;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MatrixMultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int totSize = context.getConfiguration().getInt("size", 10000);
		int splits = context.getConfiguration().getInt("splits", 10);
		int matrixSize = totSize/splits;
		double[][] matrix_A = new double[matrixSize][matrixSize];
		double[][] matrix_B = new double[matrixSize][matrixSize];
		//Memorize all values...
		for(Text v: values) {
			String[] fields = v.toString().split(",");
			int x = Integer.parseInt(fields[1]);
			int y = Integer.parseInt(fields[2]);
			double value = Double.parseDouble(fields[3]);
			if (fields[0].equals("A"))
				matrix_A[x][y] = value;
			else //Matrix B is memorized by column since it is accessed this way, in order to minimize cache faults
				matrix_B[y][x] = value;
		}
		
		for (int i = 0; i < matrixSize; i++) {
			for(int j = 0; j < matrixSize; j++) {
				double sum = 0;
				for (int k = 0; k < matrixSize; k++) {
					sum += matrix_A[i][k]*matrix_B[j][k];
				}
				if(sum > 0) context.write(new Text(i+","+j), new DoubleWritable(sum));
			}
		}
		
	}
	
}
