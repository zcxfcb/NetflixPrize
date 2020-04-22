
public class UserCFDriver {
	public static void main(String[] args) throws Exception {
		String rawInput = args[0];
		String userMovieListOutputDir = args[1];
		String coOccurrenceMatrixDir = args[2];
		String normalizeDir = args[3];
		String multiplicationDir = args[4];
		String sumDir = args[5];
		String benchmarkDir = args[6];
		String[] path1 = {rawInput, userMovieListOutputDir};
		String[] path2 = {userMovieListOutputDir, coOccurrenceMatrixDir};
		String[] path3 = {coOccurrenceMatrixDir, normalizeDir};
		String[] path4 = {normalizeDir, rawInput, multiplicationDir};
		String[] path5 = {multiplicationDir, sumDir};
		String[] path6 = {rawInput, sumDir, benchmarkDir};

		UserCFDataDivider.main(path1);
		userCFCoOccurrenceMatrixProducer.main(path2);
		UserCFNormalization.main(path3);
		UserCFMatrixMultiplication.main(path4);
		UserCFSum.main(path5);
		UserCFBenchmarker.main(path6);
	}

}
