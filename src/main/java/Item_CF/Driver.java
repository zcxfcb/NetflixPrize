public class Driver {
	public static void main(String[] args) throws Exception {
		
		UserCFDataDividerByUser dataDividerByUser = new UserCFDataDividerByUser();
		UserCFCoOccurrenceMatrixGenerator coOccurrenceMatrixGenerator = new UserCFCoOccurrenceMatrixGenerator();
		UserCFNormalize normalize = new UserCFNormalize();
		UserCFMultiplication multiplication = new UserCFMultiplication();
		UserCFSum sum = new UserCFSum();

		String rawInput = args[0];
		String userMovieListOutputDir = args[1];
		String coOccurrenceMatrixDir = args[2];
		String normalizeDir = args[3];
		String multiplicationDir = args[4];
		String sumDir = args[5];
		String[] path1 = {rawInput, userMovieListOutputDir};
		String[] path2 = {userMovieListOutputDir, coOccurrenceMatrixDir};
		String[] path3 = {coOccurrenceMatrixDir, normalizeDir};
		String[] path4 = {normalizeDir, rawInput, multiplicationDir};
		String[] path5 = {multiplicationDir, sumDir};
		
		dataDividerByUser.main(path1);
		coOccurrenceMatrixGenerator.main(path2);
		normalize.main(path3);
		multiplication.main(path4);
		sum.main(path5);

	}

}
