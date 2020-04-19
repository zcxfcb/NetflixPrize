
public class PreprocessorDriver {
	public static void main(String[] args) throws Exception {

		Preprocessor dataDividerByUser = new Preprocessor();

		String rawInput = args[0];
		String output = args[1];
		String[] path1 = {rawInput, output};

		dataDividerByUser.main(path1);
	}

}
