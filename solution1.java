import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class solution1 {

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: HDFSFileMover <sourcePath> <destinationPath>");
            System.exit(1);
        }

        String SPath = args[0];
        String DPath = args[1];

        try {
            Configuration config = new Configuration();
            FileSystem fs = FileSystem.get(config);

            Path s = new Path(SPath);
            Path d = new Path(DPath);

            switch (validatePaths(fs, s, d)) {
                case 0:
                    System.err.println("Source file does not exist");
                    System.exit(1);
                    break;
                case 1:
                    System.err.println("Destination file already exists");
                    System.exit(1);
                    break;
                case 2:
                    if (!fs.rename(s, d)) {
                        System.err.println("Failed to move the file");
                        System.exit(1);
                    }
                    System.out.println("File moved successfully");
                    break;
                default:
                    System.err.println("Unknown error occurred");
                    System.exit(1);
            }

            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static int validatePaths(FileSystem fs, Path s, Path d) throws Exception {
        if (!fs.exists(s)) {
            // Source file does not exist
            return 0;
        }

        if (fs.exists(d)) {
            // Destination file already exists
            return 1;
        }

        // Success
        return 2;
    }
}
