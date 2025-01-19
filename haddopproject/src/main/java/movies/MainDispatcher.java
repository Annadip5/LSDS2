package movies;


public class MainDispatcher {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Please specify the job to run: 'highestRated', 'joinOperation', or 'likeCount'");
            System.exit(-1);
        }
        String jobName = args[0];
        String[] remainingArgs = new String[args.length - 1];
        System.arraycopy(args, 1, remainingArgs, 0, remainingArgs.length);

        switch (jobName) {
            case "highestRated":
                HighestRatedMoviePerUser.main(remainingArgs);
                break;
            case "joinOperation":
                JoinHighestRatedMovie.main(remainingArgs);
                break;
            case "likeCount":
                GroupMoviesByLikeCount.main(remainingArgs);
                break;
            default:
                System.out.println("Invalid job name. Please specify 'highestRated', 'joinOperation', or 'likeCount'.");
                System.exit(-1);
        }
    }
}
