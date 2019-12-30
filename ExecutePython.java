private String use(String input) throws IOException {

		StringBuilder builder = new StringBuilder();
		int exitCode = 1;
		try {
			String python_val[] = new String[] { "C:\\Anaconda3\\python.exe",
					"C:\\Users\\AC38815\\Desktop\\ChatBot_Working_Files\\Python-Spyder Files\\untitled0.py",input};
			//ProcessBuilder pb = new ProcessBuilder(Arrays.asList(python_val));
			ProcessBuilder pb = new ProcessBuilder(Arrays.asList(python_val));
			Process p = null;
			//Process p = pb.start();
			

			if (pb != null) {
				try {
					p = pb.start();
				} catch (IOException e) {
					e.printStackTrace();
				}

				if (p != null) {
					try {
						exitCode = p.waitFor();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
			
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(p.getInputStream()));
		    String line = bufferedReader.readLine();
		    while (line != null) {
		      builder.append(line).append(System.lineSeparator());
		      line = bufferedReader.readLine();
		    }
		    bufferedReader.close();	

		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e);
		}
			
		return builder.toString();	
	}