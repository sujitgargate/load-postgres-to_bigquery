import os
from glob import glob

class csvFinderClass:
    
    def findCV(self):

        PATH = "resources\\csv\\"
        # EXT = "*.csv"
        csv_files = [file
                        for path, subdir, files in os.walk(PATH)
                        for file in glob(os.path.join("resources\\csv\\", "*.csv"))]

        print(csv_files[0].split("\\")[-1])

        return csv_files[0]