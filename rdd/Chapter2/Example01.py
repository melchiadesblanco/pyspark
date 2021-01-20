from pyspark import SparkContext, SparkConf

# refer to https://github.com/drabastomek/learningPySpark/blob/master/Chapter02/LearningPySpark_Chapter02.ipynb

if __name__ == "__main__":
    conf = SparkConf().setAppName("word count").setMaster("local[3]")
    sc = SparkContext(conf = conf)
    sc.setLogLevel("ERROR")

    data  = sc.parallelize(
        [   ('Amber', 22), 
            ('Alfred', 23),
            ('Skye', 4), 
            ('Albert', 12),
            ('Moxxy', 9)]
    )

    data_from_file = sc.textFile(r'C:\Users\melch\Documents\GitHub\pyspark\large\VS14MORT.DUSMCPUB', 4)
    data_from_file.take(1)