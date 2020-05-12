#
# This is a simple makefile to assist with quickly building the Exercises of MP2.
#
# To build and execute a particular exercise:
#    - For a single exercise, type 'make runA' to run exercise A.
#    - For all exercises, 'make'.
#
#
HADOOP_CLASSPATH := ${JAVA_HOME}/lib/tools.jar
export HADOOP_CLASSPATH

HDFS=user/lada32/lab5

OBJDIR=build

JAR := MapReducePSO.jar

TARGETS := $(addprefix run, T)

.PHONY: final $(TARGETS) clean

final: $(TARGETS)

runPR1: $(OBJDIR)/PageRank.class
	jar -cvf $(JAR) -C $(OBJDIR)/ ./
	hdfs dfs -rm -r -skipTrash -f /$(HDFS)/PR1-output/
	hadoop jar $(JAR) PageRank -D N=4 -D K=10 -D B=0.8 /user/adampap/wikiSmall /$(HDFS)/PR1-output
	@echo "Run the following command to read the output file:"
	@echo "hdfs dfs -cat /$(HDFS)/PR1-output/Final/part*"

runPR2: $(OBJDIR)/PageRank.class
	jar -cvf $(JAR) -C $(OBJDIR)/ ./
	hdfs dfs -rm -r -skipTrash -f /$(HDFS)/PR2-output/
	hadoop jar $(JAR) PageRank -D N=3 -D K=10 -D B=0.8 /user/adampap/wikiDeadEnds /$(HDFS)/PR2-output
	@echo "Run the following command to read the output file:"
	@echo "hdfs dfs -cat /$(HDFS)/PR2-output/Final/part*"

runPR3: $(OBJDIR)/PageRank.class
	jar -cvf $(JAR) -C $(OBJDIR)/ ./
	hdfs dfs -rm -r -skipTrash -f /$(HDFS)/PR3-output/
	hadoop jar $(JAR) PageRank -D N=4 -D K=10 -D B=0.8 /user/adampap/wikiSpiderTraps /$(HDFS)/PR3-output
	@echo "Run the following command to read the output file:"
	@echo "hdfs dfs -cat /$(HDFS)/PR3-output/Final/part*"


$(OBJDIR)/%.class: %.java | $(OBJDIR)
	hadoop com.sun.tools.javac.Main $< -d $(OBJDIR)

$(OBJDIR):
	mkdir $@

.PHONY: clean
clean:
	rm -f $(OBJDIR)/* $(JAR)
	hdfs dfs -rm -r -skipTrash -f /$(HDFS)/*-output/
