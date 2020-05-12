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

HDFS=user/lada32/lab3

OBJDIR=build

JAR := MapReducePSO.jar

TARGETS := $(addprefix run, T)

.PHONY: final $(TARGETS) clean

final: $(TARGETS)

runA: $(OBJDIR)/TitleCount.class
	jar -cvf $(JAR) -C $(OBJDIR)/ ./
	hdfs dfs -rm -r -skipTrash -f /$(HDFS)/A-output/
	hadoop jar $(JAR) TitleCount -D stopwords=/$(HDFS)/misc/stopwords.txt -D delimiters=/$(HDFS)/misc/delimiters.txt /$(HDFS)/titles /$(HDFS)/A-output
	@echo "Run the following command to read the output file:"
	@echo "hdfs dfs -cat /$(HDFS)/A-output/part*"

runT: $(OBJDIR)/Test.class
	jar -cvf $(JAR) -C $(OBJDIR)/ ./
	hdfs dfs -rm -r -skipTrash -f /$(HDFS)/Test-output/
	hadoop jar  $(JAR) Test -D property=0.1 -D ASCENDING=True /user/adampap/WHV /$(HDFS)/Test-output
	@echo "Run the following command to read the output file:"
	@echo "hdfs dfs -cat /$(HDFS)/Test-output/part*"

run2: $(OBJDIR)/FTen.class
	jar -cvf $(JAR) -C $(OBJDIR)/ ./
	hdfs dfs -rm -r -skipTrash -f /$(HDFS)/FTen-output/
	hadoop jar  $(JAR) FTen -D N=10 /user/adampap/WHV /$(HDFS)/FTen-output
	@echo "Run the following command to read the output file:"
	@echo "hdfs dfs -cat /$(HDFS)/FTen-output/part*"

run3: $(OBJDIR)/FnL.class
	jar -cvf $(JAR) -C $(OBJDIR)/ ./
	hdfs dfs -rm -r -skipTrash -f /$(HDFS)/FnL-output/
	hadoop jar  $(JAR) FnL /user/adampap/WHV /$(HDFS)/FnL-output
	@echo "Run the following command to read the output file:"
	@echo "hdfs dfs -cat /$(HDFS)/FnL-output/part*"

run4: $(OBJDIR)/fehA.class
	jar -cvf $(JAR) -C $(OBJDIR)/ ./
	hdfs dfs -rm -r -skipTrash -f /$(HDFS)/fehA-output/
	hadoop jar  $(JAR) fehA /user/adampap/WHVsmall /$(HDFS)/fehA-output
	@echo "Run the following command to read the output file:"
	@echo "hdfs dfs -cat /$(HDFS)/fehA-output/part*"


$(OBJDIR)/%.class: %.java | $(OBJDIR)
	hadoop com.sun.tools.javac.Main $< -d $(OBJDIR)

$(OBJDIR):
	mkdir $@

.PHONY: clean
clean:
	rm -f $(OBJDIR)/* $(JAR)
	hdfs dfs -rm -r -skipTrash -f /$(HDFS)/*-output/
