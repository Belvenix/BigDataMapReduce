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

HDFS=user/lada32/lab6

OBJDIR=build

JAR := MapReducePSO.jar

TARGETS := $(addprefix run, T)

.PHONY: final $(TARGETS) clean

final: $(TARGETS)

runSO: $(OBJDIR)/DriverStackOverflow.class
	jar -cvf $(JAR) -C $(OBJDIR)/ ./
	hdfs dfs -rm -r -skipTrash -f /$(HDFS)/output-SO/
	hadoop jar $(JAR) DriverStackOverflow /user/adampap/stackOverflow /$(HDFS)/output-SO
	@echo "Run the following command to read the output file:"
	@echo "hdfs dfs -cat /$(HDFS)/output-SO/part*"



$(OBJDIR)/%.class: %.java | $(OBJDIR)
	hadoop com.sun.tools.javac.Main $< -d $(OBJDIR)

$(OBJDIR):
	mkdir $@

.PHONY: clean
clean:
	rm -f $(OBJDIR)/* $(JAR)
	hdfs dfs -rm -r -skipTrash -f /$(HDFS)/*-output/
