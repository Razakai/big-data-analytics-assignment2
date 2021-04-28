# --------------------------------------------------------
#
# PYTHON PROGRAM DEFINITION
#
# The knowledge a computer has of Python can be specified in 3 levels:
# (1) Prelude knowledge --> The computer has it by default.
# (2) Borrowed knowledge --> The computer gets this knowledge from 3rd party libraries defined by others
#                            (but imported by us in this program).
# (3) Generated knowledge --> The computer gets this knowledge from the new functions defined by us in this program.
#
# When launching in a terminal the command:
# user:~$ python3 this_file.py
# our computer first processes this PYTHON PROGRAM DEFINITION section of the file.
# On it, our computer enhances its Python knowledge from levels (2) and (3) with the imports and new functions
# defined in the program. However, it still does not execute anything.
#
# --------------------------------------------------------

# ------------------------------------------
# IMPORTS
# ------------------------------------------
import pyspark
import pyspark.sql.functions
from pyspark.sql.functions import when, concat_ws

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------


def my_main(spark, my_dataset_dir, source_node):
    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType(
        [pyspark.sql.types.StructField("source", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField(
             "target", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField(
             "weight", pyspark.sql.types.IntegerType(), False)
         ])

    # 2. Operation C1: 'read' to create the DataFrame from the dataset and the schema
    inputDF = spark.read.format("csv") \
        .option("delimiter", " ") \
        .option("quote", "") \
        .option("header", "false") \
        .schema(my_schema) \
        .load(my_dataset_dir)

    # ------------------------------------------------
    # START OF YOUR CODE:
    # ------------------------------------------------

    # Remember that the entire work must be done “within Spark”:
    # (1) The function my_main must start with the creation operation 'read' above loading the dataset to Spark SQL.
    # (2) The function my_main must finish with an action operation 'collect', gathering and printing by the screen the result of the Spark SQL job.
    # (3) The function my_main must not contain any other action operation 'collect' other than the one appearing at the very end of the function.
    # (4) The resVAL iterator returned by 'collect' must be printed straight away, you cannot edit it to alter its format for printing.

    # Type all your code here. Use as many Spark SQL operations as needed.

    def getBestCandidate(pathDF, edgeCandidatesDF):

        sourceNode = pathDF.select(pathDF["source"].alias(
            "sourceNode"), pathDF.cost, pathDF.path) #.where(pathDF.source == source_node)
       

        # join all edge candidates with current source node cost
        joinCostDF = edgeCandidatesDF.join(sourceNode,
                                           edgeCandidatesDF.source == sourceNode.sourceNode, "inner")

        # getting Best Candidate (path with lowest cost)
        bestCostDF = joinCostDF.withColumn(
            "cost", joinCostDF.cost + joinCostDF.weight)

        bestCostDF = bestCostDF.select(bestCostDF["source"].alias("candidate_source"), bestCostDF["target"].alias("candidate_target"), bestCostDF["cost"].alias("candidate_cost"), bestCostDF["path"].alias("candidate_path"))\
            .orderBy(bestCostDF.cost, ascending=True)\
            .limit(1)
       
        return bestCostDF


    # make sure all edge values are positive
    assert(inputDF.filter(inputDF.source > 0).count() > 0)
    

    # create path dataframe to store cost and path to nodes
    pathDF = inputDF.select(inputDF.source).distinct()\
        .withColumn("cost", when(inputDF.source == source_node, 0)
                    .otherwise(-1))\
        .withColumn("path", when(inputDF.source == source_node, str(source_node))
                    .otherwise(""))\
        .orderBy(inputDF.source)\
        .persist()

   
    # initial edge candidates
    edgeCandidatesDF = inputDF.filter(inputDF.source == source_node).persist()
    edgeCandidatesDF.show()

    while edgeCandidatesDF.count() > 0:
        bestCandidate = getBestCandidate(pathDF, edgeCandidatesDF)
        print("best candidate:")
        bestCandidate.show()

        # --------update pathDF--------
        # union bestCandidate with pathDF
        unionPathDF = pathDF.join(bestCandidate,
                    pathDF.source == bestCandidate.candidate_target, "full")
        #unionPathDF.show()

        # update pathDF
        unionPathDF = unionPathDF.withColumn("path", when(unionPathDF.source == unionPathDF.candidate_target, concat_ws("-", unionPathDF.candidate_path, unionPathDF.source))
                                                    .otherwise(unionPathDF.path))\
                                .withColumn("cost", when(unionPathDF.source == unionPathDF.candidate_target, unionPathDF.candidate_cost)
                                                    .otherwise(unionPathDF.cost))

        pathDF = unionPathDF.select(unionPathDF.source, unionPathDF.cost, unionPathDF.path).persist()
        print("updated pathDF:")
        pathDF.show()

                                                # -------update edgeCandidatesDF-------
        # union edgeCandidatesDF
        unionEdgeCandidates = edgeCandidatesDF.join(bestCandidate,
                            edgeCandidatesDF.source == bestCandidate.candidate_source, "full")

        unionEdgeCandidates = unionEdgeCandidates.na.fill("")\
            .na.fill(-5)
        unionEdgeCandidates.show()
        # removed used candidate
        unionEdgeCandidates = unionEdgeCandidates.filter(unionEdgeCandidates.target != unionEdgeCandidates.candidate_target)\
            .select(unionEdgeCandidates["source"].alias("Edge_source"), unionEdgeCandidates["target"].alias("Edge_target"), unionEdgeCandidates["weight"].alias("Edge_weight"))

        # remove candidates whose targets already have shortest path
        usableCandidates = unionEdgeCandidates.join(pathDF,
                                        unionEdgeCandidates.Edge_target == pathDF.source, "inner")
        edgeCandidatesDF = usableCandidates.filter(usableCandidates.cost == -1)\
            .select(usableCandidates["Edge_source"].alias("source"), usableCandidates["Edge_target"].alias("target"), usableCandidates["Edge_weight"].alias("weight"))
        print("removed egde candidates list:")
        edgeCandidatesDF.show()



                                                # -------new candidates---------
        unionCandidatesDF = inputDF.join(bestCandidate,
                                    inputDF.source == bestCandidate.candidate_target, "full")
  

        # filter out already used candidates
        unionCandidatesDF = unionCandidatesDF.filter(unionCandidatesDF.source == unionCandidatesDF.candidate_target)\
            .select(unionCandidatesDF["source"].alias("potential_source"), unionCandidatesDF["target"].alias("potential_target"), unionCandidatesDF["weight"].alias("potential_weight"))
        
        # ensure that potential candidates do not already have shortest path
        unionCandidatesAndPathDF = pathDF.join(unionCandidatesDF,
                                    pathDF.source == unionCandidatesDF.potential_target)
        
        unionCandidatesAndPathDF = unionCandidatesAndPathDF.filter(unionCandidatesAndPathDF.cost == -1)\
            .select(unionCandidatesAndPathDF["potential_source"].alias("source"),
                    unionCandidatesAndPathDF["potential_target"].alias("target"),
                    unionCandidatesAndPathDF["potential_weight"].alias("weight"))

        # update candidates
        edgeCandidatesDF = edgeCandidatesDF.union(unionCandidatesAndPathDF).persist()

        

    pathDF.show()
    solutionDF = pathDF.select(pathDF["source"].alias("id"), pathDF["cost"], pathDF["path"]).orderBy(pathDF["cost"], ascending=True)


    

    # ------------------------------------------------
    # END OF YOUR CODE
    # ------------------------------------------------

    # Operation A1: 'collect' to get all results
    resVAL = solutionDF.collect()
    for item in resVAL:
        print(item)


# --------------------------------------------------------
#
# PYTHON PROGRAM EXECUTION
#
# Once our computer has finished processing the PYTHON PROGRAM DEFINITION section its knowledge is set.
# Now its time to apply this knowledge.
#
# When launching in a terminal the command:
# user:~$ python3 this_file.py
# our computer finally processes this PYTHON PROGRAM EXECUTION section, which:
# (i) Specifies the function F to be executed.
# (ii) Define any input parameter such this function F has to be called with.
#
# --------------------------------------------------------
if __name__ == '__main__':
    # 1. We use as many input arguments as needed
    source_node = 1

    # 2. Local or Databricks
    local_False_databricks_True = False

    # 3. We set the path to my_dataset and my_result
    my_local_path = "../../../../3_Code_Examples/L15-25_Spark_Environment/"
    my_databricks_path = "/"

    my_dataset_dir = "FileStore/tables/6_Assignments/my_dataset_3/"

    if local_False_databricks_True == False:
        my_dataset_dir = my_local_path + my_dataset_dir
    else:
        my_dataset_dir = my_databricks_path + my_dataset_dir

    # 4. We configure the Spark Session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We call to our main function
    my_main(spark, my_dataset_dir, source_node)
