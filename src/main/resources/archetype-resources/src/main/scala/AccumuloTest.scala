#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )

package ${package};

import org.apache.spark._
import org.apache.spark.rdd.NewHadoopRDD

import org.apache.hadoop.conf.Configuration

import org.apache.accumulo.core.client.mapreduce.InputFormatBase
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat

object AccumuloTest {
  def main(args: Array[String]) {
  
    if (args.length < 6) {
      System.err.println("Usage: AccumuloTest <master> <user> <password> <table> <instance> <zookeepers>");
      System.exit(1);
    }

    val sc = new SparkContext(args(0), "AccumuloTest",
      System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))

    val conf = new Configuration()
    InputFormatBase.setInputInfo(conf,
       args(1),			//user
       args(2).getBytes(),	//password
       args(3), 		//table
       null)			//auths
    
    InputFormatBase.setZooKeeperInstance(conf,
       args(4),	//instance
       args(5)) //zookeepers

    val accumuloRDD = sc.newAPIHadoopRDD(conf, classOf[AccumuloInputFormat], 
      classOf[org.apache.accumulo.core.data.Key],
      classOf[org.apache.accumulo.core.data.Value])

    val cnt = accumuloRDD.count()
    println("count of entries for table: " + args(3) + " is: " + cnt)

    System.exit(0)
  }
}
