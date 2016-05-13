package org.dbpedia.extraction.destinations

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.Text
import org.dbpedia.extraction.spark.io.QuadSeqWritable
import org.dbpedia.extraction.spark.io.output.DBpediaCompositeOutputFormat
import org.apache.spark.SparkContext._
import java.util.logging.{Level, Logger}
import org.apache.hadoop.fs._

/**
 * Destination where RDF graphs are deduplicated and written to a Hadoop Path.
 *
 * @param path Path used by DBpediaCompositeOutputFormat to write outputs
 * @param hadoopConfiguration Hadoop Configuration object
 */
class DistDeduplicatingWriterDestination(path: Path, hadoopConfiguration: Configuration) extends DistDestination
{
  override def open() = ()
  private val logger = Logger.getLogger(getClass.getName)
  private val fileSystem = FileSystem.get(hadoopConfiguration)
  private val grouped_output = new Path(path, "grouped")
  private val quads_text_output = new Path(path, "quads_text")
  private val quads_parquet_output = new Path(path, "quads_parquet")

  /**
   * Writes RDD of quads (after extracting unique quads) to path using DBpediaCompositeOutputFormat.
   *
   * @param rdd RDD[ Seq[Quad] ]
   */
  override def write(rdd: RDD[Seq[Quad]])
  {
    rdd.flatMap { x => x}
    .map{ q => List(q.language, q.dataset, q.subject,
      q.predicate, q.value, q.context, q.datatype).mkString("\t") }
    .saveAsTextFile(quads_text_output.toString)
  }

  override def close() = ()
}
