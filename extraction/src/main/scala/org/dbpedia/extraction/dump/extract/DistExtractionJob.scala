package org.dbpedia.extraction.dump.extract

import java.util.logging.{Level, Logger}
import org.dbpedia.extraction.destinations.{Quad, DistDestination}
import org.dbpedia.extraction.mappings.RootExtractor
import org.dbpedia.extraction.sources.WikiPage
import org.dbpedia.extraction.spark.serialize.KryoSerializationWrapper
import org.dbpedia.extraction.wikiparser.Namespace
import org.apache.spark.rdd.RDD
import org.dbpedia.extraction.util.StringUtils
import org.apache.spark.SparkContext._
import org.dbpedia.util.Exceptions

import org.apache.spark.AccumulatorParam
import java.io.{StringWriter,PrintWriter}

object LineCummulatorParam extends AccumulatorParam[String] {
  def zero(value:String) : String = value
  def addInPlace(s1:String, s2:String):String = s1 + "\n" + s2
}

/**
 * Executes an extraction using Spark.
 *
 * @param extractor The Extractor
 * @param rdd The RDD of WikiPages
 * @param namespaces Only extract pages in these namespaces
 * @param destination The extraction destination. Will be closed after the extraction has been finished.
 * @param label user readable label of this extraction job.
 */
class DistExtractionJob(extractor: => RootExtractor, rdd: => RDD[WikiPage], namespaces: Set[Namespace], destination: => DistDestination, label: String, description: => String)
{
  private val logger = Logger.getLogger(getClass.getName)

  def run(): Unit =
  {
    val sc = rdd.sparkContext
    val allPages = sc.accumulator(0)
    val failedPages = sc.accumulator(0)
    val errorMessages = sc.accumulator("","debug info")(LineCummulatorParam)

    val extractorSerializable = KryoSerializationWrapper(extractor)
    val extractorBC = sc.broadcast(extractorSerializable)
    val ns = namespaces.map(_.toString)

    val startTime = System.currentTimeMillis

    logger.info("Namespaces to broadcast: %s".format(namespaces.mkString(",")))
    val results: RDD[Seq[Quad]] =
      rdd.map
      {
        page =>
          // Take a WikiPage, perform the extraction with a set of extractors and return the results as a Seq[Quad].
          val (success, graph) = try
          {
            val ex = extractorBC.value.value
            if (!ns.contains(page.title.namespace.toString)) {
              errorMessages += "page %s skipped as namespace '%s' is not in broadcasted ns: %s".format(page.title, page.title.namespace.toString, ns.mkString(","))
              (true, None)
            }
            else if (ex == null) {
              errorMessages += "broadcasted extractor is null"
              (true, None)
            }
            else {
              (true, Some(ex.apply(page)))
            }
          }
          catch
            {
              case ex: Exception =>
                // loggerBC.value.log(Level.WARNING, "error processing page '" + page.title + "': " + Exceptions.toString(ex, 200))
                val sw = new StringWriter
                ex.printStackTrace(new PrintWriter(sw))
                errorMessages += "error processing page %s: %s.\n%s''".format(page.title, Exceptions.toString(ex, 200), sw.toString)
                (false, None)
            }
          if (success) allPages += 1 else failedPages += 1
          graph.getOrElse(Nil)
      }

    logger.info(description+" started")

    destination.open()

    logger.info("Writing outputs to destination...")

    destination.write(results)

    destination.close()

    val time = System.currentTimeMillis - startTime
    println("%s: extracted %d pages in %s (per page: %f ms; failed pages: %d).".format(label,
                                                                                       allPages.value,
                                                                                       StringUtils.prettyMillis(time),
                                                                                       time.toDouble / allPages.value,
                                                                                       failedPages.value))
    println("Errors: %s".format(errorMessages.value))

    logger.info(description+" finished")
  }
}
