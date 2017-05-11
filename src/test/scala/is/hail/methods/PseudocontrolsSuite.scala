package is.hail.methods

import is.hail.SparkSuite
import is.hail.utils._
import is.hail.variant.Variant
import org.apache.spark.sql.Row
import org.testng.annotations.Test

class PseudocontrolsSuite extends SparkSuite {
  @Test def test() {

    val out = tmpDir.createLocalTempFile("out", "txt")
    val myvds = hc.importVCF("src/test/resources/tdt.vcf", nPartitions = Some(4)) // Import the VDS
      .annotateSamplesFam("src/test/resources/tdt.fam") // Annotate with fam
    val pseudoControlsCounter = myvds.pseudoControls("src/test/resources/tdt.fam") // Run pseudocontrols
      .aggregateByKey("v=v, fam=sa.fam.famID", "c = g.flatMap(g=>[g.gtj(),g.gtk()]).counter()") // aggregate by key, count alleles
    val ped = Pedigree.read("src/test/resources/tdt.fam", hadoopConf, myvds.sampleIds) // Read pedigree
    val parentIds = ped.completeTrios.flatMap(t => Array(t.mom, t.dad)).toSet // Get list of parent IDs
    val parentsCounter = myvds.filterSamples { case (s, _) => parentIds.contains(s) } // Filter to only Parents
      .aggregateByKey("v=v, fam=sa.fam.famID", "c = g.flatMap(g=>[g.gtj(),g.gtk()]).counter()") // aggregate by key, count alleles
    println(myvds.filterSamples { case (s, _) => parentIds.contains(s) })
    println(myvds.pseudoControls("src/test/resources/tdt.fam"))
    println(pseudoControlsCounter.collect().toIndexedSeq)
    //assert(pseudoControlsCounter.collect() == parentsCounter.collect())
    val pseudoMap = pseudoControlsCounter.keyedRDD().collect().toMap
    val aggMap = parentsCounter.keyedRDD().collect().toMap

    pseudoMap.foreach { case (k, v) =>
      val aggValue = aggMap(k).getAs[Map[java.lang.Integer, _]](0)
      val pseudoValue = v.getAs[Map[java.lang.Integer, _]](0)

      val p = pseudoValue == Map((null, 4)) || pseudoValue == aggValue
      if (!p)
        print(
          s"""error at $k:
             |  pseudo: $pseudoValue
             |  aggreg: $aggValue""".stripMargin)
      assert(p)
    }
  }
