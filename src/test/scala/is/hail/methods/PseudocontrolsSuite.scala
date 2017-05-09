package is.hail.methods

import is.hail.SparkSuite
import is.hail.utils._
import is.hail.variant.Variant
import org.testng.annotations.Test
/**
  * Created by bavila on 4/25/17.
  */
class PseudocontrolsSuite extends SparkSuite{
  @Test def test() {

    val out = tmpDir.createLocalTempFile("out", "txt")
    val myvds = hc.importVCF("src/test/resources/tdt.vcf", nPartitions = Some(4)) // Import the VDS
      .annotateSamplesFam("src/test/resources/tdt.fam") // Annotate with fam
    val pseudoControlsCounter = myvds.pseudoControls("src/test/resources/tdt.fam")  // Run pseudocontrols
      .aggregateByKey("v=v, fam=sa.fam", "gs.flatMap(g=>[g.gti,g.gtj]).counter()") // aggregate by key, count alleles
    val ped = Pedigree.read("src/test/resources/tdt.fam", hadoopConf, myvds.sampleIds) // Read pedigree
    val parentIds = ped.completeTrios.flatMap(t => Array(t.mom, t.dad)).toSet // Get list of parent IDs
    val parentsCounter = myvds.filterSamples{case (s, _) => parentIds.contains(s)} // Filter to only Parents
      .aggregateByKey("v=v, fam=sa.fam", "gs.flatMap(g=>[g.gti,g.gtj]).counter()")  // aggregate by key, count alleles
    assert(pseudoControlsCounter.keyedRDD().collect().toMap == parentsCounter.keyedRDD().collect().toMap)// assert same

  }

}
