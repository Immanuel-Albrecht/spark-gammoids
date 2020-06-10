package plus.albrecht.run

/**
 * represents a configuration state
 *
 * @param currentTagSet   a set of tags that is added to every call
 * @param keyTagValue     a dictionary mapping keys to TagSet-Value combinations
 */
class Config(val currentTagSet : Set[String],
             val keyTagValue : Map[String, (Set[String], Any)]
            )
{
  /**
   *
   * @return this config with cleared currentTagSet
   */
  def clearTagSet() = {new Config(Set(), keyTagValue)}

  /**
   *
   * @param iterable  a bunch of new tags
   * @return
   */
  def addTags(iterable: Iterable[String]) = {
    new Config(currentTagSet ++ iterable, keyTagValue)
  }
}

/**
 * wrapper object for run time configuration
 */
object Config {

}
