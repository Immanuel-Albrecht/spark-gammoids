package plus.albrecht.run

/**
  * represents a configuration state
  *
  * @param currentTagSet a set of tags that is added to every call
  *
  * @param keyTagValue   a dictionary mapping keys to TagSet-Value combinations
  */
class Config(
    val currentTagSet: Set[String],
    val keyTagValue: Map[String, Map[Set[String], Any]]
) {

  /**
    * constructs an empty configuration
    */
  def this() {
    this(Set(), Map())
  }

  /**
    *
    * @return this config with cleared currentTagSet
    */
  def clearTagSet() = {
    new Config(Set(), keyTagValue)
  }

  /**
    * changes the current tag set of the configuration
    *
    * @param tagSet new tag set
    *
    * @return altered config
    */
  def setTagSet(tagSet: Set[String]) = {
    new Config(tagSet, keyTagValue)
  }

  /**
    * changes the currentTagSet
    *
    * @param iterable new tag set
    *
    * @return altered config
    */
  def ^(iterable: Iterable[String]) = setTagSet(iterable.toSet)

  /**
    * changes the currentTagSet
    *
    * @param single_tag single tag of the new tagset
    *
    * @return altered config
    */
  def ^(single_tag: String) = setTagSet(Set(single_tag))

  /**
    * go to the root of the config
    *
    * @return altered config
    */
  def ^() = clearTagSet

  /**
    * dive into a more specific realm of configuration
    *
    * @param iterable a bunch of new tags
    *
    * @return altered Config
    */
  def addTags(iterable: Iterable[String]): Config = {
    new Config(currentTagSet ++ iterable, keyTagValue)
  }

  /**
    * return to a more general realm of configuration
    *
    * @param iterable a bunch of new tags
    *
    * @return altered Config
    */
  def delTags(iterable: Iterable[String]): Config = {
    new Config(currentTagSet -- iterable, keyTagValue)
  }

  /**
    * see addTags
    *
    * @return altered config
    */
  def ++ = addTags(_)

  /**
    * adds a tag to the current tagset
    *
    * @param s new tag
    *
    * @return altered config
    */
  def ++(s: String) = addTags(s :: Nil)

  /**
    * see delTags
    *
    * @return altered config
    */
  def -- = delTags(_)

  /**
    * removes a tag
    *
    * @param s tag to remove
    *
    * @return altered config
    */
  def --(s: String) = delTags(s :: Nil)

  /**
    * get the value from the configuration that best fits the key with respect to the
    * currentTagSet
    *
    * values that belong to a more general (smaller) tag set may be returned if there is no
    * value associated
    * with the key and the current tag set. More special values are never returned, though.
    *
    * @param key     what value
    *
    * @param default default if no matching value is found
    *
    * @return either the default object, or the config value
    */
  def getValueOrDefault(key: String, default: Any): Any = {
    if (keyTagValue contains key) {
      val tag_values: Set[(Set[String], Any)] = keyTagValue
        .getOrElse(key, Map[Set[String], Any]())
        .toSet[(Set[String], Any)]
        .filter({ case (tags, value) ⇒ tags subsetOf currentTagSet })
      val (_, value) = tag_values.foldLeft((Set[String](), default))({
        case ((current_tags, current_value), (tags, value)) ⇒
          if (tags.size >= current_tags.size) (tags, value)
          else (current_tags, current_value)
      })
      value
    } else default
  }

  /**
    * get the config value corresponding to key
    *
    * @param key
    *
    * @return maybe a corresponding value
    */
  def get(key: String): Option[Any] = {
    getValueOrDefault(key, None) match {
      case None ⇒ None
      case x ⇒ Some(x)
    }
  }

  /**
    * get
    *
    * @param key
    *
    * @return maybe the corresponding value
    */
  def >>(key: String) = get(key)

  /**
    *
    * @param key_default key/default value pair
    *
    * @return default value or corresponding value
    */
  def >>(key_default: (String, Any)) = {
    val (key, default) = key_default
    getValueOrDefault(key, default)
  }

  /**
    * sets the key to the given value with respect to the currentTagSet
    *
    * @param key
    *
    * @param value
    *
    * @return altered Config
    */
  def set(key: String, value: Any): Config = {
    val tag_values: Map[Set[String], Any] =
      keyTagValue.getOrElse(key, Map[Set[String], Any]())
    val new_tag_values = tag_values ++ Map(currentTagSet → value)

    new Config(currentTagSet, keyTagValue + (key → new_tag_values))
  }

  /**
    * sets the value of a key under the currentTagSet
    *
    * @param key_value (key,value) -pair
    *
    * @return altered config
    */
  def <<(key_value: (String, Any)): Config = {
    val (key, value) = key_value
    set(key, value)
  }

  /**
    * Imports all the tags from sub_config with respect to the currentTagSet
    *
    * @param sub_config
    *
    * @return altered config
    */
  def <<(sub_config: Config): Config = {

    sub_config.keyTagValue.foldLeft(this)(
      {
        case (cfg, (key, tag_values)) ⇒
          tag_values.foldLeft(cfg)(
            {
              case (cfg, (tags, value)) ⇒
                cfg.addTags(tags).set(key, value).setTagSet(currentTagSet)
            }
          )
      }
    )
  }

}

/**
  * wrapper object for run time configuration
  */
object Config {

  var config = new Config()

  /**
    * Changes the current configuration.
    *
    * Please keep in mind that changes of currentTagSet will be undone
    * at the end of the configuration change.
    *
    * Ex: Config(_ ++ "abc" << ("key", "value"))
    * which dives into tag Set("abc"), then sets "key" to "value"
    *
    * @param alteration action on the configuration
    */
  def apply(alteration: Config ⇒ Config): Unit = {
    val old_tags = config.currentTagSet
    config = alteration(config).setTagSet(old_tags)
  }

  /**
    * Get an immutable snapshot of the current configuration.
    *
    * @return current configuration
    */
  def apply() = config

}
