package org.hathitrust.htrc.tools.deduplicatemeta

/**
  * Class holding the metadata attributes that will be used in the deduplication process and
  * for reporting the results
  *
  * @param volumeIdentifier      The volume identifier
  * @param title                 The volume title
  * @param names                 The author names
  * @param pubDate               The publication date
  * @param enumerationChronology The value extracted from the enumerationChronology fields
  */
case class DocMeta(volumeIdentifier: String, title: String, names: String,
                   pubDate: String, enumerationChronology: String)