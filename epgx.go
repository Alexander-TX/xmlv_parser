package main

import (
    "os"
    "io"
    "fmt"
    "flag"
    "time"
    "math"
    "bufio"
    "strings"
    "runtime"
    "math/bits"
    "io/ioutil"
    "database/sql"
    "compress/gzip"
)

import _ "gitlab.eltex.loc/aleksandr.rvachev/go-sqlite3.git"

var db *sql.DB

var EltexPackageVersion = "unknown"
var EltexBuilder = "unknown builder"
var EltexBuildTime = "an unknown date"

var startFrom time.Time
var spanDuration time.Duration

var snippetLength int

var dbEarliestDate *time.Time
var dbLastDate *time.Time

var mappedTotal = 0
var trimmedTotal = 0
var snippetLengthMax = 0

var archivedChannels = 0

var exitCode = 0

func Bail(format string, a ...interface{}) {
  exitCode = 1
  fmt.Fprintf(os.Stderr, "\n%s\n", "Fatal error!!")
  fmt.Fprintf(os.Stderr, format, a...)
  runtime.Goexit()
}

func main() {
  defer func() {
    if r := recover(); r != nil {
      panic(r)
    } else {
      os.Exit(exitCode)
    }
  }()

  dbEarliestDate = nil
  dbLastDate = nil

  var Usage = func() {
    fmt.Fprintf(os.Stderr, "EPGX validator\n\n")
    fmt.Fprintf(os.Stderr, "usage: %s [options] [file]\n", os.Args[0])
    flag.PrintDefaults()
  }

  flag.Usage = Usage

  showVersion := flag.Bool("version", false, "Write version information to standard output")
  flag.Parse()

  platform_init()

  if *showVersion {
    fmt.Printf("%s\n", EltexPackageVersion)
    os.Exit(0)
  }

  if flag.NArg() != 1 {
    Usage()
    os.Exit(1)
  }

  dbPath := flag.Arg(0)

  var dbFile *os.File
  var inputErr error

  dbFile, inputErr = os.Open(dbPath)
  if inputErr != nil {
    Bail("Could not open EPGX file\n %s\n", inputErr.Error())
  }

  if strings.HasSuffix(dbPath, ".gz") {
    fmt.Fprintf(os.Stderr, "Detected .gz extension, decompressing to temporary file...\n")

    gzipReader, gzErr := gzip.NewReader(bufio.NewReader(dbFile))
    if gzErr != nil {
      Bail("Failed to open gzip archive\n %s\n", gzErr.Error())
    }

    var tmpDbErr error

    dbFile, tmpDbErr = ioutil.TempFile("", "db-*.sqlite")
    if tmpDbErr != nil {
      Bail("Cannot create temporary db file\n %s\n", tmpDbErr.Error())
    }

    _, copyErr := io.Copy(dbFile, gzipReader)
    if copyErr != nil {
      Bail("Failed to extract db to temporary file\n %s\n", copyErr.Error())
    }

    defer os.Remove(dbFile.Name())
  }

  dbUrl := fmt.Sprintf("file:%s", dbFile.Name())

  db, dbErr := sql.Open("sqlite3", dbUrl)
  if dbErr != nil {
    Bail("sqlite error\n %s\n", dbErr.Error())
  }

  var ret string
  var err error

  //////////////////////////////////////////////

  fmt.Printf("Verifying application_id... ")

  var appId int32
  app_id := db.QueryRow("PRAGMA application_id;")
  err = app_id.Scan(&appId)
  if err != nil {
    Bail("Failed to fetch application_id\n %s\n", err.Error())
  }

  if appId != 1701606520 {
    Bail("Invalid application_id: expected 1701606520, but got %d\n", appId)
  }

  fmt.Printf("ok\n")

  //////////////////////////////////////////////
 
  fmt.Printf("Running SQLite integrity check... ")

  integrity := db.QueryRow("PRAGMA integrity_check(1);")
  err = integrity.Scan(&ret)
  if err != nil {
    Bail("Integrity check failed!")
  }

  if ret != "ok" {
    Bail("Integrity check found errors:\n %s\n", ret)
  }

  fmt.Printf("ok\n")

  //////////////////////////////////////////////

  fmt.Printf("Counting contents... ")

  var total int64
  totalRows := db.QueryRow("SELECT COUNT(*) FROM search_meta;")
  err = totalRows.Scan(&total)
  if err != nil {
    Bail("Failed to count rows in search_meta:\n %s\n", err.Error())
  }

  if total == 0 {
    Bail("Schedule is empty — search_meta has 0 rows\n")
  }

  fmt.Printf("ok\n")

  //////////////////////////////////////////////

  fmt.Printf("Checking integrity of string table... ")

  var haveTitle int64
  missing := db.QueryRow("SELECT COUNT(*) FROM search_meta WHERE EXISTS (SELECT 1 FROM text WHERE docid = title_id) AND EXISTS (SELECT 1 FROM text WHERE docid = description_id);")
  err = missing.Scan(&haveTitle)
  if err != nil {
    Bail("Failed to count rows in search_meta without title in text table:\n %s\n", err.Error())
  }

  if total != haveTitle {
    Bail("Schedule is corrupt: %d of %d items don't have title or description in text table\n", total - haveTitle, total)
  }

  fmt.Printf("ok\n")

  //////////////////////////////////////////////

  fmt.Printf("Checking integrity of uri table... ")

  var haveValidUri int64
  validUri := db.QueryRow("SELECT COUNT(*) FROM search_meta WHERE image_uri IS NULL OR EXISTS (SELECT 1 FROM uri WHERE uri._id = image_uri);")
  err = validUri.Scan(&haveValidUri)
  if err != nil {
    Bail("Failed to count rows in search_meta without uri in uri table:\n %s\n", err.Error())
  }

  if total != haveValidUri {
    Bail("Schedule is corrupt: %d of %d items have non-null image_uri column, but don't have matching record in uri table\n", total - haveValidUri, total)
  }

  fmt.Printf("ok\n")

  //////////////////////////////////////////////

  fmt.Printf("Checking integrity of channels table... ")

  var haveValidChannel int64
  validChannel := db.QueryRow("SELECT COUNT(*) FROM search_meta WHERE EXISTS (SELECT 1 FROM channels WHERE channels.ch_id = search_meta.ch_id);")
  err = validChannel.Scan(&haveValidChannel)
  if err != nil {
    Bail("Failed to count rows in search_meta without channel in channels table:\n %s\n", err.Error())
  }

  if total != haveValidChannel {
    Bail("Schedule is corrupt: %d of %d items don't have channel in channels table\n", total - haveValidChannel, total)
  }

  fmt.Printf("ok\n")

  //////////////////////////////////////////////

  fmt.Printf("Checking integrity of FTS table... ")

  var foobar int64
  ftsTest := db.QueryRow("SELECT COUNT(*) FROM fts_search WHERE fts_search MATCH 'howdy*';")
  err = ftsTest.Scan(&foobar)
  if err != nil {
    Bail("Failed to query FTS table:\n %s\n", err.Error())
  }

  fmt.Printf("ok\n")

  //////////////////////////////////////////////

  fmt.Printf("Checking integrity of tags table... ")

  tagTableTest := db.QueryRow("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'tags';")
  err = tagTableTest.Scan(&foobar)

  if err != nil {
    // make sure, that search_meta also does not have tags
    distinctTags, err := db.Query("SELECT tag FROM search_meta WHERE tag != 0;")
    if err == nil && distinctTags.Next() {
      Bail("Database has tags, but no tags table\n")
    } else {
      // assume not tags column
    }

    fmt.Printf("ok (no tags)\n")
  } else {
    var tagId int64
    var tagName string

    tags, err := db.Query("SELECT _id, tag FROM tags ORDER BY _id;")
    if err != nil {
      Bail("Failed to query tags table:\n %s\n", err.Error())
    }

    for {
      if !tags.Next() {
        break;
      }

      err = tags.Scan(&tagId, &tagName)
      if err != nil {
        Bail("Failed to read from tags table:\n %s\n", err.Error())
      }

      if bits.OnesCount(uint(tagId)) > 1 {
        Bail("Identifier of tag '%s' has more than 1 bit set (_id = %d)\n", tagName, tagId)
      }
    }

    fmt.Printf("ok\n")
  }

  //////////////////////////////////////////////

  var chTotal int64

  chCount := db.QueryRow("SELECT COUNT(DISTINCT(ch_id)) FROM search_meta;")
  err = chCount.Scan(&chTotal)
  if err != nil {
    Bail("Failed to count channels:\n %s\n", err.Error())
  }

  overall, err := db.Query("SELECT MIN(start_time), MAX(start_time) FROM search_meta;")
  if err != nil || !overall.Next() {
    Bail("Failed to query start times\n")
  }

  timeNow := time.Now()
  localLocation := timeNow.Location()
  eltDateFormat := "02-01-2006 15:04"

  var startTs int64
  var endTs int64

  err = overall.Scan(&startTs, &endTs)
  if err != nil {
    Bail("Failed to read start times\n", err.Error())
  }

  startTime := time.Unix(startTs, 0).In(localLocation)
  endTime := time.Unix(endTs, 0).In(localLocation)

  var avgLen float64

  averageLen := db.QueryRow("SELECT AVG(len) FROM (SELECT ch_id, (MAX(start_time) - MIN(start_time)) AS len FROM search_meta GROUP BY ch_id);")
  err = averageLen.Scan(&avgLen)
  if err != nil {
    Bail("Failed to read average programm length:\n %s\n", err.Error())
  }

  fmt.Printf("\nFinished scanning EPGX file\n\n")

  fmt.Printf("EPG contains %d channels\n", chTotal)

  fmt.Printf("First entry at %s\nLast entry at %s\n", startTime.Format(eltDateFormat), endTime.Format(eltDateFormat))

  if avgLen > 3600 * 24 {
    fmt.Printf("Average length is %d days\n", int64(math.Round(avgLen / 3600 / 24)))
  } else {
    fmt.Printf("Average length is %d hours\n", int64(avgLen / 3600))
  }

  fmt.Printf("no errors found\n")
}
