package main

import (
    "os"
    "io"
    "fmt"
    "flag"
    "time"
    "bufio"
    "regexp"
    "strings"
    "runtime"
    "io/ioutil"
    "database/sql"
    "encoding/xml"
    "path/filepath"
    "compress/gzip"
    "golang.org/x/net/html/charset"
)

import _ "gitlab.eltex.loc/aleksandr.rvachev/go-sqlite3.git"

type Programm struct {
 Start                 string             `xml:"start,attr"`
 Channel               string             `xml:"channel,attr"`
 Title                 string             `xml:"title"`
 Description           string             `xml:"desc"`
}

var db *sql.DB
var sql1, sql2 *sql.Stmt
var startFrom time.Time
var spanDuration time.Duration

var dbEarliestDate *time.Time
var dbLastDate *time.Time

var localLocation *time.Location

var eltDateFormat string

var ageRegexp *regexp.Regexp

var exitCode = 0

func Bail(format string, a ...interface{}) {
  fmt.Fprintf(os.Stderr, format, a...)
  runtime.Goexit()
}

func main() {
  defer os.Exit(exitCode)

  dbEarliestDate = nil
  dbLastDate = nil

  eltDateFormat = "02-01-2006 15:04"

  defDuration, _ := time.ParseDuration("72h")

  dbPath := flag.String("output", "output.eltex.epg", "database file.")
  xmlPath := flag.String("input", "", "XMLTV file. (default read from standard input)")
  timeStart := flag.String("offset", "", "start import from specified date. Example: 29-12-2009 16:40. (default today)")
  argDuration := flag.Duration("timespan", defDuration, "duration since start date. Example: 72h.")
  flag.Parse()

  spanDuration = *argDuration

  timeNow := time.Now()
  timeZone, _ := timeNow.Zone()
  localLocation = timeNow.Location()

  fmt.Printf("Local time zone %s (%s)\n", timeZone, localLocation)

  if (spanDuration.Nanoseconds() == 0) {
    panic("Duration must be positive")
  }

  if (*timeStart == "") {
    startFrom = time.Now()
  } else {
    var startFromErr error

    startFrom, startFromErr = time.ParseInLocation(eltDateFormat, *timeStart, localLocation)
    if (startFromErr != nil) {
      panic("Failed to parse start time: " + startFromErr.Error())
    }
  }

  outDir := filepath.Dir(*dbPath)

  tmpFile, tmpErr := ioutil.TempFile(outDir, "db-*.sqlite")
  if tmpErr != nil {
    Bail("Cannot create temporary file\n %s\n", tmpErr.Error())
  }

  dbUrl := fmt.Sprintf("file:%s", tmpFile.Name())

  db, dbErr := sql.Open("sqlite3", dbUrl)
  if dbErr != nil {
    Bail("sqlite error\n %s\n", dbErr.Error())
  }

  // do not create on-disk temporary files (we don't want to clean them up)
  db.Exec("PRAGMA journal_mode = MEMORY;")
  db.Exec("PRAGMA temp_store = MEMORY;")

  db.Exec("PRAGMA application_id = 0x656c7478;") // hint: see hex

  os.Remove(tmpFile.Name())

  db.Exec("CREATE TABLE search_meta (docid INTEGER PRIMARY KEY, ch_id, start_time INTEGER, title TEXT, description TEXT);")
  db.Exec("CREATE VIRTUAL TABLE fts_search USING fts4(content='search_meta', title, description);")

  var xmlFile io.Reader

  if (*xmlPath == "") {
    fmt.Printf("No -input argument, reading from standard input...\n");

    xmlFile = bufio.NewReader(os.Stdin)
  } else {
    var inputErr error

    xmlFile, inputErr = os.Open(*xmlPath)
    if inputErr != nil {
      Bail("Could not open XMLTV file\n %s\n", inputErr.Error())
    }
  }

  decoder := xml.NewDecoder(xmlFile)
  decoder.CharsetReader = charset.NewReaderLabel

  programme := &Programm{}

  bulkTx, txErr := db.Begin()
  if txErr != nil {
    Bail("Could not start transaction\n %s\n", txErr.Error())
  }

  sql1, _ = bulkTx.Prepare("INSERT INTO search_meta (start_time, ch_id, title, description) VALUES (?, ?, ?, ?);")
  sql2, _ = bulkTx.Prepare("INSERT INTO fts_search (docid, title, description) VALUES (?, ?, ?);")

  // skip root
root:
  for {
    token, xmlErr := decoder.Token()
    if xmlErr != nil {
      Bail("XMLTV file is malformed (failed to find root tag)")
    }

    switch xmlRoot := token.(type) {
      default:
        continue;
      case xml.StartElement:
        if (xmlRoot.Name.Local == "tv") {
          break root;
        } else {
          Bail("malformed XMLTV: <tv> tag not found, got <%s> instead", xmlRoot.Name.Local)
        }
    }
  }

  fmt.Printf("Copying XMLTV schedule to database\n")

  ageRegexp = regexp.MustCompile("(.+)\\([0-9]{1,2}\\+\\)$")

  var appendedElements = 0

  // iterate over all <programme> tags and add them to database
  for {
    t, tokenErr := decoder.Token()
    if tokenErr != nil {
      if tokenErr == io.EOF {
        break
      } else {
        Bail("Failed to read token\n %s\n", tokenErr.Error())
      }
    }

    switch startElement := t.(type) {
      default:
        continue;
      case xml.StartElement:
        //fmt.Printf("Another element: %s", startElement.Name.Local)

        if (startElement.Name.Local != "programme") {
          decoder.Skip()
          continue;
        }
        if (addElement(decoder, programme, &startElement)) {
          appendedElements += 1;
        }
        break;
    }
  }

  if (appendedElements == 0) {
    emptyErrStr := fmt.Sprintf("no elements within specified period (%s)", startFrom.Format(eltDateFormat))

    if (dbLastDate != nil) {
      emptyErrStr += fmt.Sprintf(", last slot is at %s", (*dbLastDate).Format(eltDateFormat))
    }

    if (dbEarliestDate != nil) {
      emptyErrStr += fmt.Sprintf(", first slot is at %s", (*dbEarliestDate).Format(eltDateFormat))
    }

    Bail("%s\n", emptyErrStr)
  }

  bulkTx.Commit()

  _, indexErr := db.Exec("CREATE INDEX search_idx ON search_meta (start_time);")
  if indexErr != nil {
    Bail("index creation failed\n %s\n", indexErr.Error())
  }

  _, optimizeErr := db.Exec("INSERT INTO fts_search(fts_search) VALUES('optimize');")
  if optimizeErr != nil {
    Bail("optimize() failed\n %s\n", optimizeErr.Error())
  }

  _, analyzeErr := db.Exec("ANALYZE;")
  if analyzeErr != nil {
    Bail("ANALYZE failed\n %s\n", analyzeErr.Error())
  }

  _, vacuumErr := db.Exec("VACUUM;")
  if vacuumErr != nil {
    Bail("VACUUM failed\n %s\n", vacuumErr.Error())
  }

  fmt.Printf("Compressing database file\n")

  gzTmpFile, gzTmpErr := ioutil.TempFile(outDir, "db-*.gz")
  if gzTmpErr != nil {
    Bail("Failed to create compressed output file\n %s\n", gzTmpErr.Error())
  }

  defer os.Remove(gzTmpFile.Name())

  gzipBufWriter := bufio.NewWriter(gzTmpFile)
  gzipWriter, _ := gzip.NewWriterLevel(gzipBufWriter, gzip.BestCompression)
  gzipWriter.Name = "epg.sqlite"
  gzipWriter.Comment = "eltex epg v1"

  buf := make([]byte, 128 * 1024)

  for {
    n, readErr := tmpFile.Read(buf)
    if readErr != nil && readErr != io.EOF {
      Bail("%s\n", readErr)
    }
    if n == 0 {
      break
    }
    if _, writeErr := gzipWriter.Write(buf[:n]); writeErr != nil {
      Bail("%s\n", writeErr)
    }
  }
  gzipWriter.Flush()
  gzipWriter.Close()
  gzipBufWriter.Flush()
  gzTmpFile.Close()

  renameErr := os.Rename(gzTmpFile.Name(), *dbPath)
  if (renameErr != nil) {
    Bail("Failed to move temporary file to output\n %s\n", renameErr.Error())
  }
}

func addElement(decoder *xml.Decoder, programme *Programm, xmlElement *xml.StartElement) bool {
  decErr := decoder.DecodeElement(programme, xmlElement)
  if (decErr != nil) {
    Bail("Could not decode element\n %s\n", decErr.Error())
  }

  startTime, timeErr := time.ParseInLocation("20060102150405 -0700", programme.Start, localLocation)
  if (timeErr != nil) {
    Bail("Failed to parse start time\n %s\n", timeErr.Error())
  }

  startTime = time.Unix(startTime.Unix(), 0).In(localLocation)

  if (startTime.Before(startFrom)) {
    if (dbLastDate == nil || startTime.After(*dbLastDate)) {
      dbLastDate = &startTime
    }
    return false;
  }

  if (startFrom.Add(spanDuration).Before(startTime)) {
    if (dbEarliestDate == nil || startTime.Before(*dbEarliestDate)) {
      dbEarliestDate = &startTime
    }
    return false
  }

  progTitle := programme.Title
  if (progTitle != "" && !strings.HasSuffix(progTitle, "(18+)")) {
    // a lot of slots has extraneous suffixes like '(6+)'
    // we don't care about those, except for the adult-rated stuff, so let's remove them
    pureText := ageRegexp.FindStringSubmatch(progTitle)
    if (len(pureText) > 1) {
      progTitle = strings.TrimSpace(pureText[1])
    }
  }

  result, metaErr := sql1.Exec(startTime.Unix(), programme.Channel, progTitle, programme.Description)
  if (metaErr != nil) {
    Bail("Meta INSERT failed\n %s\n", metaErr.Error())
  }

  insertId, _ := result.LastInsertId()

  _, ftsErr := sql2.Exec(insertId, progTitle, programme.Description)
  if (ftsErr != nil) {
    Bail("FTS INSERT failed\n %s\n", ftsErr.Error())
  }

  return true
}
