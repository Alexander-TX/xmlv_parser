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
    "unicode/utf8"
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
 Icons                 []string           `xml:"icon"`
}

var db *sql.DB
var sql1, sql2 *sql.Stmt
var startFrom time.Time
var spanDuration time.Duration
var stringMap map[string]int
var textIdMax, snippetLength int

var dbEarliestDate *time.Time
var dbLastDate *time.Time

var localLocation *time.Location

var eltDateFormat string

var ageRegexp *regexp.Regexp

var trimmedTotal = 0
var snippetLengthMax = 0

var exitCode = 0

func Bail(format string, a ...interface{}) {
  fmt.Fprintf(os.Stderr, format, a...)
  runtime.Goexit()
}

func main() {
  defer os.Exit(exitCode)

  dbEarliestDate = nil
  dbLastDate = nil

  textIdMax = 1

  stringMap = make(map[string]int)

  eltDateFormat = "02-01-2006 15:04"

  defDuration, _ := time.ParseDuration("72h")

  dbPath := flag.String("output", "output.eltex.epg", "database file.")
  xmlPath := flag.String("input", "", "XMLTV file. (default read from standard input)")
  timeStart := flag.String("offset", "", "start import from specified date. Example: 29-12-2009 16:40. (default today)")
  argDuration := flag.Duration("timespan", defDuration, "duration since start date. Example: 72h.")
  flag.IntVar(&snippetLength, "snippet", -1, "description length limit. If negative, descriptions aren't clipped.")
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

  defer os.Remove(tmpFile.Name())

  db.Exec("CREATE VIRTUAL TABLE fts_search USING fts4(matchinfo='fts3', text, tokenize=unicode61);")
  db.Exec("CREATE TABLE search_meta (_id INTEGER PRIMARY KEY, ch_id, start_time INTEGER, title_id INTEGER NOT NULL, description_id INTEGER NOT NULL);")

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

  sql1, _ = bulkTx.Prepare("INSERT INTO search_meta (start_time, ch_id, title_id, description_id) VALUES (?, ?, ?, ?);")
  sql2, _ = bulkTx.Prepare("INSERT INTO fts_search (docid, text) VALUES (?, ?);")

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

  fmt.Printf("Inserted %d entries, %d unique names\n", appendedElements, textIdMax)

  if (snippetLength >= 0) {
     fmt.Printf("Trimmed %d characters. Max length before trimming: %d\n", trimmedTotal, snippetLengthMax)
  }

  _, timeIdxErr := db.Exec("CREATE INDEX time_idx ON search_meta (start_time);")
  if timeIdxErr != nil {
    Bail("index creation failed\n %s\n", timeIdxErr.Error())
  }

  _, indexErr := db.Exec("CREATE INDEX ch_idx ON search_meta (ch_id, start_time);")
  if indexErr != nil {
    Bail("index creation failed\n %s\n", indexErr.Error())
  }

  _, indexErr2 := db.Exec("CREATE INDEX description_idx ON search_meta (description_id);")
  if indexErr2 != nil {
    Bail("index creation failed\n %s\n", indexErr2.Error())
  }

  _, indexErr3 := db.Exec("CREATE INDEX title_idx ON search_meta (title_id);")
  if indexErr3 != nil {
    Bail("index creation failed\n %s\n", indexErr3.Error())
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

  trimmed := 0

  progDescription := programme.Description
  if (snippetLength >= 0) {
    descrSymbols := []rune(progDescription)

    if snippetLength < len(descrSymbols) {
      trimmed = len(descrSymbols) - snippetLength

      progDescription = string(descrSymbols[:snippetLength])
    }
  }

  titleId := stringMap[progTitle]
  if titleId == 0 {
    titleId = textIdMax
    textIdMax += 1

    stringMap[progTitle] = titleId

    _, ftsTitleErr := sql2.Exec(titleId, progTitle)
    if (ftsTitleErr != nil) {
      Bail("FTS INSERT failed\n %s\n", ftsTitleErr.Error())
    }
  }

  descrId := stringMap[progDescription]
  if descrId == 0 {
    runeLength := utf8.RuneCountInString(programme.Description)

    if runeLength > snippetLengthMax {
      snippetLengthMax = runeLength
    }

    descrId = textIdMax
    textIdMax += 1

    stringMap[progDescription] = descrId

    _, ftsErr := sql2.Exec(descrId, progDescription)
    if (ftsErr != nil) {
      Bail("FTS INSERT failed\n %s\n", ftsErr.Error())
    }

    trimmedTotal += trimmed
  }

  _, metaErr := sql1.Exec(startTime.Unix(), programme.Channel, titleId, descrId)
  if (metaErr != nil) {
    Bail("Meta INSERT failed\n %s\n", metaErr.Error())
  }

  return true
}
