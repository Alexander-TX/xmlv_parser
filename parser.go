package main

import (
    "os"
    "io"
    "fmt"
    "sort"
    "flag"
    "time"
    "bufio"
    "bytes"
    "errors"
    "regexp"
    "strconv"
    "strings"
    "runtime"
    "net/url"
    "net/http"
    "io/ioutil"
    "unicode/utf8"
    "database/sql"
    "encoding/xml"
    "path/filepath"
    "compress/gzip"
    "text/template"
    "mime/multipart"
    "golang.org/x/net/html/charset"
)

import _ "github.com/Alexander-TX/go-sqlite3"

type Channel struct {
 Name                  string             `xml:"display-name"`
 Id                    string             `xml:"id,attr"`
 Icon                  ImageUri           `xml:"icon"`
}

type Programm struct {
 Start                 string             `xml:"start,attr"`
 End                   string             `xml:"stop,attr"`
 Channel               string             `xml:"channel,attr"`
 Title                 string             `xml:"title"`
 Description           string             `xml:"desc"`
 SubTitle              string             `xml:"sub-title"`
 Images                []ImageUri         `xml:"icon"`
 Categories            []string           `xml:"category"`
 Year                  string             `xml:"year"`
}

type ImageUri struct {
  Uri                  string             `xml:"src,attr"`
}

type ChannelMeta struct {
  Id                   string
  ArchiveHours         int
  ImageUrlOverride     string
  ChannelPage          string
}

type TagMeta struct {
  NumberOfUses         int64
  IdVal                int64
}

type EndMeta struct {
  StartTime            int64
  EndTime              string
}

type Track struct {
  PsFile               string             `xml:"psfile"`
  ArchiveLimit         int                `xml:"archive_limit"`
  ChannelPage          string             `xml:"subscribe"`
  Title                string             `xml:"title"`
  Image                string             `xml:"image"`
}

type RequestContext struct {
  sql1, sql2, sql3, sql4, sql5, sql6, sql7 *sql.Stmt
  db *sql.DB
  stringMap map[string]int64
  uriMap map[string]int64
  tagMap map[string]*TagMeta
  endMap map[string]*EndMeta
  uriIdMax, textIdMax int64
}

var EltexPackageVersion = "unknown"
var EltexBuilder = "unknown builder"
var EltexBuildTime = "an unknown date"

var startFrom time.Time
var spanDuration time.Duration

var idMap map[string]ChannelMeta

var snippetLength int

var dbEarliestDate *time.Time
var dbLastDate *time.Time

var localLocation *time.Location
var xmltvTzOverride *time.Location

var imageBaseUrl *url.URL
var eltDateFormat string
var useLegacyFormat bool
var startServer bool
var fakeEnd *string
var titleTemplate *string
var compiledTemplate *template.Template

var ageRegexp *regexp.Regexp
var timeRegexp1 *regexp.Regexp
var yearRegexp1 *regexp.Regexp

var excludeYear bool
var excludeTags bool

var mappedTotal = 0
var trimmedTotal = 0
var snippetLengthMax = 0

var channelBlacklist, channelWhitelist map[string]struct{}

var archivedChannels = 0

var exitCode = 0

func Bail(format string, a ...interface{}) {
  exitCode = 1
  fmt.Fprintf(os.Stderr, "%s\n", "Fatal error!!")
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

  eltDateFormat = "02-01-2006 15:04"

  defDuration, _ := time.ParseDuration("72h")

  dbPath := flag.String("output", "schedule.epgx.gz", "database file.")
  xmlPath := flag.String("input", "", "XMLTV file. (default read from standard input)")
  timeStart := flag.String("offset", "01-01-1970 00:00", "start import from specified date. Example: 29-12-2009 16:40.")
  argDuration := flag.Duration("timespan", defDuration, "duration since start date. Example: 72h.")
  flag.IntVar(&snippetLength, "snippet", -1, "description length limit. If negative, descriptions aren't clipped.")
  nameMapFile := flag.String("xmap", "", "Optional: file with pipe-separated ID mappings. (default none)")
  xspfFile := flag.String("xspf", "", "Optional: playlist with proprietary Eltex extensions (<psfile> and <archive_limit> tags). (default none)")
  xmltvTz := flag.String("tz", "", "Optional: replace timezone in XMLTV file. Example: 'Asia/Novosibirsk'. (default none)")
  flag.BoolVar(&useLegacyFormat, "legacy", true, "Deprecated: this option does nothing")
  includeCh := flag.String("include", "", "Optional: comma-separated list of channels to include in generated EPG.")
  excludeCh := flag.String("exclude", "", "Optional: comma-separated list of channels to exclude from generated EPG.")
  flag.BoolVar(&startServer, "start-server", false, "Start web server, listening on :9448")
  fakeEnd = flag.String("add-last-entry", "Конец передачи", "text of fake entry, denoting end of program. Empty string to disable")
  titleTemplate := flag.String("title-template", "{{.Title}}", "Supported variables: .Title, .SubTitle, .Description")
  imageBase := flag.String("rewrite-url", "", "Optional: replace base URL of EPG images with specified")
  showVersion := flag.Bool("version", false, "Write version information to standard output")
  omitYear := flag.Bool("exclude-year", false, "Exclude optional year data from generated EPG")
  omitTags := flag.Bool("exclude-tags", false, "Exclude optional tags data from generated EPG")
  flag.Parse()

  if flag.NArg() != 0 {
    fmt.Printf("Unrecognized argument: %s. This application does not take such arguments\n\n", flag.Arg(0))
    fmt.Printf("Hint: boolean flags and their values must be separated by '=' sign. Example: --boolean-flag=true\n\n")
    fmt.Printf("Usage:\n")
    flag.PrintDefaults()
    os.Exit(2)
  }

  seen := make(map[string]bool)

  flag.Visit(func(f *flag.Flag) { seen[f.Name] = true })

  if !seen["exclude-year"] {
    excludeYear = false
  } else {
    excludeYear = *omitYear
  }

  if !seen["exclude-tags"] {
    excludeTags = false
  } else {
    excludeTags = *omitTags
  }

  //fmt.Printf(" exclude year = %t\n exclude tags = %t\n", excludeYear, excludeTags)

  if *showVersion {
    fmt.Printf("%s\n", EltexPackageVersion)
    os.Exit(0)
  }

  fmt.Printf("Version %s built by %s on %s\n\n", EltexPackageVersion, EltexBuilder, EltexBuildTime)

  spanDuration = *argDuration

  timeNow := time.Now()
  timeZone, _ := timeNow.Zone()
  localLocation = timeNow.Location()

  fmt.Printf("Local time zone: %s (%s)\n", timeZone, localLocation)

  if *xmltvTz == "" {
    fmt.Printf("XMLTV time zone: take from XMLTV file\n")
  } else {
    var tzErr error
    xmltvTzOverride, tzErr = time.LoadLocation(*xmltvTz)
    if tzErr != nil {
      Bail("Failed to load timezone '%s'\n %s\n", *xmltvTz, tzErr.Error())
    }

    fmt.Printf("XMLTV time zone: overriden with %s\n", *xmltvTz)
  }

  if (spanDuration.Nanoseconds() == 0) {
    Bail("Duration must be positive\n")
  }

  if (*timeStart == "") {
    startFrom = time.Now()
  } else {
    var startFromErr error

    startFrom, startFromErr = time.ParseInLocation(eltDateFormat, *timeStart, localLocation)
    if (startFromErr != nil) {
      Bail("Failed to parse start time:\n %s\n", startFromErr.Error())
    }
  }

  if titleTemplate != nil {
    compiledTemplate = template.New("title")
    compiledTemplate.Option("missingkey=error")

    var templateErr error

    compiledTemplate, templateErr = compiledTemplate.Parse(*titleTemplate)
    if templateErr != nil {
      Bail("Failed to parse title template:\n %s\n", templateErr.Error())
    }
  }

  ageRegexp = regexp.MustCompile("(.+)\\([0-9]{1,2}\\+\\)$")
  timeRegexp1 = regexp.MustCompile("([0-9]{14})( (?:.+))?$")
  yearRegexp1 = regexp.MustCompile("([0-9]{4})$")

  if startServer {
    bootstrapServer()
    return
  }

  platform_init()

  if *imageBase != "" {
    var urlErr error

    imageBaseUrl, urlErr = url.Parse(*imageBase)

    if urlErr != nil {
      Bail("Invalid base url specified:\n %s\n", urlErr.Error())
    }
  }

  if !seen["offset"] {
    fmt.Fprintf(os.Stderr, "Warning: missing --offset argument, EPG start defaults to 1 January 1970\n")
  }
  if !seen["timespan"] {
    fmt.Fprintf(os.Stderr, "Warning: missing --timespan argument, EPG length defaults to 74 hours\n")
  }

  if seen["include"] {
    if len(*includeCh) == 0 {
      Bail("Bad --include argument: must contain at least one channel ID\n")
    }

    if strings.HasSuffix(*includeCh, ",") {
      Bail("Bad --include argument: must be a list of channels IDs without spaces\n")
    }

    channelWhitelist = make(map[string]struct{})

    channelsToInclude := strings.Split(*includeCh, ",")

    for _, chIdToInclude := range channelsToInclude {
      channelWhitelist[chIdToInclude] = struct{}{}
    }
  }

  if seen["exclude"] {
    if len(*excludeCh) == 0 {
      Bail("Bad --exclude argument: must contain at least one channel ID\n")
    }

    if strings.HasSuffix(*excludeCh, ",") {
      Bail("Bad --exclude argument: must be a list of channels IDs without spaces\n")
    }

    channelBlacklist = make(map[string]struct{})

    channelsToExclude := strings.Split(*excludeCh, ",")

    for _, chIdToExclude := range channelsToExclude {
      channelBlacklist[chIdToExclude] = struct{}{}
    }
  }

  if (*nameMapFile != "") {
    idMap = make(map[string]ChannelMeta)

    nameMap, idMapErr := os.Open(*nameMapFile)
    if idMapErr != nil {
      Bail("Failed to open name map file:\n %s\n", idMapErr.Error())
    }

    mapReader := bufio.NewReader(nameMap)

    lineNum := 0
    for {
      mapRule, lineErr := mapReader.ReadString('\n')

      if mapRule != "" {
        mapRule = strings.TrimSpace(mapRule)

        lineNum += 1

        sepIdx := strings.Split(mapRule, "|")

        if len(sepIdx) < 2 {
          Bail("Failed to parse map file. Bad format at line %d: the line does not contain pipe ('|')\n%s\n", lineNum, mapRule)
        }

        if len(sepIdx[0]) == 0 || len(sepIdx[1]) == 0 {
          Bail("Failed to parse map file. Bad format at line %d: second ID is missing (line starts or ends with '|'):\n%s\n", lineNum, mapRule)
        }

        mapNam := sepIdx[0]
        mapId := sepIdx[1]
        hours := 0
        chImage := ""
        chPage := ""

        if len(sepIdx) > 2 {
          hours, _ = strconv.Atoi(sepIdx[2])
        }

        if len(sepIdx) > 3 {
          chImage = sepIdx[3]
        }

        if len(sepIdx) > 4 {
          chPage = sepIdx[4]
        }

        idMap[mapId] = ChannelMeta{
          Id: mapNam,
          ArchiveHours: hours,
          ImageUrlOverride: chImage,
          ChannelPage: chPage,
        }
      }

      if lineErr != nil {
        if lineErr == io.EOF {
          break;
        }

        Bail("Received IO error during reading map file:\n %s\n", lineErr.Error())
      }
    }

    nameMap.Close()

    fmt.Printf("Parsed %d mappings\n", lineNum)
  }

  outDir := filepath.Dir(*dbPath)

  tmpFile, tmpErr := ioutil.TempFile(outDir, "db-*.sqlite")
  if tmpErr != nil {
    Bail("Cannot create temporary file\n %s\n", tmpErr.Error())
  }

  defer os.Remove(tmpFile.Name())

  dbUrl := fmt.Sprintf("file:%s", tmpFile.Name())

  db, dbErr := sql.Open("sqlite3", dbUrl)
  if dbErr != nil {
    Bail("sqlite error\n %s\n", dbErr.Error())
  }

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

  gzTmpFile, gzTmpErr := ioutil.TempFile(outDir, "db-*.gz")
  if gzTmpErr != nil {
    Bail("Failed to create compressed output file\n %s\n", gzTmpErr.Error())
  }

  defer os.Remove(gzTmpFile.Name())

  gzipBufWriter := bufio.NewWriter(gzTmpFile)
  gzipWriter, _ := gzip.NewWriterLevel(gzipBufWriter, gzip.BestCompression)
  gzipWriter.Name = "epg.sqlite"
  gzipWriter.Comment = "eltex epg v2"

  ctx := RequestContext{}

  ctx.db = db

  ctx.stringMap = make(map[string]int64)
  ctx.uriMap = make(map[string]int64)
  ctx.tagMap = make(map[string]*TagMeta)
  ctx.endMap = make(map[string]*EndMeta)

  ctx.textIdMax = 1
  ctx.uriIdMax = 1

  reqErr := processXml(&ctx, "main", xmlFile, tmpFile, gzipWriter)
  if reqErr != nil {
    Bail("%s\n", reqErr.Error())
  }

  if (*xspfFile != "") {
    processXspf(&ctx, *xspfFile)
  }

  optimizeDatabase(&ctx, "main")

  fmt.Printf("Compressing database file\n")

  if _, copyErr := io.Copy(gzipWriter, tmpFile); copyErr != nil {
    Bail("%s\n", copyErr.Error())
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

func processXspf(ctx *RequestContext, xspfFilename string) {
  nameMap, idMapErr := os.Open(xspfFilename)
  if idMapErr != nil {
    Bail("Failed to open XSPF file:\n %s\n", idMapErr.Error())
  }

  lineNum := 0
  tracksTotal := 0

  decoder := xml.NewDecoder(nameMap)
  decoder.CharsetReader = charset.NewReaderLabel

  // skip root
root:
  for {
    token, xmlErr := decoder.Token()
    if xmlErr != nil {
      Bail("XSPF file is malformed (failed to find root tag)\n")
    }

    switch xmlRoot := token.(type) {
      default:
        continue;
      case xml.StartElement:
        if (xmlRoot.Name.Local == "playlist") {
          break root;
        } else {
          Bail("malformed XSPF: <playlist> tag not found, got <%s> instead\n", xmlRoot.Name.Local)
        }
    }
  }

  fmt.Printf("Parsing XSPF playlist file\n")

  bulkTx, txErr := ctx.db.Begin()
  if txErr != nil {
    Bail("Could not start transaction\n %s\n", txErr.Error())
  }

  var track *Track

  // iterate over all <track> tags and add them to database
  for {
    t, tokenErr := decoder.Token()
    if tokenErr != nil {
      if tokenErr == io.EOF {
        break
      } else {
        Bail("Failed to read token\n %s\n", tokenErr.Error())
      }
    }

    qSql, _ := bulkTx.Prepare("SELECT ch_id FROM channels WHERE name = ?;")

    updateSql, err := bulkTx.Prepare("UPDATE channels SET archive_time = ?, ch_page = ?, image_uri = ?, ch_id = ? WHERE ch_id = ?;")
    if err != nil {
      Bail("Failed to compile UPDATE\n %s\n", err.Error())
    }

    updateSql2, err := bulkTx.Prepare("UPDATE search_meta SET ch_id = ? WHERE ch_id = ?;")
    if err != nil {
      Bail("Failed to compile UPDATE\n %s\n", err.Error())
    }

    switch startElement := t.(type) {
      default:
        continue;
      case xml.StartElement:
        //fmt.Printf("Another element: %s\n", startElement.Name.Local)

        if (startElement.Name.Local == "track") {
          // create a new value each time, or else it will be botched
          // (old values will be kept for unset JSON fields)
          track = &Track{}

          added, err := addTrack(ctx, decoder, track, &startElement, qSql, updateSql, updateSql2)
          if err != nil {
            Bail("Failed to process <track>\n %s\n", err.Error())
          }

          tracksTotal += 1

          if added {
            lineNum += 1;
          }
        } else if strings.ToLower(startElement.Name.Local) == "tracklist" {
          continue;
        } else {
          decoder.Skip()
          continue;
        }
        break;
    }
  }

  bulkTxError := bulkTx.Commit()
  if bulkTxError != nil {
    Bail("Failed to commit XSPF update transaction\n %s\n", bulkTxError.Error())
  }

  nameMap.Close()

  if lineNum == 0 {
    Bail("XSPF file has no valid Eltex tags! It is useless!")
  }

  fmt.Printf("%d out of %d XSPF tracks had useful metadata \n", lineNum, tracksTotal)
}

func addTrack(ctx *RequestContext, decoder *xml.Decoder, track *Track, xmlElement *xml.StartElement, q *sql.Stmt, updSql *sql.Stmt, updSql2 *sql.Stmt) (bool, error) {
  decErr := decoder.DecodeElement(track, xmlElement)
  if (decErr != nil) {
    return false, errors.New(s("Could not decode element\n %s\n", decErr.Error()))
  }

  if track.PsFile == "" {
    //fmt.Fprintf(os.Stderr, "No <psfile>\n")
    return false, nil
  }

  if track.ArchiveLimit == 0 && track.ChannelPage == "" && track.Image == "" {
    //fmt.Fprintf(os.Stderr, "Track has no useful info: %s\n", preprocess(track.Title))
    return false, nil
  }

  var chPageUri, chImgUri sql.NullString

  if track.ChannelPage != "" {
    chPageUri = sql.NullString{
      String: track.ChannelPage,
      Valid: true,
    }
  }

  if track.Image != "" {
    chImgUri = sql.NullString{
      String: track.Image,
      Valid: true,
    }
  }

  processedTitle := preprocess(track.Title)

  var foundChId string

  chIdQuery := q.QueryRow(processedTitle)

  scanErr := chIdQuery.Scan(&foundChId)
  if scanErr == sql.ErrNoRows {
    return false, nil
  } else if scanErr != nil {
    return false, scanErr
  }

  _, updateErr := updSql.Exec(track.ArchiveLimit, chPageUri, chImgUri, track.PsFile, foundChId)
  if updateErr != nil {
    return false, updateErr
  }

  _, updateErr2 := updSql2.Exec(track.PsFile, foundChId)
  if updateErr2 != nil {
    return false, updateErr2
  }

  return true, nil
}

func bootstrapServer() {
  fmt.Print("Starting web server on :9448\n");

  var err error

  http.HandleFunc("/", HomeRouterHandler)
  err = http.ListenAndServe("127.0.0.1:9448", nil)
  if err != nil {
    Bail("ListenAndServe: %s\n", err.Error())
  }
}

func s(format string, a ...interface{}) string {
  return fmt.Sprintf(format, a...)
}

func addOptionalCols() string {
  var b strings.Builder

  if (!excludeTags) {
    b.WriteString("tags INTEGER, ")
  }

  if (!excludeYear) {
    b.WriteString("year INTEGER, ")
  }

  return b.String()
}

func processXml(ctx *RequestContext, dbNam string, xmlFile io.Reader, dbFile io.Reader, destWriter io.Writer) error {
  db := ctx.db

  // do not create on-disk temporary files (we don't want to clean them up)
  db.Exec(s("PRAGMA %s.journal_mode = MEMORY;", dbNam))
  db.Exec(s("PRAGMA %s.temp_store = MEMORY;", dbNam))

  db.Exec(s("PRAGMA %s.application_id = 0x656c7478;", dbNam)) // hint: see hex

  var err error

  _, err = db.Exec(s("CREATE TABLE %s.text (docid INTEGER PRIMARY KEY, text TEXT);", dbNam))
  if err != nil {
    return errors.New(s("CREATE TABLE failed\n %s\n", err.Error()))
  }
  _, err = db.Exec(s("CREATE TABLE %s.channels (_id INTEGER PRIMARY KEY, image_uri TEXT, ch_id NOT NULL UNIQUE, name TEXT, archive_time INTEGER NOT NULL, ch_page TEXT);", dbNam))
  if err != nil {
    return errors.New(s("CREATE TABLE failed\n %s\n", err.Error()))
  }

  if useLegacyFormat {
    //fmt.Fprintf(os.Stderr, "Using legacy format: tokenize=porter\n")

    _, err = db.Exec(s("CREATE VIRTUAL TABLE %s.fts_search USING fts4(content='', matchinfo='fts3', prefix='3', text, tokenize=porter);", dbNam))
  } else {
    _, err = db.Exec(s("CREATE VIRTUAL TABLE %s.fts_search USING fts4(content='text', matchinfo='fts3', prefix='3', text, tokenize=unicode61);", dbNam))
  }

  if err != nil {
    return errors.New(s("CREATE TABLE failed\n %s\n", err.Error()))
  }
  _, err = db.Exec(s("CREATE TABLE %s.search_meta_0 (_id INTEGER PRIMARY KEY, ch_id NOT NULL, start_time INTEGER NOT NULL, title_id INTEGER NOT NULL, description_id INTEGER NOT NULL, tags INTEGER NOT NULL, year INTEGER, image_uri INTEGER);", dbNam))
  if err != nil {
    return errors.New(s("CREATE TABLE failed\n %s\n", err.Error()))
  }
  _, uniqIdxErr := db.Exec(s("CREATE UNIQUE INDEX %s.unique_start_time ON search_meta_0 (ch_id, start_time);", dbNam))
  if uniqIdxErr != nil {
    return errors.New(s("index creation failed\n %s\n", uniqIdxErr.Error()))
  }
  _, err = db.Exec(s("CREATE TABLE %s.uri (_id INTEGER PRIMARY KEY, uri TEXT)", dbNam))
  if err != nil {
    return errors.New(s("CREATE TABLE failed\n %s\n", err.Error()))
  }
  _, err = db.Exec(s("CREATE TABLE %s.tags (_id INTEGER PRIMARY KEY, tag TEXT NOT NULL)", dbNam))
  if err != nil {
    return errors.New(s("CREATE TABLE failed\n %s\n", err.Error()))
  }
  _, err = db.Exec(s("CREATE TABLE %s.eltex_temp_search_tags (_id INTEGER PRIMARY KEY, tag_list TEXT)", dbNam))
  if err != nil {
    return errors.New(s("CREATE TABLE failed\n %s\n", err.Error()))
  }

  bulkTx, txErr := db.Begin()
  if txErr != nil {
    return errors.New(s("Could not start transaction\n %s\n", txErr.Error()))
  }

  ctx.sql1, err = bulkTx.Prepare("INSERT INTO search_meta_0 (start_time, ch_id, image_uri, title_id, description_id, year, tags) VALUES (?, ?, ?, ?, ?, ?, ?);")
  if err != nil {
    return errors.New(s("Prepare() failed: %s\n", err.Error()))
  }
  ctx.sql2, err = bulkTx.Prepare("INSERT INTO fts_search (docid, text) VALUES (?, ?);")
  if err != nil {
    return errors.New(s("Prepare() failed: %s\n", err.Error()))
  }
  ctx.sql3, err = bulkTx.Prepare("INSERT INTO uri (_id, uri) VALUES (?, ?);")
  if err != nil {
    return errors.New(s("Prepare() failed: %s\n", err.Error()))
  }
  ctx.sql4, err = bulkTx.Prepare("INSERT INTO text (docid, text) VALUES (?, ?);")
  if err != nil {
    return errors.New(s("Prepare() failed: %s\n", err.Error()))
  }
  ctx.sql5, err = bulkTx.Prepare("INSERT INTO channels (ch_id, image_uri, name, archive_time, ch_page) VALUES (?, ?, ?, ?, ?);")
  if err != nil {
    return errors.New(s("Prepare() failed: %s\n", err.Error()))
  }
  ctx.sql6, err = bulkTx.Prepare("INSERT INTO tags (_id, tag) VALUES (?, ?);")
  if err != nil {
    return errors.New(s("Prepare() failed: %s\n", err.Error()))
  }
  ctx.sql7, err = bulkTx.Prepare("INSERT INTO eltex_temp_search_tags (_id, tag_list) VALUES (?, ?);")
  if err != nil {
    return errors.New(s("Prepare() failed: %s\n", err.Error()))
  }

  decoder := xml.NewDecoder(xmlFile)
  decoder.CharsetReader = charset.NewReaderLabel

  // skip root
root:
  for {
    token, xmlErr := decoder.Token()
    if xmlErr != nil {
      return errors.New("XMLTV file is malformed (failed to find root tag)\n")
    }

    switch xmlRoot := token.(type) {
      default:
        continue;
      case xml.StartElement:
        if (xmlRoot.Name.Local == "tv") {
          break root;
        } else {
          return errors.New(s("malformed XMLTV: <tv> tag not found, got <%s> instead\n", xmlRoot.Name.Local))
        }
    }
  }

  fmt.Printf("Copying XMLTV schedule to database\n")

  tagMap := ctx.tagMap

  var appendedElements = 0
  var appendedChannels = 0

  var programme *Programm
  var channel *Channel

  // iterate over all <programme> tags and add them to database
  for {
    t, tokenErr := decoder.Token()
    if tokenErr != nil {
      if tokenErr == io.EOF {
        break
      } else {
        return errors.New(s("Failed to read token\n %s\n", tokenErr.Error()))
      }
    }

    switch startElement := t.(type) {
      default:
        continue;
      case xml.StartElement:
        //fmt.Printf("Another element: %s", startElement.Name.Local)

        if (startElement.Name.Local == "channel") {
          // create a new value each time, or else it will be botched
          // (old values will be kept for unset JSON fields)
          channel = &Channel{}

          added, err := addChannel(ctx, decoder, channel, &startElement)
          if err != nil {
            return err
          }

          if added {
            appendedChannels += 1;
          }
        } else if (startElement.Name.Local == "programme") {
          programme = &Programm{}

          added, err := addElement(ctx, decoder, programme, &startElement)
          if err != nil {
            return err
          }

          if added {
            appendedElements += 1;
          }
        } else {
          decoder.Skip()
          continue;
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

    return errors.New(s("%s\n", emptyErrStr))
  }

  if *fakeEnd != "" {
    emptyStrId := ctx.stringMap[*fakeEnd]

    if emptyStrId == 0 {
      r, _ := bulkTx.Exec("INSERT INTO text (text) VALUES (?);", *fakeEnd)
      emptyStrId, _ = r.LastInsertId()
    }

    fakeInsert, _ := bulkTx.Prepare(fmt.Sprintf("INSERT INTO search_meta_0 (start_time, ch_id, title_id, description_id, tags) VALUES (?, ?, %d, %d, 0);", emptyStrId, emptyStrId))

    for chI, chEnd := range ctx.endMap {
      endDate, endDateErr := parseXmltvDate(chEnd.EndTime)
      if endDateErr != nil {
        continue
      }

      fakeInsert.Exec(endDate.Unix(), chI)
    }
  }

  tagList := make([]string, 0, len(tagMap))

  for tag, _ := range tagMap {
    tagList = append(tagList, tag)
  }

  sort.Slice(tagList, func(i, j int) bool {
    left := tagMap[tagList[i]].NumberOfUses
    right := tagMap[tagList[j]].NumberOfUses

    if left > right {
      return true
    } else if left < right {
      return false
    }

    return tagList[i] < tagList[j]
  })

  for pos, tag := range tagList {
    // sqlite supports only signed values, so we are limited to 63 bits
    if pos > 62 {
      break
    }

    idVal := 1 << pos

    //fmt.Printf("Adding new tag '%s' (value is %d, number of uses is %d)\n", tag, idVal, tagMap[tag].NumberOfUses)

    _, tInsertErr := ctx.sql6.Exec(idVal, tag)
    if tInsertErr != nil {
      return errors.New(s("Failed to insert into tags table: %s\n", tInsertErr.Error()))
    }

    tagMap[tag].IdVal = int64(idVal)
  }

  bulkTxError := bulkTx.Commit()
  if bulkTxError != nil {
    return errors.New(s("Failed to commit primary transaction\n %s\n", bulkTxError.Error()))
  }

  fmt.Printf("Inserted %d channels (%d archived), %d programm entries, %d unique strings\n", appendedChannels, archivedChannels, appendedElements, ctx.textIdMax)

  if (len(tagMap) > 63) {
    fmt.Printf("Original XMLTV file has %d tags, the most popular 63 will be added to EPGX\n", len(tagMap))
  }

  if mappedTotal == 0 && len(idMap) != 0 {
    fmt.Printf("WARNING: none of %d mappings were used!\n", len(idMap))
  }

  if archivedChannels == 0 {
    fmt.Printf("WARNING: none of channels have archive!\n")
  }

  if (snippetLength >= 0) {
     fmt.Printf("Trimmed %d characters. Max length before trimming: %d\n", trimmedTotal, snippetLengthMax)
  }

  rows, queryErr := db.Query("SELECT _id, tag_list FROM eltex_temp_search_tags")
  if queryErr != nil {
    Bail("Failed to request EPG rows from database\n %s\n", queryErr.Error())
  }

  dbIds := make([]int64, 0)
  dbCats := make([]int64, 0)

  for rows.Next() {
    var rowId int64
    var rowCats string

    scanErr := rows.Scan(&rowId, &rowCats)
    if scanErr != nil {
      Bail("SQLite error\n %s\n", scanErr.Error())
    }

    dbIds = append(dbIds, rowId)

    var catVal int64

    catVal = 0

    for _, catName := range strings.Split(rowCats, ",") {
      if catName == "" {
        continue
      }

      catIdx := tagMap[catName].IdVal

      if catIdx == 0 {
        continue
      }

      catVal |= catIdx
    }

    dbCats = append(dbCats, catVal)
  }

  caTx, caTxErr := db.Begin()
  if caTxErr != nil {
    return errors.New(s("Could not start transaction\n %s\n", caTxErr.Error()))
  }

  updateSql, prepErr := caTx.Prepare("UPDATE search_meta_0 SET tags = ? WHERE _id = ?;")
  if prepErr != nil {
    return errors.New(s("Prepare() failed: %s\n", err.Error()))
  }

  for pos, itemId := range dbIds {
    itemCatVal := dbCats[pos]

    updateSql.Exec(itemCatVal, itemId)
  }

  _, err = caTx.Exec("DROP TABLE eltex_temp_search_tags")
  if err != nil {
    Bail("Failed to delete aux table: %s\n", err.Error())
  }

  _, err = caTx.Exec("DROP INDEX unique_start_time")
  if err != nil {
    Bail("Failed to drop aux index: %s\n", err.Error())
  }

  _, err = caTx.Exec("CREATE TABLE search_meta (_id INTEGER PRIMARY KEY, ch_id NOT NULL, start_time INTEGER NOT NULL, title_id INTEGER NOT NULL, description_id INTEGER NOT NULL, " + addOptionalCols() + "image_uri INTEGER);")
  if err != nil {
    Bail("Failed to create dest table: %s\n", err.Error())
  }

  var copyBuilder strings.Builder

  copyBuilder.WriteString("INSERT INTO search_meta SELECT _id, ch_id, start_time, title_id, description_id")
  if (!excludeTags) {
    copyBuilder.WriteString(", tags")
  }
  if (!excludeYear) {
    copyBuilder.WriteString(", year")
  }
  copyBuilder.WriteString(", image_uri FROM search_meta_0")

  _, err = caTx.Exec(copyBuilder.String())
  if err != nil {
    Bail("Failed to copy search_meta: %s\n", err.Error())
  }

  _, err = caTx.Exec("DROP TABLE search_meta_0")
  if err != nil {
    Bail("Failed to drop scratch table: %s\n", err.Error())
  }

  if (excludeTags) {
    _, err = caTx.Exec("DROP TABLE tags")
    if err != nil {
      Bail("Failed to drop tags table: %s\n", err.Error())
    }
  }

  caTxCommitErr := caTx.Commit()
  if caTxCommitErr != nil {
    return errors.New(s("Failed to commit tags transaction\n %s\n", caTxCommitErr.Error()))
  }

  return nil
}

func optimizeDatabase(ctx *RequestContext, dbNam string) error {
  db := ctx.db

  _, timeIdxErr := db.Exec(s("CREATE INDEX %s.time_idx ON search_meta (start_time);", dbNam))
  if timeIdxErr != nil {
    return errors.New(s("index creation failed\n %s\n", timeIdxErr.Error()))
  }

  _, indexErr := db.Exec(s("CREATE INDEX %s.ch_idx ON search_meta (ch_id, start_time);", dbNam))
  if indexErr != nil {
    return errors.New(s("index creation failed\n %s\n", indexErr.Error()))
  }

  _, indexErr2 := db.Exec(s("CREATE INDEX %s.description_idx ON search_meta (description_id);", dbNam))
  if indexErr2 != nil {
    return errors.New(s("index creation failed\n %s\n", indexErr2.Error()))
  }

  _, indexErr3 := db.Exec(s("CREATE INDEX %s.title_idx ON search_meta (title_id);", dbNam))
  if indexErr3 != nil {
    return errors.New(s("index creation failed\n %s\n", indexErr3.Error()))
  }

  if !excludeTags {
    _, indexErr4 := db.Exec(s("CREATE INDEX %s.idx_tags ON search_meta(tags);", dbNam))
    if indexErr4 != nil {
      return errors.New(s("index creation failed\n %s\n", indexErr4.Error()))
    }
  }

  _, optimizeErr := db.Exec(s("INSERT INTO %s.fts_search(fts_search) VALUES('optimize');", dbNam))
  if optimizeErr != nil {
    return errors.New(s("optimize() failed\n %s\n", optimizeErr.Error()))
  }

  _, analyzeErr := db.Exec("ANALYZE;")
  if analyzeErr != nil {
    return errors.New(s("ANALYZE failed\n %s\n", analyzeErr.Error()))
  }

  _, vacuumErr := db.Exec("VACUUM;")
  if vacuumErr != nil {
    return errors.New(s("VACUUM failed\n %s\n", vacuumErr.Error()))
  }

  return nil
}

func parseXmltvDate(source string) (time.Time, error) {
  // XMLTV dates are "loosely based on ISO 8601", which is rather poorly supported by Go
  // so we have to do a bit of extra fiddling ourselves

  timeMatch := timeRegexp1.FindStringSubmatch(source)
  if timeMatch == nil {
    return time.Time{}, errors.New(s("Failed to parse date: %s\n", source))
  }

  if xmltvTzOverride != nil {
    return time.ParseInLocation("20060102150405", timeMatch[1], xmltvTzOverride)
  } else if len(timeMatch) > 2 {
    return time.ParseInLocation("20060102150405 -0700", source, localLocation)
  } else {
    return time.ParseInLocation("20060102150405", timeMatch[1], localLocation)
  }
}

func isReadyForFts(c rune) (bool) {
  // from https://www.sqlite.org/fts3.html
  //
  // A term is a contiguous sequence of eligible characters, where eligible characters are
  // all alphanumeric characters and all characters with Unicode codepoint values greater
  // than or equal to 128. All other characters are discarded

  if (c >= 'a' && c <= 'z') {
    return true;
  }

  if (c >= '0' && c <= '9') {
    return true;
  }

  return c >= 128;
}

func preprocess(name string) (string) {
  // replace punctuation and special characters with spaces
  // and trim all resulting excess space from string

  var builder strings.Builder

  lastIsSpace := false

  for _, c := range name {
    if (c >= 'A' && c <= 'Z') {
      c = c + ('a' - 'A');
    } else if (c >= 'А' && c <= 'Я') {
      // not necessary for FTS, just normalization for caller
      c = c + ('я' - 'Я');
    } else if (!isReadyForFts(c)) {
      c = ' ';
    }

    if c == ' ' && lastIsSpace {
      continue
    }

    lastIsSpace = c == ' '

    builder.WriteRune(c);
  }

  return strings.TrimSpace(builder.String())
}

func addChannel(ctx *RequestContext, decoder *xml.Decoder, channel *Channel, xmlElement *xml.StartElement) (bool, error) {
  decErr := decoder.DecodeElement(channel, xmlElement)
  if (decErr != nil) {
    return false, errors.New(s("Could not decode element\n %s\n", decErr.Error()))
  }

  var imageUri sql.NullString

  if channel.Icon.Uri != "" {
    imageUri = sql.NullString{
      String: channel.Icon.Uri,
      Valid: true,
    }
  }

  if (channel.Name == "" || channel.Id == "") {
    return false, nil
  }

  chId := channel.Id
  archived := 0

  var channelPage sql.NullString

  if mappedId, ok := idMap[chId]; ok {
    chId = mappedId.Id
    archived = mappedId.ArchiveHours

    if mappedId.ImageUrlOverride != "" {
      imageUri = sql.NullString{
        String: mappedId.ImageUrlOverride,
        Valid: true,
      }
    }

    if mappedId.ChannelPage != "" {
      channelPage = sql.NullString{
        String: mappedId.ChannelPage,
        Valid: true,
      }
    }
  }

  if len(channelWhitelist) != 0 {
    if _, ok := channelWhitelist[chId]; !ok {
      return false, nil
    }
  }

  if _, blacklisted := channelBlacklist[chId]; blacklisted {
    return false, nil
  }

  if archived > 0 {
    archivedChannels += 1

    // store archived time in seconds
    archived *= 3600;
  }

  //fmt.Printf("Inserting %s, %s %s %d\n", chId, imageUri.String, channel.Name, archived)

  _, chInsertErr := ctx.sql5.Exec(chId, imageUri, preprocess(channel.Name), archived, channelPage)
  if chInsertErr != nil {
    return false, errors.New(s("Failed to insert into channels table\n", chInsertErr.Error()))
  }

  return true, nil;
}

func addElement(ctx *RequestContext, decoder *xml.Decoder, programme *Programm, xmlElement *xml.StartElement) (bool, error) {
  decErr := decoder.DecodeElement(programme, xmlElement)
  if (decErr != nil) {
    return false, errors.New(s("Could not decode element\n %s\n", decErr.Error()))
  }

  chId := programme.Channel

  if mappedId, ok := idMap[chId]; ok {
    chId = mappedId.Id

    mappedTotal += 1
  }

  if len(channelWhitelist) != 0 {
    if _, ok := channelWhitelist[chId]; !ok {
      return false, nil
    }
  }

  if _, blacklisted := channelBlacklist[chId]; blacklisted {
    return false, nil
  }

  startTime, timeErr := parseXmltvDate(programme.Start)
  if (timeErr != nil) {
    return false, errors.New(s("Failed to parse start time\n %s\n", timeErr.Error()))
  }

  startTime = time.Unix(startTime.Unix(), 0).In(localLocation)

  if (startTime.Before(startFrom)) {
    if (dbLastDate == nil || startTime.After(*dbLastDate)) {
      dbLastDate = &startTime
    }
    return false, nil
  }

  if (startFrom.Add(spanDuration).Before(startTime)) {
    if (dbEarliestDate == nil || startTime.Before(*dbEarliestDate)) {
      dbEarliestDate = &startTime
    }
    return false, nil
  }

  lastEnd := ctx.endMap[chId]

  if lastEnd == nil || lastEnd.StartTime < startTime.Unix() {
    ctx.endMap[chId] = &EndMeta{
      StartTime: startTime.Unix(),
      EndTime: programme.End,
    }
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

  if compiledTemplate != nil {
    var buff bytes.Buffer

    tmplErr := compiledTemplate.Execute(&buff, programme)
    if tmplErr == nil {
      progTitle = buff.String()
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

  titleId := ctx.stringMap[progTitle]
  if titleId == 0 {
    titleId = ctx.textIdMax
    ctx.textIdMax += 1

    ctx.stringMap[progTitle] = titleId

    _, ftsTitleTextErr := ctx.sql4.Exec(titleId, progTitle)
    if (ftsTitleTextErr != nil) {
      return false, errors.New(s("text INSERT failed\n %s\n", ftsTitleTextErr.Error()))
    }

    if useLegacyFormat {
      progTitle = strings.ToLower(progTitle)
      progTitle = strings.ReplaceAll(progTitle, "ё", "е")
    }

    _, ftsTitleErr := ctx.sql2.Exec(titleId, progTitle)
    if (ftsTitleErr != nil) {
      return false, errors.New(s("FTS INSERT failed\n %s\n", ftsTitleErr.Error()))
    }
  }

  descrId := ctx.stringMap[progDescription]
  if descrId == 0 {
    runeLength := utf8.RuneCountInString(programme.Description)

    if runeLength > snippetLengthMax {
      snippetLengthMax = runeLength
    }

    descrId = ctx.textIdMax
    ctx.textIdMax += 1

    ctx.stringMap[progDescription] = descrId

    _, ftsDescrTextErr := ctx.sql4.Exec(descrId, progDescription)
    if (ftsDescrTextErr != nil) {
      return false, errors.New(s("text INSERT failed\n %s\n", ftsDescrTextErr.Error()))
    }

    if useLegacyFormat {
      progDescription = strings.ToLower(progDescription)
      progDescription = strings.ReplaceAll(progDescription, "ё", "е")
    }
    _, ftsErr := ctx.sql2.Exec(descrId, progDescription)
    if (ftsErr != nil) {
      return false, errors.New(s("FTS INSERT failed\n %s\n", ftsErr.Error()))
    }

    trimmedTotal += trimmed
  }

  var imageDbId sql.NullInt64

  if (len(programme.Images) != 0 && len(programme.Images[0].Uri) != 0) {
    //fmt.Printf("image = %s", programme.Images[0].Uri)

    firstUri := programme.Images[0].Uri

    if imageBaseUrl != nil {
      parsedUrl, urlErr := url.Parse(firstUri)

      if urlErr == nil && parsedUrl.IsAbs() {
        parsedUrl.Host = imageBaseUrl.Host

        if imageBaseUrl.Scheme != "" {
          parsedUrl.Scheme = imageBaseUrl.Scheme
        }

        if imageBaseUrl.Path != "" && imageBaseUrl.Path != "/" {
          parsedUrl.Path = imageBaseUrl.Path + parsedUrl.Path
        }

        firstUri = parsedUrl.String()
      }
    }

    uriId := ctx.uriMap[firstUri]
    if uriId == 0 {
      uriId = ctx.uriIdMax
      ctx.uriIdMax += 1

      ctx.uriMap[firstUri] = uriId

      _, uriErr := ctx.sql3.Exec(uriId, firstUri)
      if (uriErr != nil) {
        return false, errors.New(s("URI INSERT failed\n %s\n", uriErr.Error()))
      }
    }

    imageDbId = sql.NullInt64{
      Int64: uriId,
      Valid: true,
    }
  }

  progCategories := programme.Categories

  for _, rawCategory := range progCategories {
    nestedCats := strings.Split(rawCategory, ",")

    for _, category := range nestedCats {
      category = strings.TrimSpace(category)

      tagInfo := ctx.tagMap[category]

      if tagInfo == nil {
        newTagInfo := TagMeta{
          NumberOfUses: 1,
          IdVal: 0,
        }

        ctx.tagMap[category] = &newTagInfo

        //fmt.Printf("Adding new tag %s for %s\n", category, progTitle)
      } else {
        tagInfo.NumberOfUses += 1
      }
    }
  }

  var caStr strings.Builder

  for _, ca := range programme.Categories {
    nestedCats := strings.Split(ca, ",")

    for _, nca := range nestedCats {
      caStr.WriteString(strings.TrimSpace(nca))
      caStr.WriteString(",")
    }
  }

  catsColumn := caStr.String()

  var progYear sql.NullInt64

  if programme.Year != "" {
    yearMatch := yearRegexp1.FindStringSubmatch(programme.Year)
    if yearMatch != nil {
      parsedYear, _ := strconv.Atoi(yearMatch[1])

      progYear = sql.NullInt64{
        Int64: int64(parsedYear),
        Valid: true,
      }
    }
  }

  metaRes, metaErr := ctx.sql1.Exec(startTime.Unix(), chId, imageDbId, titleId, descrId, progYear, 0)
  if (metaErr != nil) {
    fmt.Printf("When parsing %s\n", programme.Title)

    return false, errors.New(s("Meta INSERT failed\n %s\n", metaErr.Error()))
  }

  insertId, _ := metaRes.LastInsertId()

  _, tagsErr := ctx.sql7.Exec(insertId, catsColumn)
  if tagsErr != nil {
    return false, errors.New(s("Tag INSERT failed\n %s\n", tagsErr.Error()))
  }

  return true, nil
}

func HomeRouterHandler(w http.ResponseWriter, r *http.Request) {
  r.Close = true

  if r.Method != "POST" {
    http.Error(w, "Access denied", http.StatusMethodNotAllowed)
    return
  }

  var err error

  var sizeInt int

  reqSize := r.Header.Get("Content-Length")
  if reqSize != "" {
    if sizeInt, err = strconv.Atoi(reqSize); err != nil {
      http.Error(w, "Invalid request", http.StatusBadRequest)
      return
    }

    if sizeInt > 536870912 { // 512Mb
      fmt.Fprintf(os.Stderr, "Size too big: %d\n", sizeInt)
      http.Error(w, "The file is too big", http.StatusRequestEntityTooLarge)
      return
    }
  } else {
    fmt.Fprintf(os.Stderr, "No content-length\n")
    http.Error(w, "Content length is required", http.StatusLengthRequired)
    return
  }

  fmt.Fprintf(os.Stderr, "Parsing request\n")

  var formReader *multipart.Reader

  if formReader, err = r.MultipartReader(); err != nil {
    fmt.Fprintf(os.Stderr, "Invalid multipart data:\n %s\n", err.Error())
    http.Error(w, "Invalid request", http.StatusBadRequest)
    return
  }

  var formPart *multipart.Part

  if formPart, err = formReader.NextPart(); err != nil {
    fmt.Fprintf(os.Stderr, "Invalid part:\n %s\n", err.Error())
    http.Error(w, "Invalid form component", http.StatusBadRequest)
    return
  }

  if formPart.FormName() != "xmltv" {
    http.Error(w, "Access denied", http.StatusBadRequest)
    return
  }

  fmt.Fprintf(os.Stderr, "Checks passed\n")

  tmpFile, tmpErr := ioutil.TempFile("", "db-*.sqlite")
  if tmpErr != nil {
    http.Error(w, "failed", http.StatusInternalServerError)
    return
  }

  defer tmpFile.Close()
  defer os.Remove(tmpFile.Name())

  db, dbErr := sql.Open("sqlite3", fmt.Sprintf("file:%s", tmpFile.Name()))
  if dbErr != nil {
    fmt.Fprintf(os.Stderr, "SQLite open() failed: %s\n", dbErr.Error())
    http.Error(w, "failed", http.StatusInternalServerError)
    return
  }

  // we want to delete the database file ASAP after opening it
  // to avoid leaving leftover trash behind in event of server crash
  // but go's sql bridge is lazy — it doesn't create connection until first command
  // so we force the SQLite file to be created by calling Ping()
  db.Ping()

  if err = os.Remove(tmpFile.Name()); err != nil {
    fmt.Fprintf(os.Stderr, "Failed to remove temp SQLite file: %s\n", err.Error())
  }

  defer db.Close()

  xmlStreamReader := http.MaxBytesReader(w, formPart, 536870912)

  var ctx = RequestContext{}

  ctx.db = db

  ctx.stringMap = make(map[string]int64)
  ctx.uriMap = make(map[string]int64)
  ctx.tagMap = make(map[string]*TagMeta)
  ctx.endMap = make(map[string]*EndMeta)

  ctx.textIdMax = 1
  ctx.uriIdMax = 1

  largeBuffer := bufio.NewReaderSize(xmlStreamReader, 1024 * 128)

  err = processXml(&ctx, "main", largeBuffer, tmpFile, w)

  xmlStreamReader.Close()

  optimizeDatabase(&ctx, "main")

  if err != nil {
    fmt.Fprintf(os.Stderr, "Conversion failed: %s\n", err.Error())
    http.Error(w, "failed", http.StatusInternalServerError)
    return
  }

  respHeader := w.Header()
  respHeader.Add("Content-Disposition", "attachment; filename=\"schedule.epgx\"")
  respHeader.Add("Content-Type", "application/vnd.sqlite3")
  respHeader.Del("Accept-Ranges")

  http.ServeContent(w, r, "schedule.epgx", time.Time{}, tmpFile)

  fmt.Printf("Done\n")
}
