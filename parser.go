package main

import (
    "os"
    "io"
    "fmt"
    "flag"
    "time"
    "bufio"
    "errors"
    "regexp"
    "strconv"
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

type Channel struct {
 Name                  string             `xml:"display-name"`
 Id                    string             `xml:"id,attr"`
 Icon                  ImageUri           `xml:"icon"`
}

type Programm struct {
 Start                 string             `xml:"start,attr"`
 Channel               string             `xml:"channel,attr"`
 Title                 string             `xml:"title"`
 Description           string             `xml:"desc"`
 Images                []ImageUri         `xml:"icon"`
}

type ImageUri struct {
  Uri                  string             `xml:"src,attr"`
}

type ChannelMeta struct {
  Id                   string
  ArchiveMinutes       int
  ImageUrlOverride     string
}

type RequestContext struct {
  sql1, sql2, sql3, sql4, sql5 *sql.Stmt
  db *sql.DB
}

var startFrom time.Time
var spanDuration time.Duration
var stringMap map[string]int64
var uriMap map[string]int64
var idMap map[string]ChannelMeta

var uriIdMax, textIdMax int64

var snippetLength int

var dbEarliestDate *time.Time
var dbLastDate *time.Time

var localLocation *time.Location
var xmltvTzOverride *time.Location

var eltDateFormat string
var useLegacyFormat bool

var ageRegexp *regexp.Regexp
var timeRegexp1 *regexp.Regexp

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

  textIdMax = 1
  uriIdMax = 1

  stringMap = make(map[string]int64)
  uriMap = make(map[string]int64)

  eltDateFormat = "02-01-2006 15:04"

  defDuration, _ := time.ParseDuration("72h")

  dbPath := flag.String("output", "schedule.epgx.gz", "database file.")
  xmlPath := flag.String("input", "", "XMLTV file. (default read from standard input)")
  timeStart := flag.String("offset", "01-01-1970 00:00", "start import from specified date. Example: 29-12-2009 16:40.")
  argDuration := flag.Duration("timespan", defDuration, "duration since start date. Example: 72h.")
  flag.IntVar(&snippetLength, "snippet", -1, "description length limit. If negative, descriptions aren't clipped.")
  nameMapFile := flag.String("xmap", "", "Optional: file with pipe-separated ID mappings. (default none)")
  xmltvTz := flag.String("tz", "", "Optional: replace timezone in XMLTV file. Example: 'Asia/Novosibirsk'. (default none)")
  flag.BoolVar(&useLegacyFormat, "legacy", false, "Enable generation of legacy EPGX for Android < 21 (with contentless FTS table). Created file won't support snippet() SQL function")
  includeCh := flag.String("include", "", "Optional: comma-separated list of channels to include in generated EPG.")
  excludeCh := flag.String("exclude", "", "Optional: comma-separated list of channels to exclude from generated EPG.")
  flag.Parse()

  seen := make(map[string]bool)

  flag.Visit(func(f *flag.Flag) { seen[f.Name] = true })

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

  if (*nameMapFile != "") {
    idMap = make(map[string]ChannelMeta)

    nameMap, idMapErr := os.Open(*nameMapFile)
    if idMapErr != nil {
      Bail("Failed to open name map file:\n %s\n")
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
        minutes := 0
        chImage := ""

        if len(sepIdx) > 2 {
          minutes, _ = strconv.Atoi(sepIdx[2])
        }

        if len(sepIdx) > 3 {
          chImage = sepIdx[3]
        }

        idMap[mapId] = ChannelMeta{
          Id: mapNam,
          ArchiveMinutes: minutes,
          ImageUrlOverride: chImage,
        }
      }

      if lineErr != nil {
        if lineErr == io.EOF {
          break;
        }

        Bail("Received IO error during reading map file:\n %s\n", lineErr)
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
  gzipWriter.Comment = "eltex epg v1"

  ctx := RequestContext{}

  ctx.db = db

  reqErr := processXml(ctx, "main", xmlFile, tmpFile, gzipWriter)
  if reqErr != nil {
    Bail("%s\n", reqErr.Error())
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

func s(format string, a ...interface{}) string {
  return fmt.Sprintf(format, a...)
}

func processXml(ctx RequestContext, dbNam string, xmlFile io.Reader, dbFile io.Reader, destWriter io.Writer) error {
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
  _, err = db.Exec(s("CREATE TABLE %s.channels (_id INTEGER PRIMARY KEY, image_uri TEXT, ch_id TEXT NOT NULL UNIQUE, name TEXT, archive_time INTEGER NOT NULL);", dbNam))
  if err != nil {
    return errors.New(s("CREATE TABLE failed\n %s\n", err.Error()))
  }

  if useLegacyFormat {
    fmt.Fprintf(os.Stderr, "Using legacy format: tokenize=porter\n")

    _, err = db.Exec(s("CREATE VIRTUAL TABLE %s.fts_search USING fts4(content='', matchinfo='fts3', text, tokenize=porter);", dbNam))
  } else {
    _, err = db.Exec(s("CREATE VIRTUAL TABLE %s.fts_search USING fts4(content='text', matchinfo='fts3', text, tokenize=unicode61);", dbNam))
  }

  if err != nil {
    return errors.New(s("CREATE TABLE failed\n %s\n", err.Error()))
  }
  _, err = db.Exec(s("CREATE TABLE %s.search_meta (_id INTEGER PRIMARY KEY, ch_id, image_uri INTEGER, start_time INTEGER, title_id INTEGER NOT NULL, description_id INTEGER NOT NULL);", dbNam))
  if err != nil {
    return errors.New(s("CREATE TABLE failed\n %s\n", err.Error()))
  }
  _, err = db.Exec(s("CREATE TABLE %s.uri (_id INTEGER PRIMARY KEY, uri TEXT)", dbNam))
  if err != nil {
    return errors.New(s("CREATE TABLE failed\n %s\n", err.Error()))
  }

  bulkTx, txErr := db.Begin()
  if txErr != nil {
    return errors.New(s("Could not start transaction\n %s\n", txErr.Error()))
  }

  ctx.sql1, err = bulkTx.Prepare("INSERT INTO search_meta (start_time, ch_id, image_uri, title_id, description_id) VALUES (?, ?, ?, ?, ?);")
  if err != nil {
    return errors.New(s("Prepare() failed: %s\n", err.Error()))
  }
  ctx.sql2, err = bulkTx.Prepare("INSERT INTO fts_search (docid, text) VALUES (?, ?);")
  if err != nil {
    return errors.New(s("Prepare() failed: %s\n", err.Error()))
  }
  ctx.sql3, err = bulkTx.Prepare("INSERT INTO uri (_id, uri) VALUES (?, ?);")
  if err != nil {
    Bail("Prepare() failed: %s\n", err.Error())
  }
  ctx.sql4, err = bulkTx.Prepare("INSERT INTO text (docid, text) VALUES (?, ?);")
  if err != nil {
    return errors.New(s("Prepare() failed: %s\n", err.Error()))
  }
  ctx.sql5, err = bulkTx.Prepare("INSERT INTO channels (ch_id, image_uri, name, archive_time) VALUES (?, ?, ?, ?);")
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
      Bail("XMLTV file is malformed (failed to find root tag)\n")
    }

    switch xmlRoot := token.(type) {
      default:
        continue;
      case xml.StartElement:
        if (xmlRoot.Name.Local == "tv") {
          break root;
        } else {
          Bail("malformed XMLTV: <tv> tag not found, got <%s> instead\n", xmlRoot.Name.Local)
        }
    }
  }

  fmt.Printf("Copying XMLTV schedule to database\n")

  ageRegexp = regexp.MustCompile("(.+)\\([0-9]{1,2}\\+\\)$")
  timeRegexp1 = regexp.MustCompile("([0-9]{14})( (?:.+))?$")

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
        Bail("Failed to read token\n %s\n", tokenErr.Error())
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

          if (addChannel(ctx, decoder, channel, &startElement)) {
            appendedChannels += 1;
          }
        } else if (startElement.Name.Local == "programme") {
          programme = &Programm{}

          if (addElement(ctx, decoder, programme, &startElement)) {
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

    Bail("%s\n", emptyErrStr)
  }

  bulkTx.Commit()

  fmt.Printf("Inserted %d channels (%d archived), %d programm entries, %d unique strings\n", appendedChannels, archivedChannels, appendedElements, textIdMax)

  if mappedTotal == 0 && len(idMap) != 0 {
    fmt.Printf("WARNING: none of %d mappings were used!\n", len(idMap))
  }

  if (snippetLength >= 0) {
     fmt.Printf("Trimmed %d characters. Max length before trimming: %d\n", trimmedTotal, snippetLengthMax)
  }

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

  fmt.Printf("Compressing database file\n")

  buf := make([]byte, 128 * 1024)

  for {
    n, readErr := dbFile.Read(buf)
    if readErr != nil && readErr != io.EOF {
      return errors.New(s("%s\n", readErr.Error()))
    }
    if n == 0 {
      break
    }
    if _, writeErr := destWriter.Write(buf[:n]); writeErr != nil {
      return errors.New(s("%s\n", writeErr.Error()))
    }
  }

  return nil
}

func parseXmltvDate(source string) (time.Time, error) {
  // XMLTV dates are "loosely based on ISO 8601", which is rather poorly supported by Go
  // so we have to do a bit of extra fiddling ourselves

  timeMatch := timeRegexp1.FindStringSubmatch(source)
  if timeMatch == nil {
    Bail("Failed to parse date: %s\n", source)
  }

  if xmltvTzOverride != nil {
    return time.ParseInLocation("20060102150405", timeMatch[1], xmltvTzOverride)
  } else if len(timeMatch) > 2 {
    return time.ParseInLocation("20060102150405 -0700", source, localLocation)
  } else {
    return time.ParseInLocation("20060102150405", timeMatch[1], localLocation)
  }
}

func addChannel(ctx RequestContext, decoder *xml.Decoder, channel *Channel, xmlElement *xml.StartElement) bool {
  decErr := decoder.DecodeElement(channel, xmlElement)
  if (decErr != nil) {
    Bail("Could not decode element\n %s\n", decErr.Error())
  }

  var imageUri sql.NullString

  if channel.Icon.Uri != "" {
    imageUri = sql.NullString{
      String: channel.Icon.Uri,
      Valid: true,
    }
  }

  if (channel.Name == "" || channel.Id == "") {
    return false;
  }

  chId := channel.Id
  archived := 0

  if mappedId, ok := idMap[chId]; ok {
    chId = mappedId.Id
    archived = mappedId.ArchiveMinutes

    if mappedId.ImageUrlOverride != "" {
      imageUri = sql.NullString{
        String: mappedId.ImageUrlOverride,
        Valid: true,
      }
    }
  }

  if len(channelWhitelist) != 0 {
    if _, ok := channelWhitelist[chId]; !ok {
      return false
    }
  }

  if _, blacklisted := channelBlacklist[chId]; blacklisted {
    return false
  }

  if archived > 0 {
    archivedChannels += 1

    // store archived time in seconds
    archived *= 3600;
  }

  //fmt.Printf("Inserting %s, %s %s %d\n", chId, imageUri.String, channel.Name, archived)

  _, chInsertErr := ctx.sql5.Exec(chId, imageUri, strings.ToLower(channel.Name), archived)
  if chInsertErr != nil {
    Bail("Failed to insert into channels table\n", chInsertErr.Error())
  }

  return true;
}

func addElement(ctx RequestContext, decoder *xml.Decoder, programme *Programm, xmlElement *xml.StartElement) bool {
  decErr := decoder.DecodeElement(programme, xmlElement)
  if (decErr != nil) {
    Bail("Could not decode element\n %s\n", decErr.Error())
  }

  chId := programme.Channel

  if mappedId, ok := idMap[chId]; ok {
    chId = mappedId.Id

    mappedTotal += 1
  }

  if len(channelWhitelist) != 0 {
    if _, ok := channelWhitelist[chId]; !ok {
      return false
    }
  }

  if _, blacklisted := channelBlacklist[chId]; blacklisted {
    return false
  }

  startTime, timeErr := parseXmltvDate(programme.Start)
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

    _, ftsTitleTextErr := ctx.sql4.Exec(titleId, progTitle)
    if (ftsTitleTextErr != nil) {
      Bail("text INSERT failed\n %s\n", ftsTitleTextErr.Error())
    }

    if useLegacyFormat {
      progTitle = strings.ToLower(progTitle)
    }

    _, ftsTitleErr := ctx.sql2.Exec(titleId, progTitle)
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

    _, ftsDescrTextErr := ctx.sql4.Exec(descrId, progDescription)
    if (ftsDescrTextErr != nil) {
      Bail("text INSERT failed\n %s\n", ftsDescrTextErr.Error())
    }

    if useLegacyFormat {
      progDescription = strings.ToLower(progDescription)
    }
    _, ftsErr := ctx.sql2.Exec(descrId, progDescription)
    if (ftsErr != nil) {
      Bail("FTS INSERT failed\n %s\n", ftsErr.Error())
    }

    trimmedTotal += trimmed
  }

  var imageDbId sql.NullInt64

  if (len(programme.Images) != 0 && len(programme.Images[0].Uri) != 0) {
    //fmt.Printf("image = %s", programme.Images[0].Uri)

    firstUri := programme.Images[0].Uri

    uriId := uriMap[firstUri]
    if uriId == 0 {
      uriId = uriIdMax
      uriIdMax += 1

      uriMap[firstUri] = uriId

      _, uriErr := ctx.sql3.Exec(uriId, firstUri)
      if (uriErr != nil) {
        Bail("URI INSERT failed\n %s\n", uriErr.Error())
      }
    }

    imageDbId = sql.NullInt64{
      Int64: uriId,
      Valid: true,
    }
  }

  _, metaErr := ctx.sql1.Exec(startTime.Unix(), chId, imageDbId, titleId, descrId)
  if (metaErr != nil) {
    Bail("Meta INSERT failed\n %s\n", metaErr.Error())
  }

  return true
}
